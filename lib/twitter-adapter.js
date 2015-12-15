var Juttle = require('juttle/lib/runtime').Juttle;
var JuttleMoment = require('juttle/lib/moment/juttle-moment');
var Twitter = require('twitter');
var Heap = require('heap');

var client;

var Read = Juttle.proc.source.extend({
    sourceType: 'batch',
    procName: 'readx-twitter',

    initialize: function(options, params, pname, location, program, juttle) {
        this.logger.debug('intitialize', options, params);

        // XXX hack
        if (! (params.filter_ast && params.filter_ast.type === 'SimpleFilterTerm')) {
            throw new Error('need a filter');
        }
        this.query = params.filter_ast.expression.value;

        this.do_streaming = options.stream || false;
        this.total = options.total || 1000;
        this.count = options.count || 100;

        this.delay = options.delay || JuttleMoment.duration(1, 's');

        // buffer of received tweets for search mode
        this.buffer = [];

        // live buffer for tweets in streaming mode
        this.live = new Heap(function(p1, p2) {
            return p1.time.lt(p2.time) ? -1 : 1;
        });
    },

    start: function() {
        if (this.do_streaming) {
            this.start_streaming();
        } else {
            this.search();
        }
    },

    start_streaming: function() {
        var self = this;

        this.live_window_start = new JuttleMoment().add(this.delay);
        this.live_window_next = this.live_window_start.add(this.delay);

        this.program.scheduler.schedule(this.live_window_next.unixms(), function() { self.stream_points() });

        client.stream('statuses/filter', {track: this.query}, function(stream) {
            self.stream = stream;

            stream.on('data', function(tweet) {
                var pt = self.toJuttle(tweet);
                if (pt) {
                    if (pt.time.gt(self.live_window_start)) {
                        self.logger.warn('point arrived late', pt.time.toJSON(), self.live_window_start.toJSON());
                        return;
                    }
                    self.live.push(pt);
                    self.logger.debug('buffered', self.live.size(), 'pts')
                }
            });

            stream.on('error', function(error) {
                // XXX how to handle errors
                self.logger.error(error)
            });
        });
    },

    stream_points: function() {
        var self = this;

        this.logger.debug('in stream', this.live_window_next.toJSON());

        var tosend = [];
        this.live_window_start = this.live_window_start.add(this.delay);
        while (this.live.size() && (this.live.peek().time < this.live_window_start)) {
            tosend.push(this.live.pop());
        }
        if (tosend.length !== 0) {
            this.logger.debug('emitting', tosend.length, 'points', JSON.stringify(tosend, null, 4));
            this.emit(tosend);
        } else {
            this.emit_tick(this.live_window_start);
        }

        this.live_window_next = this.live_window_next.add(this.delay);
        this.program.scheduler.schedule(this.live_window_next.unixms(), function() { self.stream_points(); });
    },

    search: function() {
        var self = this;

        var opts = {
            q: this.query,
            count: Math.min(this.count, this.total - this.buffer.length)
        };

        if (self.max_id) {
            opts.max_id = self.max_id;
            opts.count += 1;
        }

        this.logger.debug('search buffered=' + this.buffer.length, 'max_id=' + opts.max_id, 'count=' + opts.count);
        client.get('search/tweets', opts, function(err, tweets, response) {
            if (err) {
                return self.logger.error(err);
            }

            var statuses = tweets.statuses;

            // Yuck! Twitter's API returns the tweet that matches the max_id
            // instead of making it -1, and Javascript can't do 64 bit math, so
            // ignore the first tweet that arrived if this was a max_id query.
            //
            // It also pages in reverse order so we need to buffer all the
            // results up to the configured max.
            if (self.max_id) {
                statuses = statuses.slice(1);
            }

            var done = true;
            if (statuses.length !== 0) {
                self.buffer = self.buffer.concat(statuses);
                self.logger.debug('buffered', statuses.length, 'tweets', 'count=' + self.buffer.length)
                self.max_id = statuses[statuses.length - 1].id;
                done = false;
            }

            if (!done && (self.buffer.length >= self.total)) {
                self.logger.warn('reached limit of', self.total, 'tweets... stopping search');
                done = true;
            }

            if (done) {
                self.logger.debug('all done... emitting', self.buffer.length, 'tweets');
                self.emit_tweets(self.buffer);
                self.eof();
            } else {
                self.search();
            }
        });
    },

    teardown: function() {
        this.logger.debug('in teardown: stream', this.stream ? 'active' : 'not active');
        if (this.stream) {
            this.stream.destroy();
        }
    },

    toJuttle: function(tweet) {
        if (tweet.created_at === undefined) {
            return null;
        }

        return {
            time: new JuttleMoment({rawDate: new Date(tweet.created_at)}),
            user: '@' + tweet.user.screen_name,
            text: tweet.text
        };
    },

    // Emit the array of tweets in reverse order since that's how they come in.
    emit_tweets: function(tweets) {
        var pts = [];

        for (var i = tweets.length - 1; i >= 0; --i) {
            var pt = this.toJuttle(tweets[i]);
            if (pt) {
                pts.push(pt);
            }
        }

        this.emit(pts);
    }
});

var TwitterAdapter = function(config) {
    // Just pass the config directly to the twitter client.
    client = new Twitter(config);

    return {
        name: 'twitter',
        read: Read
    };
};

module.exports = TwitterAdapter;
