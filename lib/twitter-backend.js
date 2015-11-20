var Juttle = require('juttle/lib/runtime').Juttle;
var JuttleMoment = require('juttle/lib/moment/juttle-moment');
var Twitter = require('twitter');

var client;

var moment = require('juttle/node_modules/moment');
moment.createFromInputFallback = function(config) {
  config._d = new Date(config._i);
};

var Read = Juttle.proc.base.extend({
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

        // buffer of received tweets for search mode
        this.buffer = [];
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

        client.stream('statuses/filter', {track: this.query}, function(stream) {
            self.stream = stream;

            stream.on('data', function(tweet) {
                self.emit_tweets([tweet]);
            });

            stream.on('error', function(error) {
                // XXX how to handle errors
                self.logger.error(error)
            });
        });
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
        if (this.stream) {
            this.stream.destroy();
        }
    },

    // Emit the array of tweets in reverse order since that's how they come in.
    emit_tweets: function(tweets) {
        var pts = [];

        for (var i = tweets.length - 1; i >= 0; --i) {
            var tweet = tweets[i];
            if (tweet.created_at === undefined) {
                return;
            }
            var pt = {
                time: new JuttleMoment(tweet.created_at),
                user: '@' + tweet.user.screen_name,
                text: tweet.text
            };
            pts.push(pt);
        }

        this.emit(pts);
    }
});

var TwitterBackend = function(config) {
    // Just pass the config directly to the twitter client.
    client = new Twitter(config);

    return {
        name: 'twitter',
        read: Read
    };
};

module.exports = TwitterBackend;
