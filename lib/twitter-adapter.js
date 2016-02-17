'use strict';

/* global JuttleAdapterAPI */
let AdapterRead = JuttleAdapterAPI.AdapterRead;
let JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
let StaticFilterCompiler = JuttleAdapterAPI.compiler.StaticFilterCompiler;
let Twitter = require('twitter');
let Heap = require('heap');
let Promise = require('bluebird');

let config;

class TwitterFilterCompiler extends StaticFilterCompiler {
    visitFulltextFilterTerm(node) {
        return node.text;
    }
}

class Read extends AdapterRead {
    static get timeRequired() { return true; }

    constructor(options, params) {
        super(options, params);

        if (!params.filter_ast) {
            throw this.compileError('FILTER-FEATURE-NOT-SUPPORTED', {
                feature: 'read with no filter'
            });
        }

        let filterCompiler = new TwitterFilterCompiler();
        this.query = filterCompiler.visit(params.filter_ast);

        let from = this.options.from || this.defaultTimeOptions().from;
        let to   = this.options.to   || this.defaultTimeOptions().to;

        // For now the adapter only accepts the following time modes:
        //
        //    -from :0:    (-to :now:)  (pure historical)
        //   (-from :now:)  -to :end:   (pure live)
        //
        if (from.unixms() === 0 && to.eq(params.now)) {
            this.streaming = false;
        } else if (from.eq(params.now) && to.isEnd()){
            this.streaming = true;
        } else {
            throw this.compileError('INVALID-OPTION-COMBINATION', {
                option: 'from/to',
                rule: 'pure historical or pure live'
            });
        }

        this.logger.debug('initializing in', this.streaming ? 'streaming' : 'historical', 'mode');

        // Historical read limit
        this.limit = options.limit || 1000;

        // Number to fetch for each historical read
        this.fetchSize = options.fetchSize || 100;

        // Live buffer size
        this.bufferLimit = options.bufferLimit || 10000;

        // buffer of received tweets for search mode
        this.buffer = [];

        // live buffer for tweets in streaming mode
        this.reorderBuffer = new Heap(function(p1, p2) {
            return p1.time.lt(p2.time) ? -1 : 1;
        });

        // points arriving before this time are dropped
        this.reorderFrom = params.now.quantize(JuttleMoment.duration(1, 's'));

        this.warnLate = this.throttledWarning('POINTS-ARRIVED-LATE', {
            proc: 'read twitter'
        });

        this.warnOverflow = this.throttledWarning('BUFFER-LIMIT-EXCEEDED', {
            buffer: 'read twitter buffer'
        });

        this.client = Promise.promisifyAll(new Twitter(config));
    }

    static allowedOptions() {
        return AdapterRead.commonOptions().concat(['limit', 'fetchSize', 'bufferLimit']);
    }

    defaultTimeOptions() {
        return {
            from: this.params.now,
            to: this.params.now,
            lag: JuttleMoment.duration(2, 's')
        };
    }

    periodicLiveRead() {
        return true;
    }

    start() {
        if (this.streaming) {
            this._startStreaming();
        }
    }

    read(from, to, limit, state) {
        if (this.streaming) {
            return this._fetchLive(from, to, limit, state);
        } else {
            return this._fetchHistorical();
        }
    }

    _startStreaming() {
        this.logger.debug('starting streaming');
        this.client.stream('statuses/filter', {track: this.query}, (stream) => {
            this.stream = stream;

            stream.on('data', (tweet) => {
                let pt = this.toJuttle(tweet);
                if (pt) {
                    if (pt.time.lt(this.reorderFrom)) {
                        this.warnLate();
                    } else if (this.reorderBuffer.size() >= this.bufferLimit) {
                        this.warnOverflow();
                    } else {
                        this.reorderBuffer.push(pt);
                        this.logger.debug('buffered', this.reorderBuffer.size(), 'pts');
                    }
                }
            });

            stream.on('error', (error) => {
                // XXX how to handle errors
                this.logger.error(error);
            });
        });
    }

    _fetchLive(from, to) {
        this.logger.debug('_fetchLive', from.toJSON(), '-', to.toJSON(),
                          'bufferSize', this.reorderBuffer.size());

        // XXX need to handle limit
        let points = [];
        while (this.reorderBuffer.size() && (this.reorderBuffer.peek().time < to)) {
            points.push(this.reorderBuffer.pop());
        }
        this.reorderFrom = to;
        this.logger.debug('returning', points.length, 'points');
        return Promise.resolve({
            points: points,
            readEnd: to
        });
    }

    // Perform the historical search. Returns a promise when done.
    _fetchHistorical() {
        let opts = {
            q: this.query,
            count: Math.min(this.fetchSize, this.limit - this.buffer.length)
        };

        if (this.max_id) {
            opts.max_id = this.max_id;
            opts.count += 1;
        }

        this.logger.debug('search buffered=' + this.buffer.length,
                          'max_id=' + opts.max_id, 'count=' + opts.count);

        return this.client.getAsync('search/tweets', opts)
        .spread((tweets, response) => {
            let statuses = tweets.statuses;
            this.logger.debug('search returned', statuses.length, 'tweets');

            // Yuck! Twitter's API returns the tweet that matches the max_id
            // instead of making it -1, and Javascript can't do 64 bit math, so
            // ignore the first tweet that arrived if this was a max_id query.
            //
            // It also pages in reverse order so we need to buffer all the
            // results up to the configured max.
            if (this.max_id) {
                statuses = statuses.slice(1);
            }

            let done = true;
            if (statuses.length !== 0) {
                this.buffer = this.buffer.concat(statuses);
                this.logger.debug('buffered', statuses.length, 'tweets',
                                  'count=' + this.buffer.length);

                this.max_id = statuses[statuses.length - 1].id;
                done = false;
            }

            if (!done && (this.buffer.length >= this.limit)) {
                this.logger.warn('reached limit of', this.limit,
                                 'tweets... stopping search');
                done = true;
            }

            if (done) {
                this.logger.debug('all done... emitting', this.buffer.length, 'tweets');
                let points = [];
                for (let i = this.buffer.length - 1; i >= 0; --i) {
                    points.push(this.toJuttle(this.buffer[i]));
                }
                return {
                    points: points,
                    eof: true
                };
            } else {
                return this._fetchHistorical();
            }
        });
    }

    teardown() {
        this.logger.debug('in teardown: stream', this.stream ? 'active' : 'not active');
        if (this.stream) {
            this.stream.destroy();
        }
    }

    toJuttle(tweet) {
        if (tweet.created_at === undefined) {
            return null;
        }

        return {
            time: new JuttleMoment({rawDate: new Date(tweet.created_at)}),
            user: '@' + tweet.user.screen_name,
            text: tweet.text
        };
    }
}

let TwitterAdapter = function(cfg) {
    config = cfg;

    return {
        name: 'twitter',
        read: Read
    };
};

module.exports = TwitterAdapter;
