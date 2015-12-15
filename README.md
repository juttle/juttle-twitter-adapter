# Juttle Twitter Adapter

Twitter API adapter for juttle

# Installation / Setup

Check out this repository and the juttle repository into a working directory.

Run `npm link` in each.

Make sure the following is in your environment:

`NODE_PATH=/usr/local/lib/node_modules`

# Configuration

Add the following to ~/.juttle/config.json:

    "juttle-twitter-adapter": {
        "consumer_key": "...",
        "consumer_secret": "...","
        "access_token_key": "...", 
        "access_token_secret": "..."
    }

To obtain credentials, first set up a Twitter App at [https://apps.twitter.com/]  and create an OAuth token.

# Usage

For a live query, you can tap into the firehose of live tweets for a given filter term using the streaming API.

For example the following will search for all tweets mentioning "potus"

`readx twitter -stream true "potus"`

Historical searches are constrained by the fact that Twitter's API returns tweets in reverse chronological order, but Juttle semantics require data points to be emitted in order.

To handle this, the adapter buffers all the points in memory before emitting them down the flowgraph and there is a limit to control how many points are buffered, which defaults to 1000. This can be overridden using the `-total` option to the read command.

For example the following will read the last 100 tweets referencing "potus":

`readx twitter -total 100 "potus"`
