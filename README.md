# Juttle Twitter Adapter

Twitter adapter for the [Juttle data flow
language](https://github.com/juttle/juttle).

## Examples

Filter the real-time Twitter stream for all tweets containing "potus"

```juttle
read twitter -stream true 'potus'
```

Read the last 100 historical tweets referencing "potus":

```
read twitter -total 100 'potus'
```

## Installation

Like Juttle itself, the adapter is installed as a npm package. Both Juttle and
the adapter need to be installed side-by-side:

```bash
$ npm install juttle
$ npm install juttle-twitter-adapter
```

## Configuration

The adapter needs to be registered and configured so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```json
{
    "adapters": {
        "twitter": {
            "consumer_key": "...",
            "consumer_secret": "...",
            "access_token_key": "...",
            "access_token_secret": "..."
        }
    }
}
```

To obtain Twitter credentials, first set up a [Twitter App](https://apps.twitter.com/) and create an OAuth token.

## Usage

The adapter can run in two modes -- streaming or historical, controlled by the `-stream` option. In both cases it requires a single search term in the filter expression, as shown in the examples above.

Historical searches are somewhat constrained by the fact that Twitter's API returns
tweets in reverse chronological order, but Juttle semantics require data points
to be emitted in order.

To handle this, the adapter buffers all the points in memory before emitting
them down the flowgraph. There is a limit to control how many points are
buffered, which defaults to 1000. This can be overridden using the `total`
option.

In streaming mode, the adapter will buffer all points for a configurable delay
so that they can be properly sorted into increasing time order.

### Options

Name | Type | Required | Description
-----|------|----------|-------------
`stream` | boolean | no | run in live streaming mode (default: false)
`total`  | integer | no | maximum number of tweets to emit in historical mode (default: 1000)
`count`  | integer | no | number of tweets to fetch per round-trip in historical mode (default: 100, max: 100)
`delay`  | integer | no | in streaming mode, delay for the given latency (in ms) to reorder incoming points

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
