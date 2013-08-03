# Metrics Elasticsearch Reporter

This is a reporter for the excellent [metrics library](http://metrics.codahale.com), similar to the [graphite](http://metrics.codahale.com/manual/graphite/) or [ganglia](http://metrics.codahale.com/manual/ganglia/) reporters, except that it reports to an elasticsearch server.

In case, you are worried, that you need to include the 20MB elasticsearch dependency in your project, you do not need to be. As this reporter is using HTTP for putting data into elasticsearch, the only library needed is the awesome [Jackson JSON library](http://wiki.fasterxml.com/JacksonHome), more exactly the jackson databind library to easily serialize the metrics objects.

## Configuration

```
final MetricRegistry registry = new MetricRegistry();
ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry)
    .hosts("localhost:9200", "localhost:9201")
    .build();
reporter.start(60, TimeUnit.SECONDS);
```

Define your metrics and registries as usual

```
private final Meter incomingRequestsMeter = registry.meter("incoming-http-requests");

// in your app code
incomingRequestsMeter.mark(1);
```


### Options

* `hosts()`: A list of hosts used to connect to, must be in the format `hostname:port`, default is `localhost:9200`
* `timeout()`: Milliseconds to wait for an established connections, before the next host in the list is tried. Defaults to `1000`
* `bulkSize()`: Defines how many metrics are sent per bulk requests, defaults to `2500`
* `percolateMetrics()`: Regular expressions to define which metrics should be percolated against. See below for an example
* `percolateNotifier()`: An implementation of the `Notifier` interface, which is executed upon a matching percolator. See below for an example.

### Mapping

**Note**: Mapping is optional. But maybe you want to facet on the metric name, then you should not need to analyze the field.

```
curl -X PUT localhost:9200/metrics
curl -X PUT localhost:9200/metrics/timer/_mapping -d '
{ "timer" : { "properties" : { "name" : { "type" : "string", "index": "not_analyzed" } } } }'
curl -X PUT localhost:9200/metrics/counter/_mapping -d '
{ "counter" : { "properties" : { "name" : { "type" : "string", "index": "not_analyzed" } } } }'
curl -X PUT localhost:9200/metrics/meter/_mapping -d '
{ "meter" : { "properties" : { "name" : { "type" : "string", "index": "not_analyzed" } } } }'
curl -X PUT localhost:9200/metrics/gauge/_mapping -d '
{ "gauge" : { "properties" : { "name" : { "type" : "string", "index": "not_analyzed" } } } }'
curl -X PUT localhost:9200/metrics/histogram/_mapping -d '
{ "histogram" : { "properties" : { "name" : { "type" : "string", "index": "not_analyzed" } } } }'
```

## Notifications with percolations

```
ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry)
    .percolateNotifier(new PagerNotifier())
    .percolateMetrics(".*")
    .build();
reporter.start(60, TimeUnit.SECONDS);
```

Write a custom notifier

```
public class PagerNotifier implements Notifier {

  @Override
  public void notify(JsonMetrics.JsonMetric jsonMetric, String percolateMatcher) {
    // send pager duty here
  }
}
```

Add a percolation

```
curl http://localhost:9200/_percolator/metrics/http-monitor -X PUT -d '{
  "query" : { 
    "bool" : { 
      "must": [
        { "term": { "name" : "incoming-http-requests" } },
        { "range": { "m1_rate": { "to" : "10" } } }
      ]
    }
  }
}'
```

## JSON Format of metrics

This is how the serialized metrics looks like in elasticsearch

### Counter

```
{
  "name": "usa-gov-heartbearts",
  "timestamp": "2013-07-20T09:29:58.000+0000",
  "count": 18
}
```

### Timer

```
{
  "name" : "bulk-request-timer",
  "timestamp" : "2013-07-20T09:43:58.000+0000",
  "count" : 114,
  "max" : 109.681,
  "mean" : 5.439666666666667,
  "min" : 2.457,
  "p50" : 4.3389999999999995,
  "p75" : 5.0169999999999995,
  "p95" : 8.37175,
  "p98" : 9.6832,
  "p99" : 94.68429999999942,
  "p999" : 109.681,
  "stddev" : 9.956913151098842,
  "m15_rate" : 0.10779994503690074,
  "m1_rate" : 0.07283351433589833,
  "m5_rate" : 0.10101298115113727,
  "mean_rate" : 0.08251056571678642,
  "duration_units" : "milliseconds",
  "rate_units" : "calls/second"
}
```

### Meter

```
{
  "name" : "usagov-incoming-requests",
  "timestamp" : "2013-07-20T09:29:58.000+0000",
  "count" : 224,
  "m1_rate" : 0.3236309568191993,
  "m5_rate" : 0.45207208204948995,
  "m15_rate" : 0.5014348927301423,
  "mean_rate" : 0.4135529888278531,
  "units" : "events/second"
}
```

### Histogram

```
{
  "name" : "my-histgram",
  "timestamp" : "2013-07-20T09:29:58.000+0000",
  "count" : 114,
  "max" : 109.681,
  "mean" : 5.439666666666667,
  "min" : 2.457,
  "p50" : 4.3389999999999995,
  "p75" : 5.0169999999999995,
  "p95" : 8.37175,
  "p98" : 9.6832,
  "p99" : 94.68429999999942,
  "p999" : 109.681,
  "stddev" : 9.956913151098842,}
}
```

### Gauge

```
{
  "name" : "usagov-incoming-requests",
  "timestamp" : "2013-07-20T09:29:58.000+0000",
  "value" : 123
}
```


## Next steps

* Integration with Kibana would be awesoe
* Releasing an accompanying dashboard application, which shows some of the realtime monitoring features using percolations

