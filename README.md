# Metrics Elasticsearch Reporter

![Project unmaintained](https://img.shields.io/badge/project-unmaintained-red.svg)

**This project is no longer maintained. If you want to maintain it, please fork and we will link to your new repository.**

## Introduction

This is a reporter for the excellent [Metrics library](http://metrics.dropwizard.io/), similar to the [Graphite](http://metrics.dropwizard.io/3.1.0/manual/graphite/) or [Ganglia](http://metrics.dropwizard.io/3.1.0/manual/ganglia/) reporters, except that it reports to an Elasticsearch server.

In case, you are worried, that you need to include the 20MB elasticsearch dependency in your project, you do not need to be. As this reporter is using HTTP for putting data into elasticsearch, the only library needed is the awesome [Jackson JSON library](http://wiki.fasterxml.com/JacksonHome), more exactly the Jackson Databind library to easily serialize the metrics objects.

If you want to see this in action, go to the `samples/` directory and read the readme over there, to get up and running with a sample application using the Metrics library as well as a dashboard application to graph.

## Compatibility

|   Metrics-elasticsearch-reporter  |    elasticsearch    | Release date |
|-----------------------------------|---------------------|:------------:|
| 5.1.1-SNAPSHOT                    | 5.0.0  -> 5.1.x     |  master      |
| 2.3.0                             | 2.3.0  -> 2.4.x     |  TBD         |
| 2.2.0                             | 2.2.0  -> 2.2.x     |  2016-02-10  |
| 2.0                               | 1.0.0  -> 1.7.x     |  2014-02-16  |
| 1.0                               | 0.90.7 -> 0.90.x    |  2014-02-05  |

## Travis CI build status

[![Build status](https://api.travis-ci.org/elastic/elasticsearch-metrics-reporter-java.svg?branch=master)](https://travis-ci.org/elastic/elasticsearch-metrics-reporter-java)

## Installation

You can simply add a dependency in your `pom.xml` (or whatever dependency resolution system you might have)

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>metrics-elasticsearch-reporter</artifactId>
  <version>2.3.0</version>
</dependency>
```

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
* `filter()`: A `MetricFilter` to define which metrics written to the elasticsearch
* `percolationFilter()`: A `MetricFilter` to define which metrics should be percolated against. See below for an example
* `percolationNotifier()`: An implementation of the `Notifier` interface, which is executed upon a matching percolator. See below for an example.
* `index()`: The name of the index to write to, defaults to `metrics`
* `indexDateFormat()`: The date format to make sure to rotate to a new index, defaults to `yyyy-MM`
* `timestampFieldname()`: The field name of the timestamp, defaults to `@timestamp`, which makes it easy to use with kibana

### Mapping

**Note**: The reporter automatically checks for the existence of an index template called `metrics_template`. If this template does not exist, it is created. This template ensures that all strings used in metrics are set to `not_analyzed` and disables the `_all` field.


## Notifications with percolations

```
ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry)
    .percolationNotifier(new PagerNotifier())
    .percolationFilter(MetricFilter.ALL)
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

Add a percolation _(elasticsearch < 5.0)_

```
curl http://localhost:9200/metrics/.percolator/http-monitor -X PUT -d '{
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

Add a percolation _(elasticsearch >= 5.0)_

```
curl http://localhost:9200/metrics/queries/http-monitor -X PUT -d '{
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
  "@timestamp": "2013-07-20T09:29:58.000+0000",
  "count": 18
}
```

### Timer

```
{
  "name" : "bulk-request-timer",
  "@timestamp" : "2013-07-20T09:43:58.000+0000",
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
  "@timestamp" : "2013-07-20T09:29:58.000+0000",
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
  "@timestamp" : "2013-07-20T09:29:58.000+0000",
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
  "@timestamp" : "2013-07-20T09:29:58.000+0000",
  "value" : 123
}
```


## Next steps

* Integration with Kibana would be awesome

