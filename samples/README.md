# Sample applications

In order to check out these applications you need `maven` for the logfile parser and `node`/`npm` for the cockpit application.

## Logfile parser

This is a stupidly simple project, to show how the metrics library can be used.
The tool reads the stream of the currently visited US government sites and indexes the location as a geohash.

The application features several metrics, like the metering the incoming requests, counting the heartbeats or checking how many indexing requests to elasticsearch are sent.

## Cockpit - a real time notification dashboard

This little dashboard application can be used to draw graphs from the indexed metrics as well as getting realtime notifications. The application allows you to

* Draw graphs from percolations
* Add percolations for real-time notifications
* Receive real-time notifications from the logfile parser application and immediately display them in the browser using websockets.

## Getting up and running

Run this in the top level directory to put the metrics reporter into your local maven repo

```
mvn install
```

To get up and running you do not need to have elasticsearch installed, as it is started by default from the sample application.

```
cd samples/usa-gov-logfile-parser
mvn compile exec:java
```

**Note**: The above command starts an own elasticsearch instance.
If you already have an elasticsearch instance up and running, call `mvn exec:java -Dcreate.es.instance=no -Dcluster.name=YourClusterName`. The default cluster name is `metrics`, so it is likely you will have to change it, if you already have an elasticsearch instance running.

Then fire up another terminal and prepare cockpit application

```
cd samples/cockpit
npm install
node app.js
```

**Note**: You need at least node v0.8 in order to get the application up and running.

Open up your browser to `http://localhost:3000`

The little application allows you to do one of the following

* Draw a metric, like `usagov-indexing-requests` (just type it in the searchbar, it has autocomplete)
* Add a percolation: `id` must be unique, i.e. `my-percolator-match`, name can be again `usagov-indexing-requests`, field could be the `m1_rate` and range selection could be `> 0.1` to make sure it matches everytime, so you see it works
* Wait for a percolation to match, then delete it by clicking on the trashcan in the notification, if the notification is not important any more.

# Sample application: simple web application

The simple-webapp/ directory contains a very simple web application, which not only generates metrics for the incoming web requests, but also for a running background task, which reads the meetup.com reservations stream. You can start it by running `mvn clean compile exec:java`

The application also includes a `HttpNotifier` which reports to the cockpit sample application, but just prints out percolated metrics on standard out by default.

