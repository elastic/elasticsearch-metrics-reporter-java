/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

var express = require('express')
  , http = require('http')
  , sockjs = require('sockjs')
  , request = require('superagent')
  , _ = require('underscore')
  , path = require('path');

var configuration = { es : { url: "http://localhost:9200/metrics/_search", percolateUrl: "http://localhost:9200/_percolator/" } }
var app = express();

// all environments
app.set('port', process.env.PORT || 3000);
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

// sockjs init
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};

var wsConnections = [];
var sockjs_echo = sockjs.createServer(sockjs_opts);
sockjs_echo.on('connection', function(conn) {
    wsConnections.push(conn);
    conn.on('close', function() {
      for (var i = 0; i < wsConnections.length; i++) {
        if (wsConnections[i] === conn) wsConnections.splice(i, 1);
      }
    });
});

// one page app FTW
app.get('/', function(req, res) { res.sendfile('public/html/ember-app.html') /*res.render('index', { title: 'Express' })*/ });

// get all available names from es
// OUTPUT: list of [{name:'', type:''}]
app.get('/graphNames', function(req, res) {
  request.post(configuration.es.url)
    .send({ size: 0, facets: { names : { terms : { script_field: 'doc["_type"].value + ":" + doc["name"].value', size: 500 } } } })
    .type('json')
    .on('error', function(err) { console.log("Error connecting to " + configuration.es.url + ": " + err) })
    .end(function(response) {
      var data = _.map(response.body.facets.names.terms, function(entry) { d=entry.term.split(':') ; return { type: d[0], name: d[1] } } );
      res.end(JSON.stringify(data))
    });
});

// get all available data points for a graph name
// INPUT: graph name, metric name, from timestamp, to timestamp
// OUTPUT: array of data points
app.get('/graphData', function(req, res) {
  var name = req.query.name
  var type = req.query.type
  var time = req.query.time
  var fields = [ "name", "@timestamp", req.query.values ]

  request.post(configuration.es.url)
    // max size is one month, so it can be 24 * 60 * 31, usually we should calculate this dependent on input size
    .send({ size: 44640, filter: { and: [ { term: { name: name } }, { type : { value: type } }, { range: { '@timestamp': { from: 'now-' + time, to: 'now' } }} ] }, fields: fields, sort: [ { '@timestamp': { order: "asc" }} ] })
    .type('json')
    .on('error', function(err) { console.log("Error connecting to " + configuration.es.url + ": " + err) })
    .end(function(response) {
      var hits = response.body.hits.hits
      //console.log(JSON.stringify(hits));
      var valuesToGraph = req.query.values;
      var dataToRenderOut = []
      for (var i = 0 ; i < valuesToGraph.length ; i++) {
        var currentName = req.query.values[i]
        var values = _.map(hits, function(hit) { return { x: hit.sort[0], y: hit.fields[currentName] } })
        dataToRenderOut.push({key:currentName, values: values})
      }
      res.end(JSON.stringify(dataToRenderOut))
    });
});

app.post('/notify', function(req, res) {
  for (var i = 0; i < wsConnections.length; i++) {
    wsConnections[i].write(JSON.stringify({ metricName: req.body.metricName, percolatorId: req.body.percolatorId}))
  }
  res.end();
});

// TODO REGISTER NEW NOTIFICATION/PERCOLATION
app.post('/percolator', function(req, res) {
  var id = req.body.id
  var field = req.body.field
  var name = req.body.name
  var range = req.body.range
  var rangeValue = req.body.rangeValue
  
  var url = configuration.es.percolateUrl + "/metrics/" + id
  var query = { query : { bool : { must : [ { term : { name: name } }  ] } } }
  var rangeQuery =  { range : {}}
  if (range === '<')  rangeQuery.range[field] = { to: rangeValue,   include_upper:false }
  if (range === '<=') rangeQuery.range[field] = { to: rangeValue,   include_upper:true  }
  if (range === '>')  rangeQuery.range[field] = { from: rangeValue, include_lower:false }
  if (range === '=>') rangeQuery.range[field] = { from: rangeValue, include_lower:true  }

  query.query.bool.must.push(rangeQuery)
  request.post(url).type("json")
    .send(query)
    .on('error', function(err) { console.log("Error connecting to " + url + ": " + err) })
    .end(function(response) {
      //console.log(JSON.stringify(response.body))
      res.end();
    })
});

// delete percolator
app.delete('/percolator', function(req, res) {
  var id = req.body.id
  var url = configuration.es.percolateUrl + "/metrics/" + id
  console.log("URL " + url)
  request.del(url).type('json')
    .send()
    .on('error', function(err) { console.log("Error connecting to " + url + ": " + err) })
    .end(function() { res.end(); })
})

var server = http.createServer(app)
sockjs_echo.installHandlers(server, {prefix:'/ws'});

server.listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});
