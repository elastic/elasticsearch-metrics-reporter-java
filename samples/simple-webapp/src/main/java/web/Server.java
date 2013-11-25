/*
 * Licensed to ElasticSearch and Shay Banon under one
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
package web;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.metrics.ElasticsearchReporter;
import spark.Filter;
import spark.Request;
import spark.Response;
import spark.Route;
import web.notifier.SystemOutNotifier;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static spark.Spark.*;

/**
 *
 */
public class Server {

    public static void main(String[] args) throws Exception {

        // configure reporter
        final MetricRegistry metrics = new MetricRegistry();
        ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(metrics)
                                            // support for several es nodes
                                            .hosts("localhost:9200", "localhost:9201")
                                            // just create an index, no date format, means one index only
                                            .index("metrics")
                                            .indexDateFormat(null)
                                            // define a percolation check on all metrics
                                            .percolationFilter(MetricFilter.ALL)
                                            .percolationNotifier(new SystemOutNotifier())
                                            //.percolationNotifier(new HttpNotifier())
                                            .build();
        // usually you set this to one minute
        reporter.start(10, TimeUnit.SECONDS);

        // start up background thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new StreamMeetupComTask(metrics));

        // start up web app
        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                return "this is /";
            }
        });

        post(new Route("/checkout") {
            @Override
            public Object handle(Request request, Response response) {
                // emulate calling external payment API
                Timer.Context timer = metrics.timer("payment.runtime").time();

                try {
                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    // ignore
                } finally {
                    timer.stop();
                }

                response.redirect("/");
                return null;
            }
        });

        before(new Filter() {
            @Override
            public void handle(Request request, Response response) {
                metrics.counter("http.connections.concurrent").inc();
                metrics.meter("http.connections.requests").mark();
                metrics.counter("http.connections.stats." + request.raw().getMethod()).inc();
                metrics.counter("http.connections.stats." + request.raw().getMethod() + "." + request.pathInfo()).inc();
            }
        });

        after(new Filter() {
            @Override
            public void handle(Request request, Response response) {
                metrics.counter("http.connections.concurrent").dec();
            }
        });
    }

}
