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
package web.notifier;

import org.elasticsearch.metrics.JsonMetrics;
import org.elasticsearch.metrics.percolation.Notifier;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Send a post request to the cockpit sample application, so it gets shown as a notification in there
 */
public class HttpNotifier implements Notifier {

    @Override
    public void notify(JsonMetrics.JsonMetric jsonMetric, String percolateMatcher) {
        System.out.println("Sending notification for metric " + jsonMetric.name());

        try {
            URL url = new URL("http://localhost:3000/notify");
            HttpURLConnection connection = getUrlConnection(url);
            String data = String.format("metricName=%s&percolatorId=%s", jsonMetric.name(), percolateMatcher);
            connection.getOutputStream().write(data.getBytes());

            if (connection.getResponseCode() != 200) {
                System.out.println("Got broken response code " + connection.getResponseCode() + " for metric " + jsonMetric.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HttpURLConnection getUrlConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setUseCaches(false);
        connection.setDoOutput(true);
        connection.connect();
        return connection;
    }}
