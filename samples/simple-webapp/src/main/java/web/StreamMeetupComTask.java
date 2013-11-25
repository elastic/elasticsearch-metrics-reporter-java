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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;

/**
 *
 */
public class StreamMeetupComTask implements Callable<Void> {

    private MetricRegistry metrics;

    public StreamMeetupComTask(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public Void call() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectReader reader = objectMapper.reader(Map.class);
            MappingIterator<Map<String, Object>> iterator = reader.readValues(getInputStream());

            while (iterator.hasNextValue()) {
                Map<String, Object> entry = iterator.nextValue();

                // monitor the distribution of countries
                if (entry.containsKey("group") && entry.get("group") instanceof Map) {
                    Map<String, Object> group = (Map<String, Object>) entry.get("group");
                    if (group.containsKey("group_country")) {
                        metrics.meter("meetup.country." + group.get("group_country")).mark();
                        metrics.meter("meetup.country.total").mark();
                    }
                }

                // monitor the distribution of the number of guests
                if (entry.containsKey("guests") && entry.get("guests") instanceof Long) {
                    metrics.histogram("meetup.guests").update((Long)entry.get("guests"));
                }

                // monitor reservation time upfront, 1d, 4d, 1w, 2w, 1m, 2m, -
                if (entry.containsKey("event") && entry.get("event") instanceof Map) {
                    Map<String, Object> event = (Map<String, Object>) entry.get("event");
                    if (event.get("time") instanceof Long) {
                        metrics.counter("meetup.reservation.time.total").inc();
                        metrics.counter("meetup.reservation.time." + getUpfrontReservationTime((Long) event.get("time"))).inc();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private InputStream getInputStream() throws IOException {
        URL url = new URL("http://stream.meetup.com/2/rsvps");
        HttpURLConnection request = (HttpURLConnection) url.openConnection();
        return request.getInputStream();
    }

    // -1 (in past), 1d, 4d, 1w, 2w, 1m, 2m, -
    private String getUpfrontReservationTime(Long dateInMillis) {
        DateTime now = new DateTime(DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
        DateTime event = new DateTime(dateInMillis, DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
        Duration duration = new Duration(now, event);

        if (duration.getMillis() < 0) {
            return "-1";
        } else if (duration.getStandardSeconds() < 86400) {
            return "1d";
        } else if (duration.getStandardDays() < 4) {
            return "4d";
        } else if (duration.getStandardDays() < 7) {
            return "1w";
        } else if (duration.getStandardDays() < 14) {
            return "2w";
        } else if (duration.getStandardDays() < 28) {
            return "4w";
        } else if (duration.getStandardDays() < 56) {
            return "8w";
        } else {
            return "-";
        }
    }

}
