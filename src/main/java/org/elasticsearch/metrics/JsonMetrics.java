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
package org.elasticsearch.metrics;

import com.codahale.metrics.*;
import java.util.Date;

public class JsonMetrics {

    /**
     * A abstract json metric class, from which all other classes inherit
     * The other classes are simply concrete json implementations of the existing metrics classes
     */
    public static abstract class JsonMetric<T> {
        private final String name;
        private long timestamp;
        private final T value;

        public JsonMetric(String name, long timestamp, T value) {
            this.name = name;
            this.timestamp = timestamp;
            this.value = value;
        }

        public String name() {
            return name;
        }

        public long timestamp() {
            return timestamp;
        }

        public Date timestampAsDate() {
            return new Date(timestamp * 1000);
        }

        public T value() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s", type(), name, timestamp);
        }

        public abstract String type();
    }

    public static class JsonGauge extends JsonMetric<Gauge> {
        private static final String TYPE = "gauge";

        public JsonGauge(String name, long timestamp, Gauge value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    public static class JsonCounter extends JsonMetric<Counter> {
        private static final String TYPE = "counter";

        public JsonCounter(String name, long timestamp, Counter value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    public static class JsonHistogram extends JsonMetric<Histogram> {
        private static final String TYPE = "histogram";

        public JsonHistogram(String name, long timestamp, Histogram value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    public static class JsonMeter extends JsonMetric<Meter> {
        private static final String TYPE = "meter";
        public JsonMeter(String name, long timestamp, Meter value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    public static class JsonTimer extends JsonMetric<Timer> {
        private static final String TYPE = "timer";

        public JsonTimer(String name, long timestamp, Timer value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }
}
