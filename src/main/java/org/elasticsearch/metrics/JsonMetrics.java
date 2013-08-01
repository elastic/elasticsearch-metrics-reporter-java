package org.elasticsearch.metrics;

import com.codahale.metrics.*;
import java.util.Date;

public class JsonMetrics {

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
        public JsonGauge(String name, long timestamp, Gauge value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return "gauge";
        }
    }

    public static class JsonCounter extends JsonMetric<Counter> {
        public JsonCounter(String name, long timestamp, Counter value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return "counter";
        }
    }

    public static class JsonHistogram extends JsonMetric<Histogram> {
        public JsonHistogram(String name, long timestamp, Histogram value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return "histogram";
        }
    }

    public static class JsonMeter extends JsonMetric<Meter> {
        public JsonMeter(String name, long timestamp, Meter value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return "meter";
        }
    }

    public static class JsonTimer extends JsonMetric<Timer> {
        public JsonTimer(String name, long timestamp, Timer value) {
            super(name, timestamp, value);
        }

        @Override
        public String type() {
            return "timer";
        }
    }
}
