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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.metrics.JsonMetrics.JsonCounter;
import static org.elasticsearch.metrics.JsonMetrics.JsonGauge;
import static org.elasticsearch.metrics.JsonMetrics.JsonHistogram;
import static org.elasticsearch.metrics.JsonMetrics.JsonMeter;
import static org.elasticsearch.metrics.JsonMetrics.JsonTimer;

public class MetricsElasticsearchModule extends Module {

    public static final Version VERSION = new Version(3, 0, 0, "", "metrics-elasticsearch-reporter", "metrics-elasticsearch-reporter");

    private static void writeAdditionalFields(final Map<String, Object> additionalFields, final JsonGenerator json) throws IOException {
        if (additionalFields != null) {
            for (final Map.Entry<String, Object> field : additionalFields.entrySet()) {
                json.writeObjectField(field.getKey(), field.getValue());
            }
        }
    }
    
    private static class GaugeSerializer extends StdSerializer<JsonGauge> {
        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;

        private GaugeSerializer(String timestampFieldname, Map<String, Object> additionalFields) {
            super(JsonGauge.class);
            this.timestampFieldname = timestampFieldname;
            this.additionalFields = additionalFields;
        }

        @Override
        public void serialize(JsonGauge gauge,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", gauge.name());
            json.writeObjectField(timestampFieldname, gauge.timestampAsDate());
            final Object value;
            try {
                value = gauge.value().getValue();
                if (value instanceof Iterable) { // Check if writing a single value or an array
                	Iterable<?> iterable = (Iterable<?>) value;
                	json.writeArrayFieldStart("value");
                	for (Object valueItem : iterable) {
                		json.writeObject(valueItem);
                	}
                	json.writeEndArray();
                } else {
                	json.writeObjectField("value", value);	
                }
            } catch (RuntimeException e) {
                json.writeObjectField("error", e.toString());
            }
            writeAdditionalFields(additionalFields, json);
            json.writeEndObject();
        }
    }

    private static class CounterSerializer extends StdSerializer<JsonCounter> {
        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;

        private CounterSerializer(String timestampFieldname, Map<String, Object> additionalFields) {
            super(JsonCounter.class);
            this.timestampFieldname = timestampFieldname;
            this.additionalFields = additionalFields;
        }

        @Override
        public void serialize(JsonCounter counter,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", counter.name());
            json.writeObjectField(timestampFieldname, counter.timestampAsDate());
            json.writeNumberField("count", counter.value().getCount());
            writeAdditionalFields(additionalFields, json);
            json.writeEndObject();
        }
    }

    private static class HistogramSerializer extends StdSerializer<JsonHistogram> {

        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;

        private HistogramSerializer(String timestampFieldname, Map<String, Object> additionalFields) {
            super(JsonHistogram.class);
            this.timestampFieldname = timestampFieldname;
            this.additionalFields = additionalFields;
        }

        @Override
        public void serialize(JsonHistogram jsonHistogram,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", jsonHistogram.name());
            json.writeObjectField(timestampFieldname, jsonHistogram.timestampAsDate());
            Histogram histogram = jsonHistogram.value();

            final Snapshot snapshot = histogram.getSnapshot();
            json.writeNumberField("count", histogram.getCount());
            json.writeNumberField("max", snapshot.getMax());
            json.writeNumberField("mean", snapshot.getMean());
            json.writeNumberField("min", snapshot.getMin());
            json.writeNumberField("p50", snapshot.getMedian());
            json.writeNumberField("p75", snapshot.get75thPercentile());
            json.writeNumberField("p95", snapshot.get95thPercentile());
            json.writeNumberField("p98", snapshot.get98thPercentile());
            json.writeNumberField("p99", snapshot.get99thPercentile());
            json.writeNumberField("p999", snapshot.get999thPercentile());

            json.writeNumberField("stddev", snapshot.getStdDev());
            writeAdditionalFields(additionalFields, json);
            json.writeEndObject();
        }
    }

    private static class MeterSerializer extends StdSerializer<JsonMeter> {
        private final String rateUnit;
        private final double rateFactor;
        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;

        public MeterSerializer(TimeUnit rateUnit, String timestampFieldname, Map<String, Object> additionalFields) {
            super(JsonMeter.class);
            this.timestampFieldname = timestampFieldname;
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = calculateRateUnit(rateUnit, "events");
            this.additionalFields = additionalFields;
        }

        @Override
        public void serialize(JsonMeter jsonMeter,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", jsonMeter.name());
            json.writeObjectField(timestampFieldname, jsonMeter.timestampAsDate());
            Meter meter = jsonMeter.value();
            json.writeNumberField("count", meter.getCount());
            json.writeNumberField("m1_rate", meter.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", meter.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("m15_rate", meter.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", meter.getMeanRate() * rateFactor);
            json.writeStringField("units", rateUnit);
            writeAdditionalFields(additionalFields, json);
            json.writeEndObject();
        }
    }

    private static class TimerSerializer extends StdSerializer<JsonTimer> {
        private final String rateUnit;
        private final double rateFactor;
        private final String durationUnit;
        private final double durationFactor;
        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;

        private TimerSerializer(TimeUnit rateUnit, TimeUnit durationUnit, String timestampFieldname, Map<String, Object> additionalFields) {
            super(JsonTimer.class);
            this.timestampFieldname = timestampFieldname;
            this.rateUnit = calculateRateUnit(rateUnit, "calls");
            this.rateFactor = rateUnit.toSeconds(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.additionalFields = additionalFields;
        }

        @Override
        public void serialize(JsonTimer jsonTimer,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", jsonTimer.name());
            json.writeObjectField(timestampFieldname, jsonTimer.timestampAsDate());
            Timer timer = jsonTimer.value();
            final Snapshot snapshot = timer.getSnapshot();
            json.writeNumberField("count", timer.getCount());
            json.writeNumberField("max", snapshot.getMax() * durationFactor);
            json.writeNumberField("mean", snapshot.getMean() * durationFactor);
            json.writeNumberField("min", snapshot.getMin() * durationFactor);

            json.writeNumberField("p50", snapshot.getMedian() * durationFactor);
            json.writeNumberField("p75", snapshot.get75thPercentile() * durationFactor);
            json.writeNumberField("p95", snapshot.get95thPercentile() * durationFactor);
            json.writeNumberField("p98", snapshot.get98thPercentile() * durationFactor);
            json.writeNumberField("p99", snapshot.get99thPercentile() * durationFactor);
            json.writeNumberField("p999", snapshot.get999thPercentile() * durationFactor);

            /*
            if (showSamples) {
                final long[] values = snapshot.getValues();
                final double[] scaledValues = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    scaledValues[i] = values[i] * durationFactor;
                }
                json.writeObjectField("values", scaledValues);
            }
            */

            json.writeNumberField("stddev", snapshot.getStdDev() * durationFactor);
            json.writeNumberField("m1_rate", timer.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", timer.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("m15_rate", timer.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", timer.getMeanRate() * rateFactor);
            json.writeStringField("duration_units", durationUnit);
            json.writeStringField("rate_units", rateUnit);
            writeAdditionalFields(additionalFields, json);
            json.writeEndObject();
        }
    }


    /**
     * Serializer for the first line of the bulk index operation before the json metric is written
     */
    private static class BulkIndexOperationHeaderSerializer extends StdSerializer<BulkIndexOperationHeader> {

        public BulkIndexOperationHeaderSerializer() {
            super(BulkIndexOperationHeader.class);
        }

        @Override
        public void serialize(BulkIndexOperationHeader bulkIndexOperationHeader, JsonGenerator json, SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeObjectFieldStart("index");
            if (bulkIndexOperationHeader.index != null) {
                json.writeStringField("_index", bulkIndexOperationHeader.index);
            }
            if (bulkIndexOperationHeader.type != null) {
                json.writeStringField("_type", bulkIndexOperationHeader.type);
            }
            json.writeEndObject();
            json.writeEndObject();
        }
    }

    public static class BulkIndexOperationHeader {
        public String index;
        public String type;

        public BulkIndexOperationHeader(String index, String type) {
            this.index = index;
            this.type = type;
        }
    }

    private final TimeUnit rateUnit;
    private final TimeUnit durationUnit;
    private final String timestampFieldname;
    private final Map<String, Object> additionalFields;

    public MetricsElasticsearchModule(TimeUnit rateUnit, TimeUnit durationUnit, String timestampFieldname, Map<String, Object> additionalFields) {
        this.rateUnit = rateUnit;
        this.durationUnit = durationUnit;
        this.timestampFieldname = timestampFieldname;
        this.additionalFields = additionalFields;
    }

    @Override
    public String getModuleName() {
        return "metrics-elasticsearch-serialization";
    }

    @Override
    public Version version() {
        return VERSION;
    }

    @Override
    public void setupModule(SetupContext context) {
        context.addSerializers(new SimpleSerializers(Arrays.<JsonSerializer<?>>asList(
                new GaugeSerializer(timestampFieldname, additionalFields),
                new CounterSerializer(timestampFieldname, additionalFields),
                new HistogramSerializer(timestampFieldname, additionalFields),
                new MeterSerializer(rateUnit, timestampFieldname, additionalFields),
                new TimerSerializer(rateUnit, durationUnit, timestampFieldname, additionalFields),
                new BulkIndexOperationHeaderSerializer()
        )));
    }

    private static String calculateRateUnit(TimeUnit unit, String name) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return name + '/' + s.substring(0, s.length() - 1);
    }
}
