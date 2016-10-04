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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.elasticsearch.metrics.percolation.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.metrics.JsonMetrics.*;

public abstract class BaseJsonReporter extends ScheduledReporter {

    public static abstract class Builder<R extends BaseJsonReporter, B extends Builder<R, B>> {
        protected final MetricRegistry registry;
        protected Clock clock;
        protected String prefix;
        protected TimeUnit rateUnit;
        protected TimeUnit durationUnit;
        protected MetricFilter filter;
        protected String timestampFieldname = "@timestamp";
        protected Map<String, Object> additionalFields;

        protected Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Inject your custom definition of how time passes. Usually the default clock is sufficient
         */
        public B withClock(Clock clock) {
            this.clock = clock;
            return (B) this;
        }

        /**
         * Configure a prefix for each metric name. Optional, but useful to identify single hosts
         */
        public B prefixedWith(String prefix) {
            this.prefix = prefix;
            return (B) this;
        }

        /**
         * Convert all the rates to a certain timeunit, defaults to seconds
         */
        public B convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return (B) this;
        }

        /**
         * Convert all the durations to a certain timeunit, defaults to milliseconds
         */
        public B convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return (B) this;
        }

        /**
         * Allows to configure a special MetricFilter, which defines what metrics are reported
         */
        public B filter(MetricFilter filter) {
            this.filter = filter;
            return (B) this;
        }

        /**
         * Configure the name of the timestamp field, defaults to '@timestamp'
         */
        public B timestampFieldname(String fieldName) {
            this.timestampFieldname = fieldName;
            return (B) this;
        }

        /**
         * Additional fields to be included for each metric
         * @param additionalFields
         * @return
         */
        public B additionalFields(Map<String, Object> additionalFields) {
            this.additionalFields = additionalFields;
            return (B) this;
        }

        public abstract R build() throws IOException;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseJsonReporter.class);

    private final Clock clock;
    private final String prefix;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final ObjectWriter writer;

    protected BaseJsonReporter(MetricRegistry registry, String name, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit,
                            MetricFilter filter, String timestampFieldname, Map<String, Object> additionalFields) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.clock = clock;
        this.prefix = prefix;
        if (timestampFieldname == null || timestampFieldname.trim().length() == 0) {
            LOGGER.error("Timestampfieldname {} is not valid, using default @timestamp", timestampFieldname);
            timestampFieldname = "@timestamp";
        }

        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        // auto closing means, that the objectmapper is closing after the first write call, which does not work for bulk requests
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        objectMapper.registerModule(new AfterburnerModule());
        objectMapper.registerModule(new MetricsElasticsearchModule(rateUnit, durationUnit, timestampFieldname, additionalFields));
        writer = objectMapper.writer();
    }

    protected interface Report {
        /**
         * Append metric to report
         */
        void add(JsonMetric jsonMetric, AtomicInteger entriesWritten) throws IOException;

        /**
         * End report, close resources
         * @throws IOException
         */
        void close() throws IOException;
    }

    /**
     * Begin report for timestamp
     */
    protected abstract Report startReport(long timestamp);

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        // nothing to do if we dont have any metrics to report
        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()) {
            LOGGER.info("All metrics empty, nothing to report");
            return;
        }

        final long timestamp = clock.getTime() / 1000;

        try {
            Report report = startReport(timestamp);
            if (report == null) {
                LOGGER.error("Could not start report");
                return;
            }

            AtomicInteger entriesWritten = new AtomicInteger(0);

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                if (entry.getValue().getValue() != null) {
                    JsonMetric jsonMetric = new JsonGauge(name(prefix, entry.getKey()), timestamp, entry.getValue());
                    report.add(jsonMetric, entriesWritten);
                }
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                JsonCounter jsonMetric = new JsonCounter(name(prefix, entry.getKey()), timestamp, entry.getValue());
                report.add(jsonMetric, entriesWritten);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                JsonHistogram jsonMetric = new JsonHistogram(name(prefix, entry.getKey()), timestamp, entry.getValue());
                report.add(jsonMetric, entriesWritten);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                JsonMeter jsonMetric = new JsonMeter(name(prefix, entry.getKey()), timestamp, entry.getValue());
                report.add(jsonMetric, entriesWritten);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                JsonTimer jsonMetric = new JsonTimer(name(prefix, entry.getKey()), timestamp, entry.getValue());
                report.add(jsonMetric, entriesWritten);
            }

            report.close();
        // catch the exception to make sure we do not interrupt the live application
        } catch (IOException e) {
            LOGGER.error("Couldnt report to elasticsearch server", e);
        }
    }
}
