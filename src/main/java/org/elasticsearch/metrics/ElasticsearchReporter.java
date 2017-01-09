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
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import org.elasticsearch.metrics.percolation.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.metrics.JsonMetrics.*;
import static org.elasticsearch.metrics.MetricsElasticsearchModule.BulkIndexOperationHeader;

public class ElasticsearchReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String[] hosts = new String[]{ "localhost:9200" };
        private String index = "metrics";
        private String indexDateFormat = "yyyy-MM";
        private int bulkSize = 2500;
        private Notifier percolationNotifier;
        private MetricFilter percolationFilter;
        private int timeout = 1000;
        private String timestampFieldname = "@timestamp";
        private Map<String, Object> additionalFields;

        private Builder(MetricRegistry registry) {
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
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Configure a prefix for each metric name. Optional, but useful to identify single hosts
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert all the rates to a certain timeunit, defaults to seconds
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert all the durations to a certain timeunit, defaults to milliseconds
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Allows to configure a special MetricFilter, which defines what metrics are reported
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Configure an array of hosts to send data to.
         * Note: Data is always sent to only one host, but this makes sure, that even if a part of your elasticsearch cluster
         *       is not running, reporting still happens
         * A host must be in the format hostname:port
         * The port must be the HTTP port of your elasticsearch instance
         */
        public Builder hosts(String ... hosts) {
            this.hosts = hosts;
            return this;
        }

        /**
         * The timeout to wait for until a connection attempt is and the next host is tried
         */
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * The index name to index in
         */
        public Builder index(String index) {
            this.index = index;
            return this;
        }

        /**
         * The index date format used for rolling indices
         * This is appended to the index name, split by a '-'
         */
        public Builder indexDateFormat(String indexDateFormat) {
            this.indexDateFormat = indexDateFormat;
            return this;
        }

        /**
         * The bulk size per request, defaults to 2500 (as metrics are quite small)
         */
        public Builder bulkSize(int bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * A metrics filter to define the metrics which should be used for percolation/notification
         */
        public Builder percolationFilter(MetricFilter percolationFilter) {
            this.percolationFilter = percolationFilter;
            return this;
        }

        /**
         * An instance of the notifier implemention which should be executed in case of a matching percolation
         */
        public Builder percolationNotifier(Notifier notifier) {
            this.percolationNotifier = notifier;
            return this;
        }

        /**
         * Configure the name of the timestamp field, defaults to '@timestamp'
         */
        public Builder timestampFieldname(String fieldName) {
            this.timestampFieldname = fieldName;
            return this;
        }

        /**
         * Additional fields to be included for each metric
         * @param additionalFields
         * @return
         */
        public Builder additionalFields(Map<String, Object> additionalFields) {
            this.additionalFields = additionalFields;
            return this;
        }

        public ElasticsearchReporter build() throws IOException {
            return new ElasticsearchReporter(registry,
                    hosts,
                    timeout,
                    index,
                    indexDateFormat,
                    bulkSize,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    percolationFilter,
                    percolationNotifier,
                    timestampFieldname,
                    additionalFields);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

    private final String[] hosts;
    private final Clock clock;
    private final String prefix;
    private final String index;
    private final int bulkSize;
    private final int timeout;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter writer;
    private MetricFilter percolationFilter;
    private Notifier notifier;
    private String currentIndexName;
    private SimpleDateFormat indexDateFormat = null;
    private boolean checkedForIndexTemplate = false;

    public ElasticsearchReporter(MetricRegistry registry, String[] hosts, int timeout,
                                 String index, String indexDateFormat, int bulkSize, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit,
                                 MetricFilter filter, MetricFilter percolationFilter, Notifier percolationNotifier, String timestampFieldname, Map<String, Object> additionalFields) throws MalformedURLException {
        super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);
        this.hosts = hosts;
        this.index = index;
        this.bulkSize = bulkSize;
        this.clock = clock;
        this.prefix = prefix;
        this.timeout = timeout;
        if (indexDateFormat != null && indexDateFormat.length() > 0) {
            this.indexDateFormat = new SimpleDateFormat(indexDateFormat);
        }
        if (percolationNotifier != null && percolationFilter != null) {
            this.percolationFilter = percolationFilter;
            this.notifier = percolationNotifier;
        }
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
        checkForIndexTemplate();
    }

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

        if (!checkedForIndexTemplate) {
            checkForIndexTemplate();
        }
        final long timestamp = clock.getTime() / 1000;

        currentIndexName = index;
        if (indexDateFormat != null) {
            currentIndexName += "-" + indexDateFormat.format(new Date(timestamp * 1000));
        }

        try {
            HttpURLConnection connection = openConnection("/_bulk", "POST");
            if (connection == null) {
                LOGGER.error("Could not connect to any configured elasticsearch instances: {}", Arrays.asList(hosts));
                return;
            }

            List<JsonMetric> percolationMetrics = new ArrayList<>();
            AtomicInteger entriesWritten = new AtomicInteger(0);

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                if (entry.getValue().getValue() != null) {
                    JsonMetric jsonMetric = new JsonGauge(name(prefix, entry.getKey()), timestamp, entry.getValue());
                    connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                    addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
                }
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                JsonCounter jsonMetric = new JsonCounter(name(prefix, entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                JsonHistogram jsonMetric = new JsonHistogram(name(prefix, entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                JsonMeter jsonMetric = new JsonMeter(name(prefix, entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                JsonTimer jsonMetric = new JsonTimer(name(prefix, entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            closeConnection(connection);

            // execute the notifier impl, in case percolation found matches
            if (percolationMetrics.size() > 0 && notifier != null) {
                for (JsonMetric jsonMetric : percolationMetrics) {
                    List<String> matches = getPercolationMatches(jsonMetric);
                    for (String match : matches) {
                        notifier.notify(jsonMetric, match);
                    }
                }
            }
        // catch the exception to make sure we do not interrupt the live application
        } catch (IOException e) {
            LOGGER.error("Couldnt report to elasticsearch server", e);
        }
    }

    /**
     * Execute a percolation request for the specified metric
     */
    private List<String> getPercolationMatches(JsonMetric jsonMetric) throws IOException {
        HttpURLConnection connection = openConnection("/" + currentIndexName + "/_search", "POST");
        if (connection == null) {
            LOGGER.error("Could not connect to any configured elasticsearch instances for percolation: {}", Arrays.asList(hosts));
            return Collections.emptyList();
        }

        JsonGenerator json = new JsonFactory().createGenerator(connection.getOutputStream());
        json.setCodec(objectMapper);
        json.writeStartObject();
        json.writeObjectFieldStart("query");
        json.writeObjectFieldStart("percolate");
        json.writeStringField("field", "query");
        json.writeStringField("document_type", jsonMetric.type());
        json.writeObjectField("document", jsonMetric);
        json.writeEndObject();
        json.writeEndObject();
        json.writeEndObject();
        json.flush();

        closeConnection(connection);

        if (connection.getResponseCode() != 200) {
            throw new RuntimeException("Error percolating " + jsonMetric);
        }

        List<String> matches = new ArrayList<>();
        JsonNode input = objectMapper.readTree(connection.getInputStream());
        if (input.has("hits")) {
            JsonNode hits = input.get("hits");
            if (hits.has("hits") && hits.get("hits").isArray()) {
                for (JsonNode entry : (ArrayNode) hits.get("hits")) {
                    if (entry.has("_id")) {
                        matches.add(entry.get("_id").asText());
                    }
                }
            }
        }

        return matches;
    }

    /**
     * Add metric to list of matched percolation if needed
     */
    private void addJsonMetricToPercolationIfMatching(JsonMetric<? extends Metric> jsonMetric, List<JsonMetric> percolationMetrics) {
        if (percolationFilter != null && percolationFilter.matches(jsonMetric.name(), jsonMetric.value())) {
            percolationMetrics.add(jsonMetric);
        }
    }

    private HttpURLConnection writeJsonMetricAndRecreateConnectionIfNeeded(JsonMetric jsonMetric, HttpURLConnection connection,
                                                                           AtomicInteger entriesWritten) throws IOException {
        writeJsonMetric(jsonMetric, writer, connection.getOutputStream());
        return createNewConnectionIfBulkSizeReached(connection, entriesWritten.incrementAndGet());
    }

    private void closeConnection(HttpURLConnection connection) throws IOException {
        connection.getOutputStream().close();
        connection.disconnect();

        // we have to call this, otherwise out HTTP data does not get send, even though close()/disconnect was called
        // Ceterum censeo HttpUrlConnection esse delendam
        if (connection.getResponseCode() != 200) {
            LOGGER.error("Reporting returned code {} {}: {}", connection.getResponseCode(), connection.getResponseMessage());
        }
    }

    /**
     * Create a new connection when the bulk size has hit the limit
     * Checked on every write of a metric
     */
    private HttpURLConnection createNewConnectionIfBulkSizeReached(HttpURLConnection connection, int entriesWritten) throws IOException {
        if (entriesWritten % bulkSize == 0) {
            closeConnection(connection);
            return openConnection("/_bulk", "POST");
        }

        return connection;
    }

    /**
     * serialize a JSON metric over the outputstream in a bulk request
     */
    private void writeJsonMetric(JsonMetric jsonMetric, ObjectWriter writer, OutputStream out) throws IOException {
        writer.writeValue(out, new BulkIndexOperationHeader(currentIndexName, jsonMetric.type()));
        out.write("\n".getBytes());
        writer.writeValue(out, jsonMetric);
        out.write("\n".getBytes());

        out.flush();
    }

    /**
     * Open a new HttpUrlConnection, in case it fails it tries for the next host in the configured list
     */
    private HttpURLConnection openConnection(String uri, String method) {
        for (String host : hosts) {
            try {
                URL templateUrl = new URL("http://" + host  + uri);
                HttpURLConnection connection = ( HttpURLConnection ) templateUrl.openConnection();
                connection.setRequestMethod(method);
                connection.setConnectTimeout(timeout);
                connection.setUseCaches(false);
                if (method.equalsIgnoreCase("POST") || method.equalsIgnoreCase("PUT")) {
                    connection.setDoOutput(true);
                }
                connection.connect();

                return connection;
            } catch (IOException e) {
                LOGGER.error("Error connecting to {}: {}", host, e);
            }
        }

        return null;
    }

    /**
     * This index template is automatically applied to all indices which start with the index name
     * The index template simply configures the name not to be analyzed
     */
    private void checkForIndexTemplate() {
        try {
            HttpURLConnection connection = openConnection( "/_template/metrics_template", "HEAD");
            if (connection == null) {
                LOGGER.error("Could not connect to any configured elasticsearch instances: {}", Arrays.asList(hosts));
                return;
            }
            connection.disconnect();

            boolean isTemplateMissing = connection.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND;

            // nothing there, lets create it
            if (isTemplateMissing) {
                LOGGER.debug("No metrics template found in elasticsearch. Adding...");
                HttpURLConnection putTemplateConnection = openConnection( "/_template/metrics_template", "PUT");
                if(putTemplateConnection == null) {
                    LOGGER.error("Error adding metrics template to elasticsearch");
                    return;
                }
                LOGGER.info("Creating metrics template matching index pattern of {}", index + "*");
                JsonGenerator json = new JsonFactory().createGenerator(putTemplateConnection.getOutputStream());
                json.writeStartObject();
                json.writeStringField("template", index + "*");
                json.writeObjectFieldStart("mappings");

                json.writeObjectFieldStart("_default_");
                json.writeObjectFieldStart("_all");
                json.writeBooleanField("enabled", false);
                json.writeEndObject();
                json.writeObjectFieldStart("properties");
                json.writeObjectFieldStart("name");
                json.writeObjectField("type", "keyword");
                json.writeEndObject();
                json.writeEndObject();
                json.writeEndObject();

                //Percolator - ES 5.0+ changed how percolation queries are handled. Now need to be part of the mapping
                json.writeObjectFieldStart("queries");
                json.writeObjectFieldStart("properties");
                json.writeObjectFieldStart("query");
                json.writeStringField("type", "percolator");
                json.writeEndObject();
                json.writeEndObject();
                json.writeEndObject();

                json.writeEndObject();
                json.writeEndObject();
                json.flush();

                putTemplateConnection.disconnect();
                if (putTemplateConnection.getResponseCode() != 200) {
                    LOGGER.error("Error adding metrics template to elasticsearch: {}/{}", putTemplateConnection.getResponseCode(), putTemplateConnection.getResponseMessage());
                }
            }
            checkedForIndexTemplate = true;
        } catch (IOException e) {
            LOGGER.error("Error when checking/adding metrics template to elasticsearch", e);
        }
    }
}
