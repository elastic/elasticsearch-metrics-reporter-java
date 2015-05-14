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
import org.elasticsearch.metrics.percolation.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        private String[] hosts = new String[]{"localhost:9200"};
        /** Should I query the first node to find the rest of the cluster nodes? */
        private boolean discoverClusterMembers = true;
        private String index = "metrics";
        private String indexDateFormat = "yyyy-MM";
        private int bulkSize = 2500;
        private Notifier percolationNotifier;
        private MetricFilter percolationFilter;
        private int timeout = 1000;
        private String timestampFieldname = "@timestamp";
        private String hostnameField = "host";
        private String hostName;
        private boolean sendHostname = true;
        private Map<String, Object> additionalFields;
        private boolean saveEntryOnInstantiation;

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
        @SuppressWarnings("unused")
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

        /** Disables cluster node discovery, relying entirely on the provided {@link #hosts(String...)}.*/
        @SuppressWarnings("unused")
        public Builder withoutNodeDiscovery() {
            this.discoverClusterMembers = false;
            return this;
        }

        /** Enables discovery of the other members of the cluster in which {@link #hosts(String...)} resides. */
        @SuppressWarnings("unused")
        public Builder withNodeDiscovery() {
            this.discoverClusterMembers = true;
            return this;
        }

        /**
         * The timeout to wait for until a connection attempt is and the next host is tried
         */
        @SuppressWarnings("unused")
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
        @SuppressWarnings("unused")
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

        /** Configure the name of the hostname field, instead of {@code host}. */
        @SuppressWarnings("unused")
        public Builder hostnameFieldname(String value) {
            this.hostnameField = value;
            return this;
        }

        /** Set the hostname sending the provided hostname in {@code host}.*/
        public Builder withHostname(String hostname) {
            hostName = hostname;
            return this;
        }

        /**
         * Uses the traditional {@link InetAddress#getLocalHost()} mechanism to sniff the hostname,
         * sending it in the field used by {@link #withHostname(String)}.
         */
        public Builder withMyHostname() throws UnknownHostException {
            final String hostName = InetAddress.getLocalHost().getHostName();
            return this.withHostname(hostName);
        }

        /** Disables the default option of sending along the hostname. */
        @SuppressWarnings("unused")
        public Builder withoutHostname() {
            this.sendHostname = false;
            return this;
        }

        /**
         * Additional fields to be included for each metric
         */
        public Builder additionalFields(Map<String, Object> additionalFields) {
            this.additionalFields = additionalFields;
            return this;
        }

        /**
         * Custom method to save log start up time.
         */
        @SuppressWarnings("unused")
        public Builder saveEntryOnInstantiation(boolean saveEntryOnInstantiation) {
            this.saveEntryOnInstantiation = saveEntryOnInstantiation;
            return this;
        }

        public ElasticsearchReporter build() throws IOException {
            if (sendHostname) {
                if (null == hostName) {
                    withMyHostname();
                }
                if (null == additionalFields) {
                    additionalFields = new HashMap<>();
                }
                additionalFields.put(hostnameField, hostName);
            }
            return new ElasticsearchReporter(this);
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
    private SimpleDateFormat indexDateFormat;
    private boolean checkedForIndexTemplate;
    private boolean saveEntryOnInstantiation;

    public ElasticsearchReporter(Builder config)
            throws IOException {
        super(config.registry, "elasticsearch-reporter", config.filter, config.rateUnit, config.durationUnit);
        if (config.discoverClusterMembers) {
            this.hosts = discoverClusterMembers(config.hosts);
        } else {
            this.hosts = config.hosts;
        }
        this.index = config.index;
        this.bulkSize = config.bulkSize;
        this.clock = config.clock;
        this.prefix = config.prefix;
        this.timeout = config.timeout;
        this.saveEntryOnInstantiation = config.saveEntryOnInstantiation;

        if (config.indexDateFormat != null && config.indexDateFormat.length() > 0) {
            this.indexDateFormat = new SimpleDateFormat(config.indexDateFormat);
        }
        if (config.percolationNotifier != null && config.percolationFilter != null) {
            this.percolationFilter = config.percolationFilter;
            this.notifier = config.percolationNotifier;
        }
        if (config.timestampFieldname == null || config.timestampFieldname.trim().length() == 0) {
            LOGGER.error("Timestampfieldname {} is not valid, using default @timestamp", config.timestampFieldname);
            config.timestampFieldname = "@timestamp";
        }

        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        // auto closing means, that the objectmapper is closing after the first write call, which does not work for bulk requests
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        objectMapper.registerModule(new MetricsElasticsearchModule(
                config.rateUnit, config.durationUnit, config.timestampFieldname, config.additionalFields));
        writer = objectMapper.writer();
        checkForIndexTemplate();
    }

    @Override
    public void start(long period, TimeUnit unit) {
        super.start(period, unit);

        if (saveEntryOnInstantiation) {
            new HttpConnectionTemplate() {

                @Override
                protected HttpURLConnection performAction(HttpURLConnection connection, long timestamp)
                        throws IOException {
                    JsonMetric jsonMetric = new JsonStartTime(name(prefix, "metricStart"), timestamp);
                    connection =
                            writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, new AtomicInteger(0));
                    return connection;
                }
            }.write();
        }
    }


    @Override
    public void report(final SortedMap<String, Gauge> gauges,
                       final SortedMap<String, Counter> counters,
                       final SortedMap<String, Histogram> histograms,
                       final SortedMap<String, Meter> meters,
                       final SortedMap<String, Timer> timers) {

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
                    JsonGauge jsonMetric = new JsonGauge(name(prefix, entry.getKey()), timestamp, entry.getValue());
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

    private abstract class HttpConnectionTemplate {

        public void write() {
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
                    LOGGER.error("Could not connect to any configured elasticsearch instances: {}",
                            Arrays.asList(hosts));
                    return;
                }

                connection = performAction(connection, timestamp);

                closeConnection(connection);
                // catch the exception to make sure we do not interrupt the live application
            } catch (IOException e) {
                LOGGER.error("Couldnt report to elasticsearch server", e);
            }
        }

        protected abstract HttpURLConnection performAction(HttpURLConnection connection, long timestamp)
                throws IOException;

    }

    /**
     * Execute a percolation request for the specified metric
     */
    private List<String> getPercolationMatches(JsonMetric jsonMetric) throws IOException {
        HttpURLConnection connection = openConnection("/" + currentIndexName + "/" + jsonMetric.type() + "/_percolate", "POST");
        if (connection == null) {
            LOGGER.error("Could not connect to any configured elasticsearch instances for percolation: {}", Arrays.asList(hosts));
            return Collections.emptyList();
        }

        Map<String, Object> data = new HashMap<>(1);
        data.put("doc", jsonMetric);
        objectMapper.writeValue(connection.getOutputStream(), data);
        closeConnection(connection);

        if (connection.getResponseCode() != 200) {
            throw new RuntimeException("Error percolating " + jsonMetric);
        }

        @SuppressWarnings("unchecked")
        final Map<String, Object> input = objectMapper.readValue(connection.getInputStream(), Map.class);
        List<String> matches = new ArrayList<>();
        if (input.containsKey("matches") && input.get("matches") instanceof List) {
            @SuppressWarnings("unchecked")
            final List<Map<String, String>> foundMatches = (List<Map<String, String>>) input.get("matches");
            for (Map<String, String> entry : foundMatches) {
                if (entry.containsKey("_id")) {
                    matches.add(entry.get("_id"));
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
        return openConnection(hosts, uri, method);
    }
    private HttpURLConnection openConnection(String[] hostList, String uri, String method) {
        for (String host : hostList) {
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
                if (null == putTemplateConnection) {
                    LOGGER.error("Could not connect to any configured elasticsearch instances: {}", Arrays.asList(hosts));
                    return;
                }
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
                json.writeObjectField("type", "string");
                json.writeObjectField("index", "not_analyzed");
                json.writeEndObject();
                json.writeEndObject();
                json.writeEndObject();

                json.writeEndObject();
                json.writeEndObject();
                json.flush();

                putTemplateConnection.disconnect();
                if (putTemplateConnection.getResponseCode() != 200) {
                    LOGGER.error("Error adding metrics template to elasticsearch: {}/{}" + putTemplateConnection.getResponseCode(), putTemplateConnection.getResponseMessage());
                }
            }
            checkedForIndexTemplate = true;
        } catch (IOException e) {
            LOGGER.error("Error when checking/adding metrics template to elasticsearch", e);
        }
    }

    /**
     * Contacts the bootstrap hosts and asks for their clustermates, returning the authoritative cluster
     * member list.
     * @param bootstrapHosts the initial members of the cluster to contact in order until one succeeds
     * @return the list of discovered members of the cluster in which the {@code bootstrapHosts} reside.
     * @throws IOException if unable to read from the target host
     */
    private String[] discoverClusterMembers(final String[] bootstrapHosts) throws IOException {
        if (null == bootstrapHosts || 0 == bootstrapHosts.length) {
            throw new IllegalStateException("I cannot operate without bootstrap hosts");
        }
        final int retries = 5;
        IOException seen = null;
        for (int i = 0; i < retries; i++) {
            final String[] result;
            try {
                result = discoverClusterMembersAction(bootstrapHosts);
            } catch (IOException ioe) {
                LOGGER.warn("IOEx while in retry #{}", i, ioe);
                seen = ioe;
                continue;
            }
            return result;
        }
        throw seen;
    }

    /** This is split out from the shorter named function in order to facilitate retries. */
    private String[] discoverClusterMembersAction(final String[] bootstrapHosts) throws IOException {
        // "inet[/192.168.169.83:9202]"
        final Pattern httpAddressToHostPortRE = Pattern.compile("[^\\[]*\\[/([^\\]]+)\\]");

        final HttpURLConnection conn = openConnection(bootstrapHosts, "/_nodes", "GET");
        if (null == conn) {
            // openConnection logs the error, so just give what they gave and wish them luck
            LOGGER.warn("I am returning bootstrap-hosts because I could not connect to any of them: {}",
                    Arrays.asList(bootstrapHosts));
            return bootstrapHosts;
        }
        final List<String> discoveredHosts = new ArrayList<>();
        try {
            final InputStream nodeStream = conn.getInputStream();
            // all of the JsonNode javadoc indicates it will never return `null`, which explains the lack of checking
            final JsonNode clusterNodes = objectMapper.readTree(nodeStream);
            for (final JsonNode nodeObj : clusterNodes.get("nodes")) {
                final String http_address = nodeObj.get("http_address").asText();
                if (http_address.isEmpty()) {
                    // drop the entire structure into the logs
                    LOGGER.warn("Node {} has empty \"http_address\": <<{}>>", nodeObj);
                    // but send along a shorter message to sentry
                    LOGGER.error("Node {} has empty \"http_address\"");
                    continue;
                }
                final Matcher ma = httpAddressToHostPortRE.matcher(http_address);
                if (! ma.find()) {
                    LOGGER.error("Unexpected \"http_address\" format: <<{}>>", http_address);
                    continue;
                }
                final String hostPort = ma.group(1);
                discoveredHosts.add(hostPort);
            }
        } finally {
            conn.disconnect();
        }
        LOGGER.info("Discovered cluster members: {}", discoveredHosts);
        final String[] results = new String[discoveredHosts.size()];
        discoveredHosts.toArray(results);
        return results;
    }
}
