package org.elasticsearch.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.metrics.percolation.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.elasticsearch.metrics.JsonMetrics.*;
import static org.elasticsearch.metrics.MetricsElasticsearchModule.BulkIndexOperationHeader;

public class ElasticsearchReporter extends ScheduledReporter {

    private final String host;
    private final int port;

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
        private String host = "localhost";
        private int port = 9200;
        private String index = "metrics";
        private int bulkSize = 2500;
        private String percolateMetricsRegex;
        private Notifier percolateNotifier;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder bulkSize(int bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        public Builder percolateMetrics(String regex) {
            this.percolateMetricsRegex = regex;
            return this;
        }

        public Builder percolateNotifier(Notifier notifier) {
            this.percolateNotifier = notifier;
            return this;
        }

        public ElasticsearchReporter build() throws IOException {
            return new ElasticsearchReporter(registry,
                    host,
                    port,
                    index,
                    bulkSize,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    percolateMetricsRegex,
                    percolateNotifier);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

    private final Clock clock;
    private final String prefix;
    private final URL url;
    private final String index;
    private final int bulkSize;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter writer;
    private Pattern percolateMetricsRegex;
    private Notifier notifier;

    public ElasticsearchReporter(MetricRegistry registry, String host,
                                 int port, String index, int bulkSize, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit,
                                 MetricFilter filter, String percolateMetricsRegex, Notifier percolateNotifier) throws MalformedURLException {
        super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);
        this.host = host;
        this.port = port;
        this.url = new URL("http://" + host + ":" + port  + "/_bulk" );
        this.index = index;
        this.bulkSize = bulkSize;
        this.clock = clock;
        this.prefix = prefix;
        if (percolateNotifier != null && percolateMetricsRegex != null) {
            this.percolateMetricsRegex = Pattern.compile(percolateMetricsRegex);
            this.notifier = percolateNotifier;
        }

        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        objectMapper.registerModule(new MetricsElasticsearchModule(rateUnit, durationUnit));
        writer = objectMapper.writer();
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        try {
            HttpURLConnection connection = getUrlConnection(url);
            List<JsonMetric> percolationMetrics = new ArrayList<JsonMetric>();
            AtomicInteger entriesWritten = new AtomicInteger(0);

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                if (entry.getValue().getValue() != null) {
                    JsonMetric jsonMetric = new JsonGauge(prefix(entry.getKey()), timestamp, entry.getValue());
                    connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                    addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
                }
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                JsonCounter jsonMetric = new JsonCounter(prefix(entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                JsonHistogram jsonMetric = new JsonHistogram(prefix(entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                JsonMeter jsonMetric = new JsonMeter(prefix(entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                JsonTimer jsonMetric = new JsonTimer(prefix(entry.getKey()), timestamp, entry.getValue());
                connection = writeJsonMetricAndRecreateConnectionIfNeeded(jsonMetric, connection, entriesWritten);
                addJsonMetricToPercolationIfMatching(jsonMetric, percolationMetrics);
            }

            closeConnection(connection);

            if (percolationMetrics.size() > 0 && notifier != null) {
                for (JsonMetric jsonMetric : percolationMetrics) {
                    List<String> matches = getPercolationMatches(jsonMetric);
                    System.out.println(String.format("Matches for metric %s: %s", jsonMetric.name(), matches));
                    for (String match : matches) {
                        notifier.notify(jsonMetric, match);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Couldnt report to elasticsearch server", e);
        }
    }

    private List<String> getPercolationMatches(JsonMetric jsonMetric) throws IOException {
        URL percolationUrl = new URL("http://" + host + ":" + port  + "/" + index + "/" + jsonMetric.type() + "/_percolate");
        HttpURLConnection connection = getUrlConnection(percolationUrl);

        Map<String, Object> data = new HashMap<String, Object>(1);
        data.put("doc", jsonMetric);
        objectMapper.writeValue(connection.getOutputStream(), data);
        closeConnection(connection);

        if (connection.getResponseCode() != 200) {
            throw new RuntimeException("Error percolating " + jsonMetric);
        }

        Map<String, Object> input = objectMapper.readValue(connection.getInputStream(), Map.class);
        if (input.containsKey("matches")) {
            return (List<String>) input.get("matches");
        }

        return Collections.EMPTY_LIST;
    }

    private void addJsonMetricToPercolationIfMatching(JsonMetric jsonMetric, List<JsonMetric> percolationMetrics) {
        if (percolateMetricsRegex != null && percolateMetricsRegex.matcher(jsonMetric.name()).matches()) {
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

    private HttpURLConnection createNewConnectionIfBulkSizeReached(HttpURLConnection connection, int entriesWritten) throws IOException {
        if (entriesWritten % bulkSize == 0) {
            closeConnection(connection);
            return getUrlConnection(url);
        }

        return connection;
    }

    private void writeJsonMetric(JsonMetric jsonMetric, ObjectWriter writer, OutputStream out) throws IOException {
        writer.writeValue(out, new BulkIndexOperationHeader(index, jsonMetric.type()));
        out.write("\n".getBytes());
        writer.writeValue(out, jsonMetric);
        out.write("\n".getBytes());

        out.flush();
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    public HttpURLConnection getUrlConnection(URL url) throws IOException {
        HttpURLConnection connection = ( HttpURLConnection ) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setUseCaches(false);
        connection.setDoOutput(true);
        connection.connect();
        return connection;
    }
}
