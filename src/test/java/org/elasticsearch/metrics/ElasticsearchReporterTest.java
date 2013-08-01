package org.elasticsearch.metrics;

import com.codahale.metrics.*;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.metrics.percolation.Notifier;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class ElasticsearchReporterTest {

    private static Node node;
    private static Client client;
    private ElasticsearchReporter elasticsearchReporter;
    private MetricRegistry registry = new MetricRegistry();
    private String index = RandomStringGenerator.randomAlphabetic(12).toLowerCase();
    private String prefix = RandomStringGenerator.randomAlphabetic(12).toLowerCase();

    @BeforeClass
    public static void startElasticsearch() {
        Settings settings = ImmutableSettings.settingsBuilder().put("http.port", "9999").put("cluster.name", "foo").build();
        node = nodeBuilder().settings(settings).node().start();
        client = node.client();
    }

    @AfterClass
    public static void stopElasticsearch() {
        node.close();
    }

    @Before
    public void clearData() throws IOException {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate(index).setSettings(
                jsonBuilder()
                .startObject()
                    .field("index.number_of_replicas", "0")
                    .field("index.number_of_shards", "1")
                .endObject()
        ).execute().actionGet();
        client.admin().cluster().prepareHealth(index).setWaitForGreenStatus().execute().actionGet();

        List<String> types = Lists.newArrayList("counter", "gauge", "histogram", "meter", "timer");
        for (String type : types) {
            client.admin().indices().preparePutMapping(index).setType(type).setSource(
                jsonBuilder().startObject().startObject(type).startObject("properties")
                        //.startObject("timestamp").field("type", "date").endObject()
                        .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
            .execute().actionGet();
        }

        client.admin().cluster().prepareHealth(index).setWaitForGreenStatus().execute().actionGet();

        elasticsearchReporter = createElasticsearchReporterBuilder().build();
    }

    @Test
    public void testCounter() throws Exception {
        final Counter evictions = registry.counter(name("test", "cache-evictions"));
        evictions.inc(25);
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "count", 25);
        assertKey(hit, "name", prefix + ".test.cache-evictions");
    }

    @Test
    public void testHistogram() {
        final Histogram histogram = registry.histogram(name("foo", "bar"));
        histogram.update(20);
        histogram.update(40);
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("histogram").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 2);
        assertKey(hit, "max", 40);
        assertKey(hit, "min", 20);
        assertKey(hit, "mean", 30.0);
    }

    @Test
    public void testMeter() {
        final Meter meter = registry.meter(name("foo", "bar"));
        meter.mark(10);
        meter.mark(20);
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("meter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 30);
    }

    @Test
    public void testTimer() throws Exception {
        final Timer timer = registry.timer(name("foo", "bar"));
        final Timer.Context timerContext = timer.time();
        Thread.sleep(200);
        timerContext.stop();
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("timer").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 1);
    }

    @Test
    public void testGauge() throws Exception {
        registry.register(name("foo", "bar"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 1234;
            }
        });
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("gauge").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "value", 1234);
    }

    @Test
    public void testThatBulkIndexingWorks() {
        for (int i = 0 ; i < 2020; i++) {
            final Counter evictions = registry.counter(name("foo", "bar", String.valueOf(i)));
            evictions.inc(i);
        }
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(2020l));
    }

    @Test
    public void testThatPercolationNotificationWorks() throws IOException, InterruptedException {
        SimpleNotifier notifier = new SimpleNotifier();

        elasticsearchReporter = createElasticsearchReporterBuilder()
                .percolateMetrics(prefix + ".foo")
                .percolateNotifier(notifier)
            .build();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", prefix + ".foo"))
                .must(QueryBuilders.rangeQuery("count").from(20));
        String json = String.format("{ \"query\" : %s }", queryBuilder.buildAsBytes().toUtf8());
        client.prepareIndex("_percolator", index, "myName").setRefresh(true).setSource(json).execute().actionGet();

        // TODO: WTF IS WRONG HERE?! Removing this line results in a breaking test as if percolation
        // TODO: needs somehow to be initialized
        client.preparePercolate(index, "counter").setSource(String.format("{\"doc\":{ \"foo\" : \"bar\", \"count\":\"19\"}}")).execute().actionGet();
        client.preparePercolate(index, "counter").setSource(String.format("{\"doc\":{ \"foo\" : \"bar\", \"count\":\"19\"}}")).execute().actionGet();

        final Counter evictions = registry.counter("foo");
        evictions.inc(19);
        reportAndRefresh();
        assertThat(notifier.metrics.size(), is(0));

        evictions.inc(2);
        reportAndRefresh();

        assertThat(notifier.metrics.size(), is(1));
        assertThat(notifier.metrics, hasKey("myName"));
        assertThat(notifier.metrics.get("myName").name(), is(prefix + ".foo"));
    }

    private class SimpleNotifier implements Notifier {

        public Map<String, JsonMetrics.JsonMetric> metrics = new HashMap<String, JsonMetrics.JsonMetric>();

        @Override
        public void notify(JsonMetrics.JsonMetric jsonMetric, String match) {
            metrics.put(match, jsonMetric);
        }
    }

    private void reportAndRefresh() {
        elasticsearchReporter.report();
        client.admin().cluster().prepareHealth(index).execute().actionGet();
        client.admin().indices().prepareRefresh(index).execute().actionGet();
    }

    private void assertKey(Map<String, Object> hit, String key, double value) {
        assertKey(hit, key, Double.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, int value) {
        assertKey(hit, key, Integer.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, String value) {
        assertThat(hit, hasKey(key));
        assertThat(hit.get(key).toString(), is(value));
    }

    private void assertTimestamp(Map<String, Object> hit) {
        assertThat(hit, hasKey("timestamp"));
        // no exception means everything is cool
        ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(hit.get("timestamp").toString());
    }

    private ElasticsearchReporter.Builder createElasticsearchReporterBuilder() {
        return ElasticsearchReporter.forRegistry(registry)
                .port(9999)
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .index(index);
    }
}
