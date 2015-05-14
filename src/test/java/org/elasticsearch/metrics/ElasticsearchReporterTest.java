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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.metrics.percolation.Notifier;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class ElasticsearchReporterTest extends ElasticsearchIntegrationTest {

    private ElasticsearchReporter elasticsearchReporter;
    private MetricRegistry registry = new MetricRegistry();
    private String index = randomAsciiOfLength(12).replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
    private String indexWithDate = String.format("%s-%s-%02d", index, Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH)+1);
    private String prefix = randomAsciiOfLength(12).replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
    private String expectedHostname;

    @Before
    public void setup() throws IOException {
        elasticsearchReporter = createElasticsearchReporterBuilder().build();
        expectedHostname = InetAddress.getLocalHost().getHostName();
    }

    @Test
    public void testThatTemplateIsAdded() throws Exception {
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("metrics_template").get();

        assertThat(response.getIndexTemplates(), hasSize(1));
        IndexTemplateMetaData templateData = response.getIndexTemplates().get(0);
        assertThat(templateData.order(), is(0));
        assertThat(templateData.getMappings().get("_default_"), is(notNullValue()));
    }

    @Test
    public void testThatMappingFromTemplateIsApplied() throws Exception {
        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        // somehow the cluster state is not immediately updated... need to check
        Thread.sleep(500);
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setRoutingTable(false)
                .setLocal(false)
                .setNodes(true)
                .setIndices(indexWithDate)
                .execute().actionGet();

        assertThat(clusterStateResponse.getState().getMetaData().getIndices().containsKey(indexWithDate), is(true));
        IndexMetaData indexMetaData = clusterStateResponse.getState().getMetaData().getIndices().get(indexWithDate);
        assertThat(indexMetaData.getMappings().containsKey("counter"), is(true));
        Map<String, Object> properties = getAsMap(indexMetaData.mapping("counter").sourceAsMap(), "properties");
        Map<String, Object> mapping = getAsMap(properties, "name");
        assertThat(mapping, hasKey("index"));
        assertThat(mapping.get("index").toString(), is("not_analyzed"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getAsMap(Map<String, Object> map, String key) {
        assertThat(map, hasKey(key));
        assertThat(map.get(key), instanceOf(Map.class));
        return (Map<String, Object>) map.get(key);
    }

    @Test
    public void testThatTemplateIsNotOverWritten() throws Exception {
        client().admin().indices().preparePutTemplate("metrics_template").setTemplate("foo*").setSettings("{ \"index.number_of_shards\" : \"1\"}").execute().actionGet();
        //client().admin().cluster().prepareHealth().setWaitForGreenStatus();

        elasticsearchReporter = createElasticsearchReporterBuilder().build();

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("metrics_template").get();

        assertThat(response.getIndexTemplates(), hasSize(1));
        IndexTemplateMetaData templateData = response.getIndexTemplates().get(0);
        assertThat(templateData.template(), is("foo*"));
    }

    @Test
    public void testThatTimeBasedIndicesCanBeDisabled() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().indexDateFormat("").build();
        indexWithDate = index;

        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(index).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));
    }

    @Test
    public void testCounter() throws Exception {
        final String counterName = name("test", "cache-evictions");
        final Counter evictions = registry.counter(counterName);
        evictions.inc(25);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, name(prefix, counterName));
        assertKey(hit, "host", expectedHostname);
    }

    @Test
    public void testHistogram() {
        final Histogram histogram = registry.histogram(name("foo", "bar"));
        histogram.update(20);
        histogram.update(40);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("histogram").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKeyAndMap(hit, prefix + ".foo.bar")
        .has("count", 2)
        .has("max", 40)
        .has("min", 20)
        .has("mean", 30.0)
        .has("host", expectedHostname);
    }

    @Test
    public void testMeter() {
        final Meter meter = registry.meter(name("foo", "bar"));
        meter.mark(10);
        meter.mark(20);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("meter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKeyAndMap(hit, prefix + ".foo.bar")
        .has("count", 30)
        .has("host", expectedHostname);
    }

    @Test
    public void testTimer() throws Exception {
        final Timer timer = registry.timer(name("foo", "bar"));
        final Timer.Context timerContext = timer.time();
        Thread.sleep(200);
        timerContext.stop();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("timer").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKeyAndMap(hit, prefix + ".foo.bar")
        .has("count", 1)
        .has("host", expectedHostname);
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

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("gauge").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, prefix + ".foo.bar", 1234);
        assertKey(hit, "host", expectedHostname);
    }

    @Test
    public void testGaugeWithCustomHostname() throws Exception {
        final String fakeHostname = "yankee-doodle";
        final String metric = name("foo", "baz");
        final String outputMetric = name(prefix, metric);

        elasticsearchReporter = createElasticsearchReporterBuilder()
                .withHostname(fakeHostname)
                .build();
        registry.register(metric, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 1234;
            }
        });
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("gauge").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, outputMetric, 1234);
        assertKey(hit, "host", fakeHostname);
    }

    @Test
    public void testThatSpecifyingSeveralHostsWork() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000", "localhost:" + getPortOfRunningNode()).build();

        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));
    }

    @Test
    public void testGracefulFailureIfNoHostIsReachable() throws IOException {
        // if no exception is thrown during the test, we consider it all graceful, as we connected to a dead host
        elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000").build();
        registry.counter(name("test", "cache-evictions")).inc();
        elasticsearchReporter.report();
    }

    @Test
    public void testThatBulkIndexingWorks() {
        for (int i = 0 ; i < 2020; i++) {
            final Counter evictions = registry.counter(name("foo", "bar", String.valueOf(i)));
            evictions.inc(i);
        }
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(2020l));
    }

    @Test
    public void testThatPercolationNotificationWorks() throws IOException, InterruptedException {
        final String counterName = "foo";
        final String fieldName = name(prefix, counterName);
        SimpleNotifier notifier = new SimpleNotifier();

        MetricFilter percolationFilter = new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.startsWith(fieldName);
            }
        };
        elasticsearchReporter = createElasticsearchReporterBuilder()
                .percolationFilter(percolationFilter)
                .percolationNotifier(notifier)
            .build();

        final Counter evictions = registry.counter(counterName);
        evictions.inc(18);
        reportAndRefresh();

        QueryBuilder queryBuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                FilterBuilders.rangeFilter(fieldName).gte(20));
        String json = String.format("{ \"query\" : %s }", queryBuilder.buildAsBytes().toUtf8());
        client().prepareIndex(indexWithDate, ".percolator", "myName").setRefresh(true).setSource(json).execute().actionGet();

        evictions.inc(1);
        reportAndRefresh();
        assertThat(notifier.metrics.size(), is(0));

        evictions.inc(2);
        reportAndRefresh();
        assertThat(notifier.metrics.size(), is(1));
        assertThat(notifier.metrics, hasKey("myName"));
        assertThat(notifier.metrics.get("myName").name(), is(prefix + ".foo"));

        notifier.metrics.clear();
        evictions.dec(2);
        reportAndRefresh();
        assertThat(notifier.metrics.size(), is(0));
    }

    @Test
    public void testThatWronglyConfiguredHostDoesNotLeadToApplicationStop() throws IOException {
        createElasticsearchReporterBuilder().hosts("dafuq/1234").build();
        elasticsearchReporter.report();
    }

    @Test
    public void testThatTimestampFieldnameCanBeConfigured() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().timestampFieldname("myTimeStampField").build();
        registry.counter(name("myMetrics", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertThat(hit, hasKey("myTimeStampField"));
    }

    @Test // issue #6
    public void testThatEmptyMetricsDoNotResultInBrokenBulkRequest() throws Exception {
        long connectionsBeforeReporting = getTotalHttpConnections();
        elasticsearchReporter.report();
        long connectionsAfterReporting = getTotalHttpConnections();

        assertThat(connectionsAfterReporting, is(connectionsBeforeReporting));
    }

    @Test
    public void testDiscoveringClusterMembership() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        cluster.ensureAtLeastNumDataNodes(3);
        final int clusterSize = cluster.size();
        assertTrue("expected at least a 3 node cluster: found "+clusterSize, clusterSize >= 3);

        if (null != elasticsearchReporter) {
            elasticsearchReporter.stop();
        }
        // build a fresh one with our increased cluster size
        elasticsearchReporter = createElasticsearchReporterBuilder().build();

        final Field hostField = ElasticsearchReporter.class.getDeclaredField("hosts");
        hostField.setAccessible(true);
        String[] hosts = (String[]) hostField.get(elasticsearchReporter);
        assertNotNull("Expected .hosts to be non-null", hosts);
        assertEquals(clusterSize, hosts.length);
        elasticsearchReporter.stop();
    }

    private long getTotalHttpConnections() {
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().setHttp(true).get();
        int totalOpenConnections = 0;
        for (NodeStats stats : nodeStats.getNodes()) {
            totalOpenConnections += stats.getHttp().getTotalOpen();
        }
        return totalOpenConnections;
    }

    private class SimpleNotifier implements Notifier {

        public Map<String, JsonMetrics.JsonMetric> metrics = new HashMap<>();

        @Override
        public void notify(JsonMetrics.JsonMetric jsonMetric, String match) {
            metrics.put(match, jsonMetric);
        }
    }

    private void reportAndRefresh() {
        elasticsearchReporter.report();
        client().admin().indices().prepareRefresh(indexWithDate).execute().actionGet();
    }

    private void assertKey(Map<String, Object> hit, String key, double value) {
        assertKey(hit, key, Double.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, int value) {
        assertKey(hit, key, Integer.toString(value));
    }

    private void assertKey(Map<String, Object> hit, String key, String value) {
        assertKey(hit, key);
        assertThat(hit.get(key).toString(), is(value));
    }

    private void assertKey(Map<String, Object> hit, String key) {
        assertThat(hit, hasKey(key));
    }
    private MapChecker assertKeyAndMap(Map<String, Object> hit, String key) {
        assertThat(hit, hasKey(key));
        @SuppressWarnings("unchecked")
        final Map<String, Object> subMap = (Map<String, Object>) hit.get(key);
        return new MapChecker(subMap);
    }

    private void assertTimestamp(Map<String, Object> hit) {
        assertThat(hit, hasKey("@timestamp"));
        // no exception means everything is cool
        ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(hit.get("@timestamp").toString());
    }

    private int getPortOfRunningNode() {
        final InetSocketAddress[] addresses = internalCluster().httpAddresses();
        assertNotNull("Must not return NULL httpAddresses", addresses);
        assertNotEquals("httpAddress must not be empty", 0, addresses.length);
        return addresses[0].getPort();
    }

    private ElasticsearchReporter.Builder createElasticsearchReporterBuilder() {
        Map<String, Object> additionalFields = new HashMap<>();
        additionalFields.put("host", expectedHostname);
        return ElasticsearchReporter.forRegistry(registry)
                .hosts("localhost:" + getPortOfRunningNode())
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .index(index)
                .additionalFields(additionalFields);
    }
    private class MapChecker {
        private final Map<String, Object> theMap;
        private MapChecker(Map<String, Object> theMap) {
            this.theMap = theMap;
        }
        public MapChecker has(String key, double value) {
            assertKey(theMap, key, value);
            return this;
        }
        public MapChecker has(String key, int value) {
            assertKey(theMap, key, value);
            return this;
        }
        public MapChecker has(String key, String value) {
            assertKey(theMap, key, value);
            return this;
        }
    }
}
