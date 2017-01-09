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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.metrics.percolation.Notifier;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ElasticsearchReporterTest extends ESIntegTestCase {

    private ElasticsearchReporter elasticsearchReporter;
    private MetricRegistry registry = new MetricRegistry();
    private String index = randomAsciiOfLength(12).toLowerCase();
    private String indexWithDate = String.format("%s-%s-%02d", index, Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH) + 1);
    private String prefix = randomAsciiOfLength(12).toLowerCase();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("http.type", "netty4")
                .put("http.enabled", "true")
                .put("http.port", "9200-9300")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(Netty4Plugin.class);
        plugins.add(PercolatorPlugin.class);
        return plugins;
    }

    @Before
    public void setup() throws IOException {
        elasticsearchReporter = createElasticsearchReporterBuilder().build();
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
        Thread.sleep(200);
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
        assertThat(mapping, hasKey("type"));
        assertThat(mapping.get("type").toString(), is("keyword"));
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
        assertThat(searchResponse.getHits().totalHits(), is(1L));
    }

    @Test
    public void testCounter() throws Exception {
        final Counter evictions = registry.counter(name("test", "cache-evictions"));
        evictions.inc(25);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1L));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "count", 25);
        assertKey(hit, "name", prefix + ".test.cache-evictions");
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testHistogram() {
        final Histogram histogram = registry.histogram(name("foo", "bar"));
        histogram.update(20);
        histogram.update(40);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("histogram").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1L));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 2);
        assertKey(hit, "max", 40);
        assertKey(hit, "min", 20);
        assertKey(hit, "mean", 30.0);
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testMeter() {
        final Meter meter = registry.meter(name("foo", "bar"));
        meter.mark(10);
        meter.mark(20);
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("meter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1L));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 30);
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testTimer() throws Exception {
        final Timer timer = registry.timer(name("foo", "bar"));
        final Timer.Context timerContext = timer.time();
        Thread.sleep(200);
        timerContext.stop();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("timer").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1L));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 1);
        assertKey(hit, "host", "localhost");
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
        assertThat(searchResponse.getHits().totalHits(), is(1L));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "value", 1234);
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testThatSpecifyingSeveralHostsWork() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000", "localhost:" + getPortOfRunningNode()).build();

        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1L));
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
        for (int i = 0; i < 2020; i++) {
            final Counter evictions = registry.counter(name("foo", "bar", String.valueOf(i)));
            evictions.inc(i);
        }
        reportAndRefresh();

        SearchResponse searchResponse = client().prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(2020L));
    }

    @Test
    public void testThatPercolationNotificationWorks() throws IOException, InterruptedException {
        SimpleNotifier notifier = new SimpleNotifier();

        MetricFilter percolationFilter = new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.startsWith(prefix + ".foo");
            }
        };
        elasticsearchReporter = createElasticsearchReporterBuilder()
                .percolationFilter(percolationFilter)
                .percolationNotifier(notifier)
                .build();

        final Counter evictions = registry.counter("foo");
        evictions.inc(18);
        reportAndRefresh();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.rangeQuery("count").gte(20))
                                .must(QueryBuilders.termQuery("name", prefix + ".foo"))
                );
        String json = String.format("{ \"query\" : %s }", queryBuilder);
        client().prepareIndex(indexWithDate, "queries", "myName").setSource(json).execute().actionGet();

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
        assertThat(searchResponse.getHits().totalHits(), is(1L));

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
        assertThat(hit, hasKey(key));
        assertThat(hit.get(key).toString(), is(value));
    }

    private void assertTimestamp(Map<String, Object> hit) {
        assertThat(hit, hasKey("@timestamp"));
        // no exception means everything is cool
        ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(hit.get("@timestamp").toString());
    }

    private int getPortOfRunningNode() {
        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().publishAddress();
        if (transportAddress instanceof InetSocketTransportAddress) {
            return ((InetSocketTransportAddress) transportAddress).address().getPort();
        }
        throw new ElasticsearchException("Could not find running tcp port");
    }

    private ElasticsearchReporter.Builder createElasticsearchReporterBuilder() {
        Map<String, Object> additionalFields = new HashMap<>();
        additionalFields.put("host", "localhost");
        return ElasticsearchReporter.forRegistry(registry)
                .hosts("localhost:" + getPortOfRunningNode())
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .index(index)
                .additionalFields(additionalFields);
    }
}
