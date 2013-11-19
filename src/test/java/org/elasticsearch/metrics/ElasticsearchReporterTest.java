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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.codahale.metrics.*;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.metrics.percolation.Notifier;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

public class ElasticsearchReporterTest {

    private static final Random random = new Random();
    private static Node node;
    private static Client client;
    private ElasticsearchReporter elasticsearchReporter;
    private MetricRegistry registry = new MetricRegistry();
    private String index = RandomStrings.randomAsciiOfLength(random, 12).toLowerCase();
    private String indexWithDate = String.format("%s-%s-%02d", index, Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH)+1);
    private String prefix = RandomStrings.randomAsciiOfLength(random, 12).toLowerCase();;

    @BeforeClass
    public static void startElasticsearch() {
        Settings settings = ImmutableSettings.settingsBuilder().put("http.port", "9999").put("cluster.name", RandomStrings.randomAsciiOfLength(random, 10).toLowerCase()).build();
        LogConfigurator.configure(settings);
        node = nodeBuilder().settings(settings).node().start();
        client = node.client();
    }

    @AfterClass
    public static void stopElasticsearch() {
        node.close();
    }

    @Before
    public void setup() throws IOException {
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        client.admin().indices().prepareDelete().execute().actionGet();
        try {
            client.admin().indices().prepareDeleteTemplate("metrics_template").execute().actionGet();
        } catch (IndexTemplateMissingException e) {} // ignore
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        elasticsearchReporter = createElasticsearchReporterBuilder().build();
    }

    @Test
    public void testThatTemplateIsAdded() throws Exception {
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState()
                .setFilterRoutingTable(true)
                .setFilterNodes(true)
                .setFilterIndexTemplates("metrics_template").execute().actionGet();

        assertThat(clusterStateResponse.getState().metaData().templates().size(), is(1));
        IndexTemplateMetaData templateData = clusterStateResponse.getState().metaData().templates().get("metrics_template");
        assertThat(templateData.order(), is(0));
        assertThat(templateData.getMappings().get("_default_"), is(notNullValue()));
    }

    @Test
    public void testThatMappingFromTemplateIsApplied() throws Exception {
        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        // somehow the cluster state is not immediately updated... need to check
        Thread.sleep(200);
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setFilterRoutingTable(true)
                .setLocal(false)
                .setFilterNodes(true)
                .setFilterIndices(indexWithDate)
                .execute().actionGet();

        assertThat(clusterStateResponse.getState().getMetaData().getIndices(), hasKey(indexWithDate));
        IndexMetaData indexMetaData = clusterStateResponse.getState().getMetaData().getIndices().get(indexWithDate);
        assertThat(indexMetaData.getMappings(), hasKey("counter"));
        Map<String, Object> properties = getAsMap(indexMetaData.mapping("counter").sourceAsMap(), "properties");
        Map<String, Object> mapping = getAsMap(properties, "name");
        assertThat(mapping, hasKey("index"));
        assertThat(mapping.get("index").toString(), is("not_analyzed"));
    }

    private Map<String, Object> getAsMap(Map<String, Object> map, String key) {
        assertThat(map, hasKey(key));
        assertThat(map.get(key), instanceOf(Map.class));
        return (Map<String, Object>) map.get(key);
    }

    @Test
    public void testThatTemplateIsNotOverWritten() throws Exception {
        client.admin().indices().preparePutTemplate("metrics_template").setTemplate("foo*").setSettings(String.format("{ \"index.number_of_shards\" : \"1\"}")).execute().actionGet();
        //client.admin().cluster().prepareHealth().setWaitForGreenStatus();

        elasticsearchReporter = createElasticsearchReporterBuilder().build();

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState()
                .setLocal(false)
                .setFilterRoutingTable(true)
                .setFilterNodes(true)
                .setFilterIndexTemplates("metrics_template").execute().actionGet();

        assertThat(clusterStateResponse.getState().metaData().templates().size(), is(1));
        IndexTemplateMetaData templateData = clusterStateResponse.getState().metaData().templates().get("metrics_template");
        assertThat(templateData.template(), is("foo*"));
    }

    @Test
    public void testThatTimeBasedIndicesCanBeDisabled() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().indexDateFormat("").build();
        indexWithDate = index;

        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(index).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));
    }

    @Test
    public void testCounter() throws Exception {
        final Counter evictions = registry.counter(name("test", "cache-evictions"));
        evictions.inc(25);
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
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

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("histogram").execute().actionGet();
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

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("meter").execute().actionGet();
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

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("timer").execute().actionGet();
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

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("gauge").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).sourceAsMap();
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "value", 1234);
    }

    @Test
    public void testThatSpecifyingSeveralHostsWork() throws Exception {
        elasticsearchReporter = createElasticsearchReporterBuilder().hosts("localhost:10000", "localhost:9999").build();

        registry.counter(name("test", "cache-evictions")).inc();
        reportAndRefresh();

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
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

        SearchResponse searchResponse = client.prepareSearch(indexWithDate).setTypes("counter").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), is(2020l));
    }

    @Test
    public void testThatPercolationNotificationWorks() throws IOException, InterruptedException {
        SimpleNotifier notifier = new SimpleNotifier();

        elasticsearchReporter = createElasticsearchReporterBuilder()
                .percolateMetrics(prefix + ".foo")
                .percolateNotifier(notifier)
            .build();

        final Counter evictions = registry.counter("foo");
        evictions.inc(18);
        reportAndRefresh();

        QueryBuilder queryBuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                FilterBuilders.andFilter(FilterBuilders.rangeFilter("count").gte(20), FilterBuilders.termFilter("name", prefix + ".foo")));
        String json = String.format("{ \"query\" : %s }", queryBuilder.buildAsBytes().toUtf8());
        client.prepareIndex("_percolator", indexWithDate, "myName").setRefresh(true).setSource(json).execute().actionGet();

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

    private class SimpleNotifier implements Notifier {

        public Map<String, JsonMetrics.JsonMetric> metrics = new HashMap<String, JsonMetrics.JsonMetric>();

        @Override
        public void notify(JsonMetrics.JsonMetric jsonMetric, String match) {
            metrics.put(match, jsonMetric);
        }
    }

    private void reportAndRefresh() {
        elasticsearchReporter.report();
        client.admin().indices().prepareRefresh(indexWithDate).execute().actionGet();
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
                .hosts("localhost:9999")
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .index(index);
    }
}
