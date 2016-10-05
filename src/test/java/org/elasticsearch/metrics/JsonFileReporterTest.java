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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.codahale.metrics.MetricRegistry.name;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.hasKey;

public class JsonFileReporterTest extends ESIntegTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private JsonFileReporter reporter;
    private MetricRegistry registry = new MetricRegistry();
    private String file = randomAsciiOfLength(12).toLowerCase();
    private String fileExt = randomAsciiOfLength(3).toLowerCase();
    private String fileWithDate = String.format("%s-%s-%02d.%s", file, Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH) + 1, fileExt);
    private String prefix = randomAsciiOfLength(12).toLowerCase();
    private File directory;

    @Before
    public void setup() throws IOException {
        directory = temporaryFolder.newFolder(randomAsciiOfLength(12).toLowerCase());
        reporter = createJsonFileReporterBuilder().build();
    }

    private Map<String, Object> parseLine(String line) throws IOException {
        return JsonXContent.jsonXContent.createParser(line).map();
    }

    @Test
    public void testThatTimeBasedIndicesCanBeDisabled() throws Exception {
        reporter = createJsonFileReporterBuilder().fileDateFormat("").build();
        fileWithDate = file + "." + fileExt;

        registry.counter(name("test", "cache-evictions")).inc();
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"counter\"");
        assertThat(searchResponse.size(), is(1));
    }

    @Test
    public void testCounter() throws Exception {
        final Counter evictions = registry.counter(name("test", "cache-evictions"));
        evictions.inc(25);
        report();

        // "type":"counter"
        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"counter\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
        assertTimestamp(hit);
        assertKey(hit, "count", 25);
        assertKey(hit, "name", prefix + ".test.cache-evictions");
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testHistogram() throws IOException {
        final Histogram histogram = registry.histogram(name("foo", "bar"));
        histogram.update(20);
        histogram.update(40);
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"histogram\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "count", 2);
        assertKey(hit, "max", 40);
        assertKey(hit, "min", 20);
        assertKey(hit, "mean", 30.0);
        assertKey(hit, "host", "localhost");
    }

    @Test
    public void testMeter() throws IOException {
        final Meter meter = registry.meter(name("foo", "bar"));
        meter.mark(10);
        meter.mark(20);
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"meter\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
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
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"timer\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
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
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"gauge\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
        assertTimestamp(hit);
        assertKey(hit, "name", prefix + ".foo.bar");
        assertKey(hit, "value", 1234);
        assertKey(hit, "host", "localhost");
    }


    @Test
    public void testThatTimestampFieldnameCanBeConfigured() throws Exception {
        reporter = createJsonFileReporterBuilder().timestampFieldname("myTimeStampField").build();
        registry.counter(name("myMetrics", "cache-evictions")).inc();
        report();

        List<String> searchResponse = searchLineInFile(fileWithDate, "\"type\":\"counter\"");
        assertThat(searchResponse.size(), is(1));

        Map<String, Object> hit = parseLine(searchResponse.get(0));
        assertThat(hit, hasKey("myTimeStampField"));
    }

    private void report() {
        reporter.report();
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

    private JsonFileReporter.Builder createJsonFileReporterBuilder() throws IOException {
        Map<String, Object> additionalFields = new HashMap<>();
        additionalFields.put("host", "localhost");
        return JsonFileReporter.forRegistry(registry)
                .directory(directory)
                .file(file)
                .fileExtension(fileExt)
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .additionalFields(additionalFields);
    }

    private List<String> searchLineInFile(String fileName, String lineRegex) throws IOException {
        Pattern pattern = Pattern.compile(lineRegex);
        List<String> lines = new ArrayList<>();
        File file = new File(directory, fileName);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (pattern.matcher(line).find()) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }
}
