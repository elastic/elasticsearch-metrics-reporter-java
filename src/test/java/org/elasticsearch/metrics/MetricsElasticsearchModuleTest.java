package org.elasticsearch.metrics;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.metrics.JsonMetrics.JsonGauge;
import org.elasticsearch.metrics.JsonMetrics.JsonMetric;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.*;

/**
 * Tests if value is an array.
 * @author static-max
 *
 */
public class ElasticsearchReporterTestTest {
	
	@Test
	public void test() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new MetricsElasticsearchModule(TimeUnit.MINUTES, TimeUnit.MINUTES, "@timestamp", null));
		
		Gauge<String> gaugeString = new Gauge<String>() {
			@Override
			public String getValue() {
				return "STRING VALUE";
			}
		};
		
		/**
		 * Used for deadlocks in metrics-jvm ThreadStatesGaugeSet.class.
		 */
		Gauge<Set<String>> gaugeStringSet = new Gauge<Set<String>>() {
			@Override
			public Set<String> getValue() {
				HashSet<String> testSet = new HashSet<>();
				testSet.add("1");
				testSet.add("2");
				testSet.add("3");
				return testSet;
			}
		};
		
		
		JsonMetric<?> jsonMetricString = new JsonGauge("string", Long.MAX_VALUE, gaugeString);
		JsonMetric<?> jsonMetricStringSet = new JsonGauge("string", Long.MAX_VALUE, gaugeStringSet);
		
		JsonNode stringNode = objectMapper.valueToTree(jsonMetricString);
		assertTrue(stringNode.get("value").isTextual());
		assertFalse(stringNode.get("value").isNumber());
		
		JsonNode stringSetNode = objectMapper.valueToTree(jsonMetricStringSet);
		assertTrue(stringSetNode.get("value").isArray());
		assertTrue(stringSetNode.get("value").get(0).isTextual());
		assertTrue(stringSetNode.get("value").get(1).isTextual());
		assertTrue(stringSetNode.get("value").get(2).isTextual());
	}

}