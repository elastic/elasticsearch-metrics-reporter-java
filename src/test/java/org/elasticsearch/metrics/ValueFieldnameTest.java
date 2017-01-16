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
 * Tests if the fieldname for value gets correctly named when dynamicValueFieldname() is set to <code>true</code>.
 * @author static-max
 *
 */
public class ValueFieldnameTest {
	
	@Test
	public void test() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new MetricsElasticsearchModule(TimeUnit.MINUTES, TimeUnit.MINUTES, "@timestamp", null, true));
		
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
		
		Gauge<Long> gaugeLong = new Gauge<Long>() {
			
			@Override
			public Long getValue() {
				return Long.MAX_VALUE;
			}
		};
		
		Gauge<Integer> gaugeInteger = new Gauge<Integer>() {
			
			@Override
			public Integer getValue() {
				return 123;
			}
		};
		
		Gauge<Double> gaugeDouble = new Gauge<Double>() {
			
			@Override
			public Double getValue() {
				return 1.23d;
			}
		};
		
		Gauge<Float> gaugeFloat = new Gauge<Float>() {
			
			@Override
			public Float getValue() {
				return 321.0f;
			}
		};
		
		Gauge<Boolean> gaugeBoolean = new Gauge<Boolean>() {
			
			@Override
			public Boolean getValue() {
				return true;
			}
		};
		
		JsonMetric<?> jsonMetricString = new JsonGauge("string", Long.MAX_VALUE, gaugeString);
		JsonMetric<?> jsonMetricLong = new JsonGauge("long", Long.MAX_VALUE, gaugeLong);
		JsonMetric<?> jsonMetricInteger = new JsonGauge("integer", Long.MAX_VALUE, gaugeInteger);
		JsonMetric<?> jsonMetricDouble = new JsonGauge("double", Long.MAX_VALUE, gaugeDouble);
		JsonMetric<?> jsonMetricFloat = new JsonGauge("float", Long.MAX_VALUE, gaugeFloat);
		JsonMetric<?> jsonMetricBoolean = new JsonGauge("boolean", Long.MAX_VALUE, gaugeBoolean);
        		
		JsonNode stringNode = objectMapper.valueToTree(jsonMetricString);
		assertTrue(stringNode.get("value_string").isTextual());
		assertFalse(stringNode.get("value_string").isNumber());
		
		JsonNode longNode = objectMapper.valueToTree(jsonMetricLong);
		assertTrue(longNode.get("value_long").isNumber());
		assertFalse(longNode.get("value_long").isTextual());
		
		JsonNode integerNode = objectMapper.valueToTree(jsonMetricInteger);
		assertTrue(integerNode.get("value_long").isNumber());
		assertFalse(integerNode.get("value_long").isTextual());
		
		JsonNode doubleNode = objectMapper.valueToTree(jsonMetricDouble);
		assertTrue(doubleNode.get("value_double").isNumber());
		assertTrue(doubleNode.get("value_double").isDouble());
		
		JsonNode floatNode = objectMapper.valueToTree(jsonMetricFloat);
		assertTrue(floatNode.get("value_double").isNumber());
		assertTrue(floatNode.get("value_double").isDouble());
		
		JsonNode booleanNode = objectMapper.valueToTree(jsonMetricBoolean);
		assertTrue(booleanNode.get("value_boolean").isBoolean());
		assertFalse(booleanNode.get("value_boolean").isTextual());		
	}

}

