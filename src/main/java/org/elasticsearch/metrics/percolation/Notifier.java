package org.elasticsearch.metrics.percolation;

import org.elasticsearch.metrics.JsonMetrics;

public interface Notifier {

    void notify(JsonMetrics.JsonMetric jsonMetric, String match);

}
