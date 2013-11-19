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
package org.elasticsearch.metrics.percolation;

import org.elasticsearch.metrics.JsonMetrics.JsonMetric;

/**
 * A notifier interface, which is executed, in case a certain metric is matched on the percolation query
 */
public interface Notifier {

    /**
     *
     * @param jsonMetric The json metric, which matched the percolation
     * @param matchedId  The name of the percolation id, that matched
     */
    void notify(JsonMetric jsonMetric, String matchedId);

}
