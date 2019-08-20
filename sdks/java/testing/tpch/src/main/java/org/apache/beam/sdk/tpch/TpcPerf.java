/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.tpch;

import java.util.NoSuchElementException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.LoggerFactory;

/** perf profile. */
public class TpcPerf {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TpcPerf.class);

  public long getCounterMetric(
      PipelineResult result, String namespace, String name, long defaultValue) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    Iterable<MetricResult<Long>> counters = metrics.getCounters();
    try {
      MetricResult<Long> metricResult = counters.iterator().next();
      return metricResult.getAttempted();
    } catch (NoSuchElementException e) {
      LOG.error("Failed to get metric {}, from namespace {}", name, namespace);
    }
    return defaultValue;
  }

  public long getDistributionMetric(
      PipelineResult result,
      String namespace,
      String name,
      DistributionType distType,
      long defaultValue) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    Iterable<MetricResult<DistributionResult>> distributions = metrics.getDistributions();
    try {
      MetricResult<DistributionResult> distributionResult = distributions.iterator().next();
      switch (distType) {
        case MIN:
          return distributionResult.getAttempted().getMin();
        case MAX:
          return distributionResult.getAttempted().getMax();
        default:
          return defaultValue;
      }
    } catch (NoSuchElementException e) {
      LOG.error("Failed to get distribution metric {} for namespace {}", name, namespace);
    }
    return defaultValue;
  }

  /** distribution type. */
  public enum DistributionType {
    MIN,
    MAX
  }

  public TpcPerf() {}
}
