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

package org.apache.beam.runners.kafka;

import org.apache.beam.runners.kafka.translation.KafkaStreamsPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the {@link Pipeline} into an equivalent
 * Kafka Streams plan.
 */
public class KafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

  private final KafkaStreamsPipelineOptions options;

  public KafkaStreamsRunner(KafkaStreamsPipelineOptions options) {
    this.options = options;
  }

  public static KafkaStreamsRunner fromOptions(PipelineOptions options) {
    final KafkaStreamsPipelineOptions kStreamsOptions =
        PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, options);
    return new KafkaStreamsRunner(kStreamsOptions);
  }

  @Override
  public KafkaStreamsPipelineResult run(Pipeline pipeline) {
    InternalTopologyBuilder tb = new InternalTopologyBuilder();

    // Add a dummy source for use in special cases (TestStream, empty flatten)
//    final PValue dummySource = pipeline.apply("Dummy Input Source", Create.of("dummy"));

    //    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);

    LOG.info("before translation.");
    KafkaStreamsPipelineTranslator.translator(pipeline, options, tb);

    tb.build();
    LOG.info("Topology: " + tb.toString());
    return null;
  }
}
