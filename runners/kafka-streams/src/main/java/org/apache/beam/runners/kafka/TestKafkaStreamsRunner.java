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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/** Test Runner. */
public class TestKafkaStreamsRunner extends PipelineRunner<PipelineResult> {
  private final KafkaStreamsRunner delegate;

  public TestKafkaStreamsRunner(KafkaStreamsPipelineOptions options) {
    this.delegate = KafkaStreamsRunner.fromOptions(options);
  }

  public static TestKafkaStreamsRunner fromOptions(PipelineOptions options) {
    KafkaStreamsPipelineOptions ksOptions =
        PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, options);
    return new TestKafkaStreamsRunner(ksOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    KafkaStreamsPipelineResult result = delegate.run(pipeline);
    result.waitUntilFinish();
    return result;
  }
}
