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
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class KafkaStreamsRunnerTest {
  @Test
  public void testParDoChaining() {
    Pipeline p = Pipeline.create();
    long numElements = 1000;
    PCollection<Long> input = p.apply("generator", GenerateSequence.from(0).to(numElements));

    input.apply(Sum.longsGlobally());
    //            .apply(Count.globally());
    //    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(numElements);

    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);

    //TODO: should not set runner below
    options.setRunner(KafkaStreamsRunner.class);
    options.setApplicationId("beam-ks-runner");

    KafkaStreamsRunner.fromOptions(options).run(p);
  }
}
