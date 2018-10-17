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

package org.apache.beam.runners.kafka.translation;

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.kafka.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/** Helper. */
public class TranslationContext {
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private Map<PValue, KStream<?, ?>> kStreamsMap = new HashMap<>();
  //  private Topology topology;
  private StreamsBuilder streamsBuilder;

  public TranslationContext(KafkaStreamsPipelineOptions options, StreamsBuilder streamsBuilder) {
    this.pipelineOptions = options;
    this.streamsBuilder = streamsBuilder;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  public <KT, VT> void registerKStream(PValue pvalue, KStream<KT, VT> kStream) {
    if (kStreamsMap.containsKey(pvalue)) {
      throw new IllegalArgumentException("KStream already registered for pvalue: " + pvalue);
    }

    kStreamsMap.put(pvalue, kStream);
  }

  public <KT, VT> KStream<KT, VT> getKStream(PValue pvalue) {
    final KStream<KT, VT> kStream = (KStream<KT, VT>) kStreamsMap.get(pvalue);

    if (null == kStream) {
      throw new IllegalArgumentException("No KStream registered for pvalue: " + pvalue);
    }

    return kStream;
  }

  public Map<TupleTag<?>, PValue> getInputs() {
    return getCurrentTransform().getInputs();
  }

  public PValue getInput() {
    return Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(getCurrentTransform()));
  }

  public Map<TupleTag<?>, PValue> getOutputs() {
    return getCurrentTransform().getOutputs();
  }

  public PValue getOutput() {
    return Iterables.getOnlyElement(getCurrentTransform().getOutputs().values());
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  //  public void doRegisterKS() {
  //    streamsBuilder.stream();
  //  }

  public void setCurrentTopologicalId(int topologicalId) {}

  public void clearCurrentTransform() {}
}
