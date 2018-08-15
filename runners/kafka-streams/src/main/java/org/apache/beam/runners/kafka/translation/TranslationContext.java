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
import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.kafka.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Helper. */
public class TranslationContext {
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private AppliedPTransform<?, ?, ?> currentTransform;

  public TranslationContext(KafkaStreamsPipelineOptions options) {
    this.pipelineOptions = options;
  }

  public void setCurrentTransform(TransformHierarchy.Node treeNode, Pipeline pipeline) {
    this.currentTransform = treeNode.toAppliedPTransform(pipeline);
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
}
