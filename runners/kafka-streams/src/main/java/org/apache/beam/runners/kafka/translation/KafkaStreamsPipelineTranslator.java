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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.kafka.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PipelineTranslator. */
public class KafkaStreamsPipelineTranslator {
  private static final Logger lOG = LoggerFactory.getLogger(KafkaStreamsPipelineTranslator.class);

  private KafkaStreamsPipelineTranslator(TranslationContext ctxt) {
    this.translationContext = ctxt;
  }

  private final TranslationContext translationContext;

  private static final Map<String, TransformTranslator<?>> TRANSLATORS =
      ImmutableMap.<String, TransformTranslator<?>>builder()
          .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator())
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoBoundTranslator())
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionsTranslator())
          .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator())
          .put(
              PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
              new CreatePCollectionViewTranslator())
          .build();

  public static void translator(
      Pipeline pipeline, KafkaStreamsPipelineOptions options, StreamsBuilder streamsBuilder) {
    //                         InternalTopologyBuilder topology,
    //                         Map<PValue, String> idMap,
    //                         PValue naiveSource) {
    final TranslationContext ctxt = new TranslationContext(options, streamsBuilder);
    final TranslationVisitor visitor = new TranslationVisitor(ctxt);
    pipeline.traverseTopologically(visitor);
  }

  /** graph visitor. */
  public static class TranslationVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final Logger lOG = LoggerFactory.getLogger(TranslationVisitor.class);
    private final TranslationContext ctxt;
    private int topologicalId = 0;

    public TranslationVisitor(TranslationContext ctxt) {
      this.ctxt = ctxt;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      lOG.info("Entering composite transform {}", node.getTransform());
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
      lOG.info("Leaving composite transform {}", node.getTransform());
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      lOG.info("Visiting primitive transform {}", node.getTransform());
      final String urn = getUrnForTransform(node.getTransform());

      applyTransform(node.getTransform(), node, TRANSLATORS.get(urn));
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      lOG.info("Visiting value {}", value);
    }

    private <T extends PTransform<?, ?>> void applyTransform(
        T transform, TransformHierarchy.Node node, TransformTranslator<?> translator) {
      lOG.info("Applying transform");
      ctxt.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
      ctxt.setCurrentTopologicalId(topologicalId++);

      @SuppressWarnings("unchecked")
      final TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
      typedTranslator.translate(transform, node, ctxt);

      ctxt.clearCurrentTransform();
    }

    private String getUrnForTransform(PTransform<?, ?> transform) {
      return transform == null ? null : PTransformTranslation.urnForTransformOrNull(transform);
    }

    private boolean canTranslate(String urn) {
      return TRANSLATORS.containsKey(urn);
    }
  }
}
