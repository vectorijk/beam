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
package org.apache.beam.runners.tez.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.tez.TezPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TezPipelineTranslator} translates {@link Pipeline} objects into Tez logical plan {@link
 * DAG}.
 */
public class TezPipelineTranslator implements Pipeline.PipelineVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(TezPipelineTranslator.class);

  /**
   * A map from {@link PTransform} subclass to the corresponding {@link TransformTranslator} to use
   * to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, TransformTranslator> transformTranslators =
      new HashMap<>();

  private static final Map<Class<? extends PTransform>, TransformTranslator>
      compositeTransformTranslators = new HashMap<>();

  private final TranslationContext translationContext;

  private Pipeline pipeline = null;

  static {
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoTranslator<>());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator<>());
    registerTransformTranslator(Window.Assign.class, new WindowAssignTranslator<>());
    registerTransformTranslator(Read.Bounded.class, new ReadBoundedTranslator<>());
    registerTransformTranslator(Flatten.PCollections.class, new FlattenPCollectionTranslator<>());
    registerTransformTranslator(
        View.CreatePCollectionView.class, new ViewCreatePCollectionViewTranslator<>());
    registerCompositeTransformTranslator(WriteFiles.class, new WriteFilesTranslator());
  }

  public TezPipelineTranslator(TezPipelineOptions options, TezConfiguration config) {
    translationContext = new TranslationContext(options, config);
  }

  public void translate(Pipeline pipeline, DAG dag) {
    LOG.error("translate pipeline...");
    System.out.println("translate pipeline...");
    this.pipeline = pipeline;
    pipeline.traverseTopologically(this);

    translationContext.populateDAG(dag);
  }

  /**
   * Main visitor method called on each {@link PTransform} to transform them to Tez objects.
   *
   * @param node Pipeline node containing {@link PTransform} to be translated.
   */
  @Override
  public void visitPrimitiveTransform(Node node) {
    LOG.warn("visiting transform {}", node.getTransform());
    System.out.println("visiting transform " + node.getTransform());
    PTransform transform = node.getTransform();
    TransformTranslator translator = transformTranslators.get(transform.getClass());
    if (translator == null) {
      throw new UnsupportedOperationException("no translator registered for " + transform);
    }
    System.out.println("visiting transform full name: " + node.getFullName());
    translationContext.setCurrentTransform(node, this.pipeline);
    translator.translate(transform, translationContext);
  }

  @Override
  public void enterPipeline(Pipeline p) {
    LOG.error("enter pipeline...");
    System.out.println("enter pipeline...");
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    LOG.warn("entering composite transform {}", node.getTransform());
    System.out.println("entering composite transform " + node.getTransform());
    PTransform transform = node.getTransform();
    if (transform != null) {
      TransformTranslator translator = compositeTransformTranslators.get(transform.getClass());
      if (translator != null) {
        translationContext.setCurrentTransform(node, this.pipeline);
        translator.translate(transform, translationContext);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(Node node) {
    LOG.warn("leaving composite transform {}", node.getTransform());
  }

  @Override
  public void visitValue(PValue value, Node producer) {
    LOG.warn("visiting value {}", value);
  }

  @Override
  public void leavePipeline(Pipeline pipeline) {
    LOG.warn("leave pipeline...");
  }

  /**
   * Records that instances of the specified PTransform class should be translated by default by the
   * corresponding {@link TransformTranslator}.
   */
  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }

  private static <TransformT extends PTransform> void registerCompositeTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (compositeTransformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }
}
