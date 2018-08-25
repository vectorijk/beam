package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;

class FlattenPCollectionsTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translate(
      Flatten.PCollections<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {

  }
}
