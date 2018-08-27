package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadTranslator<T> implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {
  @Override
  public void translate(
      PTransform<PBegin, PCollection<T>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {}
}
