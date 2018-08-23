package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadTranslator<T> implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {
  @Override
  public void translate(
      Read.Bounded<T> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    BoundedSource<T> boundedSource = transform.getSource();
    BoundedSourceWrapper<T> sourceWrapper =
            new BoundedSourceWrapper<>(boundedSource, context.getPipelineOptions());
    JavaStream<WindowedValue<T>> sourceStream = context.getSourceStream(sourceWrapper);

    context.setOutputStream(context.getOutput(), sourceStream);
  }
}
