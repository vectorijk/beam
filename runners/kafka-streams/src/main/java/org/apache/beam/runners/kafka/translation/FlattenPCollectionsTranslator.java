package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.ArrayList;
import java.util.List;

class FlattenPCollectionsTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translate(
      Flatten.PCollections<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    final PValue output = ctx.getOutput();

    final List<ProcessorSupplier<?, ?>> inputStreams = new ArrayList<>();

    ctx.registerKStream();
  }
}
