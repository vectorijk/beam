package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupByKeyTranslator<K, InputT, OutputT>
    implements TransformTranslator<
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {
  @Override
  public void translate(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {}
}
