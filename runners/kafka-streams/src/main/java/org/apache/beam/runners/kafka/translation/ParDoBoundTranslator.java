package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;

public class ParDoBoundTranslator<InT, OutT>
    implements TransformTranslator<ParDo.MultiOutput<InT, OutT>> {
  @Override
  public void translate(
      ParDo.MultiOutput<InT, OutT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {}
}
