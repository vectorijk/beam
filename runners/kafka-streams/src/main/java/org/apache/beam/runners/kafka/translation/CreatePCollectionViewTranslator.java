package org.apache.beam.runners.kafka.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.View;

class CreatePCollectionViewTranslator<ElemT, ViewT>
    implements TransformTranslator<View.CreatePCollectionView<ElemT, ViewT>> {
  @Override
  public void translate(View.CreatePCollectionView<ElemT, ViewT> transform,
      TransformHierarchy.Node node, TranslationContext ctx) {
  }
}
