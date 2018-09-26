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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class FlattenPCollectionsTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translate(
      Flatten.PCollections<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    final PValue output = ctx.getOutput();

    final List<KStream<?, ?>> inputStreams = new ArrayList<>();

    // TODO: if inputs is empty

    for (PValue pv : node.getInputs().values()) {

      KStream<T, T> ks = (KStream<T, T>) ctx.getKStream(pv);

      inputStreams.add(ks);
    }

    KStream<T, T> merged = null;

    for (KStream<?, ?> ks: inputStreams) {
      merged = merged.merge((KStream<T, T>) ks);
    }

    ctx.registerKStream(ctx.getOutput(), merged);
    //    ctx.registerKStream();
  }
}
