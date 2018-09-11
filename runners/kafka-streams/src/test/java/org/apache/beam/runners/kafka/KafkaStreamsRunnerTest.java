package org.apache.beam.runners.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class KafkaStreamsRunnerTest {
  @Test
  public void testParDoChaining() {
    Pipeline p = Pipeline.create();
    long numElements = 1000;
    PCollection<Long> input = p.apply("generator", GenerateSequence.from(0).to(numElements));

    input.apply(Sum.longsGlobally());
    //            .apply(Count.globally());
    //    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(numElements);

    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);

    //TODO: should not set runner below
    options.setRunner(KafkaStreamsRunner.class);

    KafkaStreamsRunner.fromOptions(options).run(p);
  }
}
