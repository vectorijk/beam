package org.apache.beam.runners.kafka;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class KafkaStreamsRunnerRegistrarTest {
  @Test
  public void testFullName() {
    String[] args = new String[] {String.format("--runner=%s", KafkaStreamsRunner.class.getName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), KafkaStreamsRunner.class);
  }

  @Test
  public void testClassName() {
    String[] args =
        new String[] {String.format("--runner=%s", KafkaStreamsRunner.class.getSimpleName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), KafkaStreamsRunner.class);
  }

  @Test
  public void testOptions() {
    assertEquals(
        ImmutableList.of(KafkaStreamsPipelineOptions.class),
        new KafkaStreamsRunnerRegistrar.Options().getPipelineOptions());
  }
}
