package org.apache.beam.runners.kafka;

/**
 * A {@link PipelineRunner} that executes the operations in the {@link Pipeline} into an equivalent
 * Kafka Streams plan.
 */
public class KafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {
}
