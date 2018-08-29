package org.apache.beam.runners.kafka.translation;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class DoFn<K, V> implements ProcessorSupplier<K, V> {

    @Override
    public Processor<K, V> get() {
        return null;
    }
}
