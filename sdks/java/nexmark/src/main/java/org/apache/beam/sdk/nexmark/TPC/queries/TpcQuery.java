package org.apache.beam.sdk.nexmark.TPC.queries;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class TpcQuery extends PTransform<PCollection<Row>, PCollection<Row>> {
    final NexmarkConfiguration configuration;
    public final Monitor<Row> resultMonitor;
    //  private final Monitor<Event> endOfStreamMonitor;
//  private final Counter fatalCounter;
    private PTransform<PCollection<Row>, PCollection<Row>> queryTransform;

    public TpcQuery(
            NexmarkConfiguration configuration,
            String name,
            PTransform<PCollection<Row>, PCollection<Row>> queryTransform) {
        super(name);
        this.configuration = configuration;
        //        if (configuration.debug) {
        //            eventMonitor = new Monitor<>(name + ".Events", "event");
        //            resultMonitor = new Monitor<>(name + ".Results", "result");
        //            endOfStreamMonitor = new Monitor<>(name + ".EndOfStream", "end");
        //            fatalCounter = Metrics.counter(name , "fatal");
        //        } else {
        resultMonitor = new Monitor<>(name + ".Results", "result");
//    endOfStreamMonitor = null;
//    fatalCounter = null;
        //        }
        this.queryTransform = queryTransform;
    }

    //    /**
    //     * Implement the actual query. All we know about the result is it has a known encoded size.
    //     */
    //    protected abstract PCollection<Row> applyPrim(PCollection<Event> events);
    protected PCollection<Row> applyPrim(PCollection<Row> table) {
        PCollection<Row> resultRecordsSizes = table.apply(queryTransform);

        return resultRecordsSizes;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> table) {
        //        if (configuration.debug) {
        //            table =
        //                    table
        //                            // Monitor events as they go by.
        //                            .apply(name + ".Monitor", eventMonitor.getTransform())
        //                            // Count each type of event.
        //                            .apply(name + ".Snoop", NexmarkUtils.snoop(name));
        //        }

        if (configuration.cpuDelayMs > 0) {
            // Slow down by pegging one core at 100%.
            table =
                    table.apply(name + ".CpuDelay", NexmarkUtils.cpuDelay(name, configuration.cpuDelayMs));
        }

        if (configuration.diskBusyBytes > 0) {
            // Slow down by forcing bytes to durable store.
            table = table.apply(name + ".DiskBusy", NexmarkUtils.diskBusy(configuration.diskBusyBytes));
        }

        // Run the query.
        PCollection<Row> queryResults = applyPrim(table);

        //        if (configuration.debug) {
        //            // Monitor results as they go by.
        queryResults = queryResults.apply(name + ".Debug", resultMonitor.getTransform());
        //        }

        // Timestamp the query results.
        return queryResults;
    }
}
