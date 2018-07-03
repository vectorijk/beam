package org.apache.beam.sdk.nexmark.TPC.queries.H;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;

public class HQuery1 extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final PTransform<PInput, PCollection<Row>> QUERY =
      SqlTransform.query("SELECT * FROM PCOLLECTION");

  public HQuery1() {
    super("TpcHquery1");
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> table) {
    //        RowCoder bidRecordCoder = getBidRowCoder();

    //        PCollection<Row> bidEventsRows = table;
    //                .apply(getName() + ".ToRow", ToRow.parDo())
    //                .setCoder(bidRecordCoder);

    PCollection<Row> queryResultsRows = table.apply(QUERY);

    return queryResultsRows;
  }

  private PTransform<? super PCollection<Row>, PCollection<Row>> logBytesMetric(
      final RowCoder coder) {

    return ParDo.of(
        new DoFn<Row, Row>() {
          private final Counter bytesMetric = Metrics.counter(name, "bytes");

          @ProcessElement
          public void processElement(ProcessContext c) throws CoderException, IOException {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            coder.encode(c.element(), outStream, Coder.Context.OUTER);
            byte[] byteArray = outStream.toByteArray();
            bytesMetric.inc((long) byteArray.length);
            ByteArrayInputStream inStream = new ByteArrayInputStream(byteArray);
            Row row = coder.decode(inStream, Coder.Context.OUTER);
            c.output(row);
          }
        });
  }
}
