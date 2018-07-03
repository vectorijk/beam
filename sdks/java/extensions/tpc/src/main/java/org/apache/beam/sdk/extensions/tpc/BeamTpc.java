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
package org.apache.beam.sdk.extensions.tpc;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.tpc.query.Hquery;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

/** Tpc. */
public class BeamTpc {
  private static PCollectionTuple getHTables(
      Pipeline pipeline, CSVFormat csvFormat, TpcOptions tpcOptions) {
    ImmutableMap<String, Schema> hSchemas =
        ImmutableMap.<String, Schema>builder()
            .put("customer", SchemaUtil.customerSchema)
            .put("lineitem", SchemaUtil.lineitemSchema)
            .put("nation", SchemaUtil.nationSchema)
            .put("orders", SchemaUtil.orderSchema)
            .put("part", SchemaUtil.partSchema)
            .put("partsupp", SchemaUtil.partsuppSchema)
            .put("region", SchemaUtil.regionSchema)
            .put("supplier", SchemaUtil.supplierSchema)
            .build();

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);

    for (Map.Entry<String, Schema> tableSchema : hSchemas.entrySet()) {
      String filePattern = tpcOptions.getInputFile() + tableSchema.getKey() + ".tbl";

      PCollection<Row> table =
          new TextTable(
                  SchemaUtil.nationSchema,
                  filePattern,
                  new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat),
                  new TextTableProvider.RowToCsv(csvFormat))
              .buildIOReader(pipeline.begin())
              .setCoder(tableSchema.getValue().getRowCoder());

      tables = tables.and(new TupleTag<>(tableSchema.getKey()), table);
    }

    return tables;
  }

  public static void main(String[] args) {
    // Option for lanunch Tpc benchmark.
    TpcOptions tpcOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcOptions.class);

    Pipeline pipeline = Pipeline.create(tpcOptions);

    //    String rootPath = tpcOptions.getInputFile();

    //    String storeSalesFilePath = rootPath + "store_sales.dat";
    //    String dateDimFilePath = rootPath + "date_dim.dat";
    //    String itemFilePath = rootPath + "item.dat";
    //    String nationFilePath = rootPath + "nation.tbl";
    //    //      String dateDimFilePath = rootPath + "date_dim.dat";
    //    //      String itemFilePath = rootPath + "item.dat";
    //    //      String storeFilePath = rootPath + "store.dat";
    //    //      String storeReturnFilePath = rootPath + "store_returns.dat";
    //    String customerFilePath = rootPath + "customer.tbl";
    //    String lineitemFilePath = rootPath + "lineitem_c.tbl";
    //    String orderFilePath = rootPath + "orders.tbl";
    //    String regionFilePath = rootPath + "region.tbl";
    //    String partFilePath = rootPath + "part.tbl";
    //    String supplierFilePath = rootPath + "supplier.tbl";
    //    String partsuppFilePath = rootPath + "partsupp.tbl";
    //      String catalogSalesFilePath = rootPath + "catalog_sales.dat";
    //      String catalogReturnsFilePath = rootPath + "catalog_returns.dat";
    //      String inventoryFilePath = rootPath + "inventory.dat";
    //      String webSalesFilePath = rootPath + "web_sales.dat";
    //      String webReturnsFilePath = rootPath + "web_returns.dat";
    //      String callCenterFilePath = rootPath + "call_center.dat";
    //      String catalogPageFilePath = rootPath + "catalog_page.dat";
    //      String customerAddressFilePath = rootPath + "customer_address.dat";
    //      String customerDemographicsFilePath = rootPath + "customer_demographics.dat";
    //      String householdDemographicsFilePath = rootPath + "household_demographics.dat";
    //      String incomeBandFilePath = rootPath + "income_band.dat";
    //      String promotionFilePath = rootPath + "promotion.dat";
    //      String shipModeFilePath = rootPath + "ship_mode.dat";
    //      String timeDimFilePath = rootPath + "time_dim.dat";
    //      String warehouseFilePath = rootPath + "warehouse.dat";
    //      String webPageFilePath = rootPath + "web_page.dat";
    //      String webSiteFilePath = rootPath + "web_site.dat";

    //    PCollection<Row> storeSalesTable =
    //        new TextTable(
    //                SchemaUtil.storeSalesSchema,
    //                storeSalesFilePath,
    //                new CsvToRow(SchemaUtil.storeSalesSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.storeSalesSchema.getRowCoder());

    //    PCollection<Row> dateDimTable =
    //        new TextTable(
    //                SchemaUtil.dateDimSchema,
    //                dateDimFilePath,
    //                new CsvToRow(SchemaUtil.dateDimSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.dateDimSchema.getRowCoder());

    //    PCollection<Row> itemTable =
    //        new TextTable(
    //                SchemaUtil.itemSchema,
    //                itemFilePath,
    //                new CsvToRow(SchemaUtil.itemSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.itemSchema.getRowCoder());

    //    PCollection<Row> nationTable =
    //        new TextTable(
    //                SchemaUtil.nationSchema,
    //                nationFilePath,
    //                new TextTableProvider.CsvToRow(SchemaUtil.nationSchema, csvFormat),
    //                new TextTableProvider.RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.nationSchema.getRowCoder());
    //
    //    PCollection<Row> regionTable =
    //        new TextTable(
    //                SchemaUtil.regionSchema,
    //                regionFilePath,
    //                new CsvToRow(SchemaUtil.regionSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.regionSchema.getRowCoder());
    //
    //    PCollection<Row> partTable =
    //        new TextTable(
    //                SchemaUtil.partSchema,
    //                partFilePath,
    //                new CsvToRow(SchemaUtil.partSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.partSchema.getRowCoder());
    //
    //    PCollection<Row> supplierTable =
    //        new TextTable(
    //                SchemaUtil.supplierSchema,
    //                supplierFilePath,
    //                new CsvToRow(SchemaUtil.supplierSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.supplierSchema.getRowCoder());
    //
    //    PCollection<Row> partsuppTable =
    //        new TextTable(
    //                SchemaUtil.partsuppSchema,
    //                partsuppFilePath,
    //                new CsvToRow(SchemaUtil.partsuppSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.partsuppSchema.getRowCoder());

    //      PCollection<Row> reasonTable =
    //              new BeamTextCSVTable(reasonSchema, reasonFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(reasonSchema.getRowCoder());

    //      Schema nationSchema = SchemaUtil.storeSalesSchema;

    //          PCollection<Row> nationTable =
    //                  new BeamTextCSVTable(nationSchema, nationFilePath, format)
    //                          .buildIOReader(pipeline)
    //                          .setCoder(nationSchema.getRowCoder());

    //      PCollection<Row> dateDimTable =
    //              new BeamTextCSVTable(dateDimSchema, dateDimFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(dateDimSchema.getRowCoder());
    //
    //      PCollection<Row> itemTable =
    //              new BeamTextCSVTable(itemSchema, itemFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(itemSchema.getRowCoder());

    //      PCollection<Row> storeTable =
    //              new BeamTextCSVTable(storeSchema, storeFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(storeSchema.getRowCoder());

    //      PCollection<Row> storeReturnTable =
    //              new BeamTextCSVTable(storeReturnSchema, storeReturnFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(storeReturnSchema.getRowCoder());
    //
    //    PCollection<Row> customerTable =
    //        new TextTable(
    //                SchemaUtil.customerSchema,
    //                customerFilePath,
    //                new CsvToRow(SchemaUtil.customerSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.customerSchema.getRowCoder());
    //
    //    PCollection<Row> orderTable =
    //        new TextTable(
    //                SchemaUtil.orderSchema,
    //                orderFilePath,
    //                new CsvToRow(SchemaUtil.orderSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.orderSchema.getRowCoder());
    //
    //    PCollection<Row> lineitemTable =
    //        new TextTable(
    //                SchemaUtil.lineitemSchema,
    //                lineitemFilePath,
    //                new CsvToRow(SchemaUtil.lineitemSchema, csvFormat),
    //                new RowToCsv(csvFormat))
    //            .buildIOReader(pipeline.begin())
    //            .setCoder(SchemaUtil.lineitemSchema.getRowCoder());
    //
    //      PCollection<Row> catalogSalesTable =
    //              new BeamTextCSVTable(catalogSalesSchema, catalogSalesFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(catalogSalesSchema.getRowCoder());
    ////
    //      PCollection<Row> catalogReturnsTable =
    //              new BeamTextCSVTable(catalogReturnsSchema, catalogReturnsFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(catalogReturnsSchema.getRowCoder());
    ////
    //      PCollection<Row> inventoryTable =
    //              new BeamTextCSVTable(inventorySchema, inventoryFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(inventorySchema.getRowCoder());
    //
    //      PCollection<Row> webSalesTable =
    //              new BeamTextCSVTable(webSalesSchema, webSalesFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(webSalesSchema.getRowCoder());
    //      //
    //      PCollection<Row> webReturnsTable =
    //              new BeamTextCSVTable(webReturnsSchema, webReturnsFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(webReturnsSchema.getRowCoder());

    //        PCollection<Row> callCenterTable =
    //            new BeamTextCSVTable(callCenterSchema, callCenterFilePath, format)
    //                .buildIOReader(pipeline)
    //                .setCoder(callCenterSchema.getRowCoder());

    //    PCollection<Row> catalogPageTable =
    //        new BeamTextCSVTable(catalogPageSchema, catalogPageFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(catalogPageSchema.getRowCoder());

    //      PCollection<Row> customerAddressTable =
    //              new BeamTextCSVTable(customerAddressSchema, customerAddressFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(customerAddressSchema.getRowCoder());
    //
    //      PCollection<Row> customerDemographicsTable =
    //              new BeamTextCSVTable(customerDemographicsSchema, customerDemographicsFilePath,
    // format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(customerDemographicsSchema.getRowCoder());
    //
    //      PCollection<Row> householdDemographicsTable =
    //              new BeamTextCSVTable(householdDemographicsSchema, householdDemographicsFilePath,
    //                      format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(householdDemographicsSchema.getRowCoder());

    //    PCollection<Row> incomeBandTable =
    //        new BeamTextCSVTable(incomeBandSchema, incomeBandFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(incomeBandSchema.getRowCoder());

    //      PCollection<Row> promotionTable =
    //              new BeamTextCSVTable(promotionSchema, promotionFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(promotionSchema.getRowCoder());
    //
    //      PCollection<Row> shipModeTable =
    //              new BeamTextCSVTable(shipModeSchema, shipModeFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(shipModeSchema.getRowCoder());
    //
    //      PCollection<Row> timeDimTable =
    //              new BeamTextCSVTable(timeDimSchema, timeDimFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(timeDimSchema.getRowCoder());
    //
    //      PCollection<Row> warehouseTable =
    //              new BeamTextCSVTable(warehouseSchema, warehouseFilePath, format)
    //                      .buildIOReader(pipeline)
    //                      .setCoder(warehouseSchema.getRowCoder());

    //    PCollection<Row> webPageTable =
    //        new BeamTextCSVTable(webPageSchema, webPageFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(webPageSchema.getRowCoder());

    //    PCollection<Row> webSiteTable =
    //        new BeamTextCSVTable(webSiteSchema, webSiteFilePath, format)
    //            .buildIOReader(pipeline)
    //            .setCoder(webSiteSchema.getRowCoder());

    //        PCollectionTuple
    //            //                .of(new TupleTag<>("store_sales"), storeSalesTable)
    //            .of(new TupleTag<>("nation"), nationTable)
    //            .and(new TupleTag<>("region"), regionTable)
    //            .and(new TupleTag<>("part"), partTable)
    //            .and(new TupleTag<>("supplier"), supplierTable)
    //            .and(new TupleTag<>("partsupp"), partsuppTable)
    //            //            .and(new TupleTag<>("date_dim"), dateDimTable)
    //            //                      .and(new TupleTag<>("store_sales"), storeSalesTable)
    //            //                      .and(new TupleTag<>("store"), storeTable)
    //            //            .and(new TupleTag<>("item"), itemTable)
    //            //                        .and(new TupleTag<>("store_returns"), storeReturnTable)
    //            //            .and(new TupleTag<>("orders"), orderTable)
    //            .and(new TupleTag<>("customer"), customerTable)
    //            .and(new TupleTag<>("orders"), orderTable)
    //            .and(new TupleTag<>("lineitem"), lineitemTable)
    //                      .and(new TupleTag<>("catalog_sales"), catalogSalesTable)
    //                            .and(new TupleTag<>("catalog_returns"), catalogReturnsTable)
    //                      .and(new TupleTag<>("inventory"), inventoryTable)
    //                      .and(new TupleTag<>("web_sales"), webSalesTable)
    //                      .and(new TupleTag<>("web_returns"), webReturnsTable)
    //                            .and(new TupleTag<>("call_center"), callCenterTable)
    //                .and(new TupleTag<>("catalog_page"), catalogPageTable)
    //                      .and(new TupleTag<>("customer_address"), customerAddressTable)
    //                      .and(new TupleTag<>("customer_demographics"),
    // customerDemographicsTable)
    //                      .and(new TupleTag<>("household_demographics"),
    // householdDemographicsTable)
    //                .and(new TupleTag<>("income_band"), incomeBandTable)
    //                      .and(new TupleTag<>("promotion"), promotionTable)
    //                      .and(new TupleTag<>("ship_mode"), shipModeTable)
    //                      .and(new TupleTag<>("time_dim"), timeDimTable)
    //                      .and(new TupleTag<>("warehouse"), warehouseTable)
    //                .and(new TupleTag<>("web_page"), webPageTable)
    //                .and(new TupleTag<>("web_site"), webSiteTable)

    CSVFormat csvFormat = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

    PCollectionTuple tables = getHTables(pipeline, csvFormat, tpcOptions);

    String outputPath = tpcOptions.getOutput();
    System.out.println(tpcOptions.getInputFile());
    System.out.println(outputPath);

    Monitor<?> eventMonitor = new Monitor<>(".Events", "event");
    Monitor<?> resultMonitor = new Monitor<>(".Results", "result");

    tables
        .apply(".Monitor", eventMonitor.getTransform())
        .apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + tpcOptions.getQuery(),
            SqlTransform.query(Hquery.QUERYTEST))

        .apply(
            "exp_table",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    new SerializableFunction<Row, String>() {
                      @Override
                      public @Nullable String apply(Row input) {
                        // expect output:
                        //  PCOLLECTION: [3, row, 3.0]
                        System.out.println("row: " + input.getValues());
                        return "row: " + input.getValues();
                      }
                    }))
        .apply(TextIO.write().to(outputPath));

    pipeline.run().waitUntilFinish();
  }
}
