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
package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.BeamTextCSVTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * This is a quick TPC-DS benchmark query example.
 * ./gradlew :beam-sdks-java-extensions-sql:runTPCExample
 */
class TPC_H {
  private static final Logger LOG = LoggerFactory.getLogger(TPC_H.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOG.error("here TPC");

    Schema orderSchema =
        RowSqlTypes.builder()
            .withIntegerField("O_ORDERKEY")
            .withIntegerField("O_CUSTKEY")
            .withVarcharField("O_ORDERSTATUS")
            .withFloatField("O_TOTALPRICE")
            .withVarcharField("O_ORDERDATE")
            .withVarcharField("O_ORDERPRIORITY")
            .withVarcharField("O_CLERK")
            .withIntegerField("O_SHIPPRIORITY")
            .withVarcharField("O_COMMENT")
            .build();

    Schema customerSchema =
        RowSqlTypes.builder()
            .withIntegerField("C_CUSTKEY")
            .withVarcharField("C_NAME")
            .withVarcharField("C_ADDRESS")
            .withIntegerField("C_NATIONKEY")
            .withVarcharField("C_PHONE")
            .withFloatField("C_ACCTBAL")
            .withVarcharField("C_MKTSEGMENT")
            .withVarcharField("C_COMMENT")
            .build();

    Schema lineitemSchema =
        RowSqlTypes.builder()
            .withIntegerField("L_ORDERKEY")
            .withIntegerField("L_PARTKEY")
            .withIntegerField("L_SUPPKEY")
            .withIntegerField("L_LINENUMBER")
            .withFloatField("L_QUANTITY")
            .withFloatField("L_EXTENDEDPRICE")
            .withFloatField("L_DISCOUNT")
            .withFloatField("L_TAX")
            .withVarcharField("L_RETURNFLAG")
            .withVarcharField("L_LINESTATUS")
            .withVarcharField("L_SHIPDATE")
            .withVarcharField("L_COMMITDATE")
            .withVarcharField("L_RECEIPTDATE")
            .withVarcharField("L_SHIPINSTRUCT")
            .withVarcharField("L_SHIPMODE")
            .withVarcharField("L_COMMENT")
            .build();

    Schema nationSchema =
        RowSqlTypes.builder()
            .withIntegerField("N_NATIONKEY")
            .withVarcharField("N_NAME")
            .withIntegerField("N_REGIONKEY")
            .withVarcharField("N_COMMENT")
            .build();

    CSVFormat format = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

    String rootPath = "/Users/jiangkai/data/";

    String nationFilePath = rootPath + "nation.tbl";
    String lineitemFilePath = rootPath + "lineitem_c.tbl";
    String orderFilePath = rootPath + "orders_c.tbl";
    String customerFilePath = rootPath + "customer_c.tbl";

    String query10 = "select c_custkey,\n" + "    c_name,\n"
        + "    SUM(l_extendedprice * (1 - l_discount)) as revenue,\n"
            + "    c_acctbal,\n"
        + "    n_name,\n" + "    c_address,\n" + "    c_phone,\n" + "    c_comment\n" + "from\n"
        + "    customer,\n" + "    orders,\n" + "    lineitem,\n" + "    nation\n" + "where\n"
        + "    c_custkey = o_custkey\n" + "    and l_orderkey = o_orderkey\n"
        + "    and o_orderdate >= '1993-10-01'\n" + "    and o_orderdate < '1994-01-01'\n"
        + "    and l_returnflag = 'R'\n" + "    and c_nationkey = n_nationkey\n" + "group by\n"
        + "    c_custkey,\n" + "    c_name,\n" + "    c_acctbal,\n" + "    c_phone,\n"
        + "    n_name,\n" + "    c_address,\n" + "    c_comment\n"
            + "order by\n"
        + "    revenue desc\n"
            + "limit 20";

    String Q10 = "SELECT *\n"
        + "FROM\n"
        + "    customer, \n"
        + "    orders\n"
//        + "    orders\n";
        + " WHERE \n"
        + "    c_custkey = o_custkey and c_custkey >= 3"
        + " ";

    PCollection<Row> orderTable =
          new BeamTextCSVTable(orderSchema, orderFilePath, format)
                  .buildIOReader(pipeline)
                  .setCoder(orderSchema.getRowCoder());

    PCollection<Row> customerTable =
            new BeamTextCSVTable(customerSchema, customerFilePath, format)
                    .buildIOReader(pipeline)
                    .setCoder(customerSchema.getRowCoder());

    PCollection<Row> nationTable =
            new BeamTextCSVTable(nationSchema, nationFilePath, format)
                    .buildIOReader(pipeline)
                    .setCoder(nationSchema.getRowCoder());

    PCollection<Row> lineitemTable =
            new BeamTextCSVTable(lineitemSchema, lineitemFilePath, format)
                    .buildIOReader(pipeline)
                    .setCoder(lineitemSchema.getRowCoder());

    PCollectionTuple tables = PCollectionTuple
            .of(new TupleTag<>("nation"), nationTable)
            .and(new TupleTag<>("customer"), customerTable)
            .and(new TupleTag<>("lineitem"), lineitemTable)
            .and(new TupleTag<>("orders"), orderTable);


    tables.apply(
            BeamSql.query(query10)
    ).apply(
            "exp_table",
            MapElements.via(
                    new SimpleFunction<Row, Void>() {
                      public @Nullable
                      Void apply(Row input) {
                        // expect output:
                        //  PCOLLECTION: [3, row, 3.0]
                        //  PCOLLECTION: [2, row, 2.0]
                        System.out.println("row: " + input.getValues());
                        return null;
                      }
                    }));

    pipeline.run().waitUntilFinish();
  }
}
