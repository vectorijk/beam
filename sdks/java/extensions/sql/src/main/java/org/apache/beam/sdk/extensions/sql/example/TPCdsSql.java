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

import javax.annotation.Nullable;

/**
 * This is a quick TPC-DS benchmark query example.
 */
class TPCdsSql {
  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    Schema store_sales_schema = RowSqlTypes
            .builder()
            .withIntegerField("ss_sold_date_sk")
            .withIntegerField("ss_sold_time_sk")
            .withIntegerField("ss_item_sk")
            .withIntegerField("ss_customer_sk")
            .withIntegerField("ss_cdemo_sk")
            .withIntegerField("ss_hdemo_sk")
            .withIntegerField("ss_addr_sk")
            .withIntegerField("ss_store_sk")
            .withIntegerField("ss_promo_sk")
            .withBigIntField("ss_ticket_number")
            .withIntegerField("ss_quantity")
            .withFloatField("ss_wholesale_cost")
            .withFloatField("ss_list_price")
            .withFloatField("ss_sales_price")
            .withFloatField("ss_ext_discount_amt")
            .withFloatField("ss_ext_sales_price")
            .withFloatField("ss_ext_wholesale_cost")
            .withFloatField("ss_ext_list_price")
            .withFloatField("ss_ext_tax")
            .withFloatField("ss_coupon_amt")
            .withFloatField("ss_net_paid")
            .withFloatField("ss_net_paid_inc_tax")
            .withFloatField("ss_net_profit")
            .build();

    Schema reason_schema = RowSqlTypes.builder()
            .withIntegerField("r_reason_sk")
            .withVarcharField("r_reason_id")
            .withVarcharField("r_reason_desc")
            .build();

    Schema item_schema = RowSqlTypes.builder()
            .withIntegerField("i_item_sk")
            .withVarcharField("i_item_id") //                 .string,
            .withDateField("i_rec_start_date") //          .date,
            .withDateField("i_rec_end_date") //            .date,
            .withVarcharField("i_item_desc")  //             .string,
            .withFloatField("i_current_price") //           .decimal(7,2),
            .withFloatField("i_wholesale_cost") //               .decimal(7,2),
            .withIntegerField("i_brand_id") //                .int,
            .withVarcharField("i_brand") //                   .string,
            .withIntegerField("i_class_id") //                .int,
            .withVarcharField("i_class") //                   .string,
            .withIntegerField("i_category_id") //             .int,
            .withVarcharField("i_category")         //       .string,
            .withIntegerField("i_manufact_id") //             .int,
            .withVarcharField("i_manufact") //                .string,
            .withVarcharField("i_size") //                    .string,
            .withVarcharField("i_formulation") //             .string,
            .withVarcharField("i_color") //                   .string,
            .withVarcharField("i_units") //                   .string,
            .withVarcharField("i_container") //               .string,
            .withIntegerField("i_manager_id")             // .int,
            .withVarcharField("i_product_name")//            .string),
            .build();

    CSVFormat format = CSVFormat.MYSQL.withDelimiter(',').withNullString("");

    String store_sales_file_path = "/tmp/store_sales1.dat";
    String reason_file_path = "/tmp/reason.dat";
    String item_file_path = "/tmp/item.dat";

    String query9 = "select case when (select count(*) \n" +
            "                  from store_sales \n" +
            "                  where ss_quantity between 1 and 20) > 1071\n" +
            "            then (select avg(ss_ext_tax) \n" +
            "                  from store_sales \n" +
            "                  where ss_quantity between 1 and 20) \n" +
            "            else (select avg(ss_net_paid_inc_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 1 and 20) end bucket1 ,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40) > 39161\n" +
            "            then (select avg(ss_ext_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40) \n" +
            "            else (select avg(ss_net_paid_inc_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 21 and 40) end bucket2,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60) > 29434\n" +
            "            then (select avg(ss_ext_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60)\n" +
            "            else (select avg(ss_net_paid_inc_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 41 and 60) end bucket3,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80) > 6568\n" +
            "            then (select avg(ss_ext_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80)\n" +
            "            else (select avg(ss_net_paid_inc_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 61 and 80) end bucket4,\n" +
            "       case when (select count(*)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100) > 21216\n" +
            "            then (select avg(ss_ext_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100)\n" +
            "            else (select avg(ss_net_paid_inc_tax)\n" +
            "                  from store_sales\n" +
            "                  where ss_quantity between 81 and 100) end bucket5\n" +
            "from reason\n" +
            "where r_reason_sk = 1";

    String query9_v1 = "select * " +
            "from reason\n" +
            "where r_reason_sk = 1";

    String query41 = "select  distinct(i_product_name)\n" +
            " from item i1\n" +
            " where i_manufact_id between 704 and 704+40 \n" +
            "   and (select count(*) as item_cnt\n" +
            "        from item\n" +
            "        where (i_manufact = i1.i_manufact and\n" +
            "        ((i_category = 'Women' and \n" +
            "        (i_color = 'forest' or i_color = 'lime') and \n" +
            "        (i_units = 'Pallet' or i_units = 'Pound') and\n" +
            "        (i_size = 'economy' or i_size = 'small')\n" +
            "        ) or\n" +
            "        (i_category = 'Women' and\n" +
            "        (i_color = 'navy' or i_color = 'slate') and\n" +
            "        (i_units = 'Gross' or i_units = 'Bunch') and\n" +
            "        (i_size = 'extra large' or i_size = 'petite')\n" +
            "        ) or\n" +
            "        (i_category = 'Men' and\n" +
            "        (i_color = 'powder' or i_color = 'sky') and\n" +
            "        (i_units = 'Dozen' or i_units = 'Lb') and\n" +
            "        (i_size = 'N/A' or i_size = 'large')\n" +
            "        ) or\n" +
            "        (i_category = 'Men' and\n" +
            "        (i_color = 'maroon' or i_color = 'smoke') and\n" +
            "        (i_units = 'Ounce' or i_units = 'Case') and\n" +
            "        (i_size = 'economy' or i_size = 'small')\n" +
            "        ))) or\n" +
            "       (i_manufact = i1.i_manufact and\n" +
            "        ((i_category = 'Women' and \n" +
            "        (i_color = 'dark' or i_color = 'aquamarine') and \n" +
            "        (i_units = 'Ton' or i_units = 'Tbl') and\n" +
            "        (i_size = 'economy' or i_size = 'small')\n" +
            "        ) or\n" +
            "        (i_category = 'Women' and\n" +
            "        (i_color = 'frosted' or i_color = 'plum') and\n" +
            "        (i_units = 'Dram' or i_units = 'Box') and\n" +
            "        (i_size = 'extra large' or i_size = 'petite')\n" +
            "        ) or\n" +
            "        (i_category = 'Men' and\n" +
            "        (i_color = 'papaya' or i_color = 'peach') and\n" +
            "        (i_units = 'Bundle' or i_units = 'Carton') and\n" +
            "        (i_size = 'N/A' or i_size = 'large')\n" +
            "        ) or\n" +
            "        (i_category = 'Men' and\n" +
            "        (i_color = 'firebrick' or i_color = 'sienna') and\n" +
            "        (i_units = 'Cup' or i_units = 'Each') and\n" +
            "        (i_size = 'economy' or i_size = 'small')\n" +
            "        )))) > 0\n" +
            " order by i_product_name\n" +
            " limit 100";

    PCollection<Row> store_sales_table =
          new BeamTextCSVTable(store_sales_schema, store_sales_file_path, format)
                  .buildIOReader(pipeline)
                  .setCoder(store_sales_schema.getRowCoder());

    PCollection<Row> reason_table =
            new BeamTextCSVTable(reason_schema, reason_file_path, format)
                    .buildIOReader(pipeline)
                    .setCoder(reason_schema.getRowCoder());

    PCollection<Row> item_table =
            new BeamTextCSVTable(reason_schema, item_file_path, format)
                    .buildIOReader(pipeline)
                    .setCoder(item_schema.getRowCoder());

    PCollectionTuple tables = PCollectionTuple
            .of(new TupleTag<>("reason"), reason_table)
            .and(new TupleTag<>("store_sales"), store_sales_table)
            .and(new TupleTag<>("item"), item_table);


    tables.apply(
            BeamSql.query(query9)
    ).apply(
            "reason_table",
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
