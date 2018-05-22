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

import javax.annotation.Nullable;
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

/**
 * This is a quick TPC-DS benchmark query example.
 * ./gradlew :beam-sdks-java-extensions-sql:runTPCExample
 */
class TPCdsSql {
  private static final Logger LOG = LoggerFactory.getLogger(TPCdsSql.class);

  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOG.error("here TPC");

    Schema storeSalesSchema = RowSqlTypes
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

    Schema reasonSchema = RowSqlTypes.builder()
            .withIntegerField("r_reason_sk")
            .withVarcharField("r_reason_id")
            .withVarcharField("r_reason_desc")
            .build();

    Schema dateDimSchema = RowSqlTypes.builder()
            .withIntegerField("d_date_sk")
            .withVarcharField("d_date_id")
            .withVarcharField("d_date")
//            .withDateField("d_date")
            .withIntegerField("d_month_seq")
            .withIntegerField("d_week_seq")
            .withIntegerField("d_quarter_seq")
            .withIntegerField("d_year")
            .withIntegerField("d_dow")
            .withIntegerField("d_moy")
            .withIntegerField("d_dom")
            .withIntegerField("d_qoy")
            .withIntegerField("d_fy_year")
            .withIntegerField("d_fy_quarter_seq")
            .withIntegerField("d_fy_week_seq")
            .withVarcharField("d_day_name")
            .withVarcharField("d_quarter_name")
            .withVarcharField("d_holiday")
            .withVarcharField("d_weekend")
            .withVarcharField("d_following_holiday")
            .withIntegerField("d_first_dom")
            .withIntegerField("d_last_dom")
            .withIntegerField("d_same_day_ly")
            .withIntegerField("d_same_day_lq")
            .withVarcharField("d_current_day")
            .withVarcharField("d_current_week")
            .withVarcharField("d_current_month")
            .withVarcharField("d_current_quarter")
            .withVarcharField("d_current_year")
            .build();

//    Schema storeSchema = RowSqlTypes.builder()
//            .withVarcharField("s_store_sk")                // .int,
//            .withVarcharField("s_store_id")                // .string,
//            .withVarcharField("s_rec_start_date")          // .date,
//            .withVarcharField("s_rec_end_date")            // .date,
//            .withIntegerField("s_closed_date_sk")          // .int,
//            .withVarcharField("s_store_name")              // .string,
//            .withIntegerField("s_number_employees")        // .int,
//            .withIntegerField("s_floor_space")             // .int,
//            .withVarcharField("s_hours")                   // .string,
//            .withVarcharField("s_manager")                 // .string,
//            .withIntegerField("s_market_id")               // .int,
//            .withVarcharField("s_geography_class")         // .string,
//            .withVarcharField("s_market_desc")             // .string,
//            .withVarcharField("s_market_manager")          // .string,
//            .withIntegerField("s_division_id")             // .int,
//            .withVarcharField("s_division_name")           // .string,
//            .withIntegerField("s_company_id")              // .int,
//            .withVarcharField("s_company_name")            // .string,
//            .withVarcharField("s_street_number")           // .string,
//            .withVarcharField("s_street_name")             // .string,
//            .withVarcharField("s_street_type")             // .string,
//            .withVarcharField("s_suite_number")            // .string,
//            .withVarcharField("s_city")                    // .string,
//            .withVarcharField("s_county")                  // .string,
//            .withVarcharField("s_state")                   // .string,
//            .withVarcharField("s_zip")                     // .string,
//            .withVarcharField("s_country")                 // .string,
//            .withFloatField("s_gmt_offset")                // .decimal(5,2),
//            .withFloatField("s_tax_precentage")            // .decimal(5,2)
//            .build();

//    Schema itemSchema = RowSqlTypes.builder()
//            .withIntegerField("i_item_sk")
//            .withVarcharField("i_item_id") //                 .string,
//            .withDateField("i_rec_start_date") //          .date,
//            .withDateField("i_rec_end_date") //            .date,
//            .withVarcharField("i_item_desc")  //             .string,
//            .withFloatField("i_current_price") //           .decimal(7,2),
//            .withFloatField("i_wholesale_cost") //               .decimal(7,2),
//            .withIntegerField("i_brand_id") //                .int,
//            .withVarcharField("i_brand") //                   .string,
//            .withIntegerField("i_class_id") //                .int,
//            .withVarcharField("i_class") //                   .string,
//            .withIntegerField("i_category_id") //             .int,
//            .withVarcharField("i_category")         //       .string,
//            .withIntegerField("i_manufact_id") //             .int,
//            .withVarcharField("i_manufact") //                .string,
//            .withVarcharField("i_size") //                    .string,
//            .withVarcharField("i_formulation") //             .string,
//            .withVarcharField("i_color") //                   .string,
//            .withVarcharField("i_units") //                   .string,
//            .withVarcharField("i_container") //               .string,
//            .withIntegerField("i_manager_id")             // .int,
//            .withVarcharField("i_product_name")//            .string),
//            .build();

    CSVFormat format = CSVFormat.MYSQL.withDelimiter(',').withNullString("");

    String rootPath = "/Users/jiangkai/data/";

    String storeSalesFilePath = rootPath + "store_sales1.dat";
    String reasonFilePath = rootPath + "reason.dat";
    String dateDimFilePath = rootPath + "date_dim.dat";
//    String storeFilePath = rootPath + "store.dat";
//    String itemFilePath = "/tmp/item.dat";

    String query9 = "select "
            + "       case when (select count(*) \n"
            + "                  from store_sales \n"
            + "                  where ss_quantity between 1 and 20) > 1071\n"
            + "            then (select avg(ss_ext_tax) \n"
            + "                  from store_sales \n"
            + "                  where ss_quantity between 1 and 20) \n"
            + "            else (select avg(ss_net_paid_inc_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 1 and 20) end bucket1 ,\n"
            + "       case when (select count(*)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 21 and 40) > 39161\n"
            + "            then (select avg(ss_ext_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 21 and 40) \n"
            + "            else (select avg(ss_net_paid_inc_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 21 and 40) end bucket2,\n"
            + "       case when (select count(*)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 41 and 60) > 29434\n"
            + "            then (select avg(ss_ext_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 41 and 60)\n"
            + "            else (select avg(ss_net_paid_inc_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 41 and 60) end bucket3,\n"
            + "       case when (select count(*)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 61 and 80) > 6568\n"
            + "            then (select avg(ss_ext_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 61 and 80)\n"
            + "            else (select avg(ss_net_paid_inc_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 61 and 80) end bucket4,\n"
            + "       case when (select count(*)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 81 and 100) > 21216\n"
            + "            then (select avg(ss_ext_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 81 and 100)\n"
            + "            else (select avg(ss_net_paid_inc_tax)\n"
            + "                  from store_sales\n"
            + "                  where ss_quantity between 81 and 100) end bucket5\n"
            + " from reason\n";

    String query9simplify = "select case when (select count(*) \n"
        + "                  from store_sales \n"
        + "                  where ss_quantity between 1 and 20) > 1071\n"
        + "            then (select avg(ss_ext_tax) \n" + "                  from store_sales \n"
        + "                  where ss_quantity between 1 and 20) \n"
        + "            else (select avg(ss_net_paid_inc_tax)\n"
        + "                  from store_sales\n"
        + "                  where ss_quantity between 1 and 20) end bucket1 \n" + "from reason"
        + " where r_reason_sk = 1" ;

    String query59 = "with wss as \n"
        + " (select d_week_seq,\n"
        + "        ss_store_sk,\n"
        + "        sum(case when (d_day_name='Sunday') then ss_sales_price else null end)"
        + " sun_sales,\n"
        + "        sum(case when (d_day_name='Monday') then ss_sales_price else null end)"
        + " mon_sales,\n"
        + "        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end)"
        + " tue_sales,\n"
        + "        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end)"
        + " wed_sales,\n"
        + "        sum(case when (d_day_name='Thursday') then ss_sales_price else null end)"
        + " thu_sales,\n"
        + "        sum(case when (d_day_name='Friday') then ss_sales_price else null end)"
        + " fri_sales,\n"
        + "        sum(case when (d_day_name='Saturday') then ss_sales_price else null end)"
        + " sat_sales\n"
        + " from store_sales,date_dim\n"
        + " where d_date_sk = ss_sold_date_sk\n"
        + " group by d_week_seq,ss_store_sk\n"
        + " )\n"
        + "  select  s_store_name1,s_store_id1,d_week_seq1\n"
        + "       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2\n"
        + "       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2\n"
        + "       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2\n"
        + " from\n"
        + " (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1\n"
        + "        ,s_store_id s_store_id1,sun_sales sun_sales1\n"
        + "        ,mon_sales mon_sales1,tue_sales tue_sales1\n"
        + "        ,wed_sales wed_sales1,thu_sales thu_sales1\n"
        + "        ,fri_sales fri_sales1,sat_sales sat_sales1\n"
        + "  from wss,store,date_dim d\n"
        + "  where d.d_week_seq = wss.d_week_seq and\n"
        + "        ss_store_sk = s_store_sk and \n"
        + "        d_month_seq between 1198 and 1198 + 11) y,\n"
        + " (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2\n"
        + "        ,s_store_id s_store_id2,sun_sales sun_sales2\n"
        + "        ,mon_sales mon_sales2,tue_sales tue_sales2\n"
        + "        ,wed_sales wed_sales2,thu_sales thu_sales2\n"
        + "        ,fri_sales fri_sales2,sat_sales sat_sales2\n"
        + "  from wss,store,date_dim d\n"
        + "  where d.d_week_seq = wss.d_week_seq and\n"
        + "        ss_store_sk = s_store_sk and \n"
        + "        d_month_seq between 1198+ 12 and 1198 + 23) x\n"
        + " where s_store_id1=s_store_id2\n"
        + "   and d_week_seq1=d_week_seq2-52\n"
        + " order by s_store_name1,s_store_id1,d_week_seq1\n"
        + "limit 100";

    String query59v1 = "with wss as \n"
        + " (select r_reason_desc, r_reason_sk \n"
        + " from reason\n"
        + " )\n"
        + "  select * from wss limit 10";


//    String query9V1 = "select count(*) \n"
//            + " from store_sales \n"
//            + " where ss_quantity between 1 and 20";

    String query6sub = "select distinct (d_month_seq)\n"
        + " from date_dim\n"
        + " where d_year = 2002\n"
        + " and d_moy = 3 ";

    PCollection<Row> storeSalesTable =
          new BeamTextCSVTable(storeSalesSchema, storeSalesFilePath, format)
                  .buildIOReader(pipeline)
                  .setCoder(storeSalesSchema.getRowCoder());

    PCollection<Row> reasonTable =
            new BeamTextCSVTable(reasonSchema, reasonFilePath, format)
                    .buildIOReader(pipeline)
                    .setCoder(reasonSchema.getRowCoder());

    PCollection<Row> dateDimTable =
            new BeamTextCSVTable(dateDimSchema, dateDimFilePath, format)
                    .buildIOReader(pipeline)
                    .setCoder(dateDimSchema.getRowCoder());

//    PCollection<Row> item_table =
//            new BeamTextCSVTable(reason_schema, item_file_path, format)
//                    .buildIOReader(pipeline)
//                    .setCoder(item_schema.getRowCoder());

//    PCollection<Row> storeTable =
//            new BeamTextCSVTable(storeSchema, storeFilePath, format)
//                    .buildIOReader(pipeline)
//                    .setCoder(storeSchema.getRowCoder());

    PCollectionTuple tables = PCollectionTuple
            .of(new TupleTag<>("reason"), reasonTable)
            .and(new TupleTag<>("date_dim"), dateDimTable)
            .and(new TupleTag<>("store_sales"), storeSalesTable)
//            .and(new TupleTag<>("store"), storeTable)
            ;
//            .and(new TupleTag<>("item"), item_table);


    tables.apply(
            BeamSql.query(query9simplify)
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
