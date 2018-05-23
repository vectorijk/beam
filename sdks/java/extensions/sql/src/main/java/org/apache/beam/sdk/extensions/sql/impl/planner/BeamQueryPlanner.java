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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ConversionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core component to handle through a SQL statement, from explain execution plan,
 * to generate a Beam pipeline.
 *
 */
public class BeamQueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(BeamQueryPlanner.class);

  protected final Planner planner;
  protected final RelOptPlanner optPlanner;

  public static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  public BeamQueryPlanner(BeamSqlEnv sqlEnv, SchemaPlus schema) {
    String defaultCharsetKey = "saffron.default.charset";
    if (System.getProperty(defaultCharsetKey) == null) {
      System.setProperty(defaultCharsetKey, ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
      System.setProperty("saffron.default.nationalcharset",
        ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
      System.setProperty("saffron.default.collation.name",
        String.format("%s$%s", ConversionUtil.NATIVE_UTF16_CHARSET_NAME, "en_US"));
    }

    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
//    traitDefs.add(RelCollationTraitDef.INSTANCE);

    List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
    sqlOperatorTables.add(SqlStdOperatorTable.instance());
    sqlOperatorTables.add(
        new CalciteCatalogReader(
            CalciteSchema.from(schema), Collections.emptyList(), TYPE_FACTORY, null));

    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setParserFactory(BeamSqlParserImpl.FACTORY)
                .build())
            .defaultSchema(schema)
            .traitDefs(traitDefs)
            .context(Contexts.EMPTY_CONTEXT)
            .ruleSets(BeamRuleSets.getRuleSets(sqlEnv))
            .costFactory(null)
            .typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM)
            .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
            .build();
    this.planner = Frameworks.getPlanner(config);

    VolcanoPlanner volcanoPlanner = new VolcanoPlanner(config.getCostFactory(), Contexts.empty());
    volcanoPlanner.setExecutor(config.getExecutor());
    volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

      RelOptCluster optCluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(TYPE_FACTORY));
      optCluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
      // just set metadataProvider is not enough, see
      // https://www.mail-archive.com/dev@calcite.apache.org/msg00930.html
      RelMetadataQuery.THREAD_PROVIDERS.set(
              JaninoRelMetadataProvider.of(optCluster.getMetadataProvider()));
      this.optPlanner = optCluster.getPlanner();
  }

  /**
   * Parse input SQL query, and return a {@link SqlNode} as grammar tree.
   */
  public SqlNode parseQuery(String sqlQuery) throws SqlParseException{
    SqlNode test = planner.parse(sqlQuery);
    return test;
  }

  /**
   * {@code compileBeamPipeline} translate a SQL statement to executed as Beam data flow,
   * which is linked with the given {@code pipeline}. The final output stream is returned as
   * {@code PCollection} so more operations can be applied.
   */
  public PCollection<Row> compileBeamPipeline(String sqlStatement, Pipeline basePipeline
      , BeamSqlEnv sqlEnv) throws Exception {
    BeamRelNode relNode = convertToBeamRel(sqlStatement);

    // the input PCollectionTuple is empty, and be rebuilt in BeamIOSourceRel.
    return PCollectionTuple.empty(basePipeline).apply(relNode.toPTransform());
  }

  /**
   * It parses and validate the input query, then convert into a
   * {@link BeamRelNode} tree.
   *
   */
  public BeamRelNode convertToBeamRel(String sqlStatement)
          throws Exception {
    BeamRelNode beamRelNode;
    try {
      SqlNode test = planner.parse(sqlStatement);
      beamRelNode = (BeamRelNode) validateAndConvert(test);
    } catch (Exception e) {
        throw new Exception("here");
    } finally {
      planner.close();
    }
    return beamRelNode;
  }

  private RelNode validateAndConvert(SqlNode sqlNode)
          throws Exception {
    SqlNode validated = validateNode(sqlNode);
    LOG.info("SQL:\n" + validated);
    System.out.println("SQL:\n" + validated);
    RelNode relNode = convertToRelNode(validated);
    System.out.println("RelNode:\n" + relNode.toString());

    // optimize logic plan
    RelNode optLogicPlanRelNode = optimizeLogicPlan(relNode);
    System.out.println("OptimizeRelNode:\n" + optLogicPlanRelNode.toString());

    return convertToBeamRel(optLogicPlanRelNode);
  }

  /**
   * execute volcano planner
   */
  private RelNode runVolcanoPlanner(RuleSet logicalOptRuleSet, RelNode relNode,
                                    RelTraitSet logicalOutputProps) throws Exception {
    Program optProgram = Programs.ofRules(logicalOptRuleSet);

    RelNode output;
    try {
      output = optProgram.run(relNode.getCluster().getPlanner(), relNode, logicalOutputProps,
//      output = optProgram.run(optPlanner, relNode, logicalOutputProps,
              Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    } catch (RelOptPlanner.CannotPlanException e){
        throw new Exception(
                "Cannot generate a valid execution plan for the given query: \n\n"
                    + "This exception indicates that the query uses an unsupported SQL feature.\n"
                    + "Please check the documentation for the set of currently supported SQL features.");
    } catch (AssertionError e) {
          throw e;
    } finally{ }

      return output;
  }

  private RuleSet getLogicOptRuleSet() {
    return RuleSets.ofList(BeamRuleSets.LOGICAL_OPT_RULES);
  }

  private RelNode optimizeLogicPlan(RelNode relNode) throws Exception {
    RuleSet logicalOptRuleSet = getLogicOptRuleSet();
    RelTraitSet logicalOutputProps = relNode.getTraitSet();
//      Convention LOGICAL = new Convention.Impl("BEAM-LOGICAL", BeamRelNode.class);
      Convention LOGICAL = BeamLogicalConvention.INSTANCE;
//    RelTraitSet logicalOutputProps = relNode.getTraitSet()
    RelTraitSet tmp = RelTraitSet.createEmpty()
            .plus(logicalOutputProps.getTrait(0))
            .replace(LOGICAL)
            .simplify();
//    logicalOutputProps.remove(1);
//    RelTraitSet tmp = RelTraitSet.createEmpty().add(logicalOutputProps.get(1));
    RelNode optLogicalPlan;
    if (logicalOptRuleSet.iterator().hasNext()) {
      optLogicalPlan = runVolcanoPlanner(logicalOptRuleSet, relNode, tmp);
//      optLogicalPlan = runVolcanoPlanner(logicalOptRuleSet, relNode, logicalOutputProps);
    } else {
      optLogicalPlan = relNode;
    }

    return optLogicalPlan;
  }

  private RelNode convertToBeamRel(RelNode relNode) throws RelConversionException {
    RelTraitSet traitSet = relNode.getTraitSet();

    LOG.debug("SQLPlan>\n" + RelOptUtil.toString(relNode));
    System.out.println("SQLPlan>\n" + RelOptUtil.toString(relNode));

    // PlannerImpl.transform() optimizes RelNode with ruleset
    return planner.transform(0, traitSet.plus(BeamLogicalConvention.INSTANCE), relNode);
  }

  private RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
    RelNode test = planner.rel(sqlNode).rel;
    return test;
  }

  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    return planner.validate(sqlNode);
  }

  public Planner getPlanner() {
    return planner;
  }

}
