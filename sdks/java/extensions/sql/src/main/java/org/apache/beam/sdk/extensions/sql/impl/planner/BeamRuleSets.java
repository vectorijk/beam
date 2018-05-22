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

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamEnumerableConverterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamFilterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIntersectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamMinusRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamProjectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSortRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnionRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamValuesRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * {@link RuleSet} used in {@link BeamQueryPlanner}. It translates a standard Calcite {@link
 * RelNode} tree, to represent with {@link BeamRelNode}
 */
public class BeamRuleSets {

    public final static RuleSet LOGICAL_OPT_RULES = RuleSets.ofList(
            // push a filter into a join
            FilterJoinRule.FILTER_ON_JOIN,
            // push filter into the children of a join
            FilterJoinRule.JOIN,
            // push filter through an aggregation
//            FilterAggregateTransposeRule.INSTANCE,
//            // push filter through set operation
//            FilterSetOpTransposeRule.INSTANCE,
//            // push project through set operation
//            ProjectSetOpTransposeRule.INSTANCE,
//
//            // aggregation and projection rules
//            AggregateProjectMergeRule.INSTANCE, AggregateProjectPullUpConstantsRule.INSTANCE,
//            // push a projection past a filter or vice versa
//            ProjectFilterTransposeRule.INSTANCE, FilterProjectTransposeRule.INSTANCE,
//            // push a projection to the children of a join
//            // push all expressions to handle the time indicator correctly
//            new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
//            // merge projections
//            ProjectMergeRule.INSTANCE,
//            // remove identity project
//            ProjectRemoveRule.INSTANCE,
//            // reorder sort and projection
//            SortProjectTransposeRule.INSTANCE, ProjectSortTransposeRule.INSTANCE,

            // join rules
            JoinPushExpressionsRule.INSTANCE

//            // remove union with only a single child
//            UnionEliminatorRule.INSTANCE,
//            // convert non-all union into all-union + distinct
//            UnionToDistinctRule.INSTANCE,
//
//            // remove aggregation if it does not aggregate and input is already distinct
//            AggregateRemoveRule.INSTANCE,
//            // push aggregate through join
//            AggregateJoinTransposeRule.EXTENDED,
//            // aggregate union rule
//            AggregateUnionAggregateRule.INSTANCE,
//
//            // reduce aggregate functions like AVG, STDDEV_POP etc.
//            AggregateReduceFunctionsRule.INSTANCE,

//            // remove unnecessary sort rule
//            SortRemoveRule.INSTANCE,
//
//            // prune empty results rules
//            PruneEmptyRules.AGGREGATE_INSTANCE, PruneEmptyRules.FILTER_INSTANCE,
//            PruneEmptyRules.JOIN_LEFT_INSTANCE, PruneEmptyRules.JOIN_RIGHT_INSTANCE,
//            PruneEmptyRules.PROJECT_INSTANCE, PruneEmptyRules.SORT_INSTANCE,
//            PruneEmptyRules.UNION_INSTANCE,
//
//            // calc rules
//            FilterCalcMergeRule.INSTANCE, ProjectCalcMergeRule.INSTANCE, FilterToCalcRule.INSTANCE,
//            ProjectToCalcRule.INSTANCE, CalcMergeRule.INSTANCE

            // scan optimization
            //  PushProjectIntoTableSourceScanRule.INSTANCE,
            //  PushFilterIntoTableSourceScanRule.INSTANCE,

            // unnest rule
            //  LogicalUnnestRule.INSTANCE,

            // translate to flink logical rel nodes
 /* FlinkLogicalAggregate.CONVERTER,
  FlinkLogicalWindowAggregate.CONVERTER,
  FlinkLogicalOverWindow.CONVERTER,
  FlinkLogicalCalc.CONVERTER,
  FlinkLogicalCorrelate.CONVERTER,
  FlinkLogicalIntersect.CONVERTER,
  FlinkLogicalJoin.CONVERTER,
  FlinkLogicalMinus.CONVERTER,
  FlinkLogicalSort.CONVERTER,
  FlinkLogicalUnion.CONVERTER,
  FlinkLogicalValues.CONVERTER,
  FlinkLogicalTableSourceScan.CONVERTER,
  FlinkLogicalTableFunctionScan.CONVERTER,
  FlinkLogicalNativeTableScan.CONVERTER*/
 );

    public static RuleSet[] getRuleSets(BeamSqlEnv sqlEnv) {
        return new RuleSet[]{RuleSets.ofList(BeamProjectRule.INSTANCE, BeamFilterRule.INSTANCE,
                BeamAggregationRule.INSTANCE, BeamSortRule.INSTANCE, BeamValuesRule.INSTANCE,
                BeamIntersectRule.INSTANCE, BeamMinusRule.INSTANCE, BeamUnionRule.INSTANCE,
                BeamJoinRule.INSTANCE, BeamEnumerableConverterRule.INSTANCE)};
    }
}
