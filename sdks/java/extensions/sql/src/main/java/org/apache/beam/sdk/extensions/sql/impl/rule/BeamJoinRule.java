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

package org.apache.beam.sdk.extensions.sql.impl.rule;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;

/** {@code ConverterRule} to replace {@code Join} with {@code BeamJoinRel}. */
public class BeamJoinRule extends ConverterRule {
  public static final BeamJoinRule INSTANCE = new BeamJoinRule();

  private BeamJoinRule() {
    super(LogicalJoin.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamJoinRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    System.out.println("Join Condition Type:" + join.getCondition().getType().toString());
    System.out.println("Join Condition:" + join.getCondition().getKind());
    System.out.println("Join Type" + join.getJoinType().toString());
    System.out.println("Join Type" + join.getJoinType().toString());

    RelNode left = join.getInput(0);
    RelNode right = join.getInput(1);

    final JoinInfo info = JoinInfo.of(left, right, join.getCondition());
//    if (info.isEqui() && join.getJoinType() == JoinRelType.INNER) {
//      return null;
//    }

    return new BeamJoinRel(
        join.getCluster(),
        join.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(
            join.getLeft(), join.getLeft().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        convert(
            join.getRight(), join.getRight().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
