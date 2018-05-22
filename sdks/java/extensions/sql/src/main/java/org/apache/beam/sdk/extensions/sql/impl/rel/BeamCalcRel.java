package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

//public class BeamCalcRel extends Calc implements BeamRelNode {

//    public BeamCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
//                       List<? extends RexNode> projects, RelDataType rowType) {
//        super(cluster, traits, input, projects, rowType);
//    }
//
//    @Override
//    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
//                        RelDataType rowType) {
//        return new BeamProjectRel(getCluster(), traitSet, input, projects, rowType);
//    }
//
//    @Override
//    public PTransform<PCollectionTuple, PCollection<Row>> toPTransform() {
//        return new Transform();
//    }
//
//    private class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {
//
//        @Override
//        public PCollection<Row> expand(PCollectionTuple inputPCollections) {
//            RelNode input = getInput();
//            String stageName = BeamSqlRelUtils.getStageName(BeamProjectRel.this);
//
//            PCollection<Row> upstream =
//                    inputPCollections.apply(BeamSqlRelUtils.getBeamRelInput(input).toPTransform());
//
//            BeamSqlExpressionExecutor executor = new BeamSqlFnExecutor(BeamProjectRel.this);
//
//            PCollection<Row> projectStream =
//                    upstream.apply(
//                            stageName,
//                            ParDo.of(
//                                    new BeamSqlProjectFn(
//                                            getRelTypeName(), executor, CalciteUtils.toBeamSchema(rowType))));
//            projectStream.setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());
//
//            return projectStream;
//        }
//    }
//}
