// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package decorrelate_apply

//
//import (
//	"github.com/pingcap/tidb/pkg/expression"
//	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
//	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
//	"github.com/pingcap/tidb/pkg/planner/core/base"
//	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
//	"github.com/pingcap/tidb/pkg/util/intest"
//)
//
//var _ rule.Rule = &XFPullCorrelatedExprFromProj{}
//
//// XFPullCorrelatedExprFromProj pull the correlated expression from projection as child of apply.
//type XFPullCorrelatedExprFromProj struct {
//	*rule.BaseRule
//}
//
//// NewXFPullCorrelatedExprFromProj creates a new JoinToApply rule.
//func NewXFPullCorrelatedExprFromProj() *XFPullCorrelatedExprFromProj {
//	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
//	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly))
//	return &XFPullCorrelatedExprFromProj{
//		BaseRule: rule.NewBaseRule(rule.XFPullCorrExprsFromProj, pa),
//	}
//}
//
//// Match implements the Rule interface.
//func (*XFPullCorrelatedExprFromProj) Match(applyGE base.LogicalPlan) bool {
//	return true
//}
//
//// XForm implements thr Rule interface.
//func (*XFPullCorrelatedExprFromProj) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, rule.Type, error) {
//	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
//	outerPlan := applyGE.Children()[0].GetWrappedLogicalPlan()
//	innerPlan := applyGE.Children()[1].GetWrappedLogicalPlan()
//	proj := innerPlan.(*logicalop.LogicalProjection)
//	intest.Assert(proj != nil)
//
//	// After the column pruning, some expressions in the projection operator may be pruned.
//	// In this situation, we can decorrelate the apply operator.
//	allConst := len(proj.Exprs) > 0
//	for _, expr := range proj.Exprs {
//		if len(expression.ExtractCorColumns(expr)) > 0 || !expression.ExtractColumnSet(expr).IsEmpty() {
//			allConst = false
//			break
//		}
//	}
//	if allConst && apply.JoinType == logicalop.LeftOuterJoin {
//		// If the projection just references some constant. We cannot directly pull it up when the APPLY is an outer join.
//		//  e.g. select (select 1 from t1 where t1.a=t2.a) from t2; When the t1.a=t2.a is false the join's output is NULL.
//		//       But if we pull the projection upon the APPLY. It will return 1 since the projection is evaluated after the join.
//		// We disable the decorrelation directly for now.
//		// TODO: Actually, it can be optimized. We need to first push the projection down to the selection. And then the APPLY can be decorrelated.
//		return nil, rule.DefaultNone, nil
//	}
//	// step1: substitute the all the schema with new expressions (including correlated column maybe, but it doesn't affect the collation infer inside)
//	// eg: projection: constant("guo") --> column8, once upper layer substitution failed here, the lower layer behind
//	// projection can't supply column8 anymore.
//	//
//	//	upper OP (depend on column8)   --> projection(constant "guo" --> column8)  --> lower layer OP
//	//	          |                                                       ^
//	//	          +-------------------------------------------------------+
//	//
//	//	upper OP (depend on column8)   --> lower layer OP
//	//	          |                             ^
//	//	          +-----------------------------+      // Fail: lower layer can't supply column8 anymore.
//	hasFail := apply.ColumnSubstituteAll(proj.Schema(), proj.Exprs)
//	if hasFail {
//		return nil, rule.DefaultNone, nil
//	}
//	// step2: when it can be substituted all, we then just do the de-correlation (apply conditions included).
//	for i, expr := range proj.Exprs {
//		proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
//	}
//	apply.Decorrelate(outerPlan.Schema())
//	innerPlan = proj.Children()[0]
//	apply.SetChildren(outerPlan, innerPlan)
//	if apply.JoinType != logicalop.SemiJoin && apply.JoinType != logicalop.LeftOuterSemiJoin && apply.JoinType != logicalop.AntiSemiJoin && apply.JoinType != logicalop.AntiLeftOuterSemiJoin {
//		proj.SetSchema(apply.Schema())
//		proj.Exprs = append(expression.Column2Exprs(outerPlan.Schema().Clone().Columns), proj.Exprs...)
//		apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
//		np, planChanged, err := s.Optimize(ctx, p, opt)
//		if err != nil {
//			return nil, planChanged, err
//		}
//		proj.SetChildren(np)
//		appendMoveProjTraceStep(apply, np, proj, opt)
//		return proj, planChanged, nil
//	}
//	return nil, nil
//}
