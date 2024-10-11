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

package logicalop

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, SemiJoin, AntiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if row a in table A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if row a in table A does not match any row in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

// IsOuterJoin returns if this joiner is an outer joiner
func (tp JoinType) IsOuterJoin() bool {
	return tp == LeftOuterJoin || tp == RightOuterJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

// IsSemiJoin returns if this joiner is a semi/anti-semi joiner
func (tp JoinType) IsSemiJoin() bool {
	return tp == SemiJoin || tp == AntiSemiJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	case SemiJoin:
		return "semi join"
	case AntiSemiJoin:
		return "anti semi join"
	case LeftOuterSemiJoin:
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		return "anti left outer semi join"
	}
	return "unsupported join type"
}

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	LogicalSchemaProducer

	JoinType      JoinType
	Reordered     bool
	CartesianJoin bool
	StraightJoin  bool

	// HintInfo stores the join algorithm hint information specified by client.
	HintInfo            *utilhint.PlanHints
	PreferJoinType      uint
	PreferJoinOrder     bool
	LeftPreferJoinType  uint
	RightPreferJoinType uint

	EqualConditions []*expression.ScalarFunction
	// NAEQConditions means null aware equal conditions, which is used for null aware semi joins.
	NAEQConditions  []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	LeftProperties  [][]*expression.Column
	RightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// FullSchema contains all the columns that the Join can output. It's ordered as [outer schema..., inner schema...].
	// This is useful for natural joins and "using" joins. In these cases, the join key columns from the
	// inner side (or the right side when it's an inner join) will not be in the schema of Join.
	// But upper operators should be able to find those "redundant" columns, and the user also can specifically select
	// those columns, so we put the "redundant" columns here to make them be able to be found.
	//
	// For example:
	// create table t1(a int, b int); create table t2(a int, b int);
	// select * from t1 join t2 using (b);
	// schema of the Join will be [t1.b, t1.a, t2.a]; FullSchema will be [t1.a, t1.b, t2.a, t2.b].
	//
	// We record all columns and keep them ordered is for correctly handling SQLs like
	// select t1.*, t2.* from t1 join t2 using (b);
	// (*PlanBuilder).unfoldWildStar() handles the schema for such case.
	FullSchema *expression.Schema
	FullNames  types.NameSlice

	// EqualCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	EqualCondOutCnt float64
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx base.PlanContext, offset int) *LogicalJoin {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}

// ************************ start implementation of HashEquals interface ************************

// Hash64 implements the HashEquals.<0th> interface.
func (p *LogicalJoin) Hash64(h base2.Hasher) {
	h.HashString(plancodec.TypeJoin)
	h.HashInt(int(p.JoinType))
	h.HashInt(len(p.EqualConditions))
	for _, oneCond := range p.EqualConditions {
		oneCond.Hash64(h)
	}
	h.HashInt(len(p.NAEQConditions))
	for _, oneCond := range p.NAEQConditions {
		oneCond.Hash64(h)
	}
	h.HashInt(len(p.LeftConditions))
	for _, oneCond := range p.LeftConditions {
		oneCond.Hash64(h)
	}
	h.HashInt(len(p.RightConditions))
	for _, oneCond := range p.RightConditions {
		oneCond.Hash64(h)
	}
	h.HashInt(len(p.OtherConditions))
	for _, oneCond := range p.OtherConditions {
		oneCond.Hash64(h)
	}
}

// Equals implements the HashEquals.<1st> interface.
func (p *LogicalJoin) Equals(other any) bool {
	if other == nil {
		return false
	}
	var p2 *LogicalJoin
	switch x := other.(type) {
	case *LogicalJoin:
		p2 = x
	case LogicalJoin:
		p2 = &x
	default:
		return false
	}
	ok := p.JoinType != p2.JoinType && len(p.EqualConditions) == len(p2.EqualConditions) && len(p.NAEQConditions) == len(p2.NAEQConditions) &&
		len(p.LeftConditions) == len(p2.LeftConditions) && len(p.RightConditions) == len(p2.RightConditions) && len(p.OtherConditions) == len(p2.OtherConditions)
	if !ok {
		return false
	}
	for i, oneCond := range p.EqualConditions {
		if !oneCond.Equals(p2.EqualConditions[i]) {
			return false
		}
	}
	for i, oneCond := range p.NAEQConditions {
		if !oneCond.Equals(p2.NAEQConditions[i]) {
			return false
		}
	}
	for i, oneCond := range p.LeftConditions {
		if !oneCond.Equals(p2.LeftConditions[i]) {
			return false
		}
	}
	for i, oneCond := range p.RightConditions {
		if !oneCond.Equals(p2.RightConditions[i]) {
			return false
		}
	}
	for i, oneCond := range p.OtherConditions {
		if !oneCond.Equals(p2.OtherConditions[i]) {
			return false
		}
	}
	return true
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (p *LogicalJoin) ExplainInfo() string {
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.OtherConditions))
	}
	return buffer.String()
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (p *LogicalJoin) ReplaceExprColumns(replace map[string]*expression.Column) {
	for _, equalExpr := range p.EqualConditions {
		ruleutil.ResolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		ruleutil.ResolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		ruleutil.ResolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		ruleutil.ResolveExprAndReplace(otherExpr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements the base.LogicalPlan.<1st> interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			AppendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			AppendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), tempCond)
		tempCond = expression.PropagateConstant(p.SCtx().GetExprCtx(), tempCond)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, tempCond)
		if dual != nil {
			AppendTableDualTraceStep(p, dual, tempCond, opt)
			return ret, dual
		}
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case AntiSemiJoin:
		predicates = expression.PropagateConstant(p.SCtx().GetExprCtx(), predicates)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			AppendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// `predicates` should only contain left conditions or constant filters.
		_, leftPushCond, rightPushCond, _ = p.extractOnCondition(predicates, true, true)
		// Do not derive `is not null` for anti join, since it may cause wrong results.
		// For example:
		// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
		// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
		// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
		leftCond = leftPushCond
		rightCond = append(p.RightConditions, rightPushCond...)
		p.RightConditions = nil
	}
	leftCond = expression.RemoveDupExprs(leftCond)
	rightCond = expression.RemoveDupExprs(rightCond)
	leftRet, lCh := p.Children()[0].PredicatePushDown(leftCond, opt)
	rightRet, rCh := p.Children()[1].PredicatePushDown(rightCond, opt)
	addSelection(p, lCh, leftRet, 0, opt)
	addSelection(p, rCh, rightRet, 1, opt)
	p.updateEQCond()
	ruleutil.BuildKeyInfoPortal(p)
	return ret, p.Self()
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	leftCols, rightCols := p.ExtractUsedCols(parentUsedCols)

	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(leftCols, opt)
	if err != nil {
		return nil, err
	}

	p.Children()[1], err = p.Children()[1].PruneColumns(rightCols, opt)
	if err != nil {
		return nil, err
	}

	p.MergeSchema()
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinCol := p.Schema().Columns[len(p.Schema().Columns)-1]
		parentUsedCols = append(parentUsedCols, joinCol)
	}
	p.InlineProjection(parentUsedCols, opt)
	return p, nil
}

// FindBestTask inherits the BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements the base.LogicalPlan.<4th> interface.
func (p *LogicalJoin) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin:
		selfSchema.Keys = childSchema[0].Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		lOk := false
		rOk := false
		// Such as 'select * from t1 join t2 where t1.a = t2.a and t1.b = t2.b'.
		// If one sides (a, b) is a unique key, then the unique key information is remained.
		// But we don't consider this situation currently.
		// Only key made by one column is considered now.
		evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
		for _, expr := range p.EqualConditions {
			ln := expr.GetArgs()[0].(*expression.Column)
			rn := expr.GetArgs()[1].(*expression.Column)
			for _, key := range childSchema[0].Keys {
				if len(key) == 1 && key[0].Equal(evalCtx, ln) {
					lOk = true
					break
				}
			}
			for _, key := range childSchema[1].Keys {
				if len(key) == 1 && key[0].Equal(evalCtx, rn) {
					rOk = true
					break
				}
			}
		}
		// For inner join, if one side of one equal condition is unique key,
		// another side's unique key information will all be reserved.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != LeftOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[1].Keys...)
		}
		if rOk && p.JoinType != RightOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[0].Keys...)
		}
	}
}

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalJoin) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		p.Children()[0] = p.pushDownTopNToChild(topN, 0, opt)
		p.Children()[1] = p.Children()[1].PushDownTopN(nil, opt)
	case RightOuterJoin:
		p.Children()[1] = p.pushDownTopNToChild(topN, 1, opt)
		p.Children()[0] = p.Children()[0].PushDownTopN(nil, opt)
	default:
		return p.BaseLogicalPlan.PushDownTopN(topN, opt)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		return topN.AttachChild(p.Self(), opt)
	}
	return p.Self()
}

// DeriveTopN inherits the BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits the BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation implements the base.LogicalPlan.<8th> interface.
// about the logic of constant propagation in From List.
// Query: select * from t, (select a, b from s where s.a>1) tmp where tmp.a=t.a
// Origin logical plan:
/*
            +----------------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
//  1. 'PullUpConstantPredicates': Call this function until find selection and pull up the constant predicate layer by layer
//     LogicalSelection: find the s.a>1
//     LogicalProjection: get the s.a>1 and pull up it, changed to tmp.a>1
//  2. 'addCandidateSelection': Add selection above of LogicalJoin,
//     put all predicates pulled up from the lower layer into the current new selection.
//     LogicalSelection: tmp.a >1
//
// Optimized plan:
/*
            +----------------+
            |  Selection     |
            |    tmp.a>1     |
            +-------^--------+
                    |
            +-------+--------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
// Return nil if the root of plan has not been changed
// Return new root if the root of plan is changed to selection
func (p *LogicalJoin) ConstantPropagation(parentPlan base.LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) (newRoot base.LogicalPlan) {
	// step1: get constant predicate from left or right according to the JoinType
	var getConstantPredicateFromLeft bool
	var getConstantPredicateFromRight bool
	switch p.JoinType {
	case LeftOuterJoin:
		getConstantPredicateFromLeft = true
	case RightOuterJoin:
		getConstantPredicateFromRight = true
	case InnerJoin:
		getConstantPredicateFromLeft = true
		getConstantPredicateFromRight = true
	default:
		return
	}
	var candidateConstantPredicates []expression.Expression
	if getConstantPredicateFromLeft {
		candidateConstantPredicates = p.Children()[0].PullUpConstantPredicates()
	}
	if getConstantPredicateFromRight {
		candidateConstantPredicates = append(candidateConstantPredicates, p.Children()[1].PullUpConstantPredicates()...)
	}
	if len(candidateConstantPredicates) == 0 {
		return
	}

	// step2: add selection above of LogicalJoin
	return addCandidateSelection(p, currentChildIdx, parentPlan, candidateConstantPredicates, opt)
}

// PullUpConstantPredicates inherits the BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits the BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose NDV should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the NDV of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.StatsInfo(), nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	p.EqualCondOutCnt = cardinality.EstimateFullJoinRowCount(p.SCtx(),
		0 == len(p.EqualConditions),
		leftProfile, rightProfile,
		leftJoinKeys, rightJoinKeys,
		childSchema[0], childSchema[1],
		nil, nil)
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount * cost.SelectionFactor,
			ColNDVs:  make(map[int64]float64, len(leftProfile.ColNDVs)),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c * cost.SelectionFactor
		}
		return p.StatsInfo(), nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount,
			ColNDVs:  make(map[int64]float64, selfSchema.Len()),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c
		}
		p.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.StatsInfo(), nil
	}
	count := p.EqualCondOutCnt
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	colNDVs := make(map[int64]float64, selfSchema.Len())
	for id, c := range leftProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	for id, c := range rightProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	p.SetStats(&property.StatsInfo{
		RowCount: count,
		ColNDVs:  colNDVs,
	})
	p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.StatsInfo(), nil
}

// ExtractColGroups implements the base.LogicalPlan.<12th> interface.
func (p *LogicalJoin) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.Column, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == InnerJoin || p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin) {
		extracted = append(extracted, expression.SortColumns(leftJoinKeys), expression.SortColumns(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == RightOuterJoin {
		outerSchema = p.Children()[1].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return extracted
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return extracted
	}
	for _, offset := range offsets {
		extracted = append(extracted, colGroups[offset])
	}
	return extracted
}

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalJoin) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	leftProperties := childrenProperties[0]
	rightProperties := childrenProperties[1]
	// TODO: We should consider properties propagation.
	p.LeftProperties = leftProperties
	p.RightProperties = rightProperties
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin {
		rightProperties = nil
	} else if p.JoinType == RightOuterJoin {
		leftProperties = nil
	}
	resultProperties := make([][]*expression.Column, len(leftProperties)+len(rightProperties))
	for i, cols := range leftProperties {
		resultProperties[i] = make([]*expression.Column, len(cols))
		copy(resultProperties[i], cols)
	}
	leftLen := len(leftProperties)
	for i, cols := range rightProperties {
		resultProperties[leftLen+i] = make([]*expression.Column, len(cols))
		copy(resultProperties[leftLen+i], cols)
	}
	return resultProperties
}

// ExhaustPhysicalPlans implements the base.LogicalPlan.<14th> interface.
// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalJoin(p, prop)
}

// ExtractCorrelatedCols implements the base.LogicalPlan.<15th> interface.
func (p *LogicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// MaxOneRow inherits the BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits the BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits the BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits the BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits the BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits the BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD implements the base.LogicalPlan.<22th> interface.
func (p *LogicalJoin) ExtractFD() *funcdep.FDSet {
	switch p.JoinType {
	case InnerJoin:
		return p.ExtractFDForInnerJoin(nil)
	case LeftOuterJoin, RightOuterJoin:
		return p.ExtractFDForOuterJoin(nil)
	case SemiJoin:
		return p.ExtractFDForSemiJoin(nil)
	default:
		return &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// GetBaseLogicalPlan inherits the BaseLogicalPlan.LogicalPlan.<23th> implementation.

// ConvertOuterToInnerJoin implements base.LogicalPlan.<24th> interface.
func (p *LogicalJoin) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	innerTable := p.Children()[0]
	outerTable := p.Children()[1]
	switchChild := false

	if p.JoinType == LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
		switchChild = true
	}

	// First, simplify this join
	if p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin {
		canBeSimplified := false
		for _, expr := range predicates {
			isOk := util.IsNullRejected(p.SCtx(), innerTable.Schema(), expr)
			if isOk {
				canBeSimplified = true
				break
			}
		}
		if canBeSimplified {
			p.JoinType = InnerJoin
		}
	}

	// Next simplify join children

	combinedCond := mergeOnClausePredicates(p, predicates)
	if p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	} else if p.JoinType == InnerJoin || p.JoinType == SemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else if p.JoinType == AntiSemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	}

	if switchChild {
		p.SetChild(0, outerTable)
		p.SetChild(1, innerTable)
	} else {
		p.SetChild(0, innerTable)
		p.SetChild(1, outerTable)
	}

	return p
}

// *************************** end implementation of logicalPlan interface ***************************

// IsNAAJ checks if the join is a non-adjacent-join.
func (p *LogicalJoin) IsNAAJ() bool {
	return len(p.NAEQConditions) > 0
}

// Shallow copies a LogicalJoin struct.
func (p *LogicalJoin) Shallow() *LogicalJoin {
	join := *p
	return join.Init(p.SCtx(), p.QueryBlockOffset())
}

// ExtractFDForSemiJoin extracts FD for semi join.
func (p *LogicalJoin) ExtractFDForSemiJoin(filtersFromApply []expression.Expression) *funcdep.FDSet {
	// 1: since semi join will keep the part or all rows of the outer table, it's outer FD can be saved.
	// 2: the un-projected column will be left for the upper layer projection or already be pruned from bottom up.
	outerFD, _ := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	fds := outerFD

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, filtersFromApply...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(p.LeftConditions, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	p.SetFDs(fds)
	return fds
}

// ExtractFDForInnerJoin extracts FD for inner join.
func (p *LogicalJoin) ExtractFDForInnerJoin(filtersFromApply []expression.Expression) *funcdep.FDSet {
	leftFD, rightFD := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	fds := leftFD
	fds.MakeCartesianProduct(rightFD)

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	// some join eq conditions are stored in the OtherConditions.
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, filtersFromApply...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), fds)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	// merge the not-null-cols/registered-map from both side together.
	fds.NotNullCols.UnionWith(rightFD.NotNullCols)
	if fds.HashCodeToUniqueID == nil {
		fds.HashCodeToUniqueID = rightFD.HashCodeToUniqueID
	} else {
		for k, v := range rightFD.HashCodeToUniqueID {
			// If there's same constant in the different subquery, we might go into this IF branch.
			if _, ok := fds.HashCodeToUniqueID[k]; ok {
				continue
			}
			fds.HashCodeToUniqueID[k] = v
		}
	}
	for i, ok := rightFD.GroupByCols.Next(0); ok; i, ok = rightFD.GroupByCols.Next(i + 1) {
		fds.GroupByCols.Insert(i)
	}
	fds.HasAggBuilt = fds.HasAggBuilt || rightFD.HasAggBuilt
	p.SetFDs(fds)
	return fds
}

// ExtractFDForOuterJoin extracts FD for outer join.
func (p *LogicalJoin) ExtractFDForOuterJoin(filtersFromApply []expression.Expression) *funcdep.FDSet {
	outerFD, innerFD := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	innerCondition := p.RightConditions
	outerCondition := p.LeftConditions
	outerCols, innerCols := intset.NewFastIntSet(), intset.NewFastIntSet()
	for _, col := range p.Children()[0].Schema().Columns {
		outerCols.Insert(int(col.UniqueID))
	}
	for _, col := range p.Children()[1].Schema().Columns {
		innerCols.Insert(int(col.UniqueID))
	}
	if p.JoinType == RightOuterJoin {
		innerFD, outerFD = outerFD, innerFD
		innerCondition = p.LeftConditions
		outerCondition = p.RightConditions
		innerCols, outerCols = outerCols, innerCols
	}

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, innerCondition...)
	allConds = append(allConds, outerCondition...)
	allConds = append(allConds, filtersFromApply...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	filterFD := &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), filterFD)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), filterFD)

	filterFD.AddConstants(constUniqueIDs)
	equivOuterUniqueIDs := intset.NewFastIntSet()
	equivAcrossNum := 0
	for _, equiv := range equivUniqueIDs {
		filterFD.AddEquivalence(equiv[0], equiv[1])
		if equiv[0].SubsetOf(outerCols) && equiv[1].SubsetOf(innerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[0])
			equivAcrossNum++
			continue
		}
		if equiv[0].SubsetOf(innerCols) && equiv[1].SubsetOf(outerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[1])
			equivAcrossNum++
		}
	}
	filterFD.MakeNotNull(notNullColsFromFilters)

	// pre-perceive the filters for the convenience judgement of 3.3.1.
	var opt funcdep.ArgOpts
	if equivAcrossNum > 0 {
		// find the equivalence FD across left and right cols.
		var outConditionCols []*expression.Column
		if len(outerCondition) != 0 {
			outConditionCols = append(outConditionCols, expression.ExtractColumnsFromExpressions(nil, outerCondition, nil)...)
		}
		if len(p.OtherConditions) != 0 {
			// other condition may contain right side cols, it doesn't affect the judgement of intersection of non-left-equiv cols.
			outConditionCols = append(outConditionCols, expression.ExtractColumnsFromExpressions(nil, p.OtherConditions, nil)...)
		}
		outerConditionUniqueIDs := intset.NewFastIntSet()
		for _, col := range outConditionCols {
			outerConditionUniqueIDs.Insert(int(col.UniqueID))
		}
		// judge whether left filters is on non-left-equiv cols.
		if outerConditionUniqueIDs.Intersects(outerCols.Difference(equivOuterUniqueIDs)) {
			opt.SkipFDRule331 = true
		}
	} else {
		// if there is none across equivalence condition, skip rule 3.3.1.
		opt.SkipFDRule331 = true
	}

	opt.OnlyInnerFilter = len(eqCondSlice) == 0 && len(outerCondition) == 0 && len(p.OtherConditions) == 0
	if opt.OnlyInnerFilter {
		// if one of the inner condition is constant false, the inner side are all null, left make constant all of that.
		for _, one := range innerCondition {
			if c, ok := one.(*expression.Constant); ok && c.DeferredExpr == nil && c.ParamMarker == nil {
				if isTrue, err := c.Value.ToBool(p.SCtx().GetSessionVars().StmtCtx.TypeCtx()); err == nil {
					if isTrue == 0 {
						// c is false
						opt.InnerIsFalse = true
					}
				}
			}
		}
	}

	fds := outerFD
	fds.MakeOuterJoin(innerFD, filterFD, outerCols, innerCols, &opt)
	p.SetFDs(fds)
	return fds
}

// GetJoinKeys extracts join keys(columns) from EqualConditions. It returns left join keys, right
// join keys and an `isNullEQ` array which means the `joinKey[i]` is a `NullEQ` function. The `hasNullEQ`
// means whether there is a `NullEQ` of a join key.
func (p *LogicalJoin) GetJoinKeys() (leftKeys, rightKeys []*expression.Column, isNullEQ []bool, hasNullEQ bool) {
	for _, expr := range p.EqualConditions {
		leftKeys = append(leftKeys, expr.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, expr.GetArgs()[1].(*expression.Column))
		isNullEQ = append(isNullEQ, expr.FuncName.L == ast.NullEQ)
		hasNullEQ = hasNullEQ || expr.FuncName.L == ast.NullEQ
	}
	return
}

// GetNAJoinKeys extracts join keys(columns) from NAEqualCondition.
func (p *LogicalJoin) GetNAJoinKeys() (leftKeys, rightKeys []*expression.Column) {
	for _, expr := range p.NAEQConditions {
		leftKeys = append(leftKeys, expr.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, expr.GetArgs()[1].(*expression.Column))
	}
	return
}

// GetPotentialPartitionKeys return potential partition keys for join, the potential partition keys are
// the join keys of EqualConditions
func (p *LogicalJoin) GetPotentialPartitionKeys() (leftKeys, rightKeys []*property.MPPPartitionColumn) {
	for _, expr := range p.EqualConditions {
		_, coll := expr.CharsetAndCollation()
		collateID := property.GetCollateIDByNameForPartition(coll)
		leftKeys = append(leftKeys, &property.MPPPartitionColumn{Col: expr.GetArgs()[0].(*expression.Column), CollateID: collateID})
		rightKeys = append(rightKeys, &property.MPPPartitionColumn{Col: expr.GetArgs()[1].(*expression.Column), CollateID: collateID})
	}
	return
}

// Decorrelate eliminate the correlated column with if the col is in schema.
func (p *LogicalJoin) Decorrelate(schema *expression.Schema) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.RightConditions {
		p.RightConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.EqualConditions {
		p.EqualConditions[i] = cond.Decorrelate(schema).(*expression.ScalarFunction)
	}
}

// ColumnSubstituteAll is used in projection elimination in apply de-correlation.
// Substitutions for all conditions should be successful, otherwise, we should keep all conditions unchanged.
func (p *LogicalJoin) ColumnSubstituteAll(schema *expression.Schema, exprs []expression.Expression) (hasFail bool) {
	// make a copy of exprs for convenience of substitution (may change/partially change the expr tree)
	cpLeftConditions := make(expression.CNFExprs, len(p.LeftConditions))
	cpRightConditions := make(expression.CNFExprs, len(p.RightConditions))
	cpOtherConditions := make(expression.CNFExprs, len(p.OtherConditions))
	cpEqualConditions := make([]*expression.ScalarFunction, len(p.EqualConditions))
	copy(cpLeftConditions, p.LeftConditions)
	copy(cpRightConditions, p.RightConditions)
	copy(cpOtherConditions, p.OtherConditions)
	copy(cpEqualConditions, p.EqualConditions)

	exprCtx := p.SCtx().GetExprCtx()
	// try to substitute columns in these condition.
	for i, cond := range cpLeftConditions {
		if hasFail, cpLeftConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpRightConditions {
		if hasFail, cpRightConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpOtherConditions {
		if hasFail, cpOtherConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpEqualConditions {
		var tmp expression.Expression
		if hasFail, tmp = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
		cpEqualConditions[i] = tmp.(*expression.ScalarFunction)
	}

	// if all substituted, change them atomically here.
	p.LeftConditions = cpLeftConditions
	p.RightConditions = cpRightConditions
	p.OtherConditions = cpOtherConditions
	p.EqualConditions = cpEqualConditions

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := p.EqualConditions[i]

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		_, lhsIsCol := newCond.GetArgs()[0].(*expression.Column)
		_, rhsIsCol := newCond.GetArgs()[1].(*expression.Column)

		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !(lhsIsCol && rhsIsCol) {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		p.EqualConditions[i] = newCond
	}
	return false
}

// AttachOnConds extracts on conditions for join and set the `EqualConditions`, `LeftConditions`, `RightConditions` and
// `OtherConditions` by the result of extract.
func (p *LogicalJoin) AttachOnConds(onConds []expression.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.AppendJoinConds(eq, left, right, other)
}

// AppendJoinConds appends new join conditions.
func (p *LogicalJoin) AppendJoinConds(eq []*expression.ScalarFunction, left, right, other []expression.Expression) {
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

// ExtractJoinKeys extract join keys as a schema for child with childIdx.
func (p *LogicalJoin) ExtractJoinKeys(childIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[childIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// ExtractUsedCols extracts all the needed columns.
func (p *LogicalJoin) ExtractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	for _, naeqCond := range p.NAEQConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(naeqCond)...)
	}
	lChild := p.Children()[0]
	rChild := p.Children()[1]
	for _, col := range parentUsedCols {
		if lChild.Schema().Contains(col) {
			leftCols = append(leftCols, col)
		} else if rChild.Schema().Contains(col) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

// MergeSchema merge the schema of left and right child of join.
func (p *LogicalJoin) MergeSchema() {
	p.SetSchema(BuildLogicalJoinSchema(p.JoinType, p))
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	if topN == nil {
		return p.Children()[idx].PushDownTopN(nil, opt)
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.Children()[idx].Schema().Contains(col) {
				return p.Children()[idx].PushDownTopN(nil, opt)
			}
		}
	}

	newTopN := LogicalTopN{
		Count:            topN.Count + topN.Offset,
		ByItems:          make([]*util.ByItems, len(topN.ByItems)),
		PreferLimitToCop: topN.PreferLimitToCop,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	appendTopNPushDownJoinTraceStep(p, newTopN, idx, opt)
	return p.Children()[idx].PushDownTopN(newTopN, opt)
}

// Add a new selection between parent plan and current plan with candidate predicates
/*
+-------------+                                    +-------------+
| parentPlan  |                                    | parentPlan  |
+-----^-------+                                    +-----^-------+
      |           --addCandidateSelection--->            |
+-----+-------+                              +-----------+--------------+
| currentPlan |                              |        selection         |
+-------------+                              |   candidate predicate    |
                                             +-----------^--------------+
                                                         |
                                                         |
                                                    +----+--------+
                                                    | currentPlan |
                                                    +-------------+
*/
// If the currentPlan at the top of query plan, return new root plan (selection)
// Else return nil
func addCandidateSelection(currentPlan base.LogicalPlan, currentChildIdx int, parentPlan base.LogicalPlan,
	candidatePredicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (newRoot base.LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.QueryBlockOffset())
	// add selection above of p
	if parentPlan == nil {
		newRoot = selection
	} else {
		parentPlan.SetChild(currentChildIdx, selection)
	}
	selection.SetChildren(currentPlan)
	AppendAddSelectionTraceStep(parentPlan, currentPlan, selection, opt)
	if parentPlan == nil {
		return newRoot
	}
	return nil
}

func (p *LogicalJoin) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 && len(colGroups) > 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// PreferAny checks whether the join type is in the joinFlags.
func (p *LogicalJoin) PreferAny(joinFlags ...uint) bool {
	for _, flag := range joinFlags {
		if p.PreferJoinType&flag > 0 {
			return true
		}
	}
	return false
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	ctx := p.SCtx()
	for _, expr := range conditions {
		// For queries like `select a in (select a from s where s.b = t.b) from t`,
		// if subquery is empty caused by `s.b = t.b`, the result should always be
		// false even if t.a is null or s.a is null. To make this join "empty aware",
		// we should differentiate `t.a = s.a` from other column equal conditions, so
		// we put it into OtherConditions instead of EqualConditions of join.
		if expression.IsEQCondFromIn(expr) {
			otherCond = append(otherCond, expr)
			continue
		}
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			arg0, lOK := binop.GetArgs()[0].(*expression.Column)
			arg1, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if util.IsNullRejected(ctx, leftSchema, expr) && !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if util.IsNullRejected(ctx, rightSchema, expr) && !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					if binop.FuncName.L == ast.EQ {
						cond := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case SemiJoin, InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	return p.ExtractOnCondition(conditions, p.Children()[0].Schema(), p.Children()[1].Schema(), deriveLeft, deriveRight)
}

// SetPreferredJoinTypeAndOrder sets the preferred join type and order for the LogicalJoin.
func (p *LogicalJoin) SetPreferredJoinTypeAndOrder(hintInfo *utilhint.PlanHints) {
	if hintInfo == nil {
		return
	}

	lhsAlias := util.ExtractTableAlias(p.Children()[0], p.QueryBlockOffset())
	rhsAlias := util.ExtractTableAlias(p.Children()[1], p.QueryBlockOffset())
	if hintInfo.IfPreferMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferMergeJoin
	}
	if hintInfo.IfPreferMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferMergeJoin
		p.RightPreferJoinType |= utilhint.PreferMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferNoMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoMergeJoin
		p.RightPreferJoinType |= utilhint.PreferNoMergeJoin
	}
	if hintInfo.IfPreferBroadcastJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferBCJoin
		p.LeftPreferJoinType |= utilhint.PreferBCJoin
	}
	if hintInfo.IfPreferBroadcastJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferBCJoin
		p.RightPreferJoinType |= utilhint.PreferBCJoin
	}
	if hintInfo.IfPreferShuffleJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferShuffleJoin
		p.LeftPreferJoinType |= utilhint.PreferShuffleJoin
	}
	if hintInfo.IfPreferShuffleJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferShuffleJoin
		p.RightPreferJoinType |= utilhint.PreferShuffleJoin
	}
	if hintInfo.IfPreferHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferHashJoin
		p.LeftPreferJoinType |= utilhint.PreferHashJoin
	}
	if hintInfo.IfPreferHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferHashJoin
		p.RightPreferJoinType |= utilhint.PreferHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoHashJoin
		p.LeftPreferJoinType |= utilhint.PreferNoHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoHashJoin
		p.RightPreferJoinType |= utilhint.PreferNoHashJoin
	}
	if hintInfo.IfPreferINLJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLJInner
		p.LeftPreferJoinType |= utilhint.PreferINLJ
	}
	if hintInfo.IfPreferINLJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLJInner
		p.RightPreferJoinType |= utilhint.PreferINLJ
	}
	if hintInfo.IfPreferINLHJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLHJInner
		p.LeftPreferJoinType |= utilhint.PreferINLHJ
	}
	if hintInfo.IfPreferINLHJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLHJInner
		p.RightPreferJoinType |= utilhint.PreferINLHJ
	}
	if hintInfo.IfPreferINLMJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLMJInner
		p.LeftPreferJoinType |= utilhint.PreferINLMJ
	}
	if hintInfo.IfPreferINLMJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLMJInner
		p.RightPreferJoinType |= utilhint.PreferINLMJ
	}
	if hintInfo.IfPreferNoIndexJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexHashJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexHashJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexMergeJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferHJBuild(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsHJBuild
		p.LeftPreferJoinType |= utilhint.PreferHJBuild
	}
	if hintInfo.IfPreferHJBuild(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsHJBuild
		p.RightPreferJoinType |= utilhint.PreferHJBuild
	}
	if hintInfo.IfPreferHJProbe(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsHJProbe
		p.LeftPreferJoinType |= utilhint.PreferHJProbe
	}
	if hintInfo.IfPreferHJProbe(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsHJProbe
		p.RightPreferJoinType |= utilhint.PreferHJProbe
	}
	hasConflict := false
	if !p.SCtx().GetSessionVars().EnableAdvancedJoinHint || p.SCtx().GetSessionVars().StmtCtx.StraightJoinOrder {
		if containDifferentJoinTypes(p.PreferJoinType) {
			hasConflict = true
		}
	} else if p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		if containDifferentJoinTypes(p.LeftPreferJoinType) || containDifferentJoinTypes(p.RightPreferJoinType) {
			hasConflict = true
		}
	}
	if hasConflict {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints are conflict, you can only specify one type of join")
		p.PreferJoinType = 0
	}
	// set the join order
	if hintInfo.LeadingJoinOrder != nil {
		p.PreferJoinOrder = hintInfo.MatchTableName([]*utilhint.HintedTable{lhsAlias, rhsAlias}, hintInfo.LeadingJoinOrder)
	}
	// set hintInfo for further usage if this hint info can be used.
	if p.PreferJoinType != 0 || p.PreferJoinOrder {
		p.HintInfo = hintInfo
	}
}

// SetPreferredJoinType generates hint information for the logicalJoin based on the hint information of its left and right children.
func (p *LogicalJoin) SetPreferredJoinType() {
	if p.LeftPreferJoinType == 0 && p.RightPreferJoinType == 0 {
		return
	}
	p.PreferJoinType = setPreferredJoinTypeFromOneSide(p.LeftPreferJoinType, true) | setPreferredJoinTypeFromOneSide(p.RightPreferJoinType, false)
	if containDifferentJoinTypes(p.PreferJoinType) {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints conflict after join reorder phase, you can only specify one type of join")
		p.PreferJoinType = 0
	}
}

// updateEQCond will extract the arguments of a equal condition that connect two expressions.
func (p *LogicalJoin) updateEQCond() {
	lChild, rChild := p.Children()[0], p.Children()[1]
	var lKeys, rKeys []expression.Expression
	var lNAKeys, rNAKeys []expression.Expression
	// We need two steps here:
	// step1: try best to extract normal EQ condition from OtherCondition to join EqualConditions.
	for i := len(p.OtherConditions) - 1; i >= 0; i-- {
		need2Remove := false
		if eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction); ok && eqCond.FuncName.L == ast.EQ {
			// If it is a column equal condition converted from `[not] in (subq)`, do not move it
			// to EqualConditions, and keep it in OtherConditions. Reference comments in `extractOnCondition`
			// for detailed reasons.
			if expression.IsEQCondFromIn(eqCond) {
				continue
			}
			lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
			if expression.ExprFromSchema(lExpr, lChild.Schema()) && expression.ExprFromSchema(rExpr, rChild.Schema()) {
				lKeys = append(lKeys, lExpr)
				rKeys = append(rKeys, rExpr)
				need2Remove = true
			} else if expression.ExprFromSchema(lExpr, rChild.Schema()) && expression.ExprFromSchema(rExpr, lChild.Schema()) {
				lKeys = append(lKeys, rExpr)
				rKeys = append(rKeys, lExpr)
				need2Remove = true
			}
		}
		if need2Remove {
			p.OtherConditions = append(p.OtherConditions[:i], p.OtherConditions[i+1:]...)
		}
	}
	// eg: explain select * from t1, t3 where t1.a+1 = t3.a;
	// tidb only accept the join key in EqualCondition as a normal column (join OP take granted for that)
	// so once we found the left and right children's schema can supply the all columns in complicated EQ condition that used by left/right key.
	// we will add a layer of projection here to convert the complicated expression of EQ's left or right side to be a normal column.
	adjustKeyForm := func(leftKeys, rightKeys []expression.Expression, isNA bool) {
		if len(leftKeys) > 0 {
			needLProj, needRProj := false, false
			for i := range leftKeys {
				_, lOk := leftKeys[i].(*expression.Column)
				_, rOk := rightKeys[i].(*expression.Column)
				needLProj = needLProj || !lOk
				needRProj = needRProj || !rOk
			}

			var lProj, rProj *LogicalProjection
			if needLProj {
				lProj = p.getProj(0)
			}
			if needRProj {
				rProj = p.getProj(1)
			}
			for i := range leftKeys {
				lKey, rKey := leftKeys[i], rightKeys[i]
				if lProj != nil {
					lKey = lProj.AppendExpr(lKey)
				}
				if rProj != nil {
					rKey = rProj.AppendExpr(rKey)
				}
				eqCond := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), lKey, rKey)
				if isNA {
					p.NAEQConditions = append(p.NAEQConditions, eqCond.(*expression.ScalarFunction))
				} else {
					p.EqualConditions = append(p.EqualConditions, eqCond.(*expression.ScalarFunction))
				}
			}
		}
	}
	adjustKeyForm(lKeys, rKeys, false)

	// Step2: when step1 is finished, then we can determine whether we need to extract NA-EQ from OtherCondition to NAEQConditions.
	// when there are still no EqualConditions, let's try to be a NAAJ.
	// todo: by now, when there is already a normal EQ condition, just keep NA-EQ as other-condition filters above it.
	// eg: select * from stu where stu.name not in (select name from exam where exam.stu_id = stu.id);
	// combination of <stu.name NAEQ exam.name> and <exam.stu_id EQ stu.id> for join key is little complicated for now.
	canBeNAAJ := (p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin) && len(p.EqualConditions) == 0
	if canBeNAAJ && p.SCtx().GetSessionVars().OptimizerEnableNAAJ {
		var otherCond expression.CNFExprs
		for i := 0; i < len(p.OtherConditions); i++ {
			eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction)
			if ok && eqCond.FuncName.L == ast.EQ && expression.IsEQCondFromIn(eqCond) {
				// here must be a EQCondFromIn.
				lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
				if expression.ExprFromSchema(lExpr, lChild.Schema()) && expression.ExprFromSchema(rExpr, rChild.Schema()) {
					lNAKeys = append(lNAKeys, lExpr)
					rNAKeys = append(rNAKeys, rExpr)
				} else if expression.ExprFromSchema(lExpr, rChild.Schema()) && expression.ExprFromSchema(rExpr, lChild.Schema()) {
					lNAKeys = append(lNAKeys, rExpr)
					rNAKeys = append(rNAKeys, lExpr)
				}
				continue
			}
			otherCond = append(otherCond, p.OtherConditions[i])
		}
		p.OtherConditions = otherCond
		// here is for cases like: select (a+1, b*3) not in (select a,b from t2) from t1.
		adjustKeyForm(lNAKeys, rNAKeys, true)
	}
}

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.Children()[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.Init(p.SCtx(), child.QueryBlockOffset())
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.Children()[idx] = proj
	return proj
}

// outerJoinPropConst propagates constant equal and column equal conditions over outer join.
func (p *LogicalJoin) outerJoinPropConst(predicates []expression.Expression) []expression.Expression {
	outerTable := p.Children()[0]
	innerTable := p.Children()[1]
	if p.JoinType == RightOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}
	lenJoinConds := len(p.EqualConditions) + len(p.LeftConditions) + len(p.RightConditions) + len(p.OtherConditions)
	joinConds := make([]expression.Expression, 0, lenJoinConds)
	for _, equalCond := range p.EqualConditions {
		joinConds = append(joinConds, equalCond)
	}
	joinConds = append(joinConds, p.LeftConditions...)
	joinConds = append(joinConds, p.RightConditions...)
	joinConds = append(joinConds, p.OtherConditions...)
	p.EqualConditions = nil
	p.LeftConditions = nil
	p.RightConditions = nil
	p.OtherConditions = nil
	nullSensitive := p.JoinType == AntiLeftOuterSemiJoin || p.JoinType == LeftOuterSemiJoin
	joinConds, predicates = expression.PropConstOverOuterJoin(p.SCtx().GetExprCtx(), joinConds, predicates, outerTable.Schema(), innerTable.Schema(), nullSensitive)
	p.AttachOnConds(joinConds)
	return predicates
}

func mergeOnClausePredicates(p *LogicalJoin, predicates []expression.Expression) []expression.Expression {
	combinedCond := make([]expression.Expression, 0,
		len(p.LeftConditions)+len(p.RightConditions)+
			len(p.EqualConditions)+len(p.OtherConditions)+
			len(predicates))
	combinedCond = append(combinedCond, p.LeftConditions...)
	combinedCond = append(combinedCond, p.RightConditions...)
	combinedCond = append(combinedCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	combinedCond = append(combinedCond, p.OtherConditions...)
	combinedCond = append(combinedCond, predicates...)
	return combinedCond
}

func appendTopNPushDownJoinTraceStep(p *LogicalJoin, topN *LogicalTopN, idx int, opt *optimizetrace.LogicalOptimizeOp) {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v is added and pushed into %v_%v's ",
			topN.TP(), topN.ID(), p.TP(), p.ID()))
		if idx == 0 {
			buffer.WriteString("left ")
		} else {
			buffer.WriteString("right ")
		}
		buffer.WriteString("table")
		return buffer.String()
	}
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's joinType is %v, and all ByItems[", p.TP(), p.ID(), p.JoinType.String()))
		for i, item := range topN.ByItems {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		buffer.WriteString("] contained in ")
		if idx == 0 {
			buffer.WriteString("left ")
		} else {
			buffer.WriteString("right ")
		}
		buffer.WriteString("table")
		return buffer.String()
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

// AppendAddSelectionTraceStep appends a trace step for adding a selection operator.
func AppendAddSelectionTraceStep(p base.LogicalPlan, child base.LogicalPlan, sel *LogicalSelection, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("add %v_%v to connect %v_%v and %v_%v", sel.TP(), sel.ID(), p.TP(), p.ID(), child.TP(), child.ID())
	}
	opt.AppendStepToCurrent(sel.ID(), sel.TP(), reason, action)
}

// containDifferentJoinTypes checks whether `PreferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	preferJoinType &= ^utilhint.PreferNoHashJoin
	preferJoinType &= ^utilhint.PreferNoMergeJoin
	preferJoinType &= ^utilhint.PreferNoIndexJoin
	preferJoinType &= ^utilhint.PreferNoIndexHashJoin
	preferJoinType &= ^utilhint.PreferNoIndexMergeJoin

	inlMask := utilhint.PreferRightAsINLJInner ^ utilhint.PreferLeftAsINLJInner
	inlhjMask := utilhint.PreferRightAsINLHJInner ^ utilhint.PreferLeftAsINLHJInner
	inlmjMask := utilhint.PreferRightAsINLMJInner ^ utilhint.PreferLeftAsINLMJInner
	hjRightBuildMask := utilhint.PreferRightAsHJBuild ^ utilhint.PreferLeftAsHJProbe
	hjLeftBuildMask := utilhint.PreferLeftAsHJBuild ^ utilhint.PreferRightAsHJProbe

	mppMask := utilhint.PreferShuffleJoin ^ utilhint.PreferBCJoin
	mask := inlMask ^ inlhjMask ^ inlmjMask ^ hjRightBuildMask ^ hjLeftBuildMask
	onesCount := bits.OnesCount(preferJoinType & ^mask & ^mppMask)
	if onesCount > 1 || onesCount == 1 && preferJoinType&mask > 0 {
		return true
	}

	cnt := 0
	if preferJoinType&inlMask > 0 {
		cnt++
	}
	if preferJoinType&inlhjMask > 0 {
		cnt++
	}
	if preferJoinType&inlmjMask > 0 {
		cnt++
	}
	if preferJoinType&hjLeftBuildMask > 0 {
		cnt++
	}
	if preferJoinType&hjRightBuildMask > 0 {
		cnt++
	}
	return cnt > 1
}

func setPreferredJoinTypeFromOneSide(preferJoinType uint, isLeft bool) (resJoinType uint) {
	if preferJoinType == 0 {
		return
	}
	if preferJoinType&utilhint.PreferINLJ > 0 {
		preferJoinType &= ^utilhint.PreferINLJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLJInner
		}
	}
	if preferJoinType&utilhint.PreferINLHJ > 0 {
		preferJoinType &= ^utilhint.PreferINLHJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLHJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLHJInner
		}
	}
	if preferJoinType&utilhint.PreferINLMJ > 0 {
		preferJoinType &= ^utilhint.PreferINLMJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLMJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLMJInner
		}
	}
	if preferJoinType&utilhint.PreferHJBuild > 0 {
		preferJoinType &= ^utilhint.PreferHJBuild
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsHJBuild
		} else {
			resJoinType |= utilhint.PreferRightAsHJBuild
		}
	}
	if preferJoinType&utilhint.PreferHJProbe > 0 {
		preferJoinType &= ^utilhint.PreferHJProbe
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsHJProbe
		} else {
			resJoinType |= utilhint.PreferRightAsHJProbe
		}
	}
	resJoinType |= preferJoinType
	return
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin, leftSchema *expression.Schema, rightSchema *expression.Schema,
	deriveLeft bool, deriveRight bool) (
	leftCond []expression.Expression, rightCond []expression.Expression) {
	isOuterSemi := (p.JoinType == LeftOuterSemiJoin) || (p.JoinType == AntiLeftOuterSemiJoin)
	ctx := p.SCtx()
	exprCtx := ctx.GetExprCtx()
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, leftSchema)
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, leftSchema)
			if notNullExpr != nil {
				leftCond = append(leftCond, notNullExpr)
			}
		}
		if deriveRight {
			rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, rightSchema)
			if rightRelaxedCond != nil {
				rightCond = append(rightCond, rightRelaxedCond)
			}
			// For LeftOuterSemiJoin and AntiLeftOuterSemiJoin, we can actually generate
			// `col is not null` according to expressions in `OtherConditions` now, but we
			// are putting column equal condition converted from `in (subq)` into
			// `OtherConditions`(@sa https://github.com/pingcap/tidb/pull/9051), then it would
			// cause wrong results, so we disable this optimization for outer semi joins now.
			// TODO enable this optimization for outer semi joins later by checking whether
			// condition in `OtherConditions` is converted from `in (subq)`.
			if isOuterSemi {
				continue
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, rightSchema)
			if notNullExpr != nil {
				rightCond = append(rightCond, notNullExpr)
			}
		}
	}
	return
}

// deriveNotNullExpr generates a new expression `not(isnull(col))` given `col1 op col2`,
// in which `col` is in specified schema. Caller guarantees that only one of `col1` or
// `col2` is in schema.
func deriveNotNullExpr(ctx base.PlanContext, expr expression.Expression, schema *expression.Schema) expression.Expression {
	binop, ok := expr.(*expression.ScalarFunction)
	if !ok || len(binop.GetArgs()) != 2 {
		return nil
	}
	arg0, lOK := binop.GetArgs()[0].(*expression.Column)
	arg1, rOK := binop.GetArgs()[1].(*expression.Column)
	if !lOK || !rOK {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if util.IsNullRejected(ctx, schema, expr) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx.GetExprCtx(), childCol)
	}
	return nil
}

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p base.LogicalPlan, conds []expression.Expression) base.LogicalPlan {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault()); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// BuildLogicalJoinSchema builds the schema for join operator.
func BuildLogicalJoinSchema(joinType JoinType, join base.LogicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return leftSchema.Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == LeftOuterJoin {
		util.ResetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == RightOuterJoin {
		util.ResetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}
