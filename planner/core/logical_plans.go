// Copyright 2016 PingCAP, Inc.
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

package core

import (
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	fd "github.com/pingcap/tidb/planner/funcdep"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

var (
	_ LogicalPlan = &LogicalJoin{}
	_ LogicalPlan = &LogicalAggregation{}
	_ LogicalPlan = &LogicalProjection{}
	_ LogicalPlan = &LogicalSelection{}
	_ LogicalPlan = &LogicalApply{}
	_ LogicalPlan = &LogicalMaxOneRow{}
	_ LogicalPlan = &LogicalTableDual{}
	_ LogicalPlan = &DataSource{}
	_ LogicalPlan = &TiKVSingleGather{}
	_ LogicalPlan = &LogicalTableScan{}
	_ LogicalPlan = &LogicalIndexScan{}
	_ LogicalPlan = &LogicalUnionAll{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalLock{}
	_ LogicalPlan = &LogicalLimit{}
	_ LogicalPlan = &LogicalWindow{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
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

const (
	preferLeftAsINLJInner uint = 1 << iota
	preferRightAsINLJInner
	preferLeftAsINLHJInner
	preferRightAsINLHJInner
	preferLeftAsINLMJInner
	preferRightAsINLMJInner
	preferHashJoin
	preferMergeJoin
	preferBCJoin
	preferHashAgg
	preferStreamAgg
)

const (
	preferTiKV = 1 << iota
	preferTiFlash
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	logicalSchemaProducer

	JoinType      JoinType
	reordered     bool
	cartesianJoin bool
	StraightJoin  bool

	// hintInfo stores the join algorithm hint information specified by client.
	hintInfo        *tableHintInfo
	preferJoinType  uint
	preferJoinOrder bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	leftProperties  [][]*expression.Column
	rightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// fullSchema contains all the columns that the Join can output. It's ordered as [outer schema..., inner schema...].
	// This is useful for natural joins and "using" joins. In these cases, the join key columns from the
	// inner side (or the right side when it's an inner join) will not be in the schema of Join.
	// But upper operators should be able to find those "redundant" columns, and the user also can specifically select
	// those columns, so we put the "redundant" columns here to make them be able to be found.
	//
	// For example:
	// create table t1(a int, b int); create table t2(a int, b int);
	// select * from t1 join t2 using (b);
	// schema of the Join will be [t1.b, t1.a, t2.a]; fullSchema will be [t1.a, t1.b, t2.a, t2.b].
	//
	// We record all columns and keep them ordered is for correctly handling SQLs like
	// select t1.*, t2.* from t1 join t2 using (b);
	// (*PlanBuilder).unfoldWildStar() handles the schema for such case.
	fullSchema *expression.Schema
	fullNames  types.NameSlice

	// equalCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	equalCondOutCnt float64
}

// Shallow shallow copies a LogicalJoin struct.
func (p *LogicalJoin) Shallow() *LogicalJoin {
	join := *p
	return join.Init(p.ctx, p.blockOffset)
}

// ExtractFD implements the interface LogicalPlan.
func (p *LogicalJoin) ExtractFD() *fd.FDSet {
	switch p.JoinType {
	case InnerJoin:
		return p.extractFDForInnerJoin(nil)
	case LeftOuterJoin, RightOuterJoin:
		return p.extractFDForOuterJoin(nil)
	case SemiJoin:
		return p.extractFDForSemiJoin(nil)
	default:
		return &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

func (p *LogicalJoin) extractFDForSemiJoin(filtersFromApply []expression.Expression) *fd.FDSet {
	// 1: since semi join will keep the part or all rows of the outer table, it's outer FD can be saved.
	// 2: the un-projected column will be left for the upper layer projection or already be pruned from bottom up.
	outerFD, _ := p.children[0].ExtractFD(), p.children[1].ExtractFD()
	fds := outerFD

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, filtersFromApply...)
	notNullColsFromFilters := extractNotNullFromConds(allConds, p)

	constUniqueIDs := extractConstantCols(p.LeftConditions, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	p.fdSet = fds
	return fds
}

func (p *LogicalJoin) extractFDForInnerJoin(filtersFromApply []expression.Expression) *fd.FDSet {
	leftFD, rightFD := p.children[0].ExtractFD(), p.children[1].ExtractFD()
	fds := leftFD
	fds.MakeCartesianProduct(rightFD)

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	// some join eq conditions are stored in the OtherConditions.
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, filtersFromApply...)
	notNullColsFromFilters := extractNotNullFromConds(allConds, p)

	constUniqueIDs := extractConstantCols(allConds, p.SCtx(), fds)

	equivUniqueIDs := extractEquivalenceCols(allConds, p.SCtx(), fds)

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
	p.fdSet = fds
	return fds
}

func (p *LogicalJoin) extractFDForOuterJoin(filtersFromApply []expression.Expression) *fd.FDSet {
	outerFD, innerFD := p.children[0].ExtractFD(), p.children[1].ExtractFD()
	innerCondition := p.RightConditions
	outerCondition := p.LeftConditions
	outerCols, innerCols := fd.NewFastIntSet(), fd.NewFastIntSet()
	for _, col := range p.children[0].Schema().Columns {
		outerCols.Insert(int(col.UniqueID))
	}
	for _, col := range p.children[1].Schema().Columns {
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
	notNullColsFromFilters := extractNotNullFromConds(allConds, p)

	filterFD := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}

	constUniqueIDs := extractConstantCols(allConds, p.SCtx(), filterFD)

	equivUniqueIDs := extractEquivalenceCols(allConds, p.SCtx(), filterFD)

	filterFD.AddConstants(constUniqueIDs)
	equivOuterUniqueIDs := fd.NewFastIntSet()
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
	var opt fd.ArgOpts
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
		outerConditionUniqueIDs := fd.NewFastIntSet()
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
				if isTrue, err := c.Value.ToBool(p.ctx.GetSessionVars().StmtCtx); err == nil {
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
	p.fdSet = fds
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

func (p *LogicalJoin) columnSubstitute(schema *expression.Schema, exprs []expression.Expression) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i, cond := range p.RightConditions {
		p.RightConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := expression.ColumnSubstitute(p.EqualConditions[i], schema, exprs).(*expression.ScalarFunction)

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.children[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.children[1].Schema()) {
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

// ExtractCorrelatedCols implements LogicalPlan interface.
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

// ExtractJoinKeys extract join keys as a schema for child with childIdx.
func (p *LogicalJoin) ExtractJoinKeys(childIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[childIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []expression.Expression

	// CalculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	CalculateNoDelay bool

	// AvoidColumnEvaluator is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the expressions of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	AvoidColumnEvaluator bool
}

// ExtractFD implements the logical plan interface, extracting the FD from bottom up.
func (p *LogicalProjection) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := p.logicalSchemaProducer.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := fd.NewFastIntSet()
	notnullColsUniqueIDs := fd.NewFastIntSet()
	outputColsUniqueIDsArray := make([]int, 0, len(p.Schema().Columns))
	// here schema extended columns may contain expr, const and column allocated with uniqueID.
	for _, one := range p.Schema().Columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
		outputColsUniqueIDsArray = append(outputColsUniqueIDsArray, int(one.UniqueID))
	}
	// map the expr hashCode with its unique ID.
	for idx, expr := range p.Exprs {
		switch x := expr.(type) {
		case *expression.Column:
			continue
		case *expression.CorrelatedColumn:
			// t1(a,b,c), t2(m,n)
			// select a, (select c from t2 where m=b) from t1;
			// take c as constant column here.
			continue
		case *expression.Constant:
			hashCode := string(x.HashCode(p.ctx.GetSessionVars().StmtCtx))
			var (
				ok               bool
				constantUniqueID int
			)
			if constantUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
				constantUniqueID = outputColsUniqueIDsArray[idx]
				fds.RegisterUniqueID(string(x.HashCode(p.ctx.GetSessionVars().StmtCtx)), constantUniqueID)
			}
			fds.AddConstants(fd.NewFastIntSet(constantUniqueID))
		case *expression.ScalarFunction:
			// t1(a,b,c), t2(m,n)
			// select a, (select c+n from t2 where m=b) from t1;
			// expr(c+n) contains correlated column , but we can treat it as constant here.
			hashCode := string(x.HashCode(p.ctx.GetSessionVars().StmtCtx))
			var (
				ok             bool
				scalarUniqueID int
			)
			// If this function is not deterministic, we skip it since it not a stable value.
			if expression.CheckNonDeterministic(x) {
				if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
					fds.RegisterUniqueID(hashCode, scalarUniqueID)
				}
				continue
			}
			if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
				scalarUniqueID = outputColsUniqueIDsArray[idx]
				fds.RegisterUniqueID(hashCode, scalarUniqueID)
			} else {
				// since the scalar's hash code has been registered before, the equivalence exists between the unique ID
				// allocated by phase of building-projection-for-scalar and that of previous registered unique ID.
				fds.AddEquivalence(fd.NewFastIntSet(scalarUniqueID), fd.NewFastIntSet(outputColsUniqueIDsArray[idx]))
			}
			determinants := fd.NewFastIntSet()
			extractedColumns := expression.ExtractColumns(x)
			extractedCorColumns := expression.ExtractCorColumns(x)
			for _, one := range extractedColumns {
				determinants.Insert(int(one.UniqueID))
				// the dependent columns in scalar function should be also considered as output columns as well.
				outputColsUniqueIDs.Insert(int(one.UniqueID))
			}
			for _, one := range extractedCorColumns {
				determinants.Insert(int(one.UniqueID))
				// the dependent columns in scalar function should be also considered as output columns as well.
				outputColsUniqueIDs.Insert(int(one.UniqueID))
			}
			notnull := isNullRejected(p.ctx, p.schema, x)
			if notnull || determinants.SubsetOf(fds.NotNullCols) {
				notnullColsUniqueIDs.Insert(scalarUniqueID)
			}
			fds.AddStrictFunctionalDependency(determinants, fd.NewFastIntSet(scalarUniqueID))
		}
	}

	// apply operator's characteristic's FD setting.
	// since the distinct attribute is built as firstRow agg func, we don't need to think about it here.
	// let the fds itself to trace the not null, because after the outer join, some not null column can be nullable.
	fds.MakeNotNull(notnullColsUniqueIDs)
	// select max(a) from t group by b, we should project both `a` & `b` to maintain the FD down here, even if select-fields only contain `a`.
	fds.ProjectCols(outputColsUniqueIDs.Union(fds.GroupByCols))
	// just trace it down in every operator for test checking.
	p.fdSet = fds
	return fds
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (p *LogicalProjection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// GetUsedCols extracts all of the Columns used by proj.
func (p *LogicalProjection) GetUsedCols() (usedCols []*expression.Column) {
	for _, expr := range p.Exprs {
		usedCols = append(usedCols, expression.ExtractColumns(expr)...)
	}
	return usedCols
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression

	// aggHints stores aggregation hint information.
	aggHints aggHintInfo

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.

	// noCopPushDown indicates if planner must not push this agg down to coprocessor.
	// It is true when the agg is in the outer child tree of apply.
	noCopPushDown bool
}

// HasDistinct shows whether LogicalAggregation has functions with distinct.
func (la *LogicalAggregation) HasDistinct() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			return true
		}
	}
	return false
}

// HasOrderBy shows whether LogicalAggregation has functions with order-by items.
func (la *LogicalAggregation) HasOrderBy() bool {
	for _, aggFunc := range la.AggFuncs {
		if len(aggFunc.OrderByItems) > 0 {
			return true
		}
	}
	return false
}

// ExtractFD implements the logical plan interface, extracting the FD from bottom up.
// 1:
// In most of the cases, using FDs to check the only_full_group_by problem should be done in the buildAggregation phase
// by extracting the bottom-up FDs graph from the `p` --- the sub plan tree that has already been built.
//
// 2:
// and this requires that some conditions push-down into the `p` like selection should be done before building aggregation,
// otherwise, 'a=1 and a can occur in the select lists of a group by' will be miss-checked because it doesn't be implied in the known FDs graph.
//
// 3:
// when a logical agg is built, it's schema columns indicates what the permitted-non-agg columns is. Therefore, we shouldn't
// depend on logicalAgg.ExtractFD() to finish the only_full_group_by checking problem rather than by 1 & 2.
func (la *LogicalAggregation) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := la.logicalSchemaProducer.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := fd.NewFastIntSet()
	notnullColsUniqueIDs := fd.NewFastIntSet()
	groupByColsUniqueIDs := fd.NewFastIntSet()
	groupByColsOutputCols := fd.NewFastIntSet()
	// Since the aggregation is build ahead of projection, the latter one will reuse the column with UniqueID allocated in aggregation
	// via aggMapper, so we don't need unnecessarily maintain the <aggDes, UniqueID> mapping in the FDSet like expr did, just treating
	// it as normal column.
	for _, one := range la.Schema().Columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
	}
	// For one like sum(a), we don't need to build functional dependency from a --> sum(a), cause it's only determined by the
	// group-by-item (group-by-item --> sum(a)).
	for _, expr := range la.GroupByItems {
		switch x := expr.(type) {
		case *expression.Column:
			groupByColsUniqueIDs.Insert(int(x.UniqueID))
		case *expression.CorrelatedColumn:
			// shouldn't be here, intercepted by plan builder as unknown column.
			continue
		case *expression.Constant:
			// shouldn't be here, interpreted as pos param by plan builder.
			continue
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode(la.ctx.GetSessionVars().StmtCtx))
			var (
				ok             bool
				scalarUniqueID int
			)
			if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); ok {
				groupByColsUniqueIDs.Insert(scalarUniqueID)
			} else {
				// retrieve unique plan column id.  1: completely new one, allocating new unique id. 2: registered by projection earlier, using it.
				if scalarUniqueID, ok = la.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol[hashCode]; !ok {
					scalarUniqueID = int(la.ctx.GetSessionVars().AllocPlanColumnID())
				}
				fds.RegisterUniqueID(hashCode, scalarUniqueID)
				groupByColsUniqueIDs.Insert(scalarUniqueID)
			}
			determinants := fd.NewFastIntSet()
			extractedColumns := expression.ExtractColumns(x)
			extractedCorColumns := expression.ExtractCorColumns(x)
			for _, one := range extractedColumns {
				determinants.Insert(int(one.UniqueID))
				groupByColsOutputCols.Insert(int(one.UniqueID))
			}
			for _, one := range extractedCorColumns {
				determinants.Insert(int(one.UniqueID))
				groupByColsOutputCols.Insert(int(one.UniqueID))
			}
			notnull := isNullRejected(la.ctx, la.schema, x)
			if notnull || determinants.SubsetOf(fds.NotNullCols) {
				notnullColsUniqueIDs.Insert(scalarUniqueID)
			}
			fds.AddStrictFunctionalDependency(determinants, fd.NewFastIntSet(scalarUniqueID))
		}
	}

	// Some details:
	// For now, select max(a) from t group by c, tidb will see `max(a)` as Max aggDes and `a,b,c` as firstRow aggDes,
	// and keep them all in the schema columns before projection does the pruning. If we build the fake FD eg: {c} ~~> {b}
	// here since we have seen b as firstRow aggDes, for the upper layer projection of `select max(a), b from t group by c`,
	// it will take b as valid projection field of group by statement since it has existed in the FD with {c} ~~> {b}.
	//
	// and since any_value will NOT be pushed down to agg schema, which means every firstRow aggDes in the agg logical operator
	// is meaningless to build the FD with. Let's only store the non-firstRow FD down: {group by items} ~~> {real aggDes}
	realAggFuncUniqueID := fd.NewFastIntSet()
	for i, aggDes := range la.AggFuncs {
		if aggDes.Name != "firstrow" {
			realAggFuncUniqueID.Insert(int(la.schema.Columns[i].UniqueID))
		}
	}

	// apply operator's characteristic's FD setting.
	if len(la.GroupByItems) == 0 {
		// 1: as the details shown above, output cols (normal column seen as firstrow) of group by are not validated.
		// we couldn't merge them as constant FD with origin constant FD together before projection done.
		// fds.MaxOneRow(outputColsUniqueIDs.Union(groupByColsOutputCols))
		//
		// 2: for the convenience of later judgement, when there is no group by items, we will store a FD: {0} -> {real aggDes}
		// 0 unique id is only used for here.
		groupByColsUniqueIDs.Insert(0)
		for i, ok := realAggFuncUniqueID.Next(0); ok; i, ok = realAggFuncUniqueID.Next(i + 1) {
			fds.AddStrictFunctionalDependency(groupByColsUniqueIDs, fd.NewFastIntSet(i))
		}
	} else {
		// eliminating input columns that are un-projected.
		fds.ProjectCols(outputColsUniqueIDs.Union(groupByColsOutputCols).Union(groupByColsUniqueIDs))

		// note: {a} --> {b,c} is not same with {a} --> {b} and {a} --> {c}
		for i, ok := realAggFuncUniqueID.Next(0); ok; i, ok = realAggFuncUniqueID.Next(i + 1) {
			// group by phrase always produce strict FD.
			// 1: it can always distinguish and group the all-null/part-null group column rows.
			// 2: the rows with all/part null group column are unique row after group operation.
			// 3: there won't be two same group key with different agg values, so strict FD secured.
			fds.AddStrictFunctionalDependency(groupByColsUniqueIDs, fd.NewFastIntSet(i))
		}

		// agg funcDes has been tag not null flag when building aggregation.
		fds.MakeNotNull(notnullColsUniqueIDs)
	}
	fds.GroupByCols = groupByColsUniqueIDs
	fds.HasAggBuilt = true
	// just trace it down in every operator for test checking.
	la.fdSet = fds
	return fds
}

// CopyAggHints copies the aggHints from another LogicalAggregation.
func (la *LogicalAggregation) CopyAggHints(agg *LogicalAggregation) {
	// TODO: Copy the hint may make the un-applicable hint throw the
	// same warning message more than once. We'd better add a flag for
	// `HaveThrownWarningMessage` to avoid this. Besides, finalAgg and
	// partialAgg (in cascades planner) should share the same hint, instead
	// of a copy.
	la.aggHints = agg.aggHints
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// IsCompleteModeAgg returns if all of the AggFuncs are CompleteMode.
func (la *LogicalAggregation) IsCompleteModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.CompleteMode
}

// GetGroupByCols returns the columns that are group-by items.
// For example, `group by a, b, c+d` will return [a, b].
func (la *LogicalAggregation) GetGroupByCols() []*expression.Column {
	groupByCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, col)
		}
	}
	return groupByCols
}

// GetPotentialPartitionKeys return potential partition keys for aggregation, the potential partition keys are the group by keys
func (la *LogicalAggregation) GetPotentialPartitionKeys() []*property.MPPPartitionColumn {
	groupByCols := make([]*property.MPPPartitionColumn, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, &property.MPPPartitionColumn{
				Col:       col,
				CollateID: property.GetCollateIDByNameForPartition(col.GetType().GetCollate()),
			})
		}
	}
	return groupByCols
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (la *LogicalAggregation) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(la.GroupByItems)+len(la.AggFuncs))
	for _, expr := range la.GroupByItems {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, fun := range la.AggFuncs {
		for _, arg := range fun.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
		for _, arg := range fun.OrderByItems {
			corCols = append(corCols, expression.ExtractCorColumns(arg.Expr)...)
		}
	}
	return corCols
}

// GetUsedCols extracts all of the Columns used by agg including GroupByItems and AggFuncs.
func (la *LogicalAggregation) GetUsedCols() (usedCols []*expression.Column) {
	for _, groupByItem := range la.GroupByItems {
		usedCols = append(usedCols, expression.ExtractColumns(groupByItem)...)
	}
	for _, aggDesc := range la.AggFuncs {
		for _, expr := range aggDesc.Args {
			usedCols = append(usedCols, expression.ExtractColumns(expr)...)
		}
		for _, expr := range aggDesc.OrderByItems {
			usedCols = append(usedCols, expression.ExtractColumns(expr.Expr)...)
		}
	}
	return usedCols
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

func extractNotNullFromConds(Conditions []expression.Expression, p LogicalPlan) fd.FastIntSet {
	// extract the column NOT NULL rejection characteristic from selection condition.
	// CNF considered only, DNF doesn't have its meanings (cause that condition's eval may don't take effect)
	//
	// Take this case: select * from t where (a = 1) and (b is null):
	//
	// If we wanna where phrase eval to true, two pre-condition: {a=1} and {b is null} both need to be true.
	// Hence, we assert that:
	//
	// 1: `a` must not be null since `NULL = 1` is evaluated as NULL.
	// 2: `b` must be null since only `NULL is NULL` is evaluated as true.
	//
	// As a result,	`a` will be extracted as not-null column to abound the FDSet.
	notnullColsUniqueIDs := fd.NewFastIntSet()
	for _, condition := range Conditions {
		var cols []*expression.Column
		cols = expression.ExtractColumnsFromExpressions(cols, []expression.Expression{condition}, nil)
		if isNullRejected(p.SCtx(), p.Schema(), condition) {
			for _, col := range cols {
				notnullColsUniqueIDs.Insert(int(col.UniqueID))
			}
		}
	}
	return notnullColsUniqueIDs
}

func extractConstantCols(Conditions []expression.Expression, sctx sessionctx.Context, fds *fd.FDSet) fd.FastIntSet {
	// extract constant cols
	// eg: where a=1 and b is null and (1+c)=5.
	// TODO: Some columns can only be determined to be constant from multiple constraints (e.g. x <= 1 AND x >= 1)
	var (
		constObjs      []expression.Expression
		constUniqueIDs = fd.NewFastIntSet()
	)
	constObjs = expression.ExtractConstantEqColumnsOrScalar(sctx, constObjs, Conditions)
	for _, constObj := range constObjs {
		switch x := constObj.(type) {
		case *expression.Column:
			constUniqueIDs.Insert(int(x.UniqueID))
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode(sctx.GetSessionVars().StmtCtx))
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				constUniqueIDs.Insert(uniqueID)
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode(sctx.GetSessionVars().StmtCtx)), scalarUniqueID)
				constUniqueIDs.Insert(scalarUniqueID)
			}
		}
	}
	return constUniqueIDs
}

func extractEquivalenceCols(Conditions []expression.Expression, sctx sessionctx.Context, fds *fd.FDSet) [][]fd.FastIntSet {
	var equivObjsPair [][]expression.Expression
	equivObjsPair = expression.ExtractEquivalenceColumns(equivObjsPair, Conditions)
	equivUniqueIDs := make([][]fd.FastIntSet, 0, len(equivObjsPair))
	for _, equivObjPair := range equivObjsPair {
		// lhs of equivalence.
		var (
			lhsUniqueID int
			rhsUniqueID int
		)
		switch x := equivObjPair[0].(type) {
		case *expression.Column:
			lhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode(sctx.GetSessionVars().StmtCtx))
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				lhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode(sctx.GetSessionVars().StmtCtx)), scalarUniqueID)
				lhsUniqueID = scalarUniqueID
			}
		}
		// rhs of equivalence.
		switch x := equivObjPair[1].(type) {
		case *expression.Column:
			rhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode(sctx.GetSessionVars().StmtCtx))
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				rhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode(sctx.GetSessionVars().StmtCtx)), scalarUniqueID)
				rhsUniqueID = scalarUniqueID
			}
		}
		equivUniqueIDs = append(equivUniqueIDs, []fd.FastIntSet{fd.NewFastIntSet(lhsUniqueID), fd.NewFastIntSet(rhsUniqueID)})
	}
	return equivUniqueIDs
}

// ExtractFD implements the LogicalPlan interface.
func (p *LogicalSelection) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := p.baseLogicalPlan.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := fd.NewFastIntSet()
	notnullColsUniqueIDs := fd.NewFastIntSet()
	// eg: select t2.a, count(t2.b) from t1 join t2 using (a) where t1.a = 1
	// join's schema will miss t2.a while join.full schema has. since selection
	// itself doesn't contain schema, extracting schema should tell them apart.
	var columns []*expression.Column
	if join, ok := p.children[0].(*LogicalJoin); ok && join.fullSchema != nil {
		columns = join.fullSchema.Columns
	} else {
		columns = p.Schema().Columns
	}
	for _, one := range columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
	}

	// extract the not null attributes from selection conditions.
	notnullColsUniqueIDs.UnionWith(extractNotNullFromConds(p.Conditions, p))

	// extract the constant cols from selection conditions.
	constUniqueIDs := extractConstantCols(p.Conditions, p.SCtx(), fds)

	// extract equivalence cols.
	equivUniqueIDs := extractEquivalenceCols(p.Conditions, p.SCtx(), fds)

	// apply operator's characteristic's FD setting.
	fds.MakeNotNull(notnullColsUniqueIDs)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	fds.ProjectCols(outputColsUniqueIDs)
	// just trace it down in every operator for test checking.
	p.fdSet = fds
	return fds
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (p *LogicalSelection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	CorCols []*expression.CorrelatedColumn
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (la *LogicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.LogicalJoin.ExtractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

// ExtractFD implements the LogicalPlan interface.
func (la *LogicalApply) ExtractFD() *fd.FDSet {
	innerPlan := la.children[1]
	// build the join correlated equal condition for apply join, this equal condition is used for deriving the transitive FD between outer and inner side.
	correlatedCols := ExtractCorrelatedCols4LogicalPlan(innerPlan)
	deduplicateCorrelatedCols := make(map[int64]*expression.CorrelatedColumn)
	for _, cc := range correlatedCols {
		if _, ok := deduplicateCorrelatedCols[cc.UniqueID]; !ok {
			deduplicateCorrelatedCols[cc.UniqueID] = cc
		}
	}
	eqCond := make([]expression.Expression, 0, 4)
	// for case like select (select t1.a from t2) from t1. <t1.a> will be assigned with new UniqueID after sub query projection is built.
	// we should distinguish them out, building the equivalence relationship from inner <t1.a> == outer <t1.a> in the apply-join for FD derivation.
	for _, cc := range deduplicateCorrelatedCols {
		// for every correlated column, find the connection with the inner newly built column.
		for _, col := range innerPlan.Schema().Columns {
			if cc.UniqueID == col.CorrelatedColUniqueID {
				ccc := &cc.Column
				cond := expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ccc, col)
				eqCond = append(eqCond, cond.(*expression.ScalarFunction))
			}
		}
	}
	switch la.JoinType {
	case InnerJoin:
		return la.extractFDForInnerJoin(eqCond)
	case LeftOuterJoin, RightOuterJoin:
		return la.extractFDForOuterJoin(eqCond)
	case SemiJoin:
		return la.extractFDForSemiJoin(eqCond)
	default:
		return &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// LogicalMaxOneRow checks if a query returns no more than one row.
type LogicalMaxOneRow struct {
	baseLogicalPlan
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	logicalSchemaProducer

	RowCount int
}

// LogicalMemTable represents a memory table or virtual table
// Some memory tables wants to take the ownership of some predications
// e.g
// SELECT * FROM cluster_log WHERE type='tikv' AND address='192.16.5.32'
// Assume that the table `cluster_log` is a memory table, which is used
// to retrieve logs from remote components. In the above situation we should
// send log search request to the target TiKV (192.16.5.32) directly instead of
// requesting all cluster components log search gRPC interface to retrieve
// log message and filtering them in TiDB node.
type LogicalMemTable struct {
	logicalSchemaProducer

	Extractor MemTablePredicateExtractor
	DBName    model.CIStr
	TableInfo *model.TableInfo
	Columns   []*model.ColumnInfo
	// QueryTimeRange is used to specify the time range for metrics summary tables and inspection tables
	// e.g: select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary_by_label;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_result;
	QueryTimeRange QueryTimeRange
}

// LogicalUnionScan is used in non read-only txn or for scanning a local temporary table whose snapshot data is located in memory.
type LogicalUnionScan struct {
	baseLogicalPlan

	conditions []expression.Expression

	handleCols HandleCols
}

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	astIndexHints []*ast.IndexHint
	IndexHints    []indexHintInfo
	table         table.Table
	tableInfo     *model.TableInfo
	Columns       []*model.ColumnInfo
	DBName        model.CIStr

	TableAsName *model.CIStr
	// indexMergeHints are the hint for indexmerge.
	indexMergeHints []indexHintInfo
	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used in partition pruning/index merge.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*util.AccessPath

	// The data source may be a partition, rather than a real table.
	isPartition     bool
	physicalTableID int64
	partitionNames  []model.CIStr

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	// handleCol *expression.Column
	handleCols HandleCols
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// commonHandleCols and commonHandleLens save the info of primary key which is the clustered index.
	commonHandleCols []*expression.Column
	commonHandleLens []int
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from statisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl
	// preferStoreType means the DataSource is enforced to which storage.
	preferStoreType int
	// preferPartitions store the map, the key represents store type, the value represents the partition name list.
	preferPartitions map[int][]model.CIStr
	SampleInfo       *TableSampleInfo
	is               infoschema.InfoSchema
	// isForUpdateRead should be true in either of the following situations
	// 1. use `inside insert`, `update`, `delete` or `select for update` statement
	// 2. isolation level is RC
	isForUpdateRead bool

	// contain unique index and the first field is tidb_shard(),
	// such as (tidb_shard(a), a ...), the fields are more than 2
	containExprPrefixUk bool
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (ds *DataSource) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ds.pushedDownConds))
	for _, expr := range ds.pushedDownConds {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// TiKVSingleGather is a leaf logical operator of TiDB layer to gather
// tuples from TiKV regions.
type TiKVSingleGather struct {
	logicalSchemaProducer
	Source *DataSource
	// IsIndexGather marks if this TiKVSingleGather gathers tuples from an IndexScan.
	// in implementation phase, we need this flag to determine whether to generate
	// PhysicalTableReader or PhysicalIndexReader.
	IsIndexGather bool
	Index         *model.IndexInfo
}

// LogicalTableScan is the logical table scan operator for TiKV.
type LogicalTableScan struct {
	logicalSchemaProducer
	Source      *DataSource
	HandleCols  HandleCols
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range
}

// LogicalIndexScan is the logical index scan operator for TiKV.
type LogicalIndexScan struct {
	logicalSchemaProducer
	// DataSource should be read-only here.
	Source       *DataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range

	Index          *model.IndexInfo
	Columns        []*model.ColumnInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
}

// MatchIndexProp checks if the indexScan can match the required property.
func (p *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsSortItemEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	for i, col := range p.IdxCols {
		if col.Equal(nil, prop.SortItems[0].Col) {
			return matchIndicesProp(p.IdxCols[i:], p.IdxColLens[i:], prop.SortItems)
		} else if i >= p.EqCondCount {
			break
		}
	}
	return false
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath() {
			return path
		}
	}
	return nil
}

func (ds *DataSource) buildTableGather() LogicalPlan {
	ts := LogicalTableScan{Source: ds, HandleCols: ds.handleCols}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.Schema())
	sg := TiKVSingleGather{Source: ds, IsIndexGather: false}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *util.AccessPath) LogicalPlan {
	is := LogicalIndexScan{
		Source:         ds,
		IsDoubleRead:   false,
		Index:          path.Index,
		FullIdxCols:    path.FullIdxCols,
		FullIdxColLens: path.FullIdxColLens,
		IdxCols:        path.IdxCols,
		IdxColLens:     path.IdxColLens,
	}.Init(ds.ctx, ds.blockOffset)

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.schema.Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical TiKVSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []LogicalPlan) {
	tg := ds.buildTableGather()
	gathers = append(gathers, tg)
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
			path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if ds.isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

func (ds *DataSource) deriveCommonHandleTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.Ranges = ranger.FullNotNullRange()
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if len(conds) == 0 {
		return nil
	}
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.ConstCols = make([]bool, len(path.IdxCols))
		if res.ColumnValues != nil {
			for i := range path.ConstCols {
				path.ConstCols[i] = res.ColumnValues[i] != nil
			}
		}
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(ds.ctx, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.TableFilters = conds
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	return nil
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	if path.IsCommonHandlePath {
		return ds.deriveCommonHandleTablePathStats(path, conds, isIm)
	}
	var err error
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.TableFilters = conds
	var pkCol *expression.Column
	columnLen := len(ds.schema.Columns)
	isUnsigned := false
	if ds.tableInfo.PKIsHandle {
		if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			pkCol = expression.ColInfo2Col(ds.schema.Columns, pkColInfo)
		}
	} else if columnLen > 0 && ds.schema.Columns[columnLen-1].ID == model.ExtraHandleID {
		pkCol = ds.schema.Columns[columnLen-1]
	}
	if pkCol == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return nil
	}
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.ctx, conds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.AccessConds) == 0 {
		for i, filter := range path.TableFilters {
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.ctx, pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.ctx, pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	if corColInAccessConds {
		path.CountAfterAccess = 1
		return nil
	}
	path.Ranges, err = ranger.BuildTableRange(path.AccessConds, ds.ctx, pkCol.RetType)
	if err != nil {
		return err
	}
	path.CountAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(ds.ctx, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	return err
}

func (ds *DataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
			alreadyHandle := false
			for _, col := range path.IdxCols {
				if col.ID == model.ExtraHandleID || col.Equal(nil, handleCol) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.IdxCols = append(path.IdxCols, handleCol)
				path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
				// Also updates the map that maps the index id to its prefix column ids.
				if len(ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID]) == len(path.Index.Columns) {
					ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID] = append(ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID], handleCol.UniqueID)
				}
			}
		}
	}
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.ConstCols = make([]bool, len(path.IdxCols))
		if res.ColumnValues != nil {
			for i := range path.ConstCols {
				path.ConstCols[i] = res.ColumnValues[i] != nil
			}
		}
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(ds.ctx, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.TableFilters = conds
	}
	return nil
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveIndexPathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) {
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	var indexFilters []expression.Expression
	indexFilters, path.TableFilters = ds.splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.stats.RowCount)
		}
	}
}

func getPKIsHandleColFromSchema(cols []*model.ColumnInfo, schema *expression.Schema, pkIsHandle bool) *expression.Column {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range cols {
			if col.ID == model.ExtraHandleID {
				return schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range cols {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			return schema.Columns[i]
		}
	}
	return nil
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.schema, ds.tableInfo.PKIsHandle)
}

func (p *LogicalIndexScan) getPKIsHandleCol(schema *expression.Schema) *expression.Column {
	// We cannot use p.Source.getPKIsHandleCol() here,
	// Because we may re-prune p.Columns and p.schema during the transformation.
	// That will make p.Columns different from p.Source.Columns.
	return getPKIsHandleColFromSchema(p.Columns, schema, p.Source.tableInfo.PKIsHandle)
}

// TableInfo returns the *TableInfo of data source.
func (ds *DataSource) TableInfo() *model.TableInfo {
	return ds.tableInfo
}

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	logicalSchemaProducer
}

// LogicalPartitionUnionAll represents the LogicalUnionAll plan is for partition table.
type LogicalPartitionUnionAll struct {
	LogicalUnionAll
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	baseLogicalPlan

	ByItems []*util.ByItems
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (ls *LogicalSort) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalPlan

	ByItems    []*util.ByItems
	Offset     uint64
	Count      uint64
	limitHints limitHintInfo
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (lt *LogicalTopN) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	return len(lt.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	logicalSchemaProducer

	Offset     uint64
	Count      uint64
	limitHints limitHintInfo
}

// LogicalLock represents a select lock plan.
type LogicalLock struct {
	baseLogicalPlan

	Lock         *ast.SelectLockInfo
	tblID2Handle map[int64][]HandleCols

	// tblID2phyTblIDCol is used for partitioned tables,
	// the child executor need to return an extra column containing
	// the Physical Table ID (i.e. from which partition the row came from)
	tblID2PhysTblIDCol map[int64]*expression.Column
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// Clone copies a window frame totally.
func (wf *WindowFrame) Clone() *WindowFrame {
	cloned := new(WindowFrame)
	*cloned = *wf

	cloned.Start = wf.Start.Clone()
	cloned.End = wf.End.Clone()

	return cloned
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []expression.Expression
	// CmpFuncs is used to decide whether one row is included in the current frame.
	CmpFuncs []expression.CompareFunc
}

// Clone copies a frame bound totally.
func (fb *FrameBound) Clone() *FrameBound {
	cloned := new(FrameBound)
	*cloned = *fb

	cloned.CalcFuncs = make([]expression.Expression, 0, len(fb.CalcFuncs))
	for _, it := range fb.CalcFuncs {
		cloned.CalcFuncs = append(cloned.CalcFuncs, it.Clone())
	}
	cloned.CmpFuncs = fb.CmpFuncs

	return cloned
}

// LogicalWindow represents a logical window function plan.
type LogicalWindow struct {
	logicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.SortItem
	OrderBy         []property.SortItem
	Frame           *WindowFrame
}

// EqualPartitionBy checks whether two LogicalWindow.Partitions are equal.
func (p *LogicalWindow) EqualPartitionBy(ctx sessionctx.Context, newWindow *LogicalWindow) bool {
	if len(p.PartitionBy) != len(newWindow.PartitionBy) {
		return false
	}
	partitionByColsMap := make(map[int64]struct{})
	for _, item := range p.PartitionBy {
		partitionByColsMap[item.Col.UniqueID] = struct{}{}
	}
	for _, item := range newWindow.PartitionBy {
		if _, ok := partitionByColsMap[item.Col.UniqueID]; !ok {
			return false
		}
	}
	return true
}

// EqualOrderBy checks whether two LogicalWindow.OrderBys are equal.
func (p *LogicalWindow) EqualOrderBy(ctx sessionctx.Context, newWindow *LogicalWindow) bool {
	if len(p.OrderBy) != len(newWindow.OrderBy) {
		return false
	}
	for i, item := range p.OrderBy {
		if !item.Col.Equal(ctx, newWindow.OrderBy[i].Col) ||
			item.Desc != newWindow.OrderBy[i].Desc {
			return false
		}
	}
	return true
}

// EqualFrame checks whether two LogicalWindow.Frames are equal.
func (p *LogicalWindow) EqualFrame(ctx sessionctx.Context, newWindow *LogicalWindow) bool {
	if (p.Frame == nil && newWindow.Frame != nil) ||
		(p.Frame != nil && newWindow.Frame == nil) {
		return false
	}
	if p.Frame == nil && newWindow.Frame == nil {
		return true
	}
	if p.Frame.Type != newWindow.Frame.Type ||
		p.Frame.Start.Type != newWindow.Frame.Start.Type ||
		p.Frame.Start.UnBounded != newWindow.Frame.Start.UnBounded ||
		p.Frame.Start.Num != newWindow.Frame.Start.Num ||
		p.Frame.End.Type != newWindow.Frame.End.Type ||
		p.Frame.End.UnBounded != newWindow.Frame.End.UnBounded ||
		p.Frame.End.Num != newWindow.Frame.End.Num {
		return false
	}
	for i, expr := range p.Frame.Start.CalcFuncs {
		if !expr.Equal(ctx, newWindow.Frame.Start.CalcFuncs[i]) {
			return false
		}
	}
	for i, expr := range p.Frame.End.CalcFuncs {
		if !expr.Equal(ctx, newWindow.Frame.End.CalcFuncs[i]) {
			return false
		}
	}
	return true
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (p *LogicalWindow) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
	}
	return corCols
}

// GetWindowResultColumns returns the columns storing the result of the window function.
func (p *LogicalWindow) GetWindowResultColumns() []*expression.Column {
	return p.schema.Columns[p.schema.Len()-len(p.WindowFuncDescs):]
}

// ExtractCorColumnsBySchema only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorColumnsBySchema(corCols []*expression.CorrelatedColumn, schema *expression.Schema, resolveIndex bool) []*expression.CorrelatedColumn {
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	resultCorCols = resultCorCols[:length]

	if resolveIndex {
		for _, corCol := range resultCorCols {
			corCol.Index = schema.ColumnIndex(&corCol.Column)
		}
	}

	return resultCorCols
}

// extractCorColumnsBySchema4LogicalPlan only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func extractCorColumnsBySchema4LogicalPlan(p LogicalPlan, schema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := ExtractCorrelatedCols4LogicalPlan(p)
	return ExtractCorColumnsBySchema(corCols, schema, false)
}

// ExtractCorColumnsBySchema4PhysicalPlan only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorColumnsBySchema4PhysicalPlan(p PhysicalPlan, schema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := ExtractCorrelatedCols4PhysicalPlan(p)
	return ExtractCorColumnsBySchema(corCols, schema, true)
}

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    string
	Table     *ast.TableName  // Used for showing columns.
	Partition model.CIStr     // Use for showing partition
	Column    *ast.ColumnName // Used for `desc table column`.
	IndexName model.CIStr
	Flag      int                  // Some flag parsed from sql, such as FULL.
	User      *auth.UserIdentity   // Used for show grants.
	Roles     []*auth.RoleIdentity // Used for show grants.

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`.
	GlobalScope bool // Used by show variables.
	Extended    bool // Used for `show extended columns from ...`
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalSchemaProducer
	ShowContents

	Extractor ShowPredicateExtractor
}

// LogicalShowDDLJobs is for showing DDL job list.
type LogicalShowDDLJobs struct {
	logicalSchemaProducer

	JobNumber int64
}

// CTEClass holds the information and plan for a CTE. Most of the fields in this struct are the same as cteInfo.
// But the cteInfo is used when building the plan, and CTEClass is used also for building the executor.
type CTEClass struct {
	// The union between seed part and recursive part is DISTINCT or DISTINCT ALL.
	IsDistinct bool
	// seedPartLogicalPlan and recursivePartLogicalPlan are the logical plans for the seed part and recursive part of this CTE.
	seedPartLogicalPlan      LogicalPlan
	recursivePartLogicalPlan LogicalPlan
	// seedPartPhysicalPlan and recursivePartPhysicalPlan are the physical plans for the seed part and recursive part of this CTE.
	seedPartPhysicalPlan      PhysicalPlan
	recursivePartPhysicalPlan PhysicalPlan
	// storageID for this CTE.
	IDForStorage int
	// optFlag is the optFlag for the whole CTE.
	optFlag   uint64
	HasLimit  bool
	LimitBeg  uint64
	LimitEnd  uint64
	IsInApply bool
	// pushDownPredicates may be push-downed by different references.
	pushDownPredicates []expression.Expression
	ColumnMap          map[string]*expression.Column
}

// LogicalCTE is for CTE.
type LogicalCTE struct {
	logicalSchemaProducer

	cte            *CTEClass
	cteAsName      model.CIStr
	seedStat       *property.StatsInfo
	isOuterMostCTE bool
}

// LogicalCTETable is for CTE table
type LogicalCTETable struct {
	logicalSchemaProducer

	seedStat     *property.StatsInfo
	name         string
	idForStorage int

	// seedSchema is only used in columnStatsUsageCollector to get column mapping
	seedSchema *expression.Schema
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (p *LogicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := ExtractCorrelatedCols4LogicalPlan(p.cte.seedPartLogicalPlan)
	if p.cte.recursivePartLogicalPlan != nil {
		corCols = append(corCols, ExtractCorrelatedCols4LogicalPlan(p.cte.recursivePartLogicalPlan)...)
	}
	return corCols
}
