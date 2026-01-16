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
	"maps"
	"math"
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	JoinType     base.JoinType `hash64-equals:"true"`
	Reordered    bool
	StraightJoin bool

	// HintInfo stores the join algorithm hint information specified by client.
	HintInfo            *utilhint.PlanHints
	PreferJoinType      uint
	PreferJoinOrder     bool
	LeftPreferJoinType  uint
	RightPreferJoinType uint

	EqualConditions []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	// NAEQConditions means null aware equal conditions, which is used for null aware semi joins.
	NAEQConditions  []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	LeftConditions  expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	RightConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	OtherConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`

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

	// allJoinLeaf is used to identify the table where the column is located during constant propagation.
	allJoinLeaf []*expression.Schema
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx base.PlanContext, offset int) *LogicalJoin {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
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
	for i, equalExpr := range p.EqualConditions {
		p.EqualConditions[i] = ruleutil.ResolveExprAndReplace(equalExpr, replace).(*expression.ScalarFunction)
	}
	for i, leftExpr := range p.LeftConditions {
		p.LeftConditions[i] = ruleutil.ResolveExprAndReplace(leftExpr, replace)
	}
	for i, rightExpr := range p.RightConditions {
		p.RightConditions[i] = ruleutil.ResolveExprAndReplace(rightExpr, replace)
	}
	for i, otherExpr := range p.OtherConditions {
		p.OtherConditions[i] = ruleutil.ResolveExprAndReplace(otherExpr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements the base.LogicalPlan.<1st> interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan base.LogicalPlan, err error) {
	simplifyOuterJoin(p, predicates)
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	p.allJoinLeaf = getAllJoinLeaf(p)
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin)
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), false, nil)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
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
	case base.RightOuterJoin:
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), true, nil)
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForRightOuterJoin)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
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
	case base.SemiJoin, base.InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), tempCond)
		tempCond = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), tempCond,
			p.Children()[0].Schema(), p.Children()[1].Schema(),
			true, p.isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, tempCond)
		if dual != nil {
			return ret, dual, nil
		}
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case base.AntiSemiJoin:
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin)
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), true,
			p.isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
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
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	children := p.Children()
	rightChild := children[1]
	leftChild := children[0]
	rightCond = constraint.DeleteTrueExprsBySchema(evalCtx, rightChild.Schema(), rightCond)
	leftCond = constraint.DeleteTrueExprsBySchema(evalCtx, leftChild.Schema(), leftCond)
	leftRet, lCh, err := leftChild.PredicatePushDown(leftCond)
	if err != nil {
		return nil, nil, err
	}
	rightRet, rCh, err := rightChild.PredicatePushDown(rightCond)
	if err != nil {
		return nil, nil, err
	}
	AddSelection(p, lCh, leftRet, 0)
	AddSelection(p, rCh, rightRet, 1)
	p.updateEQCond()
	ruleutil.BuildKeyInfoPortal(p)
	newnChild, err := p.SemiJoinRewrite()
	return ret, newnChild, err
}

// isNullFromInner is to iterate all equal condition's inner columns except NullEQ ones
// and all other condition's columns except GT, GE, LE, LT, NE ones. and then check where
// is null columns is from the join key in the inner table.
func isNullFromInner(innerSchSet *intset.FastIntSet, isNullColumnID int64, eq []*expression.ScalarFunction, other []expression.Expression) bool {
	for _, s := range eq {
		if s.FuncName.L == ast.NullEQ {
			continue
		}
		col1, col2, ok := expression.IsColOpCol(s)
		if ok {
			if innerSchSet.Has(int(col1.UniqueID)) {
				if isNullColumnID == col1.UniqueID {
					return true
				}
			} else if innerSchSet.Has(int(col2.UniqueID)) {
				if isNullColumnID == col2.UniqueID {
					return true
				}
			}
		}
	}
	for _, s := range other {
		if sf, ok := s.(*expression.ScalarFunction); ok {
			switch sf.FuncName.L {
			case ast.GT, ast.GE, ast.LE, ast.LT, ast.NE:
				col1, col2, ok := expression.IsColOpCol(sf)
				if ok {
					if innerSchSet.Has(int(col1.UniqueID)) && isNullColumnID == col1.UniqueID {
						return true
					} else if innerSchSet.Has(int(col2.UniqueID)) && isNullColumnID == col2.UniqueID {
						return true
					}
				}
			default:
			}
		}
	}
	return false
}

func vaildProj4ConvertAntiJoin(proj *LogicalProjection) bool {
	if proj == nil {
		return true
	}
	// Sometimes there is a projection between select and join.
	// We require that this projection does not make additional changes to any columns,
	// then we can continue converting to a semi join.
	//  Pass: Projection[col1,col2]
	//  Fail: Projection[col2->ABC, col2+1->col2]
	for idx, c := range proj.Schema().Columns {
		if !c.Equals(proj.Exprs[idx]) {
			return false
		}
	}
	return true
}

// CanConvertAntiJoin is used in outer-join-to-semi-join rule.
/*
#### Scenario 1: IS NULL on the Join Condition Column

- In this scenario, the IS NULL filter is applied directly to the join key from the inner table.

##### Table Schema
CREATE TABLE Table_A (id INT, name VARCHAR(50));
CREATE TABLE Table_B (id INT, info VARCHAR(50));

##### Plan
```
-- The optimizer rewrites the LEFT JOIN ... WHERE ... IS NULL pattern into an efficient
-- Anti Semi Join to retrieve rows from Table A that have no match in Table B.
SELECT Table_A.id, Table_A.name
FROM
	Table_A
LEFT JOIN
	Table_B ON Table_A.id = Table_B.id
WHERE
	Table_B.id IS NULL;
+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
| id                       | task      | access object | operator info                                                                        |
+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
| Projection               | root      |               | test.table_a.id, test.table_a.name                                                   |
| └─Selection              | root      |               | isnull(test.table_b.id)                                                              |
|   └─HashJoin             | root      |               | left outer join, left side:TableReader, equal:[eq(test.table_a.id, test.table_b.id)] |
|     ├─TableReader(Build) | root      |               | data:Selection                                                                       |
|     │ └─Selection        | cop[tikv] |               | not(isnull(test.table_b.id))                                                         |
|     │   └─TableFullScan  | cop[tikv] | table:B       | keep order:false, stats:pseudo                                                       |
|     └─TableReader(Probe) | root      |               | data:TableFullScan                                                                   |
|       └─TableFullScan    | cop[tikv] | table:A       | keep order:false, stats:pseudo                                                       |
+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
=>
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
| id                   | task      | access object | operator info                                                                       |
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
| HashJoin             | root      |               | anti semi join, left side:TableReader, equal:[eq(test.table_a.id, test.table_b.id)] |
| ├─TableReader(Build) | root      |               | data:Selection                                                                      |
| │ └─Selection        | cop[tikv] |               | not(isnull(test.table_b.id))                                                        |
| │   └─TableFullScan  | cop[tikv] | table:B       | keep order:false, stats:pseudo                                                      |
| └─TableReader(Probe) | root      |               | data:TableFullScan                                                                  |
|   └─TableFullScan    | cop[tikv] | table:A       | keep order:false, stats:pseudo                                                      |
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
```
#### Scenario 2: IS NULL on a Non-Join-Key NOT NULL Column

If a column in the inner table is defined as `NOT NULL` in the schema, but is filtered as `IS NULL` after the join,
it implies that the join failed to find a match. This allows us to convert the join into ```ANTI SEMI JOIN```
even if the column is not part of the join keys.

##### Table Schema:

CREATE TABLE Table_A (id INT,name VARCHAR(50));
CREATE TABLE Table_B (
	id INT,
	status VARCHAR(20) NOT NULL -- Non-join column with NOT NULL constraint
);

##### Plan
```
-- Even though 'status' is not a join key, its NOT NULL constraint
-- ensures that 'B.status IS NULL' only occurs when no match is found.
SELECT A.* FROM Table_A A
LEFT JOIN Table_B B ON A.id = B.id
WHERE B.status IS NULL;

+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
| id                       | task      | access object | operator info                                                                        |
+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
| Projection               | root      |               | test.table_a.id, test.table_a.name                                                   |
| └─Selection              | root      |               | isnull(test.table_b.status)                                                          |
|   └─HashJoin             | root      |               | left outer join, left side:TableReader, equal:[eq(test.table_a.id, test.table_b.id)] |
|     ├─TableReader(Build) | root      |               | data:Selection                                                                       |
|     │ └─Selection        | cop[tikv] |               | not(isnull(test.table_b.id))                                                         |
|     │   └─TableFullScan  | cop[tikv] | table:B       | keep order:false, stats:pseudo                                                       |
|     └─TableReader(Probe) | root      |               | data:TableFullScan                                                                   |
|       └─TableFullScan    | cop[tikv] | table:A       | keep order:false, stats:pseudo                                                       |
+--------------------------+-----------+---------------+--------------------------------------------------------------------------------------+
 =>
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
| id                   | task      | access object | operator info                                                                       |
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
| HashJoin             | root      |               | anti semi join, left side:TableReader, equal:[eq(test.table_a.id, test.table_b.id)] |
| ├─TableReader(Build) | root      |               | data:Selection                                                                      |
| │ └─Selection        | cop[tikv] |               | not(isnull(test.table_b.id))                                                        |
| │   └─TableFullScan  | cop[tikv] | table:B       | keep order:false, stats:pseudo                                                      |
| └─TableReader(Probe) | root      |               | data:TableFullScan                                                                  |
|   └─TableFullScan    | cop[tikv] | table:A       | keep order:false, stats:pseudo                                                      |
+----------------------+-----------+---------------+-------------------------------------------------------------------------------------+
```

Additionally, we found that there may be a projection between select and join.

	Select[isnull(col1)] <- Projection[col1,col2,col3,col4] <- left outer join[outer side: col1 col2, inner side: col3 col4]

Currently, we only support projections with column mapping relationships, not transformation relationships.

	Projection[null->col1,null->col2,col3,col4] <- anti semi join[outer side: col1 col2, inner side: col3 col4]

*/
func (p *LogicalJoin) CanConvertAntiJoin(selectCond []expression.Expression, selectSch *expression.Schema, proj *LogicalProjection) (resultProj *LogicalProjection, selConditionColInInner bool) {
	if len(selectCond) != 1 || (len(p.EqualConditions) == 0 && len(p.OtherConditions) == 0) {
		// selectCond can only have one expression.
		// The inner expression can definitely be pushed down, so selectCond must be the outer expression.
		// If they are semantically similar, leaving only one and the other should be eliminable.
		return nil, false
	}
	if _, ok := p.Self().(*LogicalApply); ok {
		return nil, false
	}
	var outerChildIdx int
	switch p.JoinType {
	case base.LeftOuterJoin:
		outerChildIdx = 0
	case base.RightOuterJoin:
		outerChildIdx = 1
	default:
		return nil, false
	}

	var sf *expression.ScalarFunction
	var ok bool
	var isNullcol *expression.Column
	sf, ok = selectCond[0].(*expression.ScalarFunction)
	if !ok || sf == nil {
		return nil, false
	}
	if sf.FuncName.L != ast.IsNull {
		return nil, false
	}
	args := sf.GetArgs()
	// Get the Column in the IsNull
	isNullcol, ok = args[0].(*expression.Column)
	if !ok {
		return nil, false
	}
	outer := p.children[OuterChildIdx]
	outerSchema := outer.Schema()
	innerSchemaSet := intset.NewFastIntSet()
	// Obtain all the columns that meet the requirements in the eq condition and other condition.
	// If the column that is isnull is an inner column in the eq/other condition,
	// It can be directly converted into an anti semi join.
	expression.ExtractColumnsSetFromExpressions(&innerSchemaSet, func(c *expression.Column) bool {
		return !outerSchema.Contains(c)
	}, expression.Column2Exprs(p.Schema().Columns)...)
	if !vaildProj4ConvertAntiJoin(proj) {
		return nil, false
	}
	selConditionColInInner = isNullFromInner(&innerSchemaSet, isNullcol.UniqueID, p.EqualConditions, p.OtherConditions)
	// resultProj is to generate the NULL values for the columns of the inner table, which is the
	// expected result for this kind of anti-join query.
	if selConditionColInInner {
		// Scenario 1: column in IsNull expression is from the inner side columns in the eq/other condition.
		if proj != nil {
			resultProj = p.generateProject4ConvertAntiJoin(&innerSchemaSet, proj.Schema())
		} else {
			resultProj = p.generateProject4ConvertAntiJoin(&innerSchemaSet, selectSch)
		}
	} else if innerSchemaSet.Has(int(isNullcol.UniqueID)) {
		// Scenario 2:
		//  column in IsNull expression is from the inner side columns.
		//  but it is not in the equal/other condition.
		//  We need to check whether inner column is not null in the origin table schema.
		//  If it is not null column, it can be directly converted into an anti-semi join.
		innerSch := p.Children()[1^OuterChildIdx].Schema()
		idx := innerSch.ColumnIndex(isNullcol)
		if mysql.HasNotNullFlag(innerSch.Columns[idx].RetType.GetFlag()) {
			if proj != nil {
				resultProj = p.generateProject4ConvertAntiJoin(&innerSchemaSet, proj.Schema())
			} else {
				resultProj = p.generateProject4ConvertAntiJoin(&innerSchemaSet, selectSch)
			}
			selConditionColInInner = true
		}
	}

	if selConditionColInInner {
		// Anti-semi join's first child is outer, the second child is inner. join condition is outer op inner
		// right outer join's first child is inner, the second child is outer, join condition is inner op outer
		// left outer join's  first child is outer, the second child is inner. join condition is outer op inner
		// So we have to transfer it here.
		ctx := p.SCtx().GetExprCtx()
		if p.JoinType == base.RightOuterJoin {
			for idx, expr := range p.EqualConditions {
				args := expr.GetArgs()
				p.EqualConditions[idx] = expression.NewFunctionInternal(ctx, expr.FuncName.L, expr.GetType(ctx.GetEvalCtx()), args[1], args[0]).(*expression.ScalarFunction)
			}
			args := p.Children()
			p.SetChildren(args[1], args[0])
		}
		p.JoinType = base.AntiSemiJoin
	}
	return resultProj, selConditionColInInner
}

// generateProject4ConvertAntiJoin is to generate projection and put it on the anti-semi join which is from outer join.
// inner column will not be changed. outer column will output the null.
func (p *LogicalJoin) generateProject4ConvertAntiJoin(innerSchSet *intset.FastIntSet, selectSch *expression.Schema) (proj *LogicalProjection) {
	projExprs := make([]expression.Expression, 0, len(selectSch.Columns))
	for _, c := range selectSch.Columns {
		if innerSchSet.Has(int(c.UniqueID)) {
			projExprs = append(projExprs, expression.NewNull())
		} else {
			projExprs = append(projExprs, c.Clone())
		}
	}
	proj = LogicalProjection{Exprs: projExprs}.Init(p.SCtx(), p.QueryBlockOffset())
	proj.SetSchema(selectSch.Clone())
	return
}

// simplifyOuterJoin transforms "LeftOuterJoin/RightOuterJoin" to "InnerJoin" if possible.
func simplifyOuterJoin(p *LogicalJoin, predicates []expression.Expression) {
	if p.JoinType != base.LeftOuterJoin && p.JoinType != base.RightOuterJoin && p.JoinType != base.InnerJoin {
		return
	}

	innerTable := p.children[0]
	outerTable := p.children[1]
	if p.JoinType == base.LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}

	if p.JoinType == base.InnerJoin {
		return
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		// avoid the case where the expr only refers to the schema of outerTable
		if expression.ExprFromSchema(expr, outerTable.Schema()) {
			continue
		}
		isOk := isNullRejected(p.SCtx(), innerTable.Schema(), expr)
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = base.InnerJoin
	}
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	exprCtx := exprctx.WithNullRejectCheck(ctx.GetExprCtx())
	expr = expression.PushDownNot(exprCtx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	for _, cond := range expression.SplitCNFItems(expr) {
		if isNullRejectedSpecially(ctx, schema, expr) {
			return true
		}

		result, err := expression.EvaluateExprWithNull(exprCtx, schema, cond, true)
		if err != nil {
			return false
		}
		x, ok := result.(*expression.Constant)
		if !ok {
			continue
		}
		if x.Value.IsNull() {
			return true
		} else if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
			return true
		}
	}
	return false
}

// isNullRejectedSpecially handles some null-rejected cases specially, since the current in
// EvaluateExprWithNull is too strict for some cases, e.g. #49616.
func isNullRejectedSpecially(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	return specialNullRejectedCase1(ctx, schema, expr) // only 1 case now
}

// specialNullRejectedCase1 is mainly for #49616.
// Case1 specially handles `null-rejected OR (null-rejected AND {others})`, then no matter what the result
// of `{others}` is (True, False or Null), the result of this predicate is null, so this predicate is null-rejected.
func specialNullRejectedCase1(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	isFunc := func(e expression.Expression, lowerFuncName string) *expression.ScalarFunction {
		f, ok := e.(*expression.ScalarFunction)
		if !ok {
			return nil
		}
		if f.FuncName.L == lowerFuncName {
			return f
		}
		return nil
	}
	orFunc := isFunc(expr, ast.LogicOr)
	if orFunc == nil {
		return false
	}
	for i := range 2 {
		andFunc := isFunc(orFunc.GetArgs()[i], ast.LogicAnd)
		if andFunc == nil {
			continue
		}
		if !isNullRejected(ctx, schema, orFunc.GetArgs()[1-i]) {
			continue // the other side should be null-rejected: null-rejected OR (... AND ...)
		}
		for _, andItem := range expression.SplitCNFItems(andFunc) {
			if isNullRejected(ctx, schema, andItem) {
				return true // hit the case in the comment: null-rejected OR (null-rejected AND ...)
			}
		}
	}
	return false
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	leftCols, rightCols := p.ExtractUsedCols(parentUsedCols)

	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(leftCols)
	if err != nil {
		return nil, err
	}

	p.Children()[1], err = p.Children()[1].PruneColumns(rightCols)
	if err != nil {
		return nil, err
	}

	p.MergeSchema()
	if p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		joinCol := p.Schema().Columns[len(p.Schema().Columns)-1]
		parentUsedCols = append(parentUsedCols, joinCol)
	}
	p.InlineProjection(parentUsedCols)
	return p, nil
}

// FindBestTask inherits the BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements the base.LogicalPlan.<4th> interface.
func (p *LogicalJoin) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	switch p.JoinType {
	case base.SemiJoin, base.LeftOuterSemiJoin, base.AntiSemiJoin, base.AntiLeftOuterSemiJoin:
		selfSchema.PKOrUK = childSchema[0].Clone().PKOrUK
	case base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		// Extract all left and right columns from equal conditions
		leftCols := make([]*expression.Column, 0, len(p.EqualConditions))
		rightCols := make([]*expression.Column, 0, len(p.EqualConditions))
		for _, expr := range p.EqualConditions {
			l, r := expression.ExtractColumnsFromColOpCol(expr)
			leftCols = append(leftCols, l)
			rightCols = append(rightCols, r)
		}
		checkColumnsMatchPKOrUK := func(cols []*expression.Column, pkOrUK []expression.KeyInfo) bool {
			if len(pkOrUK) == 0 {
				return false
			}
			colSet := intset.NewFastIntSet()
			for _, col := range cols {
				colSet.Insert(int(col.UniqueID))
			}
			// Check if any key is a subset of the join key columns
			// A match is considered valid if join key columns contain all columns of any PKOrUK
			for _, key := range pkOrUK {
				allMatch := true
				for _, k := range key {
					if !colSet.Has(int(k.UniqueID)) {
						allMatch = false
						break
					}
				}
				if allMatch {
					return true
				}
			}
			return false
		}
		lOk := checkColumnsMatchPKOrUK(leftCols, childSchema[0].PKOrUK)
		rOk := checkColumnsMatchPKOrUK(rightCols, childSchema[1].PKOrUK)

		// When both sides match different unique keys (e.g., left side matches unique key setA, right side matches unique key setB),
		// we can derive additional functional dependencies through join conditions:
		// 1. Original FDs: setA -> left row, setB -> right row
		// 2. Join condition: setA from left = setA from right
		// 3. By augmentation rule (X->Y implies XZ->YZ): setA from right + setB -> left row + setB
		// 4. By transitivity rule (X->Y and Y->Z implies X->Z): setA from right + setB -> left row + right row
		// This allows us to preserve unique key information from both sides when join keys provide stronger uniqueness guarantees.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != base.LeftOuterJoin {
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, childSchema[1].PKOrUK...)
		}
		if rOk && p.JoinType != base.RightOuterJoin {
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, childSchema[0].PKOrUK...)
		}
	}
}

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalJoin) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	topnEliminated := false
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		p.Children()[0], topnEliminated = p.pushDownTopNToChild(topN, 0)
		p.Children()[1] = p.Children()[1].PushDownTopN(nil)
	case base.RightOuterJoin:
		p.Children()[1], topnEliminated = p.pushDownTopNToChild(topN, 1)
		p.Children()[0] = p.Children()[0].PushDownTopN(nil)
	default:
		return p.BaseLogicalPlan.PushDownTopN(topN)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		if topnEliminated {
			// Add a sort if the topN has order by items.
			if len(topN.ByItems) > 0 {
				sort := LogicalSort{ByItems: topN.ByItems}.Init(p.SCtx(), p.QueryBlockOffset())
				sort.SetChildren(p.Self())
				return sort
			}
			// If the topN has no order by items, simply return the join itself.
			return p.Self()
		}
		return topN.AttachChild(p.Self())
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
func (p *LogicalJoin) ConstantPropagation(parentPlan base.LogicalPlan, currentChildIdx int) (newRoot base.LogicalPlan) {
	// step1: get constant predicate from left or right according to the JoinType
	var getConstantPredicateFromLeft bool
	var getConstantPredicateFromRight bool
	switch p.JoinType {
	case base.LeftOuterJoin:
		getConstantPredicateFromLeft = true
	case base.RightOuterJoin:
		getConstantPredicateFromRight = true
	case base.InnerJoin:
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
	return addCandidateSelection(p, currentChildIdx, parentPlan, candidateConstantPredicates)
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
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	for _, one := range reloads {
		reload = reload || one
	}
	if !reload && p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
		return p.StatsInfo(), false, nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	p.EqualCondOutCnt = cardinality.EstimateFullJoinRowCount(p.SCtx(),
		0 == len(p.EqualConditions),
		leftProfile, rightProfile,
		leftJoinKeys, rightJoinKeys,
		childSchema[0], childSchema[1],
		nil, nil)
	if p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount * cost.SelectionFactor,
			ColNDVs:  make(map[int64]float64, len(leftProfile.ColNDVs)),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c * cost.SelectionFactor
		}
		return p.StatsInfo(), true, nil
	}
	if p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount,
			ColNDVs:  make(map[int64]float64, selfSchema.Len()),
		})
		maps.Copy(p.StatsInfo().ColNDVs, leftProfile.ColNDVs)
		p.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
		return p.StatsInfo(), true, nil
	}
	count := p.EqualCondOutCnt
	if p.JoinType == base.LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == base.RightOuterJoin {
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
	p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
	return p.StatsInfo(), true, nil
}

// ExtractColGroups implements the base.LogicalPlan.<12th> interface.
func (p *LogicalJoin) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.Column, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == base.InnerJoin || p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin) {
		extracted = append(extracted, expression.SortColumns(leftJoinKeys), expression.SortColumns(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == base.RightOuterJoin {
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
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin {
		rightProperties = nil
	} else if p.JoinType == base.RightOuterJoin {
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
	case base.InnerJoin:
		return p.ExtractFDForInnerJoin(nil)
	case base.LeftOuterJoin, base.RightOuterJoin:
		return p.ExtractFDForOuterJoin(nil)
	case base.SemiJoin:
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

	if p.JoinType == base.LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
		switchChild = true
	}

	// First, simplify this join
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		canBeSimplified := false
		for _, expr := range predicates {
			isOk := util.IsNullRejected(p.SCtx(), innerTable.Schema(), expr, true)
			if isOk {
				canBeSimplified = true
				break
			}
		}
		if canBeSimplified {
			p.JoinType = base.InnerJoin
		}
	}

	// Next simplify join children

	combinedCond := mergeOnClausePredicates(p, predicates)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	} else if p.JoinType == base.InnerJoin || p.JoinType == base.SemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else if p.JoinType == base.AntiSemiJoin {
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

// GetJoinChildStatsAndSchema gets the stats and schema of join children.
func (p *LogicalJoin) GetJoinChildStatsAndSchema() (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	stats1, schema1 = p.Children()[1].StatsInfo(), p.Children()[1].Schema()
	stats0, schema0 = p.Children()[0].StatsInfo(), p.Children()[0].Schema()
	return
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
func (p *LogicalJoin) ExtractFDForSemiJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	// 1: since semi join will keep the part or all rows of the outer table, it's outer FD can be saved.
	// 2: the un-projected column will be left for the upper layer projection or already be pruned from bottom up.
	outerFD, _ := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	fds := outerFD

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(p.LeftConditions, p.SCtx(), fds)

	for _, equiv := range equivFromApply {
		fds.AddEquivalence(equiv[0], equiv[1])
	}

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	p.SetFDs(fds)
	return fds
}

// ExtractFDForInnerJoin extracts FD for inner join.
func (p *LogicalJoin) ExtractFDForInnerJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	child := p.Children()
	rightFD := child[1].ExtractFD()
	leftFD := child[0].ExtractFD()
	fds := leftFD
	fds.MakeCartesianProduct(rightFD)

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	// some join eq conditions are stored in the OtherConditions.
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), fds)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	for _, equiv := range equivFromApply {
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
func (p *LogicalJoin) ExtractFDForOuterJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
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
	if p.JoinType == base.RightOuterJoin {
		innerFD, outerFD = outerFD, innerFD
		innerCondition = p.LeftConditions
		outerCondition = p.RightConditions
		innerCols, outerCols = outerCols, innerCols
	}

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, innerCondition...)
	allConds = append(allConds, outerCondition...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	filterFD := &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), filterFD)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), filterFD)

	filterFD.AddConstants(constUniqueIDs)
	equivOuterUniqueIDs := intset.NewFastIntSet()
	equivAcrossNum := 0
	// apply (left join)
	//   +--- child0 [t1.a]
	//   +--- project [correlated col{t1.a} --> col#9]
	// since project correlated col{t1.a} is equivalent to t1.a, we should maintain t1.a == col#9. since apply is
	// a left join, the probe side will append null value for the non-matched row, so we should maintain the equivalence
	// before the fds.MakeOuterJoin(). Even correlated col{t1.a} is not from outer side here, we could also maintain
	// the equivalence before the fds.MakeOuterJoin().
	equivUniqueIDs = append(equivUniqueIDs, equivFromApply...)
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
		outerConditionUniqueIDs := intset.NewFastIntSet()
		if len(outerCondition) != 0 {
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, outerCondition...)
		}
		if len(p.OtherConditions) != 0 {
			// other condition may contain right side cols, it doesn't affect the judgement of intersection of non-left-equiv cols.
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, p.OtherConditions...)
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
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
		isNullEQ = append(isNullEQ, expr.FuncName.L == ast.NullEQ)
		hasNullEQ = hasNullEQ || expr.FuncName.L == ast.NullEQ
	}
	return
}

// GetNAJoinKeys extracts join keys(columns) from NAEqualCondition.
func (p *LogicalJoin) GetNAJoinKeys() (leftKeys, rightKeys []*expression.Column) {
	for _, expr := range p.NAEQConditions {
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
	}
	return
}

// GetPotentialPartitionKeys return potential partition keys for join, the potential partition keys are
// the join keys of EqualConditions
func (p *LogicalJoin) GetPotentialPartitionKeys() (leftKeys, rightKeys []*property.MPPPartitionColumn) {
	for _, expr := range p.EqualConditions {
		_, coll := expr.CharsetAndCollation()
		collateID := property.GetCollateIDByNameForPartition(coll)
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, &property.MPPPartitionColumn{Col: l, CollateID: collateID})
		rightKeys = append(rightKeys, &property.MPPPartitionColumn{Col: r, CollateID: collateID})
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
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}
		_, _, ok := expression.IsColOpCol(newCond)
		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !ok {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
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

func (p *LogicalJoin) isAllUniqueIDInTheSameLeaf(cond expression.Expression) bool {
	colset := slices.Collect(
		maps.Keys(expression.ExtractColumnsMapFromExpressions(nil, cond)),
	)
	if len(colset) == 1 {
		return true
	}

	for _, schema := range p.allJoinLeaf {
		inTheSameSchema := true
		// if we find a column in the table, the other is not in the same table.
		// They are impossible in the same table. we can directly return.
		findedSchema := false
		for _, unique := range colset {
			if !slices.ContainsFunc(schema.Columns, func(c *expression.Column) bool {
				return c.UniqueID == unique
			}) {
				inTheSameSchema = false
				break
			}
			findedSchema = true
		}
		if inTheSameSchema {
			return true
		} else if findedSchema {
			return false
		}
	}
	return false
}

// getAllJoinLeaf is to get all datasource's schema
func getAllJoinLeaf(plan base.LogicalPlan) []*expression.Schema {
	switch p := plan.(type) {
	case *DataSource, *LogicalAggregation, *LogicalProjection:
		// Because sometimes we put the output of the aggregation into the schema,
		// we can consider it as a new table.
		return []*expression.Schema{p.Schema()}
	default:
		result := make([]*expression.Schema, 0, len(p.Children()))
		for _, child := range p.Children() {
			result = append(result, getAllJoinLeaf(child)...)
		}
		return result
	}
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
	lSchema := lChild.Schema()
	rSchema := rChild.Schema()
	var lFullSchema, rFullSchema *expression.Schema
	// parentused col = t2.a
	// leftChild schema = t1.a(t2.a) + and others
	// rightChild schema = t3 related + and others
	if join, ok := lChild.(*LogicalJoin); ok {
		lFullSchema = join.FullSchema
	}
	if join, ok := rChild.(*LogicalJoin); ok {
		rFullSchema = join.FullSchema
	}
	for _, col := range parentUsedCols {
		if (lSchema != nil && lSchema.Contains(col)) ||
			(lFullSchema != nil && lFullSchema.Contains(col)) {
			leftCols = append(leftCols, col)
		} else if (rSchema != nil && rSchema.Contains(col)) ||
			(rFullSchema != nil && rFullSchema.Contains(col)) {
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
// When it's outer join and there's unique key information. The TopN can be totally pushed down to the join.
// We just need reserve the ORDER informaion
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) (base.LogicalPlan, bool) {
	if topN == nil {
		return p.Children()[idx].PushDownTopN(nil), false
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.Children()[idx].Schema().Contains(col) {
				return p.Children()[idx].PushDownTopN(nil), false
			}
		}
	}
	count, offset := topN.Count+topN.Offset, uint64(0)
	selfEliminated := false
	if p.JoinType == base.LeftOuterJoin {
		innerChild := p.Children()[1]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[1].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		// If it's unique key(unique with not null), we can push the offset down safely whatever the join key is normal eq or nulleq.
		// If the join key is nulleq, then we can only push the offset down when the inner side is unique key.
		// Only when the join key is normal eq, we can push the offset down when the inner side is unique(could be null).
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	} else if p.JoinType == base.RightOuterJoin {
		innerChild := p.Children()[0]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[0].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	}

	newTopN := LogicalTopN{
		Count:            count,
		Offset:           offset,
		ByItems:          make([]*util.ByItems, len(topN.ByItems)),
		PreferLimitToCop: topN.PreferLimitToCop,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	return p.Children()[idx].PushDownTopN(newTopN), selfEliminated
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
	candidatePredicates []expression.Expression) (newRoot base.LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.QueryBlockOffset())
	// add selection above of p
	if parentPlan == nil {
		newRoot = selection
	} else {
		parentPlan.SetChild(currentChildIdx, selection)
	}
	selection.SetChildren(currentPlan)
	if parentPlan == nil {
		return newRoot
	}
	return nil
}

// logical join group ndv is just to output the corresponding child's groupNDV is asked previously and only outer side is cared.
func (p *LogicalJoin) getGroupNDVs(childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == base.RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 {
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

// This function is only used with inner join and semi join.
func (p *LogicalJoin) isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, true, true, true, true)
}

// This function is only used in LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, AntiSemiJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, false, true)
}

// This function is only used in RightOuterJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForRightOuterJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, true, false)
}

// isVaildConstantPropagationExpression is to judge whether the expression is created by PropagationContant is vaild.
//
// Some expressions are not suitable for constant propagation. After constant propagation,
// these expressions will only become a projection, increasing the computational load without
// being able to filter data directly from the data source.
//
// `deriveLeft` and `driveRight` are used in conjunction with `extractOnCondition`.
//
// `canLeftPushDown` and `canRightPushDown` are used to mark that for some joins,
// the left or right condition will not be pushed down. For these conditions that cannot be pushed down,
// we can reject the new expressions from constant propagation.
func (p *LogicalJoin) isVaildConstantPropagationExpression(cond expression.Expression, deriveLeft, deriveRight, canLeftPushDown, canRightPushDown bool) bool {
	_, leftCond, rightCond, otherCond := p.extractOnCondition([]expression.Expression{cond}, deriveLeft, deriveRight)
	if len(otherCond) > 0 {
		// a new expression which is created by constant propagation, is a other condtion, we don't put it
		// into our final result.
		return false
	}
	intest.Assert(len(leftCond) == 0 || len(rightCond) == 0, "An expression cannot be both a left and a right condition at the same time.")
	// When the expression is a left/right condition, we want it to filter more of the underlying data.
	if len(leftCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canLeftPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	if len(rightCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canRightPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	return true
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
			arg0, arg1, ok := expression.IsColOpCol(binop)
			if ok {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if util.IsNullRejected(ctx, leftSchema, expr, true) && !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if util.IsNullRejected(ctx, rightSchema, expr, true) && !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					switch binop.FuncName.L {
					case ast.EQ, ast.NullEQ:
						cond := expression.NewFunctionInternal(ctx.GetExprCtx(), binop.FuncName.L, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
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
			// The IsMutableEffectsExpr check is primarily designed to prevent mutable expressions
			// like rand() > 0.5 from being pushed down; instead, such expressions should remain
			// in other conditions.
			// Checking len(columns) == 0 first is to let filter like rand() > tbl.col
			// to be able pushdown as left or right condition
			if expression.IsMutableEffectsExpr(expr) {
				otherCond = append(otherCond, expr)
				continue
			}
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
	rightCond []expression.Expression, filterCond bool) (_, _ []expression.Expression) {
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case base.RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case base.SemiJoin, base.InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case base.AntiSemiJoin:
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
	child := p.Children()
	rightSchema := child[1].Schema()
	leftSchema := child[0].Schema()
	return p.ExtractOnCondition(conditions, leftSchema, rightSchema, deriveLeft, deriveRight)
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
			p.OtherConditions = slices.Delete(p.OtherConditions, i, i+1)
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
	canBeNAAJ := (p.JoinType == base.AntiSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin) && len(p.EqualConditions) == 0
	if canBeNAAJ && p.SCtx().GetSessionVars().OptimizerEnableNAAJ {
		var otherCond expression.CNFExprs
		for i := range p.OtherConditions {
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

// outerJoinPropConst propagates constant equal and column equal conditions over outer join or anti semi join.
func (p *LogicalJoin) outerJoinPropConst(predicates []expression.Expression, vaildExprFunc expression.VaildConstantPropagationExpressionFuncType) []expression.Expression {
	children := p.Children()
	innerTable := children[1]
	outerTable := children[0]
	if p.JoinType == base.RightOuterJoin {
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
	nullSensitive := p.JoinType == base.AntiLeftOuterSemiJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiSemiJoin
	exprCtx := p.SCtx().GetExprCtx()
	outerTableSchema := outerTable.Schema()
	innerTableSchema := innerTable.Schema()
	joinConds, predicates = expression.PropConstForOuterJoin(exprCtx, joinConds, predicates, outerTableSchema,
		innerTableSchema, p.SCtx().GetSessionVars().AlwaysKeepJoinKey, nullSensitive, vaildExprFunc)
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

// SemiJoinRewrite rewrites semi join to inner join with aggregation.
// Note: This rewriter is only used for exists subquery.
// And it also requires the hint `SEMI_JOIN_REWRITE` or variable tidb_opt_enable_sem_join_rewrite
// to be set.
// For example:
//
//	select * from t where exists (select /*+ SEMI_JOIN_REWRITE() */ * from s where s.a = t.a);
//
// will be rewriten to:
//
//	select * from t join (select a from s group by a) s on t.a = s.a;
func (p *LogicalJoin) SemiJoinRewrite() (base.LogicalPlan, error) {
	// If it's not a join, or not a (outer) semi join. We just return it since no optimization is needed.
	// Actually the check of the preferRewriteSemiJoin is a superset of checking the join type. We remain them for a better understanding.
	if !(p.JoinType == base.SemiJoin || p.JoinType == base.LeftOuterSemiJoin) {
		return p.Self(), nil
	}
	if _, ok := p.Self().(*LogicalApply); ok {
		return p.Self(), nil
	}
	// Get by hint or session variable.
	if (p.PreferJoinType&utilhint.PreferRewriteSemiJoin) == 0 && !p.SCtx().GetSessionVars().EnableSemiJoinRewrite {
		return p.Self(), nil
	}
	// The preferRewriteSemiJoin flag only be used here. We should reset it in order to not affect other parts.
	p.PreferJoinType &= ^utilhint.PreferRewriteSemiJoin

	if p.JoinType == base.LeftOuterSemiJoin {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for LeftOuterSemiJoin.")
		return p.Self(), nil
	}

	// If we have jumped the above if condition. We can make sure that the current join is a non-correlated one.

	// If there's left condition or other condition, we cannot rewrite
	if len(p.LeftConditions) > 0 || len(p.OtherConditions) > 0 {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for SemiJoin with left conditions or other conditions.")
		return p.Self(), nil
	}

	innerChild := p.Children()[1]

	// If there's right conditions:
	//   - If it's semi join, then right condition should be pushed.
	//   - If it's outer semi join, then it still should be pushed since the outer join should not remain any cond of the inner side.
	// But the aggregation we added may block the predicate push down since we've not maintained the functional dependency to pass the equiv class to guide the push down.
	// So we create a selection before we build the aggregation.
	if len(p.RightConditions) > 0 {
		sel := LogicalSelection{Conditions: make([]expression.Expression, len(p.RightConditions))}.Init(p.SCtx(), innerChild.QueryBlockOffset())
		copy(sel.Conditions, p.RightConditions)
		sel.SetChildren(innerChild)
		innerChild = sel
	}

	subAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(p.EqualConditions)),
		GroupByItems: make([]expression.Expression, 0, len(p.EqualConditions)),
	}.Init(p.SCtx(), p.Children()[1].QueryBlockOffset())

	aggOutputCols := make([]*expression.Column, 0, len(p.EqualConditions))
	for i := range p.EqualConditions {
		innerCol := p.EqualConditions[i].GetArgs()[1].(*expression.Column)
		firstRow, err := aggregation.NewAggFuncDesc(p.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{innerCol}, false)
		if err != nil {
			return nil, err
		}
		subAgg.AggFuncs = append(subAgg.AggFuncs, firstRow)
		subAgg.GroupByItems = append(subAgg.GroupByItems, innerCol)
		aggOutputCols = append(aggOutputCols, innerCol)
	}
	subAgg.SetChildren(innerChild)
	subAgg.SetSchema(expression.NewSchema(aggOutputCols...))
	subAgg.BuildSelfKeyInfo(subAgg.Schema())
	innerJoin := LogicalJoin{
		JoinType:        base.InnerJoin,
		HintInfo:        p.HintInfo,
		PreferJoinType:  p.PreferJoinType,
		PreferJoinOrder: p.PreferJoinOrder,
		EqualConditions: make([]*expression.ScalarFunction, 0, len(p.EqualConditions)),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	innerJoin.SetChildren(p.Children()[0], subAgg)
	innerJoin.SetSchema(expression.MergeSchema(p.Children()[0].Schema().Clone(), subAgg.Schema().Clone()))
	innerJoin.AttachOnConds(expression.ScalarFuncs2Exprs(p.EqualConditions))
	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(p.Children()[0].Schema().Columns),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	proj.SetChildren(innerJoin)
	proj.SetSchema(p.Children()[0].Schema().Clone())
	return proj, nil
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
	isOuterSemi := (p.JoinType == base.LeftOuterSemiJoin) || (p.JoinType == base.AntiLeftOuterSemiJoin)
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
	arg0, arg1, ok := expression.IsColOpCol(binop)
	if !ok {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if util.IsNullRejected(ctx, schema, expr, true) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx.GetExprCtx(), childCol)
	}
	return nil
}

// BuildLogicalJoinSchema builds the schema for join operator.
func BuildLogicalJoinSchema(joinType base.JoinType, join base.LogicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case base.SemiJoin, base.AntiSemiJoin:
		return leftSchema.Clone()
	case base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == base.LeftOuterJoin {
		util.ResetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == base.RightOuterJoin {
		util.ResetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}
