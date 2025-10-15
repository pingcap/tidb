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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	BaseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression `hash64-equals:"true"`
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx base.PlanContext, qbOffset int) *LogicalSelection {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeSel, &p, qbOffset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (p *LogicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.Conditions))
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (p *LogicalSelection) ReplaceExprColumns(replace map[string]*expression.Column) {
	for i, expr := range p.Conditions {
		p.Conditions[i] = ruleutil.ResolveExprAndReplace(expr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode implements base.LogicalPlan.<0th> interface.
func (p *LogicalSelection) HashCode() []byte {
	// PlanType + SelectOffset + ConditionNum + [Conditions]
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Conditions)*25)
	result = util.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.TP()))
	result = util.EncodeIntAsUint32(result, p.QueryBlockOffset())
	result = util.EncodeIntAsUint32(result, len(p.Conditions))

	condHashCodes := make([][]byte, len(p.Conditions))
	for i, expr := range p.Conditions {
		condHashCodes[i] = expr.HashCode()
	}
	// Sort the conditions, so `a > 1 and a < 100` can equal to `a < 100 and a > 1`.
	slices.SortFunc(condHashCodes, func(i, j []byte) int { return bytes.Compare(i, j) })

	for _, condHashCode := range condHashCodes {
		result = util.EncodeIntAsUint32(result, len(condHashCode))
		result = append(result, condHashCode...)
	}
	return result
}

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, base.LogicalPlan, error) {
	exprCtx := p.SCtx().GetExprCtx()
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	predicates = constraint.DeleteTrueExprs(exprCtx, stmtCtx, predicates)
	// Apply predicate simplification to the conditions. because propagateConstant has been dealed in the ConstantPropagationSolver
	// so we don't need to do it again.
	p.Conditions = ruleutil.ApplyPredicateSimplification(p.SCtx(), p.Conditions, false, false, nil)
	var child base.LogicalPlan
	var retConditions []expression.Expression
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	var err error
	retConditions, child, err = p.Children()[0].PredicatePushDown(append(canBePushDown, predicates...))
	if err != nil {
		return nil, nil, err
	}
	retConditions = append(retConditions, canNotBePushDown...)
	sctx := p.SCtx()
	if len(retConditions) > 0 {
		p.Conditions = ruleutil.ApplyPredicateSimplification(sctx, retConditions, true, false, nil)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, p.Conditions)
		if dual != nil {
			return nil, dual, nil
		}
		return nil, p, nil
	}
	p.Conditions = p.Conditions[:0]
	return nil, child, nil
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	child := p.Children()[0]
	parentUsedCols = append(parentUsedCols, expression.ExtractColumnsFromExpressions(p.Conditions, nil)...)
	var err error
	p.Children()[0], err = child.PruneColumns(parentUsedCols)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// FindBestTask implements base.LogicalPlan.<3rd> interface.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (p *LogicalSelection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.BaseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.MaxOneRow() {
		return
	}
	eqCols := make(map[int64]struct{}, len(childSchema[0].Columns))
	for _, cond := range p.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			for i, arg := range sf.GetArgs() {
				if col, isCol := arg.(*expression.Column); isCol {
					_, isCon := sf.GetArgs()[1-i].(*expression.Constant)
					_, isCorCol := sf.GetArgs()[1-i].(*expression.CorrelatedColumn)
					if isCon || isCorCol {
						eqCols[col.UniqueID] = struct{}{}
					}
					break
				}
			}
		}
	}
	p.SetMaxOneRow(ruleutil.CheckMaxOneRowCond(eqCols, childSchema[0]))
}

// PushDownTopN inherits BaseLogicalPlan.<5th> implementation.

// DeriveTopN implements the base.LogicalPlan.<6th> interface.
func (p *LogicalSelection) DeriveTopN() base.LogicalPlan {
	s := p.Self().(*LogicalSelection)
	windowIsTopN, limitValue := p.windowIsTopN()
	if windowIsTopN {
		child := s.Children()[0].(*LogicalWindow)
		grandChild := child.Children()[0]
		// Build order by for derived Limit
		byItems := make([]*util.ByItems, 0, len(child.OrderBy))
		for _, col := range child.OrderBy {
			byItems = append(byItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
		}
		// Build derived Limit
		derivedTopN := LogicalTopN{Count: limitValue, ByItems: byItems, PartitionBy: child.GetPartitionBy()}.Init(grandChild.SCtx(), grandChild.QueryBlockOffset())
		derivedTopN.SetChildren(grandChild)
		/* return select->datasource->topN->window */
		child.SetChildren(derivedTopN)
		s.SetChildren(child)
		return s
	}
	return s
}

// PredicateSimplification inherits BaseLogicalPlan.<7th> implementation.
func (p *LogicalSelection) PredicateSimplification() base.LogicalPlan {
	// it is only test
	pp := p.Self().(*LogicalSelection)
	intest.AssertFunc(func() bool {
		ectx := p.SCtx().GetExprCtx().GetEvalCtx()
		expected := make([]string, 0, len(pp.Conditions))
		for _, cond := range pp.Conditions {
			expected = append(expected, cond.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		actualExprs := ruleutil.ApplyPredicateSimplification(p.SCtx(), pp.Conditions, false, false, nil)
		actual := make([]string, 0, len(actualExprs))
		for _, cond := range actualExprs {
			actual = append(actual, cond.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		return slices.Equal(expected, actual)
	})
	return pp.BaseLogicalPlan.PredicateSimplification()
}

// ConstantPropagation inherits BaseLogicalPlan.<8th> implementation.

// PullUpConstantPredicates implements the base.LogicalPlan.<9th> interface.
func (p *LogicalSelection) PullUpConstantPredicates() []expression.Expression {
	var result []expression.Expression
	for _, candidatePredicate := range p.Conditions {
		// the candidate predicate should be a constant and compare predicate
		match := expression.ValidCompareConstantPredicate(p.SCtx().GetExprCtx().GetEvalCtx(), candidatePredicate)
		if match {
			result = append(result, candidatePredicate)
		}
	}
	return result
}

// RecursiveDeriveStats inherits BaseLogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	p.SetStats(childStats[0].Scale(p.SCtx().GetSessionVars(), cost.SelectionFactor))
	p.StatsInfo().GroupNDVs = nil
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (*LogicalSelection) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (p *LogicalSelection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// MaxOneRow inherits BaseLogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.<21st> interface.

// ExtractFD implements the base.LogicalPlan.<22nd> interface.
func (p *LogicalSelection) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := p.BaseLogicalPlan.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := intset.NewFastIntSet()
	notnullColsUniqueIDs := intset.NewFastIntSet()
	// eg: select t2.a, count(t2.b) from t1 join t2 using (a) where t1.a = 1
	// join's schema will miss t2.a while join.full schema has. since selection
	// itself doesn't contain schema, extracting schema should tell them apart.
	var columns []*expression.Column
	if join, ok := p.Children()[0].(*LogicalJoin); ok && join.FullSchema != nil {
		columns = join.FullSchema.Columns
	} else {
		columns = p.Schema().Columns
	}
	for _, one := range columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
	}

	// extract the not null attributes from selection conditions.
	notnullColsUniqueIDs.UnionWith(util.ExtractNotNullFromConds(p.Conditions, p))

	// extract the constant cols from selection conditions.
	constUniqueIDs := util.ExtractConstantCols(p.Conditions, p.SCtx(), fds)

	// extract equivalence cols.
	equivUniqueIDs := util.ExtractEquivalenceCols(p.Conditions, p.SCtx(), fds)

	// apply operator's characteristic's FD setting.
	fds.MakeNotNull(notnullColsUniqueIDs)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	fds.ProjectCols(outputColsUniqueIDs)
	// just trace it down in every operator for test checking.
	p.SetFDs(fds)
	return fds
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin implements base.LogicalPlan.<24th> interface.
func (p *LogicalSelection) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	s := p.Self().(*LogicalSelection)
	combinedCond := append(predicates, s.Conditions...)
	child := s.Children()[0]
	child = child.ConvertOuterToInnerJoin(combinedCond)
	s.SetChildren(child)
	return s
}

// *************************** end implementation of logicalPlan interface ***************************

func splitSetGetVarFunc(filters []expression.Expression) (canBePushDown, canNotBePushDown []expression.Expression) {
	canBePushDown = make([]expression.Expression, 0, len(filters))
	canNotBePushDown = make([]expression.Expression, 0, len(filters))
	for _, expr := range filters {
		if expression.HasGetSetVarFunc(expr) {
			canNotBePushDown = append(canNotBePushDown, expr)
		} else {
			canBePushDown = append(canBePushDown, expr)
		}
	}
	return canBePushDown, canNotBePushDown
}

/*
		Check the following pattern of filter over row number window function:
	  - Filter is simple condition of row_number < value or row_number <= value
	  - The window function is a simple row number
	  - With default frame: rows between current row and current row. Check is not necessary since
	    current row is only frame applicable to row number
	  - Child is a data source with no tiflash option.
*/
func (p *LogicalSelection) windowIsTopN() (bool, uint64) {
	// Check if child is window function.
	child, isLogicalWindow := p.Children()[0].(*LogicalWindow)
	if !isLogicalWindow {
		return false, 0
	}

	if len(p.Conditions) != 1 {
		return false, 0
	}

	// Check if filter is column < constant or column <= constant. If it is in this form find column and constant.
	column, limitValue := expression.FindUpperBound(p.Conditions[0])
	if column == nil || limitValue <= 0 {
		return false, 0
	}

	// Check if filter on window function
	windowColumns := child.GetWindowResultColumns()
	if len(windowColumns) != 1 || !(column.Equal(p.SCtx().GetExprCtx().GetEvalCtx(), windowColumns[0])) {
		return false, 0
	}

	grandChild := child.Children()[0]
	dataSource, isDataSource := grandChild.(*DataSource)
	if !isDataSource {
		return false, 0
	}

	// Give up if TiFlash is one possible access path of all. Pushing down window aggregation is good enough in this case.
	for _, path := range dataSource.AllPossibleAccessPaths {
		if path.StoreType == kv.TiFlash {
			return false, 0
		}
	}

	if len(child.WindowFuncDescs) == 1 && child.WindowFuncDescs[0].Name == "row_number" &&
		child.Frame.Type == ast.Rows && child.Frame.Start.Type == ast.CurrentRow && child.Frame.End.Type == ast.CurrentRow &&
		checkPartitionBy(child, dataSource) {
		return true, uint64(limitValue)
	}
	return false, 0
}

// checkPartitionBy mainly checks if partition by of window function is a prefix of
// data order (clustered index) of the data source. TiFlash is allowed only for empty partition by.
func checkPartitionBy(p *LogicalWindow, d *DataSource) bool {
	// No window partition by. We are OK.
	if len(p.PartitionBy) == 0 {
		return true
	}

	// Table not clustered and window has partition by. Can not do the TopN push down.
	if d.HandleCols == nil {
		return false
	}

	if len(p.PartitionBy) > d.HandleCols.NumCols() {
		return false
	}

	for i, col := range p.PartitionBy {
		if !(col.Col.EqualColumn(d.HandleCols.GetCol(i))) {
			return false
		}
	}
	return true
}
