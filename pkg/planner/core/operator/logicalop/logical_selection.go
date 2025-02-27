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
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
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
	for _, expr := range p.Conditions {
		ruleutil.ResolveExprAndReplace(expr, replace)
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
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	predicates = constraint.DeleteTrueExprs(p, predicates)
	p.Conditions = constraint.DeleteTrueExprs(p, p.Conditions)
	var child base.LogicalPlan
	var retConditions []expression.Expression
	var originConditions []expression.Expression
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	originConditions = canBePushDown
	retConditions, child = p.Children()[0].PredicatePushDown(append(canBePushDown, predicates...), opt)
	retConditions = append(retConditions, canNotBePushDown...)
	exprCtx := p.SCtx().GetExprCtx()
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(exprCtx, retConditions)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, p.Conditions)
		if dual != nil {
			AppendTableDualTraceStep(p, dual, p.Conditions, opt)
			return nil, dual
		}
		return nil, p
	}
	appendSelectionPredicatePushDownTraceStep(p, originConditions, opt)
	return nil, child
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := p.Children()[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	var err error
	p.Children()[0], err = child.PruneColumns(parentUsedCols, opt)
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
func (p *LogicalSelection) DeriveTopN(opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	s := p.Self().(*LogicalSelection)
	windowIsTopN, limitValue := utilfuncp.WindowIsTopN(s)
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
		appendDerivedTopNTrace(s, opt)
		return s
	}
	return s
}

// PredicateSimplification inherits BaseLogicalPlan.<7th> implementation.

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
	p.SetStats(childStats[0].Scale(cost.SelectionFactor))
	p.StatsInfo().GroupNDVs = nil
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (*LogicalSelection) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalSelection) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalSelection(p, prop)
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

// CanPushDown is utility function to check whether we can push down Selection to TiKV or TiFlash
func (p *LogicalSelection) CanPushDown(storeTp kv.StoreType) bool {
	return !expression.ContainVirtualColumn(p.Conditions) &&
		p.CanPushToCop(storeTp) &&
		expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Conditions, storeTp)
}

func splitSetGetVarFunc(filters []expression.Expression) ([]expression.Expression, []expression.Expression) {
	canBePushDown := make([]expression.Expression, 0, len(filters))
	canNotBePushDown := make([]expression.Expression, 0, len(filters))
	for _, expr := range filters {
		if expression.HasGetSetVarFunc(expr) {
			canNotBePushDown = append(canNotBePushDown, expr)
		} else {
			canBePushDown = append(canBePushDown, expr)
		}
	}
	return canBePushDown, canNotBePushDown
}

// AppendTableDualTraceStep appends a trace step for replacing a plan with a dual table.
func AppendTableDualTraceStep(replaced base.LogicalPlan, dual base.LogicalPlan, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is replaced by %v_%v", replaced.TP(), replaced.ID(), dual.TP(), dual.ID())
	}
	ectx := replaced.SCtx().GetExprCtx().GetEvalCtx()
	reason := func() string {
		buffer := bytes.NewBufferString("The conditions[")
		for i, cond := range conditions {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		buffer.WriteString("] are constant false or null")
		return buffer.String()
	}
	opt.AppendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}

func appendSelectionPredicatePushDownTraceStep(p *LogicalSelection, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is removed", p.TP(), p.ID())
	}
	reason := func() string {
		return ""
	}
	if len(conditions) > 0 {
		evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
		reason = func() string {
			buffer := bytes.NewBufferString("The conditions[")
			for i, cond := range conditions {
				if i > 0 {
					buffer.WriteString(",")
				}
				buffer.WriteString(cond.StringWithCtx(evalCtx, errors.RedactLogDisable))
			}
			fmt.Fprintf(buffer, "] in %v_%v are pushed down", p.TP(), p.ID())
			return buffer.String()
		}
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendDerivedTopNTrace(topN base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	child := topN.Children()[0]
	action := func() string {
		return fmt.Sprintf("%v_%v top N added below  %v_%v ", topN.TP(), topN.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v filter on row number", topN.TP())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}
