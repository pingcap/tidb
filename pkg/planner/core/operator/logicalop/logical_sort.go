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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	BaseLogicalPlan

	ByItems []*util.ByItems `hash64-equals:"true" shallow-ref:"true"`
}

// Init initializes LogicalSort.
func (ls LogicalSort) Init(ctx base.PlanContext, offset int) *LogicalSort {
	ls.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeSort, &ls, offset)
	return &ls
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (ls *LogicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	eCtx := ls.SCtx().GetExprCtx().GetEvalCtx()
	return util.ExplainByItems(eCtx, buffer, ls.ByItems).String()
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (ls *LogicalSort) ReplaceExprColumns(replace map[string]*expression.Column) {
	for i, byItem := range ls.ByItems {
		ls.ByItems[i].Expr = ruleutil.ResolveExprAndReplace(byItem.Expr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns implements base.LogicalPlan.<2nd> interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	var cols []*expression.Column
	ls.ByItems, cols = pruneByItems(ls, ls.ByItems)
	parentUsedCols = append(parentUsedCols, cols...)
	var err error
	ls.Children()[0], err = ls.Children()[0].PruneColumns(parentUsedCols)
	if err != nil {
		return nil, err
	}
	return ls, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (ls *LogicalSort) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN == nil {
		return ls.BaseLogicalPlan.PushDownTopN(nil)
	} else if topN.IsLimit() {
		topN.ByItems = ls.ByItems
		return ls.Children()[0].PushDownTopN(topN)
	}
	// If a TopN is pushed down, this sort is useless.
	return ls.Children()[0].PushDownTopN(topN)
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats inherits BaseLogicalPlan.LogicalPlan.<11th> implementation.

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (ls *LogicalSort) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	propCols := getPossiblePropertyFromByItems(ls.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (ls *LogicalSort) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// GetUsedCols extracts all of the Columns used by agg including ByItems.
func (ls *LogicalSort) GetUsedCols() (usedCols []*expression.Column) {
	for _, byItem := range ls.ByItems {
		usedCols = append(usedCols, expression.ExtractColumns(byItem.Expr)...)
	}
	return usedCols
}

func getPossiblePropertyFromByItems(items []*util.ByItems) []*expression.Column {
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			break
		}
		cols = append(cols, col)
	}
	return cols
}
