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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalUnionScan is used in non read-only txn or for scanning a local temporary table whose snapshot data is located in memory.
type LogicalUnionScan struct {
	BaseLogicalPlan

	Conditions []expression.Expression

	HandleCols util.HandleCols
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx base.PlanContext, qbOffset int) *LogicalUnionScan {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeUnionScan, &p, qbOffset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (p *LogicalUnionScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "conds:%s",
		expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.Conditions))
	fmt.Fprintf(buffer, ", handle:%s", p.HandleCols)
	return buffer.String()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	var predicatesWithVCol, predicatesWithoutVCol []expression.Expression
	// predicates with virtual columns can't be pushed down to TiKV/TiFlash so they'll be put into a Projection
	// below the UnionScan, but the current UnionScan doesn't support placing Projection below it, see #53951.
	for _, expr := range predicates {
		if expression.ContainVirtualColumn([]expression.Expression{expr}) {
			predicatesWithVCol = append(predicatesWithVCol, expr)
		} else {
			predicatesWithoutVCol = append(predicatesWithoutVCol, expr)
		}
	}
	predicates = predicatesWithoutVCol
	retainedPredicates, _ := p.Children()[0].PredicatePushDown(predicates, opt)
	p.Conditions = make([]expression.Expression, 0, len(predicates))
	p.Conditions = append(p.Conditions, predicates...)
	// The conditions in UnionScan is only used for added rows, so parent Selection should not be removed.
	retainedPredicates = append(retainedPredicates, predicatesWithVCol...)
	return retainedPredicates, p
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	for i := 0; i < p.HandleCols.NumCols(); i++ {
		parentUsedCols = append(parentUsedCols, p.HandleCols.GetCol(i))
	}
	for _, col := range p.Schema().Columns {
		if col.ID == model.ExtraPhysTblID {
			parentUsedCols = append(parentUsedCols, col)
		}
	}
	condCols := expression.ExtractColumnsFromExpressions(nil, p.Conditions, nil)
	parentUsedCols = append(parentUsedCols, condCols...)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats inherits BaseLogicalPlan.LogicalPlan.<11th> implementation.

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalUnionScan) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalUnionScan(p, prop)
}

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

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
