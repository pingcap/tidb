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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalLock represents a select lock plan.
type LogicalLock struct {
	BaseLogicalPlan

	Lock         *ast.SelectLockInfo
	TblID2Handle map[int64][]util.HandleCols

	// tblID2phyTblIDCol is used for partitioned tables,
	// the child executor need to return an extra column containing
	// the Physical Table ID (i.e. from which partition the row came from)
	TblID2PhysTblIDCol map[int64]*expression.Column
}

// Init initializes LogicalLock.
func (p LogicalLock) Init(ctx base.PlanContext) *LogicalLock {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeLock, &p, 0)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var err error
	if !IsSelectForUpdateLockType(p.Lock.LockType) {
		// when use .baseLogicalPlan to call the PruneColumns, it means current plan itself has
		// nothing to pruning or plan change, so they resort to its children's column pruning logic.
		// so for the returned logical plan here, p is definitely determined, we just need to collect
		// those extra deeper call error in handling children's column pruning.
		_, err = p.BaseLogicalPlan.PruneColumns(parentUsedCols, opt)
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	for tblID, cols := range p.TblID2Handle {
		for _, col := range cols {
			for i := 0; i < col.NumCols(); i++ {
				parentUsedCols = append(parentUsedCols, col.GetCol(i))
			}
		}
		if physTblIDCol, ok := p.TblID2PhysTblIDCol[tblID]; ok {
			// If the children include partitioned tables, there is an extra partition ID column.
			parentUsedCols = append(parentUsedCols, physTblIDCol)
		}
	}
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalLock) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		p.Children()[0] = p.Children()[0].PushDownTopN(topN, opt)
	}
	return p.Self()
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats inherits BaseLogicalPlan.LogicalPlan.<11th> implementation.

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalLock) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalLock(p, prop)
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

// IsSelectForUpdateLockType checks if the select lock type is for update type.
func IsSelectForUpdateLockType(lockType ast.SelectLockType) bool {
	if lockType == ast.SelectLockForUpdate ||
		lockType == ast.SelectLockForShare ||
		lockType == ast.SelectLockForUpdateNoWait ||
		lockType == ast.SelectLockForUpdateWaitN {
		return true
	}
	return false
}
