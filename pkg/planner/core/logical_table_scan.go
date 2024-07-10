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

package core

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// LogicalTableScan is the logical table scan operator for TiKV.
type LogicalTableScan struct {
	logicalop.LogicalSchemaProducer
	Source      *DataSource
	HandleCols  util.HandleCols
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range
}

// Init initializes LogicalTableScan.
func (ts LogicalTableScan) Init(ctx base.PlanContext, offset int) *LogicalTableScan {
	ts.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeTableScan, &ts, offset)
	return &ts
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (ts *LogicalTableScan) ExplainInfo() string {
	ectx := ts.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString(ts.Source.ExplainInfo())
	if ts.Source.HandleCols != nil {
		fmt.Fprintf(buffer, ", pk col:%s", ts.Source.HandleCols.StringWithCtx(ectx))
	}
	if len(ts.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", ts.AccessConds)
	}
	return buffer.String()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.<1st> implementation.

// PruneColumns inherits BaseLogicalPlan.<2nd> implementation.

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (ts *LogicalTableScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	ts.Source.BuildKeyInfo(selfSchema, childSchema)
}

// PushDownTopN inherits BaseLogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (ts *LogicalTableScan) DeriveStats(_ []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, _ [][]*expression.Column) (_ *property.StatsInfo, err error) {
	ts.Source.initStats(nil)
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	exprCtx := ts.SCtx().GetExprCtx()
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(exprCtx, expr)
	}
	ts.SetStats(ts.Source.deriveStatsByFilter(ts.AccessConds, nil))
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleCols != nil {
		// TODO: restrict mem usage of table ranges.
		ts.Ranges, _, _, err = ranger.BuildTableRange(ts.AccessConds, ts.SCtx().GetRangerCtx(), ts.HandleCols.GetCol(0).RetType, 0)
	} else {
		isUnsigned := false
		if ts.Source.TableInfo.PKIsHandle {
			if pkColInfo := ts.Source.TableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, err
	}
	return ts.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (ts *LogicalTableScan) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	if ts.HandleCols != nil {
		cols := make([]*expression.Column, ts.HandleCols.NumCols())
		for i := 0; i < ts.HandleCols.NumCols(); i++ {
			cols[i] = ts.HandleCols.GetCol(i)
		}
		return [][]*expression.Column{cols}
	}
	return nil
}

// ExhaustPhysicalPlans inherits BaseLogicalPlan.<14th> implementation.

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
