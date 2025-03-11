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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	ByItems []*util.ByItems `hash64-equals:"true"`
	// PartitionBy is used for extended TopN to consider K heaps. Used by rule_derive_topn_from_window
	PartitionBy      []property.SortItem `hash64-equals:"true"` // This is used for enhanced topN optimization
	Offset           uint64              `hash64-equals:"true"`
	Count            uint64              `hash64-equals:"true"`
	PreferLimitToCop bool                `hash64-equals:"true"`
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx base.PlanContext, offset int) *LogicalTopN {
	lt.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeTopN, &lt, offset)
	return &lt
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (lt *LogicalTopN) ExplainInfo() string {
	ectx := lt.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString("")
	buffer = util.ExplainPartitionBy(ectx, buffer, lt.GetPartitionBy(), false)
	if len(lt.GetPartitionBy()) > 0 && len(lt.ByItems) > 0 {
		buffer.WriteString("order by ")
	}
	buffer = util.ExplainByItems(lt.SCtx().GetExprCtx().GetEvalCtx(), buffer, lt.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", lt.Offset, lt.Count)
	return buffer.String()
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (lt *LogicalTopN) ReplaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range lt.ByItems {
		ruleutil.ResolveExprAndReplace(byItem.Expr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns implements base.LogicalPlan.<2nd> interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := lt.Children()[0]
	var cols []*expression.Column

	snapParentUsedCols := make([]*expression.Column, 0, len(parentUsedCols))
	snapParentUsedCols = append(snapParentUsedCols, parentUsedCols...)

	lt.ByItems, cols = pruneByItems(lt, lt.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	var err error
	lt.Children()[0], err = child.PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	// If the length of parentUsedCols is 0, it means that the parent plan does not need this plan to output related
	// results, such as: select count(*) from t
	// So we set the schema of topN to 0. After inlineprojection, the schema of topN will be set to the shortest column
	// in its child plan, and this column will not be used later.
	if len(snapParentUsedCols) == 0 {
		lt.SetSchema(nil)
	}
	lt.InlineProjection(snapParentUsedCols, opt)

	return lt, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (lt *LogicalTopN) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	lt.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	if lt.Count == 1 {
		lt.SetMaxOneRow(true)
	}
}

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5rd> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> interface.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && lt.StatsInfo() != nil {
		return lt.StatsInfo(), false, nil
	}
	lt.SetStats(util.DeriveLimitStats(childStats[0], float64(lt.Count)))
	return lt.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (lt *LogicalTopN) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	propCols := getPossiblePropertyFromByItems(lt.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (lt *LogicalTopN) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalTopN(lt, prop)
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (lt *LogicalTopN) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
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

// GetPartitionBy returns partition by fields
func (lt *LogicalTopN) GetPartitionBy() []property.SortItem {
	return lt.PartitionBy
}

// IsLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) IsLimit() bool {
	return len(lt.ByItems) == 0
}

// AttachChild set p as topn's child, for difference with LogicalPlan.SetChild().
// AttachChild will tracer the children change while SetChild doesn't.
func (lt *LogicalTopN) AttachChild(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	// Remove this TopN if its child is a TableDual.
	dual, isDual := p.(*LogicalTableDual)
	if isDual {
		numDualRows := uint64(dual.RowCount)
		if numDualRows < lt.Offset {
			dual.RowCount = 0
			return dual
		}
		dual.RowCount = int(min(numDualRows-lt.Offset, lt.Count))
		return dual
	}

	if lt.IsLimit() {
		limit := LogicalLimit{
			Count:            lt.Count,
			Offset:           lt.Offset,
			PreferLimitToCop: lt.PreferLimitToCop,
			PartitionBy:      lt.GetPartitionBy(),
		}.Init(lt.SCtx(), lt.QueryBlockOffset())
		limit.SetChildren(p)
		appendTopNPushDownTraceStep(limit, p, opt)
		return limit
	}
	// Then lt must be topN.
	lt.SetChildren(p)
	appendTopNPushDownTraceStep(lt, p, opt)
	return lt
}

func appendTopNPushDownTraceStep(parent base.LogicalPlan, child base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is added as %v_%v's parent", parent.TP(), parent.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v is pushed down", parent.TP())
	}
	opt.AppendStepToCurrent(parent.ID(), parent.TP(), reason, action)
}
