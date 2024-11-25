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
	"encoding/binary"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	PartitionBy      []property.SortItem `hash64-equals:"true"` // This is used for enhanced topN optimization
	Offset           uint64              `hash64-equals:"true"`
	Count            uint64              `hash64-equals:"true"`
	PreferLimitToCop bool
	IsPartial        bool
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx base.PlanContext, offset int) *LogicalLimit {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeLimit, &p, offset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString("")
	if len(p.GetPartitionBy()) > 0 {
		buffer = util.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), false)
		fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	} else {
		fmt.Fprintf(buffer, "offset:%v, count:%v", p.Offset, p.Count)
	}
	return buffer.String()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode implements LogicalPlan.<0th> interface.
func (p *LogicalLimit) HashCode() []byte {
	// PlanType + SelectOffset + Offset + Count
	result := make([]byte, 24)
	binary.BigEndian.PutUint32(result, uint32(plancodec.TypeStringToPhysicalID(p.TP())))
	binary.BigEndian.PutUint32(result[4:], uint32(p.QueryBlockOffset()))
	binary.BigEndian.PutUint64(result[8:], p.Offset)
	binary.BigEndian.PutUint64(result[16:], p.Count)
	return result
}

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalLimit) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	// Limit forbids any condition to push down.
	p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	savedUsedCols := make([]*expression.Column, len(parentUsedCols))
	copy(savedUsedCols, parentUsedCols)
	var err error
	if p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt); err != nil {
		return nil, err
	}
	p.SetSchema(nil)
	p.InlineProjection(savedUsedCols, opt)
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (p *LogicalLimit) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.SetMaxOneRow(true)
	}
}

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalLimit) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	child := p.Children()[0].PushDownTopN(p.convertToTopN(opt), opt)
	if topN != nil {
		return topN.AttachChild(child, opt)
	}
	return child
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}
	p.SetStats(util.DeriveLimitStats(childStats[0], float64(p.Count)))
	return p.StatsInfo(), nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties inherits BaseLogicalPlan.LogicalPlan.<13th> implementation.

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalLimit) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalLimit(p, prop)
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

// GetPartitionBy returns partition by fields
func (p *LogicalLimit) GetPartitionBy() []property.SortItem {
	return p.PartitionBy
}

func (p *LogicalLimit) convertToTopN(opt *optimizetrace.LogicalOptimizeOp) *LogicalTopN {
	topn := LogicalTopN{Offset: p.Offset, Count: p.Count, PreferLimitToCop: p.PreferLimitToCop}.Init(p.SCtx(), p.QueryBlockOffset())
	appendConvertTopNTraceStep(p, topn, opt)
	return topn
}

func appendConvertTopNTraceStep(p base.LogicalPlan, topN *LogicalTopN, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is converted into %v_%v", p.TP(), p.ID(), topN.TP(), topN.ID())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}
