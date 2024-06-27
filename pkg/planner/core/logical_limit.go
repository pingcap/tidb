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
	"encoding/binary"
	"fmt"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	logicalop.LogicalSchemaProducer

	PartitionBy      []property.SortItem // This is used for enhanced topN optimization
	Offset           uint64
	Count            uint64
	PreferLimitToCop bool
	IsPartial        bool
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx base.PlanContext, offset int) *LogicalLimit {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeLimit, &p, offset)
	return &p
}

// GetPartitionBy returns partition by fields
func (lt *LogicalLimit) GetPartitionBy() []property.SortItem {
	return lt.PartitionBy
}

// BuildKeyInfo implements base.LogicalPlan BuildKeyInfo interface.
func (p *LogicalLimit) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.SetMaxOneRow(true)
	}
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalLimit) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	// Limit forbids any condition to push down.
	p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		return p.StatsInfo(), nil
	}
	p.SetStats(deriveLimitStats(childStats[0], float64(p.Count)))
	return p.StatsInfo(), nil
}

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.GetPartitionBy()) > 0 {
		buffer = explainPartitionBy(buffer, p.GetPartitionBy(), false)
		fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	} else {
		fmt.Fprintf(buffer, "offset:%v, count:%v", p.Offset, p.Count)
	}
	return buffer.String()
}

// HashCode implements LogicalPlan interface.
func (p *LogicalLimit) HashCode() []byte {
	// PlanType + SelectOffset + Offset + Count
	result := make([]byte, 24)
	binary.BigEndian.PutUint32(result, uint32(plancodec.TypeStringToPhysicalID(p.TP())))
	binary.BigEndian.PutUint32(result[4:], uint32(p.QueryBlockOffset()))
	binary.BigEndian.PutUint64(result[8:], p.Offset)
	binary.BigEndian.PutUint64(result[16:], p.Count)
	return result
}

// PushDownTopN implements the LogicalPlan interface.
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

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalLimit) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return getLimitPhysicalPlans(p, prop)
}


// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	if len(parentUsedCols) == 0 { // happens when LIMIT appears in UPDATE.
		return p, nil
	}

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

func (p *LogicalLimit) convertToTopN(opt *optimizetrace.LogicalOptimizeOp) *LogicalTopN {
	topn := LogicalTopN{Offset: p.Offset, Count: p.Count, PreferLimitToCop: p.PreferLimitToCop}.Init(p.SCtx(), p.QueryBlockOffset())
	appendConvertTopNTraceStep(p, topn, opt)
	return topn
}