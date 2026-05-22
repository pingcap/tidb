// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
)

var (
	_ base.PhysicalJoin = &PhysicalHashJoin{}
	_ base.PhysicalJoin = &PhysicalMergeJoin{}
	_ base.PhysicalJoin = &PhysicalIndexJoin{}
	_ base.PhysicalJoin = &PhysicalIndexHashJoin{}
	_ base.PhysicalJoin = &PhysicalIndexMergeJoin{}
)

// BasePhysicalJoin is the base struct for all physical join operators.
// TODO: it is temporarily to be public for all physical join operators to embed it.
type BasePhysicalJoin struct {
	PhysicalSchemaProducer

	JoinType base.JoinType

	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	InnerChildIdx int
	OuterJoinKeys []*expression.Column
	InnerJoinKeys []*expression.Column
	LeftJoinKeys  []*expression.Column
	RightJoinKeys []*expression.Column
	// IsNullEQ is used for cases like Except statement where null key should be matched with null key.
	// <1,null> is exactly matched with <1,null>, where the null value should not be filtered and
	// the null is exactly matched with null only. (while in NAAJ null value should also be matched
	// with other non-null item as well)
	IsNullEQ      []bool
	DefaultValues []types.Datum

	LeftNAJoinKeys  []*expression.Column
	RightNAJoinKeys []*expression.Column
}

// GetJoinType returns the type of the join operation.
func (p *BasePhysicalJoin) GetJoinType() base.JoinType {
	return p.JoinType
}

// PhysicalJoinImplement implements base.PhysicalJoin interface.
func (*BasePhysicalJoin) PhysicalJoinImplement() {}

// GetInnerChildIdx returns the index of the inner child in the join operator.
func (p *BasePhysicalJoin) GetInnerChildIdx() int {
	return p.InnerChildIdx
}

// CloneForPlanCacheWithSelf clones the BasePhysicalJoin for plan cache with self.
func (p *BasePhysicalJoin) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalJoin, bool) {
	cloned := new(BasePhysicalJoin)
	base, ok := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, newSelf)
	if !ok {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = utilfuncp.CloneExpressionsForPlanCache(p.LeftConditions, nil)
	cloned.RightConditions = utilfuncp.CloneExpressionsForPlanCache(p.RightConditions, nil)
	cloned.OtherConditions = utilfuncp.CloneExpressionsForPlanCache(p.OtherConditions, nil)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.OuterJoinKeys, nil)
	cloned.InnerJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.InnerJoinKeys, nil)
	cloned.LeftJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.LeftJoinKeys, nil)
	cloned.RightJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.RightJoinKeys, nil)
	cloned.IsNullEQ = make([]bool, len(p.IsNullEQ))
	copy(cloned.IsNullEQ, p.IsNullEQ)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	cloned.LeftNAJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.LeftNAJoinKeys, nil)
	cloned.RightNAJoinKeys = utilfuncp.CloneColumnsForPlanCache(p.RightNAJoinKeys, nil)
	return cloned, true
}

// CloneWithSelf clones the BasePhysicalJoin with self.
func (p *BasePhysicalJoin) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalJoin, error) {
	cloned := new(BasePhysicalJoin)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, newSelf)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = util.CloneExprs(p.LeftConditions)
	cloned.RightConditions = util.CloneExprs(p.RightConditions)
	cloned.OtherConditions = util.CloneExprs(p.OtherConditions)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = util.CloneCols(p.OuterJoinKeys)
	cloned.InnerJoinKeys = util.CloneCols(p.InnerJoinKeys)
	cloned.LeftJoinKeys = util.CloneCols(p.LeftJoinKeys)
	cloned.RightJoinKeys = util.CloneCols(p.RightJoinKeys)
	cloned.LeftNAJoinKeys = util.CloneCols(p.LeftNAJoinKeys)
	cloned.RightNAJoinKeys = util.CloneCols(p.RightNAJoinKeys)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *BasePhysicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
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

const emptyBasePhysicalJoinSize = int64(unsafe.Sizeof(BasePhysicalJoin{}))

// MemoryUsage return the memory usage of BasePhysicalJoin
func (p *BasePhysicalJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyBasePhysicalJoinSize + p.PhysicalSchemaProducer.MemoryUsage() + int64(cap(p.IsNullEQ))*size.SizeOfBool +
		int64(cap(p.LeftConditions)+cap(p.RightConditions)+cap(p.OtherConditions))*size.SizeOfInterface +
		int64(cap(p.OuterJoinKeys)+cap(p.InnerJoinKeys)+cap(p.LeftJoinKeys)+cap(p.RightNAJoinKeys)+cap(p.LeftNAJoinKeys)+
			cap(p.RightNAJoinKeys))*size.SizeOfPointer + int64(cap(p.DefaultValues))*types.EmptyDatumSize

	for _, cond := range p.LeftConditions {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.RightConditions {
		sum += cond.MemoryUsage()
	}
	for _, cond := range p.OtherConditions {
		sum += cond.MemoryUsage()
	}
	for _, col := range p.LeftJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.RightJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.InnerJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.OuterJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, datum := range p.DefaultValues {
		sum += datum.MemUsage()
	}
	for _, col := range p.LeftNAJoinKeys {
		sum += col.MemoryUsage()
	}
	for _, col := range p.RightNAJoinKeys {
		sum += col.MemoryUsage()
	}
	return
}

// BuildPhysicalJoinSchema builds the schema of PhysicalJoin from it's children's schema.
func BuildPhysicalJoinSchema(joinType base.JoinType, join base.PhysicalPlan) *expression.Schema {
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
