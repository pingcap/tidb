// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"fmt"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalIndexLookUp represents a index lookup
// It contains two parts: indexPlan and tablePlan
// IndexPlan maybe hierarchical, e.g. IndexScan -> Selection -> Limit
// TablePlan contains only TableScan executor
type PhysicalIndexLookUp struct {
	PhysicalSchemaProducer
	// handle offsets in the indexPlan's output schema
	IndexHandleOffsets []uint32
}

func resetPlanIDRecursively(ctx base.PlanContext, p base.PhysicalPlan) {
	p.SetID(int(ctx.GetSessionVars().PlanID.Add(1)))
	for _, child := range p.Children() {
		resetPlanIDRecursively(ctx, child)
	}
}

func buildPushDownIndexLookUpPlan(
	ctx base.PlanContext, indexPlan base.PhysicalPlan, tablePlan base.PhysicalPlan, keepOrder bool,
) (indexLookUpPlan base.PhysicalPlan, err error) {
	if keepOrder {
		return nil, errors.NewNoStackError("keep order is not supported")
	}

	tablePlan, err = tablePlan.Clone(ctx)
	if err != nil {
		return nil, err
	}
	resetPlanIDRecursively(ctx, tablePlan)

	tableScanPlan, tableScanParent, err := detachRootTableScanPlan(tablePlan)
	if err != nil {
		return nil, err
	}

	// to make sure tableScanPlan can be cloned,
	// runtimeFilterList has tag `plan-cache-clone:"must-nil"`
	if len(tableScanPlan.runtimeFilterList) == 0 {
		tableScanPlan.runtimeFilterList = nil
	}

	// to make sure tableScanPlan can be cloned,
	// UsedColumnarIndexes has tag `plan-cache-clone:"must-nil"`
	if len(tableScanPlan.UsedColumnarIndexes) == 0 {
		tableScanPlan.UsedColumnarIndexes = nil
	}

	if tableScanPlan.Table.IsCommonHandle {
		return nil, errors.NewNoStackError("common handle table is not supported")
	}

	if tableScanPlan.Table.Partition != nil {
		return nil, errors.NewNoStackError("partition table is not supported")
	}

	if tableScanPlan.Table.TempTableType != model.TempTableNone {
		return nil, errors.NewNoStackError("temporary table is not supported")
	}

	if tableScanPlan.Table.TableCacheStatusType != model.TableCacheStatusDisable {
		return nil, errors.NewNoStackError("cached table is not supported")
	}

	indexLookUpPlan = PhysicalIndexLookUp{
		// Only int handle is supported now, so the handle is always the last column of index schema.
		IndexHandleOffsets: []uint32{uint32(indexPlan.Schema().Len()) - 1},
	}.Init(ctx, indexPlan, tableScanPlan, tablePlan.QueryBlockOffset())

	if tableScanParent != nil {
		tableScanParent.SetChildren(indexLookUpPlan)
		indexLookUpPlan = tablePlan
	}
	return
}

// Init initializes PhysicalIndexLookUp.
func (p PhysicalIndexLookUp) Init(ctx base.PlanContext, indexPlan base.PhysicalPlan, tableScan *PhysicalTableScan, offset int) *PhysicalIndexLookUp {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.SetChildren(indexPlan, tableScan)
	p.SetStats(tableScan.StatsInfo())
	p.SetSchema(tableScan.Schema())
	return &p
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexLookUp) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeIndexLookUp,
		IndexLookup: &tipb.IndexLookUp{
			IndexHandleOffsets: p.IndexHandleOffsets,
		},
	}, nil
}

// ExplainInfo implements PhysicalPlan ExplainInfo interface.
func (p *PhysicalIndexLookUp) ExplainInfo() string {
	return fmt.Sprintf("index handle offsets:%v", p.IndexHandleOffsets)
}

// MemoryUsage return the memory usage of PhysicalIndexLookUp
func (p *PhysicalIndexLookUp) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	const physicalIndexLookUpStructSize = int64(unsafe.Sizeof(PhysicalIndexLookUp{}) - unsafe.Sizeof(PhysicalSchemaProducer{}))
	return p.PhysicalSchemaProducer.MemoryUsage() +
		physicalIndexLookUpStructSize +
		size.SizeOfInt32*int64(len(p.IndexHandleOffsets))
}

// Clone implements the base.PhysicalPlan.<14th> interface.
func (p *PhysicalIndexLookUp) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexLookUp)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	basePlan, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *basePlan
	cloned.IndexHandleOffsets = make([]uint32, len(p.IndexHandleOffsets))
	copy(cloned.IndexHandleOffsets, p.IndexHandleOffsets)
	return cloned, nil
}

func detachRootTableScanPlan(p base.PhysicalPlan) (root *PhysicalTableScan, rootParent base.PhysicalPlan, err error) {
	var currentParent base.PhysicalPlan
	for {
		children := p.Children()
		if l := len(children); l == 0 {
			var ok bool
			if root, ok = p.(*PhysicalTableScan); !ok {
				return nil, nil, errors.New("the root is not a PhysicalTableScan")
			}
			rootParent = currentParent
			break
		} else if l > 1 {
			return nil, nil, errors.New(
				"some execution of in table plan has multiple children which is not supported yet, plan: " + p.TP(),
			)
		}

		currentParent = p
		p = children[0]
	}

	if rootParent != nil {
		// set children to empty because root is detached
		rootParent.SetChildren()
	}
	return
}
