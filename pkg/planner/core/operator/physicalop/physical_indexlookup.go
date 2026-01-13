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
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalLocalIndexLookUp represents an index lookup locally.
// It contains two parts: indexPlan and tablePlan
// IndexPlan maybe hierarchical, e.g. IndexScan -> Selection -> Limit
// TablePlan contains only TableScan executor
type PhysicalLocalIndexLookUp struct {
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
	ctx base.PlanContext, indexPlan base.PhysicalPlan, tablePlan base.PhysicalPlan, isCommonHandle bool,
) (indexLookUpPlan base.PhysicalPlan, err error) {
	tablePlan, err = tablePlan.Clone(ctx)
	if err != nil {
		return nil, err
	}
	resetPlanIDRecursively(ctx, tablePlan)

	var indexHandleOffsets []uint32
	if !isCommonHandle {
		// - If common handle, we don't need to set the indexHandleOffsets to build the common handle key
		// which can be read from the index value directly.
		// - If int handle, it is the last column in the index schema.
		//   - If the last column is ExtraHandleID, or a non-negative column ID, handle is the last column.
		//   - Otherwise, we need to find the last column whose ID is not ExtraHandleID and is negative.
		//     For example, when a partition table needs to append ExtraPhysTblID
		//     to the end for the upper UnionScanExec.
		offset := indexPlan.Schema().Len() - 1
		for offset >= 0 {
			col := indexPlan.Schema().Columns[offset]
			if col.ID >= 0 || col.ID == model.ExtraHandleID {
				break
			}
			offset--
			intest.Assert(offset >= 0, "cannot find handle column in index schema")
		}

		if offset < 0 {
			return nil, errors.New("cannot find handle column in index schema")
		}
		indexHandleOffsets = []uint32{uint32(offset)}
	}

	tableScanPlan, parentOfTableScan := detachRootTableScanPlan(tablePlan)
	indexLookUpPlan = PhysicalLocalIndexLookUp{
		IndexHandleOffsets: indexHandleOffsets,
	}.Init(ctx, indexPlan, tableScanPlan, tablePlan.QueryBlockOffset())

	if parentOfTableScan != nil {
		parentOfTableScan.SetChildren(indexLookUpPlan)
		indexLookUpPlan = tablePlan
	}
	return
}

// Init initializes PhysicalLocalIndexLookUp.
func (p PhysicalLocalIndexLookUp) Init(ctx base.PlanContext, indexPlan base.PhysicalPlan, tableScan *PhysicalTableScan, offset int) *PhysicalLocalIndexLookUp {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeLocalIndexLookUp, &p, offset)
	p.SetChildren(indexPlan, tableScan)
	p.SetStats(tableScan.StatsInfo())
	p.SetSchema(tableScan.Schema())
	return &p
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLocalIndexLookUp) ToPB(_ *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType != kv.TiKV {
		return nil, errors.Errorf("unsupported store type %v for LocalIndexLookUp", storeType)
	}
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeIndexLookUp,
		IndexLookup: &tipb.IndexLookUp{
			IndexHandleOffsets: p.IndexHandleOffsets,
		},
	}, nil
}

// ExplainInfo implements PhysicalPlan ExplainInfo interface.
func (p *PhysicalLocalIndexLookUp) ExplainInfo() string {
	return fmt.Sprintf("index handle offsets:%v", p.IndexHandleOffsets)
}

// MemoryUsage return the memory usage of PhysicalLocalIndexLookUp
func (p *PhysicalLocalIndexLookUp) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	const physicalIndexLookUpStructSize = int64(unsafe.Sizeof(PhysicalLocalIndexLookUp{}) - unsafe.Sizeof(PhysicalSchemaProducer{}))
	return p.PhysicalSchemaProducer.MemoryUsage() +
		physicalIndexLookUpStructSize +
		size.SizeOfInt32*int64(len(p.IndexHandleOffsets))
}

// Clone implements the base.PhysicalPlan.<14th> interface.
func (p *PhysicalLocalIndexLookUp) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalLocalIndexLookUp)
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

func detachRootTableScanPlan(p base.PhysicalPlan) (root *PhysicalTableScan, rootParent base.PhysicalPlan) {
	var currentParent base.PhysicalPlan
	for {
		children := p.Children()
		l := len(children)
		if l == 0 {
			var ok bool
			root, ok = p.(*PhysicalTableScan)
			intest.Assert(ok && root != nil)
			rootParent = currentParent
			break
		}
		intest.Assert(l == 1)
		currentParent = p
		p = children[0]
	}

	if rootParent != nil {
		// set children to empty because root is detached
		rootParent.SetChildren()
	}
	return
}
