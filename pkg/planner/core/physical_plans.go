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

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
)

//go:generate go run ./generator/plan_cache/plan_clone_generator.go -- plan_clone_generated.go

var (
	_ base.PhysicalPlan = &physicalop.PhysicalSelection{}
	_ base.PhysicalPlan = &physicalop.PhysicalProjection{}
	_ base.PhysicalPlan = &physicalop.PhysicalTopN{}
	_ base.PhysicalPlan = &physicalop.PhysicalMaxOneRow{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableDual{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionAll{}
	_ base.PhysicalPlan = &physicalop.PhysicalSort{}
	_ base.PhysicalPlan = &physicalop.NominalSort{}
	_ base.PhysicalPlan = &physicalop.PhysicalLock{}
	_ base.PhysicalPlan = &physicalop.PhysicalLimit{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexLookUpReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexMergeReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalStreamAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalApply{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalMergeJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalWindow{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffle{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffleReceiverStub{}
	_ base.PhysicalPlan = &physicalop.BatchPointGetPlan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableSample{}
	_ base.PhysicalPlan = &physicalop.PhysicalSequence{}
)

// GetPhysicalIndexReader returns PhysicalIndexReader for logical TiKVSingleGather.
func GetPhysicalIndexReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *physicalop.PhysicalIndexReader {
	reader := physicalop.PhysicalIndexReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// GetPhysicalTableReader returns PhysicalTableReader for logical TiKVSingleGather.
func GetPhysicalTableReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *physicalop.PhysicalTableReader {
	reader := physicalop.PhysicalTableReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.PlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   sg.Source.AllConds,
		PartitionNames: sg.Source.PartitionNames,
		Columns:        sg.Source.TblCols,
		ColumnNames:    sg.Source.OutputNames(),
	}
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// AddExtraPhysTblIDColumn for partition table.
// For keepOrder with partition table,
// we need use partitionHandle to distinct two handles,
// the `_tidb_rowid` in different partitions can have the same value.
func AddExtraPhysTblIDColumn(sctx base.PlanContext, columns []*model.ColumnInfo, schema *expression.Schema) ([]*model.ColumnInfo, *expression.Schema, bool) {
	// Not adding the ExtraPhysTblID if already exists
	if model.FindColumnInfoByID(columns, model.ExtraPhysTblID) != nil {
		return columns, schema, false
	}
	columns = append(columns, model.NewExtraPhysTblIDColInfo())
	schema.Append(&expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPhysTblID,
	})
	return columns, schema, true
}

// PhysicalExchangeReceiver accepts connection and receives data passively.
type PhysicalExchangeReceiver struct {
	physicalop.BasePhysicalPlan

	Tasks []*kv.MPPTask
	frags []*Fragment

	IsCTEReader bool
}

// Clone implment op.PhysicalPlan interface.
func (p *PhysicalExchangeReceiver) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExchangeReceiver)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base

	np.IsCTEReader = p.IsCTEReader
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExchangeReceiver
func (p *PhysicalExchangeReceiver) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice*2 + int64(cap(p.Tasks)+cap(p.frags))*size.SizeOfPointer
	for _, frag := range p.frags {
		sum += frag.MemoryUsage()
	}
	return
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan base.PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *physicalop.PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.TablePlan, statsInfos)
	case *physicalop.PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *physicalop.PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *physicalop.PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	case *physicalop.PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	}

	return statsInfos
}

// SafeClone clones this op.PhysicalPlan and handles its panic.
func SafeClone(sctx base.PlanContext, v base.PhysicalPlan) (_ base.PhysicalPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone(sctx)
}
