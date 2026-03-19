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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// ReadReqType is the read request type of the operator. Currently, only PhysicalTableReader uses this.
type ReadReqType uint8

const (
	// Cop means read from storage by cop request.
	Cop ReadReqType = iota
	// BatchCop means read from storage by BatchCop request, only used for TiFlash
	BatchCop
	// MPP means read from storage by MPP request, only used for TiFlash
	MPP
)

// Name returns the name of read request type.
func (r ReadReqType) Name() string {
	switch r {
	case BatchCop:
		return "batchCop"
	case MPP:
		return "mpp"
	default:
		// return cop by default
		return "cop"
	}
}

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	PhysicalSchemaProducer

	// TablePlan is the tree format of executor plan.
	TablePlan base.PhysicalPlan
	// TablePlans flats the TablePlan to construct executor pb.
	TablePlans []base.PhysicalPlan

	// StoreType indicates table read from which type of store.
	StoreType kv.StoreType

	// ReadReqType is the read request type for current physical table reader, there are 3 kinds of read request: Cop,
	// BatchCop and MPP, currently, the latter two are only used in TiFlash
	ReadReqType ReadReqType

	IsCommonHandle bool

	// Used by partition table.
	PlanPartInfo *PhysPlanPartInfo
	// Used by MPP, because MPP plan may contain join/union/union all, it is possible that a physical table reader contains more than 1 table scan
	TableScanAndPartitionInfos []TableScanAndPartitionInfo `plan-cache-clone:"must-nil"`
}

// Init initializes PhysicalTableReader.
func (p PhysicalTableReader) Init(ctx base.PlanContext, offset int) *PhysicalTableReader {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeTableReader, &p, offset)
	p.ReadReqType = Cop
	if p.TablePlan == nil {
		return &p
	}
	p.TablePlans = FlattenListPushDownPlan(p.TablePlan)
	p.SetSchema(p.TablePlan.Schema())
	p.adjustReadReqType(ctx)
	if p.ReadReqType == BatchCop || p.ReadReqType == MPP {
		setMppOrBatchCopForTableScan(p.TablePlan)
	}
	return &p
}

// LoadTableStats loads the stats of the table read by this plan.
func (p *PhysicalTableReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	stats.LoadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// SetTablePlanForTest sets TablePlan field for test usage only
func (p *PhysicalTableReader) SetTablePlanForTest(pp base.PhysicalPlan) {
	p.TablePlan = pp
}

// GetTablePlan exports the TablePlan.
func (p *PhysicalTableReader) GetTablePlan() base.PhysicalPlan {
	return p.TablePlan
}

// GetTableScans exports the tableScan that contained in tablePlans.
func (p *PhysicalTableReader) GetTableScans() []*PhysicalTableScan {
	tableScans := make([]*PhysicalTableScan, 0, 1)
	for _, tablePlan := range p.TablePlans {
		tableScan, ok := tablePlan.(*PhysicalTableScan)
		if ok {
			tableScans = append(tableScans, tableScan)
		}
	}
	return tableScans
}

// GetTableScan exports the tableScan that contained in tablePlans and return error when the count of table scan != 1.
func (p *PhysicalTableReader) GetTableScan() (*PhysicalTableScan, error) {
	tableScans := p.GetTableScans()
	if len(tableScans) != 1 {
		return nil, errors.New("the count of table scan != 1")
	}
	return tableScans[0], nil
}

// GetAvgRowSize return the average row size of this plan.
func (p *PhysicalTableReader) GetAvgRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
}

// MemoryUsage return the memory usage of PhysicalTableReader
func (p *PhysicalTableReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfUint8*2 + size.SizeOfBool + p.PlanPartInfo.MemoryUsage()
	if p.TablePlan != nil {
		sum += p.TablePlan.MemoryUsage()
	}
	// since TablePlans is the flats of TablePlan, so we don't count it
	for _, pInfo := range p.TableScanAndPartitionInfos {
		sum += pInfo.MemoryUsage()
	}
	return
}

// GetNetDataSize calculates the estimated total data size fetched from storage.
func (p *PhysicalTableReader) GetNetDataSize() float64 {
	rowSize := cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
	return p.TablePlan.StatsCount() * rowSize
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalTableReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	if len(p.TableScanAndPartitionInfos) == 0 {
		ts, ok := p.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return access.OtherAccessObject("")
		}
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := GetDynamicAccessPartition(sctx, ts.Table, p.PlanPartInfo, asName)
		if res == nil {
			return access.DynamicPartitionAccessObjects(nil)
		}
		return access.DynamicPartitionAccessObjects{res}
	}
	if len(p.TableScanAndPartitionInfos) == 1 {
		tp := p.TableScanAndPartitionInfos[0]
		ts := tp.TableScan
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := GetDynamicAccessPartition(sctx, ts.Table, tp.PhysPlanPartInfo, asName)
		if res == nil {
			return access.DynamicPartitionAccessObjects(nil)
		}
		return access.DynamicPartitionAccessObjects{res}
	}

	res := make(access.DynamicPartitionAccessObjects, 0)
	for _, info := range p.TableScanAndPartitionInfos {
		if info.TableScan.Table.GetPartitionInfo() == nil {
			continue
		}
		ts := info.TableScan
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		accessObj := GetDynamicAccessPartition(sctx, ts.Table, info.PhysPlanPartInfo, asName)
		if accessObj != nil {
			res = append(res, accessObj)
		}
	}
	if len(res) == 0 {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return res
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalTableReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalTableReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.StoreType = p.StoreType
	cloned.ReadReqType = p.ReadReqType
	cloned.IsCommonHandle = p.IsCommonHandle
	if cloned.TablePlan, err = p.TablePlan.Clone(newCtx); err != nil {
		return nil, err
	}
	// TablePlans are actually the flattened plans in TablePlan, so can't copy them, just need to extract from TablePlan
	cloned.TablePlans = FlattenListPushDownPlan(cloned.TablePlan)
	return cloned, nil
}

// SetChildren overrides op.PhysicalPlan SetChildren interface.
func (p *PhysicalTableReader) SetChildren(children ...base.PhysicalPlan) {
	p.TablePlan = children[0]
	p.TablePlans = FlattenListPushDownPlan(p.TablePlan)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalTableReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	tablePlanInfo := "data:" + p.TablePlan.ExplainID().String()

	if p.ReadReqType == MPP {
		return fmt.Sprintf("MppVersion: %d, %s", p.SCtx().GetSessionVars().ChooseMppVersion(), tablePlanInfo)
	}

	return tablePlanInfo
}

// ExplainNormalizedInfo implements Plan interface.
func (*PhysicalTableReader) ExplainNormalizedInfo() string {
	return ""
}

// OperatorInfo return other operator information to be explained.
func (p *PhysicalTableReader) OperatorInfo(_ bool) string {
	return "data:" + p.TablePlan.ExplainID().String()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableReader) ResolveIndices() error {
	err := ResolveIndicesForVirtualColumn(p.Schema().Columns, p.Schema())
	if err != nil {
		return err
	}
	return p.TablePlan.ResolveIndices()
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableReader) GetPlanCostVer1(_ property.TaskType,
	option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalTableReader(p, option)
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableReader) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalTableReader(p, taskType, option)
}

func (p *PhysicalTableReader) adjustReadReqType(ctx base.PlanContext) {
	if p.StoreType == kv.TiFlash {
		_, ok := p.TablePlan.(*PhysicalExchangeSender)
		if ok {
			p.ReadReqType = MPP
			return
		}
		tableScans := p.GetTableScans()
		// When PhysicalTableReader's store type is tiflash, has table scan
		// and all table scans contained are not keepOrder, try to use batch cop.
		if len(tableScans) > 0 {
			for _, tableScan := range tableScans {
				if tableScan.KeepOrder {
					return
				}
			}

			// When allow batch cop is 1, only agg / topN uses batch cop.
			// When allow batch cop is 2, every query uses batch cop.
			switch ctx.GetSessionVars().AllowBatchCop {
			case 1:
				for _, plan := range p.TablePlans {
					switch plan.(type) {
					case *PhysicalHashAgg, *PhysicalStreamAgg, *PhysicalTopN:
						p.ReadReqType = BatchCop
						return
					}
				}
			case 2:
				p.ReadReqType = BatchCop
			}
		}
	}
}

// setMppOrBatchCopForTableScan set IsMPPOrBatchCop for all TableScan.
func setMppOrBatchCopForTableScan(curPlan base.PhysicalPlan) {
	if ts, ok := curPlan.(*PhysicalTableScan); ok {
		ts.IsMPPOrBatchCop = true
	}
	children := curPlan.Children()
	for _, child := range children {
		setMppOrBatchCopForTableScan(child)
	}
}

// GetPhysicalTableReader returns PhysicalTableReader for logical TiKVSingleGather.
func GetPhysicalTableReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalTableReader {
	reader := PhysicalTableReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.PlanPartInfo = &PhysPlanPartInfo{
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
