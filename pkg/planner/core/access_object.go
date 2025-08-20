// Copyright 2022 PingCAP, Inc.
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
	"sort"

	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// AccessObject implements DataAccesser interface.
func (p *PointGetPlan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.dbName,
		Table:    p.TblInfo.Name.O,
	}
	if idxPointer := p.PartitionIdx; idxPointer != nil {
		idx := *idxPointer
		if idx < 0 {
			res.Partitions = []string{"dual"}
		} else {
			if pi := p.TblInfo.GetPartitionInfo(); pi != nil {
				res.Partitions = []string{pi.Definitions[idx].Name.O}
			}
		}
	}
	if p.IndexInfo != nil {
		index := access.IndexAccess{
			Name:             p.IndexInfo.Name.O,
			IsClusteredIndex: p.IndexInfo.Primary && p.TblInfo.IsCommonHandle,
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []access.IndexAccess{index}
	}
	return res
}

// AccessObject implements DataAccesser interface.
func (p *BatchPointGetPlan) AccessObject() base.AccessObject {
	res := &access.ScanAccessObject{
		Database: p.dbName,
		Table:    p.TblInfo.Name.O,
	}
	uniqueIdx := make(map[int]struct{})
	for _, idx := range p.PartitionIdxs {
		uniqueIdx[idx] = struct{}{}
	}
	if len(uniqueIdx) > 0 {
		idxs := make([]int, 0, len(uniqueIdx))
		for k := range uniqueIdx {
			idxs = append(idxs, k)
		}
		sort.Ints(idxs)
		for _, idx := range idxs {
			res.Partitions = append(res.Partitions, p.TblInfo.Partition.Definitions[idx].Name.O)
		}
	}
	if p.IndexInfo != nil {
		index := access.IndexAccess{
			Name:             p.IndexInfo.Name.O,
			IsClusteredIndex: p.IndexInfo.Primary && p.TblInfo.IsCommonHandle,
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []access.IndexAccess{index}
	}
	return res
}

func getAccessObjectFromIndexScan(sctx base.PlanContext, is *physicalop.PhysicalIndexScan, p *physicalop.PhysPlanPartInfo) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	asName := ""
	if is.TableAsName != nil && len(is.TableAsName.O) > 0 {
		asName = is.TableAsName.O
	}
	res := physicalop.GetDynamicAccessPartition(sctx, is.Table, p, asName)
	if res == nil {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return access.DynamicPartitionAccessObjects{res}
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalIndexLookUpReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	return getAccessObjectFromIndexScan(sctx, p.IndexPlans[0].(*physicalop.PhysicalIndexScan), p.PlanPartInfo)
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalIndexMergeReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	ts := p.TablePlans[0].(*physicalop.PhysicalTableScan)
	asName := ""
	if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
		asName = ts.TableAsName.O
	}
	res := physicalop.GetDynamicAccessPartition(sctx, ts.Table, p.PlanPartInfo, asName)
	if res == nil {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return access.DynamicPartitionAccessObjects{res}
}
