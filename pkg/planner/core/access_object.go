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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tipb/go-tipb"
)

// A plan is dataAccesser means it can access underlying data.
// Include `PhysicalTableScan`, `PhysicalIndexScan`, `PointGetPlan`, `BatchPointScan` and `PhysicalMemTable`.
// ExplainInfo = AccessObject + OperatorInfo
type dataAccesser interface {

	// AccessObject return plan's `table`, `partition` and `index`.
	AccessObject() base.AccessObject

	// OperatorInfo return other operator information to be explained.
	OperatorInfo(normalized bool) string
}

type partitionAccesser interface {
	accessObject(base.PlanContext) base.AccessObject
}

// DynamicPartitionAccessObject represents the partitions accessed by the children of this operator.
// It's mainly used in dynamic pruning mode.
type DynamicPartitionAccessObject struct {
	Database      string
	Table         string
	AllPartitions bool
	Partitions    []string
	err           string
}

func (d *DynamicPartitionAccessObject) String() string {
	if len(d.err) > 0 {
		return d.err
	}
	if d.AllPartitions {
		return "partition:all"
	} else if len(d.Partitions) == 0 {
		return "partition:dual"
	}
	return "partition:" + strings.Join(d.Partitions, ",")
}

// DynamicPartitionAccessObjects is a list of DynamicPartitionAccessObject.
type DynamicPartitionAccessObjects []*DynamicPartitionAccessObject

func (d DynamicPartitionAccessObjects) String() string {
	if len(d) == 0 {
		return ""
	}
	if len(d) == 1 {
		return d[0].String()
	}
	var b strings.Builder
	for i, access := range d {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(access.String())
		b.WriteString(" of " + access.Table)
	}
	return b.String()
}

// NormalizedString implements AccessObject.
func (d DynamicPartitionAccessObjects) NormalizedString() string {
	return d.String()
}

// SetIntoPB implements AccessObject.
func (d DynamicPartitionAccessObjects) SetIntoPB(pb *tipb.ExplainOperator) {
	if len(d) == 0 || pb == nil {
		return
	}
	pbObjSlice := make([]tipb.DynamicPartitionAccessObject, len(d))
	for i, obj := range d {
		if len(obj.err) > 0 {
			continue
		}
		pbObj := &pbObjSlice[i]
		pbObj.Database = obj.Database
		pbObj.Table = obj.Table
		pbObj.AllPartitions = obj.AllPartitions
		pbObj.Partitions = obj.Partitions
	}
	pbObjs := tipb.DynamicPartitionAccessObjects{Objects: make([]*tipb.DynamicPartitionAccessObject, 0, len(d))}
	for i := range pbObjSlice {
		pbObjs.Objects = append(pbObjs.Objects, &pbObjSlice[i])
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_DynamicPartitionObjects{DynamicPartitionObjects: &pbObjs},
		},
	}
}

// IndexAccess represents the index accessed by an operator.
type IndexAccess struct {
	Name             string
	Cols             []string
	IsClusteredIndex bool
}

// ToPB turns itself into a protobuf message.
func (a *IndexAccess) ToPB() *tipb.IndexAccess {
	if a == nil {
		return nil
	}
	return &tipb.IndexAccess{
		Name:             a.Name,
		Cols:             a.Cols,
		IsClusteredIndex: a.IsClusteredIndex,
	}
}

// ScanAccessObject represents the access to a table.
// It may also represent the access to indexes and partitions of a table.
type ScanAccessObject struct {
	Database   string
	Table      string
	Indexes    []IndexAccess
	Partitions []string
}

// NormalizedString implements AccessObject.
func (s *ScanAccessObject) NormalizedString() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partitions) > 0 {
		b.WriteString(", partition:?")
	}
	for _, index := range s.Indexes {
		if index.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(index.Name + "(" + strings.Join(index.Cols, ", ") + ")")
	}
	return b.String()
}

func (s *ScanAccessObject) String() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partitions) > 0 {
		b.WriteString(", partition:" + strings.Join(s.Partitions, ","))
	}
	for _, index := range s.Indexes {
		if index.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(index.Name + "(" + strings.Join(index.Cols, ", ") + ")")
	}
	return b.String()
}

// SetIntoPB implements AccessObject.
func (s *ScanAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if s == nil || pb == nil {
		return
	}
	pbObj := tipb.ScanAccessObject{
		Database:   s.Database,
		Table:      s.Table,
		Partitions: s.Partitions,
	}
	for i := range s.Indexes {
		pbObj.Indexes = append(pbObj.Indexes, s.Indexes[i].ToPB())
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_ScanObject{ScanObject: &pbObj},
		},
	}
}

// OtherAccessObject represents other kinds of access.
type OtherAccessObject string

func (o OtherAccessObject) String() string {
	return string(o)
}

// NormalizedString implements AccessObject.
func (o OtherAccessObject) NormalizedString() string {
	return o.String()
}

// SetIntoPB implements AccessObject.
func (o OtherAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if pb == nil {
		return
	}
	if o == "" {
		return
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_OtherObject{OtherObject: string(o)},
		},
	}
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject() base.AccessObject {
	res := &ScanAccessObject{
		Database: p.DBName.O,
	}
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	res.Table = tblName
	if p.isPartition {
		pi := p.Table.GetPartitionInfo()
		if pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			res.Partitions = []string{partitionName}
		}
	}
	if len(p.Index.Columns) > 0 {
		index := IndexAccess{
			Name: p.Index.Name.O,
		}
		for _, idxCol := range p.Index.Columns {
			if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []IndexAccess{index}
	}
	return res
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalTableScan) AccessObject() base.AccessObject {
	res := &ScanAccessObject{
		Database: p.DBName.O,
	}
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	res.Table = tblName
	if p.isPartition {
		pi := p.Table.GetPartitionInfo()
		if pi != nil {
			partitionName := pi.GetNameByID(p.physicalTableID)
			res.Partitions = []string{partitionName}
		}
	}
	if p.AnnIndexExtra != nil {
		index := IndexAccess{
			Name: p.AnnIndexExtra.IndexInfo.Name.O,
		}
		for _, idxCol := range p.AnnIndexExtra.IndexInfo.Columns {
			if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
				index.Cols = append(index.Cols, tblCol.GeneratedExprString)
			} else {
				index.Cols = append(index.Cols, idxCol.Name.O)
			}
		}
		res.Indexes = []IndexAccess{index}
	}
	return res
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalMemTable) AccessObject() base.AccessObject {
	return &ScanAccessObject{
		Database: p.DBName.O,
		Table:    p.Table.Name.O,
	}
}

// AccessObject implements dataAccesser interface.
func (p *PointGetPlan) AccessObject() base.AccessObject {
	res := &ScanAccessObject{
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
		index := IndexAccess{
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
		res.Indexes = []IndexAccess{index}
	}
	return res
}

// AccessObject implements physicalScan interface.
func (p *BatchPointGetPlan) AccessObject() base.AccessObject {
	res := &ScanAccessObject{
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
		index := IndexAccess{
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
		res.Indexes = []IndexAccess{index}
	}
	return res
}

func getDynamicAccessPartition(sctx base.PlanContext, tblInfo *model.TableInfo, physPlanPartInfo *PhysPlanPartInfo, asName string) (res *DynamicPartitionAccessObject) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return nil
	}

	res = &DynamicPartitionAccessObject{}
	tblName := tblInfo.Name.O
	if len(asName) > 0 {
		tblName = asName
	}
	res.Table = tblName
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	db, ok := infoschema.SchemaByTable(is, tblInfo)
	if ok {
		res.Database = db.Name.O
	}
	tmp, ok := is.TableByID(context.Background(), tblInfo.ID)
	if !ok {
		res.err = "partition table not found:" + strconv.FormatInt(tblInfo.ID, 10)
		return res
	}
	tbl := tmp.(table.PartitionedTable)

	idxArr, err := PartitionPruning(sctx, tbl, physPlanPartInfo.PruningConds, physPlanPartInfo.PartitionNames, physPlanPartInfo.Columns, physPlanPartInfo.ColumnNames)
	if err != nil {
		res.err = "partition pruning error:" + err.Error()
		return res
	}

	if len(idxArr) == 1 && idxArr[0] == FullRange {
		res.AllPartitions = true
		return res
	}

	for _, idx := range idxArr {
		res.Partitions = append(res.Partitions, pi.Definitions[idx].Name.O)
	}
	return res
}

func (p *PhysicalTableReader) accessObject(sctx base.PlanContext) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return DynamicPartitionAccessObjects(nil)
	}
	if len(p.TableScanAndPartitionInfos) == 0 {
		ts, ok := p.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return OtherAccessObject("")
		}
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := getDynamicAccessPartition(sctx, ts.Table, p.PlanPartInfo, asName)
		if res == nil {
			return DynamicPartitionAccessObjects(nil)
		}
		return DynamicPartitionAccessObjects{res}
	}
	if len(p.TableScanAndPartitionInfos) == 1 {
		tp := p.TableScanAndPartitionInfos[0]
		ts := tp.tableScan
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := getDynamicAccessPartition(sctx, ts.Table, tp.physPlanPartInfo, asName)
		if res == nil {
			return DynamicPartitionAccessObjects(nil)
		}
		return DynamicPartitionAccessObjects{res}
	}

	res := make(DynamicPartitionAccessObjects, 0)
	for _, info := range p.TableScanAndPartitionInfos {
		if info.tableScan.Table.GetPartitionInfo() == nil {
			continue
		}
		ts := info.tableScan
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		accessObj := getDynamicAccessPartition(sctx, ts.Table, info.physPlanPartInfo, asName)
		if accessObj != nil {
			res = append(res, accessObj)
		}
	}
	if len(res) == 0 {
		return DynamicPartitionAccessObjects(nil)
	}
	return res
}

func getAccessObjectFromIndexScan(sctx base.PlanContext, is *PhysicalIndexScan, p *PhysPlanPartInfo) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return DynamicPartitionAccessObjects(nil)
	}
	asName := ""
	if is.TableAsName != nil && len(is.TableAsName.O) > 0 {
		asName = is.TableAsName.O
	}
	res := getDynamicAccessPartition(sctx, is.Table, p, asName)
	if res == nil {
		return DynamicPartitionAccessObjects(nil)
	}
	return DynamicPartitionAccessObjects{res}
}

func (p *PhysicalIndexReader) accessObject(sctx base.PlanContext) base.AccessObject {
	return getAccessObjectFromIndexScan(sctx, p.IndexPlans[0].(*PhysicalIndexScan), p.PlanPartInfo)
}

func (p *PhysicalIndexLookUpReader) accessObject(sctx base.PlanContext) base.AccessObject {
	return getAccessObjectFromIndexScan(sctx, p.IndexPlans[0].(*PhysicalIndexScan), p.PlanPartInfo)
}

func (p *PhysicalIndexMergeReader) accessObject(sctx base.PlanContext) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return DynamicPartitionAccessObjects(nil)
	}
	ts := p.TablePlans[0].(*PhysicalTableScan)
	asName := ""
	if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
		asName = ts.TableAsName.O
	}
	res := getDynamicAccessPartition(sctx, ts.Table, p.PlanPartInfo, asName)
	if res == nil {
		return DynamicPartitionAccessObjects(nil)
	}
	return DynamicPartitionAccessObjects{res}
}

// AccessObject implements physicalScan interface.
func (p *PhysicalCTE) AccessObject() base.AccessObject {
	if p.cteName == p.cteAsName {
		return OtherAccessObject(fmt.Sprintf("CTE:%s", p.cteName.L))
	}
	return OtherAccessObject(fmt.Sprintf("CTE:%s AS %s", p.cteName.L, p.cteAsName.L))
}
