package core

import (
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tipb/go-tipb"
	"strconv"
	"strings"
)

// A plan is dataAccesser means it can access underlying data.
// Include `PhysicalTableScan`, `PhysicalIndexScan`, `PointGetPlan`, `BatchPointScan` and `PhysicalMemTable`.
// ExplainInfo = AccessObject + OperatorInfo
type dataAccesser interface {

	// AccessObject return plan's `table`, `partition` and `index`.
	AccessObject() AccessObject

	// OperatorInfo return other operator information to be explained.
	OperatorInfo(normalized bool) string
}

type partitionAccesser interface {
	accessObject(sessionctx.Context) AccessObject
}

type AccessObject interface {
	String() string
	NormalizedString() string
	SetIntoPB(*tipb.ExplainOperator)
}

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

func (d DynamicPartitionAccessObjects) NormalizedString() string {
	return d.String()
}

func (d DynamicPartitionAccessObjects) SetIntoPB(pb *tipb.ExplainOperator) {
	if d == nil || pb == nil {
		return
	}
	pbObjSlice := make([]tipb.DynamicPartitionAccessObject, len(d))
	for i, obj := range d {
		if len(obj.err) > 0 {
			continue
		}
		pbObj := pbObjSlice[i]
		pbObj.Database = obj.Database
		pbObj.Table = obj.Table
		pbObj.AllPartitions = obj.AllPartitions
		pbObj.Partitions = obj.Partitions
	}
	pbObjs := tipb.DynamicPartitionAccessObjects{Objects: make([]*tipb.DynamicPartitionAccessObject, 0, len(d))}
	for _, obj := range pbObjSlice {
		pbObjs.Objects = append(pbObjs.Objects, &obj)
	}
	pb.AccessObject = &tipb.ExplainOperator_DynamicPartitionObjects{DynamicPartitionObjects: &pbObjs}
}

type ScanAccessObject struct {
	Database         string
	Table            string
	Index            string
	IndexCols        []string
	IsClusteredIndex bool
	Partition        string
}

func (s *ScanAccessObject) NormalizedString() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partition) > 0 {
		b.WriteString(", partition:?")
	}
	if len(s.Index) > 0 {
		if s.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(s.Index + "(" + strings.Join(s.IndexCols, ",") + ")")
	}
	return b.String()
}

func (s *ScanAccessObject) String() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partition) > 0 {
		b.WriteString(", partition:" + s.Partition)
	}
	if len(s.Index) > 0 {
		if s.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(s.Index + "(" + strings.Join(s.IndexCols, ",") + ")")
	}
	return b.String()
}

func (s *ScanAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if s == nil || pb == nil {
		return
	}
	pbObj := tipb.ScanAccessObject{
		Database:         s.Database,
		Table:            s.Table,
		Index:            s.Index,
		IndexCols:        s.IndexCols,
		IsClusteredIndex: s.IsClusteredIndex,
		Partition:        s.Partition,
	}
	pb.AccessObject = &tipb.ExplainOperator_ScanObject{ScanObject: &pbObj}
}

type OtherAccessObject string

func (o OtherAccessObject) String() string {
	return string(o)
}

func (o OtherAccessObject) NormalizedString() string {
	return o.String()
}

func (o OtherAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if pb == nil {
		return
	}
	pb.AccessObject = &tipb.ExplainOperator_OtherObject{OtherObject: string(o)}
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject() AccessObject {
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
			res.Partition = partitionName
		}
	}
	if len(p.Index.Columns) > 0 {
		res.Index = p.Index.Name.O
		for _, idxCol := range p.Index.Columns {
			if tblCol := p.Table.Columns[idxCol.Offset]; tblCol.Hidden {
				res.IndexCols = append(res.IndexCols, tblCol.GeneratedExprString)
			} else {
				res.IndexCols = append(res.IndexCols, idxCol.Name.O)
			}
		}
	}
	return res
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalTableScan) AccessObject() AccessObject {
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
			res.Partition = partitionName
		}
	}
	return res
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalMemTable) AccessObject() AccessObject {
	return &ScanAccessObject{Table: p.Table.Name.O}
}

// AccessObject implements dataAccesser interface.
func (p *PointGetPlan) AccessObject() AccessObject {
	res := &ScanAccessObject{
		Database: p.dbName,
		Table:    p.TblInfo.Name.O,
	}
	if p.PartitionInfo != nil {
		res.Partition = p.PartitionInfo.Name.O
	}
	if p.IndexInfo != nil {
		res.Index = p.IndexInfo.Name.O
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			res.IsClusteredIndex = true
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				res.IndexCols = append(res.IndexCols, tblCol.GeneratedExprString)
			} else {
				res.IndexCols = append(res.IndexCols, idxCol.Name.O)
			}
		}
	}
	return res
}

// AccessObject implements physicalScan interface.
func (p *BatchPointGetPlan) AccessObject() AccessObject {
	res := &ScanAccessObject{
		Database: p.dbName,
		Table:    p.TblInfo.Name.O,
	}
	if p.IndexInfo != nil {
		res.Index = p.IndexInfo.Name.O
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			res.IsClusteredIndex = true
		}
		for _, idxCol := range p.IndexInfo.Columns {
			if tblCol := p.TblInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				res.IndexCols = append(res.IndexCols, tblCol.GeneratedExprString)
			} else {
				res.IndexCols = append(res.IndexCols, idxCol.Name.O)
			}
		}
	}
	return res
}

func getDynamicAccessPartition(sctx sessionctx.Context, tblInfo *model.TableInfo, partitionInfo *PartitionInfo, asName string) (res *DynamicPartitionAccessObject) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return nil
	}

	res = &DynamicPartitionAccessObject{}
	tblName := tblInfo.Name.O
	if len(asName) > 0 {
		tblName = asName
	}
	res.Table = tblName
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	db, ok := is.SchemaByTable(tblInfo)
	if ok {
		res.Database = db.Name.O
	}
	tmp, ok := is.TableByID(tblInfo.ID)
	if !ok {
		res.err = "partition table not found:" + strconv.FormatInt(tblInfo.ID, 10)
		return res
	}
	tbl := tmp.(table.PartitionedTable)

	idxArr, err := PartitionPruning(sctx, tbl, partitionInfo.PruningConds, partitionInfo.PartitionNames, partitionInfo.Columns, partitionInfo.ColumnNames)
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

func (p *PhysicalTableReader) accessObject(sctx sessionctx.Context) AccessObject {
	if !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return nil
	}
	if len(p.PartitionInfos) == 0 {
		ts := p.TablePlans[0].(*PhysicalTableScan)
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := getDynamicAccessPartition(sctx, ts.Table, &p.PartitionInfo, asName)
		if res == nil {
			return nil
		}
		return &DynamicPartitionAccessObjects{res}
	}
	if len(p.PartitionInfos) == 1 {
		ts := p.PartitionInfos[0].tableScan
		partInfo := p.PartitionInfos[0].partitionInfo
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		res := getDynamicAccessPartition(sctx, ts.Table, &partInfo, asName)
		if res == nil {
			return nil
		}
		return &DynamicPartitionAccessObjects{res}
	}

	res := make(DynamicPartitionAccessObjects, 0)
	for _, info := range p.PartitionInfos {
		if info.tableScan.Table.GetPartitionInfo() == nil {
			continue
		}
		ts := info.tableScan
		partInfo := info.partitionInfo
		asName := ""
		if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
			asName = ts.TableAsName.O
		}
		accessObj := getDynamicAccessPartition(sctx, ts.Table, &partInfo, asName)
		if accessObj != nil {
			res = append(res, accessObj)
		}
	}
	if len(res) == 0 {
		return nil
	}
	return &res
}

func (p *PhysicalIndexReader) accessObject(sctx sessionctx.Context) AccessObject {
	if !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return nil
	}
	is := p.IndexPlans[0].(*PhysicalIndexScan)
	asName := ""
	if is.TableAsName != nil && len(is.TableAsName.O) > 0 {
		asName = is.TableAsName.O
	}
	res := getDynamicAccessPartition(sctx, is.Table, &p.PartitionInfo, asName)
	if res == nil {
		return nil
	}
	return &DynamicPartitionAccessObjects{res}
}

func (p *PhysicalIndexLookUpReader) accessObject(sctx sessionctx.Context) AccessObject {
	if !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return nil
	}
	ts := p.TablePlans[0].(*PhysicalTableScan)
	asName := ""
	if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
		asName = ts.TableAsName.O
	}
	res := getDynamicAccessPartition(sctx, ts.Table, &p.PartitionInfo, asName)
	if res == nil {
		return nil
	}
	return &DynamicPartitionAccessObjects{res}
}

func (p *PhysicalIndexMergeReader) accessObject(sctx sessionctx.Context) AccessObject {
	if !sctx.GetSessionVars().UseDynamicPartitionPrune() {
		return nil
	}
	ts := p.TablePlans[0].(*PhysicalTableScan)
	asName := ""
	if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
		asName = ts.TableAsName.O
	}
	res := getDynamicAccessPartition(sctx, ts.Table, &p.PartitionInfo, asName)
	if res == nil {
		return nil
	}
	return &DynamicPartitionAccessObjects{res}
}

// AccessObject implements physicalScan interface.
func (p *PhysicalCTE) AccessObject() AccessObject {
	return OtherAccessObject(fmt.Sprintf("CTE:%s", p.cteAsName.L))
}
