// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package tables

import (
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tipb/go-tipb"
)

// FindIndexByColName returns a public table index containing only one column named `name`.
func FindIndexByColName(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		// only public index can be read.
		if idx.Meta().State != model.StatePublic {
			continue
		}

		if len(idx.Meta().Columns) == 1 && strings.EqualFold(idx.Meta().Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

func getDuplicateError(tblInfo *model.TableInfo, handle kv.Handle, row []types.Datum) error {
	keyName := tblInfo.Name.String() + ".PRIMARY"

	if handle.IsInt() {
		return kv.GenKeyExistsErr([]string{handle.String()}, keyName)
	}
	pkIdx := FindPrimaryIndex(tblInfo)
	if pkIdx == nil {
		handleData, err := handle.Data()
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		colStrVals, err := genIndexKeyStrs(handleData)
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		return kv.GenKeyExistsErr(colStrVals, keyName)
	}
	pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkDts = append(pkDts, row[idxCol.Offset])
	}
	tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
	colStrVals, err := genIndexKeyStrs(pkDts)
	if err != nil {
		// if genIndexKeyStrs failed, return ErrKeyExists with handle.String().
		return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
	}
	return kv.GenKeyExistsErr(colStrVals, keyName)
}

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
	tableutil.TempTableFromMeta = TempTableFromMeta
}

// TryGetHandleRestoredDataWrapper tries to get the restored data for handle if needed. The argument can be a slice or a map.
func TryGetHandleRestoredDataWrapper(tblInfo *model.TableInfo, row []types.Datum, rowMap map[int64]types.Datum, idx *model.IndexInfo) []types.Datum {
	if !collate.NewCollationEnabled() || !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return nil
	}
	rsData := make([]types.Datum, 0, 4)
	pkIdx := FindPrimaryIndex(tblInfo)
	for _, pkIdxCol := range pkIdx.Columns {
		pkCol := tblInfo.Columns[pkIdxCol.Offset]
		if !types.NeedRestoredData(&pkCol.FieldType) {
			continue
		}
		var datum types.Datum
		if len(rowMap) > 0 {
			datum = rowMap[pkCol.ID]
		} else {
			datum = row[pkCol.Offset]
		}
		TryTruncateRestoredData(&datum, pkCol, pkIdxCol, idx)
		ConvertDatumToTailSpaceCount(&datum, pkCol)
		rsData = append(rsData, datum)
	}
	return rsData
}

// TryTruncateRestoredData tries to truncate index values.
// Says that primary key(a (8)),
// For index t(a), don't truncate the value.
// For index t(a(9)), truncate to a(9).
// For index t(a(7)), truncate to a(8).
func TryTruncateRestoredData(datum *types.Datum, pkCol *model.ColumnInfo,
	pkIdxCol *model.IndexColumn, idx *model.IndexInfo) {
	truncateTargetCol := pkIdxCol
	for _, idxCol := range idx.Columns {
		if idxCol.Offset == pkIdxCol.Offset {
			truncateTargetCol = maxIndexLen(pkIdxCol, idxCol)
			break
		}
	}
	tablecodec.TruncateIndexValue(datum, truncateTargetCol, pkCol)
}

// ConvertDatumToTailSpaceCount converts a string datum to an int datum that represents the tail space count.
func ConvertDatumToTailSpaceCount(datum *types.Datum, col *model.ColumnInfo) {
	if collate.IsBinCollation(col.GetCollate()) {
		*datum = types.NewIntDatum(stringutil.GetTailSpaceCount(datum.GetString()))
	}
}

func maxIndexLen(idxA, idxB *model.IndexColumn) *model.IndexColumn {
	if idxA.Length == types.UnspecifiedLength {
		return idxA
	}
	if idxB.Length == types.UnspecifiedLength {
		return idxB
	}
	if idxA.Length > idxB.Length {
		return idxA
	}
	return idxB
}


// BuildTableScanFromInfos build tipb.TableScan with *model.TableInfo and *model.ColumnInfo.
func BuildTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, isTiFlashStore bool) *tipb.TableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.TableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, isTiFlashStore),
		PrimaryColumnIds: pkColIDs,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// BuildPartitionTableScanFromInfos build tipb.PartitonTableScan with *model.TableInfo and *model.ColumnInfo.
func BuildPartitionTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, fastScan bool) *tipb.PartitionTableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.PartitionTableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, false),
		PrimaryColumnIds: pkColIDs,
		IsFastScan:       &fastScan,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfo.
func SetPBColumnsDefaultValue(ctx expression.BuildContext, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be computed later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		evalCtx := ctx.GetEvalCtx()
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx, c)
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(evalCtx.Location(), nil, d)
		ec := evalCtx.ErrCtx()
		err = ec.HandleError(err)
		if err != nil {
			return err
		}
	}
	return nil
}

// TemporaryTable is used to store transaction-specific or session-specific information for global / local temporary tables.
// For example, stats and autoID should have their own copies of data, instead of being shared by all sessions.
type TemporaryTable struct {
	// Whether it's modified in this transaction.
	modified bool
	// The stats of this table. So far it's always pseudo stats.
	stats *statistics.Table
	// The autoID allocator of this table.
	autoIDAllocator autoid.Allocator
	// Table size.
	size int64

	meta *model.TableInfo
}

// TempTableFromMeta builds a TempTable from model.TableInfo.
func TempTableFromMeta(tblInfo *model.TableInfo) tableutil.TempTable {
	return &TemporaryTable{
		modified:        false,
		stats:           statistics.PseudoTable(tblInfo, false, false),
		autoIDAllocator: autoid.NewAllocatorFromTempTblInfo(tblInfo),
		meta:            tblInfo,
	}
}

// GetAutoIDAllocator is implemented from TempTable.GetAutoIDAllocator.
func (t *TemporaryTable) GetAutoIDAllocator() autoid.Allocator {
	return t.autoIDAllocator
}

// SetModified is implemented from TempTable.SetModified.
func (t *TemporaryTable) SetModified(modified bool) {
	t.modified = modified
}

// GetModified is implemented from TempTable.GetModified.
func (t *TemporaryTable) GetModified() bool {
	return t.modified
}

// GetStats is implemented from TempTable.GetStats.
func (t *TemporaryTable) GetStats() any {
	return t.stats
}

// GetSize gets the table size.
func (t *TemporaryTable) GetSize() int64 {
	return t.size
}

// SetSize sets the table size.
func (t *TemporaryTable) SetSize(v int64) {
	t.size = v
}

// GetMeta gets the table meta.
func (t *TemporaryTable) GetMeta() *model.TableInfo {
	return t.meta
}
