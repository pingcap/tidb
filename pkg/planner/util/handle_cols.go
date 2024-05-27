// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/size"
)

// HandleCols is the interface that holds handle columns.
type HandleCols interface {
	// BuildHandle builds a Handle from a row.
	BuildHandle(row chunk.Row) (kv.Handle, error)
	// BuildHandleByDatums builds a Handle from a datum slice.
	BuildHandleByDatums(row []types.Datum) (kv.Handle, error)
	// BuildHandleFromIndexRow builds a Handle from index row data.
	// The last column(s) of `row` must be the handle column(s).
	BuildHandleFromIndexRow(row chunk.Row) (kv.Handle, error)
	// BuildHandleFromIndexRow builds a Handle from index row data.
	// The last column of `row` must be the pids,
	// and the second to last column(s) of `row` must be the handle column(s).
	BuildPartitionHandleFromIndexRow(row chunk.Row) (kv.PartitionHandle, error)
	// ResolveIndices resolves handle column indices.
	ResolveIndices(schema *expression.Schema) (HandleCols, error)
	// IsInt returns if the HandleCols is a single int column.
	IsInt() bool
	// String implements the fmt.Stringer interface.
	String() string
	// GetCol gets the column by idx.
	GetCol(idx int) *expression.Column
	// NumCols returns the number of columns.
	NumCols() int
	// Compare compares two datum rows by handle order.
	Compare(a, b []types.Datum, ctors []collate.Collator) (int, error)
	// GetFieldsTypes return field types of columns.
	GetFieldsTypes() []*types.FieldType
	// MemoryUsage return the memory usage
	MemoryUsage() int64
}

// CommonHandleCols implements the kv.HandleCols interface.
type CommonHandleCols struct {
	tblInfo *model.TableInfo
	idxInfo *model.IndexInfo
	columns []*expression.Column
	sc      *stmtctx.StatementContext
}

// GetColumns returns all the internal columns out.
func (cb *CommonHandleCols) GetColumns() []*expression.Column {
	return cb.columns
}

func (cb *CommonHandleCols) buildHandleByDatumsBuffer(datumBuf []types.Datum) (kv.Handle, error) {
	tablecodec.TruncateIndexValues(cb.tblInfo, cb.idxInfo, datumBuf)
	handleBytes, err := codec.EncodeKey(cb.sc.TimeZone(), nil, datumBuf...)
	err = cb.sc.HandleError(err)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

// BuildHandle implements the kv.HandleCols interface.
func (cb *CommonHandleCols) BuildHandle(row chunk.Row) (kv.Handle, error) {
	datumBuf := make([]types.Datum, 0, 4)
	for _, col := range cb.columns {
		datumBuf = append(datumBuf, row.GetDatum(col.Index, col.RetType))
	}
	return cb.buildHandleByDatumsBuffer(datumBuf)
}

// BuildHandleFromIndexRow implements the kv.HandleCols interface.
func (cb *CommonHandleCols) BuildHandleFromIndexRow(row chunk.Row) (kv.Handle, error) {
	datumBuf := make([]types.Datum, 0, 4)
	for i := 0; i < cb.NumCols(); i++ {
		datumBuf = append(datumBuf, row.GetDatum(row.Len()-cb.NumCols()+i, cb.columns[i].RetType))
	}
	return cb.buildHandleByDatumsBuffer(datumBuf)
}

// BuildPartitionHandleFromIndexRow implements the kv.HandleCols interface.
func (cb *CommonHandleCols) BuildPartitionHandleFromIndexRow(row chunk.Row) (kv.PartitionHandle, error) {
	datumBuf := make([]types.Datum, 0, 4)
	for i := 0; i < cb.NumCols(); i++ {
		datumBuf = append(datumBuf, row.GetDatum(row.Len()-1-cb.NumCols()+i, cb.columns[i].RetType))
	}
	handle, err := cb.buildHandleByDatumsBuffer(datumBuf)
	if err != nil {
		return kv.PartitionHandle{}, err
	}
	return kv.NewPartitionHandle(
		row.GetInt64(row.Len()-1),
		handle,
	), nil
}

// BuildHandleByDatums implements the kv.HandleCols interface.
func (cb *CommonHandleCols) BuildHandleByDatums(row []types.Datum) (kv.Handle, error) {
	datumBuf := make([]types.Datum, 0, 4)
	for _, col := range cb.columns {
		datumBuf = append(datumBuf, row[col.Index])
	}
	return cb.buildHandleByDatumsBuffer(datumBuf)
}

// ResolveIndices implements the kv.HandleCols interface.
func (cb *CommonHandleCols) ResolveIndices(schema *expression.Schema) (HandleCols, error) {
	ncb := &CommonHandleCols{
		tblInfo: cb.tblInfo,
		idxInfo: cb.idxInfo,
		sc:      cb.sc,
		columns: make([]*expression.Column, len(cb.columns)),
	}
	for i, col := range cb.columns {
		newCol, err := col.ResolveIndices(schema)
		if err != nil {
			return nil, err
		}
		ncb.columns[i] = newCol.(*expression.Column)
	}
	return ncb, nil
}

// IsInt implements the kv.HandleCols interface.
func (*CommonHandleCols) IsInt() bool {
	return false
}

// GetCol implements the kv.HandleCols interface.
func (cb *CommonHandleCols) GetCol(idx int) *expression.Column {
	return cb.columns[idx]
}

// NumCols implements the kv.HandleCols interface.
func (cb *CommonHandleCols) NumCols() int {
	return len(cb.columns)
}

// String implements the kv.HandleCols interface.
func (cb *CommonHandleCols) String() string {
	b := new(strings.Builder)
	b.WriteByte('[')
	for i, col := range cb.columns {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(col.ColumnExplainInfo(false))
	}
	b.WriteByte(']')
	return b.String()
}

// Compare implements the kv.HandleCols interface.
func (cb *CommonHandleCols) Compare(a, b []types.Datum, ctors []collate.Collator) (int, error) {
	for i, col := range cb.columns {
		aDatum := &a[col.Index]
		bDatum := &b[col.Index]
		cmp, err := aDatum.Compare(cb.sc.TypeCtx(), bDatum, ctors[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

// GetFieldsTypes implements the kv.HandleCols interface.
func (cb *CommonHandleCols) GetFieldsTypes() []*types.FieldType {
	fieldTps := make([]*types.FieldType, 0, len(cb.columns))
	for _, col := range cb.columns {
		fieldTps = append(fieldTps, col.RetType)
	}
	return fieldTps
}

const emptyCommonHandleColsSize = int64(unsafe.Sizeof(CommonHandleCols{}))

// MemoryUsage return the memory usage of CommonHandleCols
func (cb *CommonHandleCols) MemoryUsage() (sum int64) {
	if cb == nil {
		return
	}

	sum = emptyCommonHandleColsSize + int64(cap(cb.columns))*size.SizeOfPointer
	for _, col := range cb.columns {
		sum += col.MemoryUsage()
	}
	return
}

// NewCommonHandleCols creates a new CommonHandleCols.
func NewCommonHandleCols(sc *stmtctx.StatementContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	tableColumns []*expression.Column) *CommonHandleCols {
	cols := &CommonHandleCols{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		sc:      sc,
		columns: make([]*expression.Column, len(idxInfo.Columns)),
	}
	for i, idxCol := range idxInfo.Columns {
		cols.columns[i] = tableColumns[idxCol.Offset]
	}
	return cols
}

// NewCommonHandlesColsWithoutColsAlign creates a new CommonHandleCols without internal col align.
func NewCommonHandlesColsWithoutColsAlign(sc *stmtctx.StatementContext, tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo, cols []*expression.Column) *CommonHandleCols {
	return &CommonHandleCols{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		sc:      sc,
		columns: cols,
	}
}

// IntHandleCols implements the kv.HandleCols interface.
type IntHandleCols struct {
	col *expression.Column
}

// BuildHandle implements the kv.HandleCols interface.
func (ib *IntHandleCols) BuildHandle(row chunk.Row) (kv.Handle, error) {
	return kv.IntHandle(row.GetInt64(ib.col.Index)), nil
}

// BuildHandleFromIndexRow implements the kv.HandleCols interface.
func (*IntHandleCols) BuildHandleFromIndexRow(row chunk.Row) (kv.Handle, error) {
	return kv.IntHandle(row.GetInt64(row.Len() - 1)), nil
}

// BuildPartitionHandleFromIndexRow implements the kv.HandleCols interface.
func (*IntHandleCols) BuildPartitionHandleFromIndexRow(row chunk.Row) (kv.PartitionHandle, error) {
	return kv.NewPartitionHandle(
		row.GetInt64(row.Len()-1),
		kv.IntHandle(row.GetInt64(row.Len()-2)),
	), nil
}

// BuildHandleByDatums implements the kv.HandleCols interface.
func (ib *IntHandleCols) BuildHandleByDatums(row []types.Datum) (kv.Handle, error) {
	return kv.IntHandle(row[ib.col.Index].GetInt64()), nil
}

// ResolveIndices implements the kv.HandleCols interface.
func (ib *IntHandleCols) ResolveIndices(schema *expression.Schema) (HandleCols, error) {
	newCol, err := ib.col.ResolveIndices(schema)
	if err != nil {
		return nil, err
	}
	return &IntHandleCols{col: newCol.(*expression.Column)}, nil
}

// IsInt implements the kv.HandleCols interface.
func (*IntHandleCols) IsInt() bool {
	return true
}

// String implements the kv.HandleCols interface.
func (ib *IntHandleCols) String() string {
	return ib.col.ColumnExplainInfo(false)
}

// GetCol implements the kv.HandleCols interface.
func (ib *IntHandleCols) GetCol(idx int) *expression.Column {
	if idx != 0 {
		return nil
	}
	return ib.col
}

// NumCols implements the kv.HandleCols interface.
func (*IntHandleCols) NumCols() int {
	return 1
}

// Compare implements the kv.HandleCols interface.
func (ib *IntHandleCols) Compare(a, b []types.Datum, ctors []collate.Collator) (int, error) {
	aVal := &a[ib.col.Index]
	bVal := &b[ib.col.Index]
	return aVal.Compare(types.DefaultStmtNoWarningContext, bVal, ctors[ib.col.Index])
}

// GetFieldsTypes implements the kv.HandleCols interface.
func (*IntHandleCols) GetFieldsTypes() []*types.FieldType {
	return []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
}

// MemoryUsage return the memory usage of IntHandleCols
func (ib *IntHandleCols) MemoryUsage() (sum int64) {
	if ib == nil {
		return
	}

	if ib.col != nil {
		sum = ib.col.MemoryUsage()
	}
	return
}

// NewIntHandleCols creates a new IntHandleCols.
func NewIntHandleCols(col *expression.Column) HandleCols {
	return &IntHandleCols{col: col}
}

// GetCommonHandleDatum gets the original data for the common handle.
func GetCommonHandleDatum(cols HandleCols, row chunk.Row) []types.Datum {
	if cols.IsInt() {
		return nil
	}
	cb := cols.(*CommonHandleCols)

	datumBuf := make([]types.Datum, 0, 4)
	for _, col := range cb.columns {
		datumBuf = append(datumBuf, row.GetDatum(col.Index, col.RetType))
	}

	return datumBuf
}
