// Copyright 2019 PingCAP, Inc.
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

// TODO combine with the pkg/kv package outside.

package kv

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql" //nolint: goimports
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
)

type tableKVEncoder struct {
	*BaseKVEncoder
	metrics *metric.Metrics
}

// GetSession4test is only used for test.
func GetSession4test(encoder encode.Encoder) sessionctx.Context {
	return encoder.(*tableKVEncoder).SessionCtx
}

// NewTableKVEncoder creates a new tableKVEncoder.
func NewTableKVEncoder(
	config *encode.EncodingConfig,
	metrics *metric.Metrics,
) (encode.Encoder, error) {
	if metrics != nil {
		metrics.KvEncoderCounter.WithLabelValues("open").Inc()
	}
	baseKVEncoder, err := NewBaseKVEncoder(config)
	if err != nil {
		return nil, err
	}

	return &tableKVEncoder{
		BaseKVEncoder: baseKVEncoder,
		metrics:       metrics,
	}, nil
}

// CollectGeneratedColumns collects all expressions required to evaluate the
// results of all generated columns. The returning slice is in evaluation order.
func CollectGeneratedColumns(se *Session, meta *model.TableInfo, cols []*table.Column) ([]GeneratedCol, error) {
	hasGenCol := false
	for _, col := range cols {
		if col.GeneratedExpr != nil {
			hasGenCol = true
			break
		}
	}

	if !hasGenCol {
		return nil, nil
	}

	// the expression rewriter requires a non-nil TxnCtx.
	se.Vars.TxnCtx = new(variable.TransactionContext)
	defer func() {
		se.Vars.TxnCtx = nil
	}()

	// not using TableInfo2SchemaAndNames to avoid parsing all virtual generated columns again.
	exprColumns := make([]*expression.Column, 0, len(cols))
	names := make(types.NameSlice, 0, len(cols))
	for i, col := range cols {
		names = append(names, &types.FieldName{
			OrigTblName: meta.Name,
			OrigColName: col.Name,
			TblName:     meta.Name,
			ColName:     col.Name,
		})
		exprColumns = append(exprColumns, &expression.Column{
			RetType:  col.FieldType.Clone(),
			ID:       col.ID,
			UniqueID: int64(i),
			Index:    col.Offset,
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		})
	}
	schema := expression.NewSchema(exprColumns...)

	// as long as we have a stored generated column, all columns it referred to must be evaluated as well.
	// for simplicity we just evaluate all generated columns (virtual or not) before the last stored one.
	var genCols []GeneratedCol
	for i, col := range cols {
		if col.GeneratedExpr != nil {
			expr, err := expression.BuildSimpleExpr(
				se.GetExprCtx(),
				col.GeneratedExpr.Internal(),
				expression.WithInputSchemaAndNames(schema, names, meta),
				expression.WithAllowCastArray(true),
			)
			if err != nil {
				return nil, err
			}
			genCols = append(genCols, GeneratedCol{
				Index: i,
				Expr:  expr,
			})
		}
	}

	// order the result by column offset so they match the evaluation order.
	slices.SortFunc(genCols, func(i, j GeneratedCol) int {
		return cmp.Compare(cols[i.Index].Offset, cols[j.Index].Offset)
	})
	return genCols, nil
}

// Close implements the Encoder interface.
func (kvcodec *tableKVEncoder) Close() {
	kvcodec.SessionCtx.Close()
	if kvcodec.metrics != nil {
		kvcodec.metrics.KvEncoderCounter.WithLabelValues("close").Inc()
	}
}

// Pairs implements the Encoder interface.
type Pairs struct {
	Pairs    []common.KvPair
	BytesBuf *BytesBuf
	MemBuf   *MemBuf
}

// GroupedPairs is a map from index ID to KvPairs.
type GroupedPairs map[int64][]common.KvPair

// SplitIntoChunks implements the encode.Rows interface. It just satisfies the
// type system and should never be called.
func (GroupedPairs) SplitIntoChunks(int) []encode.Rows {
	panic("not implemented")
}

// Clear implements the encode.Rows interface. It just satisfies the type system
// and should never be called.
func (GroupedPairs) Clear() encode.Rows {
	panic("not implemented")
}

// MakeRowsFromKvPairs converts a KvPair slice into a Rows instance. This is
// mainly used for testing only. The resulting Rows instance should only be used
// for the importer backend.
func MakeRowsFromKvPairs(pairs []common.KvPair) encode.Rows {
	return &Pairs{Pairs: pairs}
}

// MakeRowFromKvPairs converts a KvPair slice into a Row instance. This is
// mainly used for testing only. The resulting Row instance should only be used
// for the importer backend.
func MakeRowFromKvPairs(pairs []common.KvPair) encode.Row {
	return &Pairs{Pairs: pairs}
}

// Rows2KvPairs converts a Rows instance constructed from MakeRowsFromKvPairs
// back into a slice of KvPair. This method panics if the Rows is not
// constructed in such way.
func Rows2KvPairs(rows encode.Rows) []common.KvPair {
	switch v := rows.(type) {
	case *Pairs:
		return v.Pairs
	case GroupedPairs:
		cnt := 0
		for _, pairs := range v {
			cnt += len(pairs)
		}
		res := make([]common.KvPair, 0, cnt)
		for _, pairs := range v {
			res = append(res, pairs...)
		}
		return res
	}
	panic(fmt.Sprintf("unknown Rows type %T", rows))
}

// Row2KvPairs converts a Row instance constructed from MakeRowFromKvPairs
// back into a slice of KvPair. This method panics if the Row is not
// constructed in such way.
func Row2KvPairs(row encode.Row) []common.KvPair {
	return row.(*Pairs).Pairs
}

// ClearRow recycles the memory used by the row.
func ClearRow(row encode.Row) {
	if pairs, ok := row.(*Pairs); ok {
		pairs.Clear()
	}
}

// Encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *tableKVEncoder) Encode(row []types.Datum,
	rowID int64, columnPermutation []int, _ int64) (encode.Row, error) {
	// we ignore warnings when encoding rows now, but warnings uses the same memory as parser, since the input
	// row []types.Datum share the same underlying buf, and when doing CastValue, we're using hack.String/hack.Slice.
	// when generating error such as mysql.ErrDataOutOfRange, the data will be part of the error, causing the buf
	// unable to release. So we truncate the warnings here.
	defer kvcodec.TruncateWarns()
	var value types.Datum
	var err error

	record := kvcodec.GetOrCreateRecord()
	for i, col := range kvcodec.Columns {
		var theDatum *types.Datum
		j := columnPermutation[i]
		if j >= 0 && j < len(row) {
			theDatum = &row[j]
		}
		value, err = kvcodec.ProcessColDatum(col, rowID, theDatum)
		if err != nil {
			return nil, kvcodec.LogKVConvertFailed(row, j, col.ToInfo(), err)
		}

		record = append(record, value)
	}

	if common.TableHasAutoRowID(kvcodec.Table.Meta()) {
		rowValue := rowID
		j := columnPermutation[len(kvcodec.Columns)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.SessionCtx, row[j],
				ExtraHandleColumnInfo, false, false)
			rowValue = value.GetInt64()
		} else {
			rowID := kvcodec.AutoIDFn(rowID)
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, kvcodec.LogKVConvertFailed(row, j, ExtraHandleColumnInfo, err)
		}
		record = append(record, value)
		alloc := kvcodec.Table.Allocators(kvcodec.SessionCtx.GetTableCtx()).Get(autoid.RowIDAllocType)
		if err := alloc.Rebase(context.Background(), rowValue, false); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(kvcodec.GenCols) > 0 {
		if errCol, err := kvcodec.EvalGeneratedColumns(record, kvcodec.Columns); err != nil {
			return nil, kvcodec.LogEvalGenExprFailed(row, errCol, err)
		}
	}

	return kvcodec.Record2KV(record, row, rowID)
}

// IsAutoIncCol return true if the column is auto increment column.
func IsAutoIncCol(colInfo *model.ColumnInfo) bool {
	return mysql.HasAutoIncrementFlag(colInfo.GetFlag())
}

// GetEncoderIncrementalID return Auto increment id.
func GetEncoderIncrementalID(encoder encode.Encoder, id int64) int64 {
	return encoder.(*tableKVEncoder).AutoIDFn(id)
}

// GetEncoderSe return session.
func GetEncoderSe(encoder encode.Encoder) *Session {
	return encoder.(*tableKVEncoder).SessionCtx
}

// GetActualDatum export getActualDatum function.
func GetActualDatum(encoder encode.Encoder, col *table.Column, rowID int64,
	inputDatum *types.Datum) (types.Datum, error) {
	return encoder.(*tableKVEncoder).getActualDatum(col, rowID, inputDatum)
}

// GetAutoRecordID returns the record ID for an auto-increment field.
// get record value for auto-increment field
//
// See:
//
//	https://github.com/pingcap/tidb/blob/47f0f15b14ed54fc2222f3e304e29df7b05e6805/executor/insert_common.go#L781-L852
func GetAutoRecordID(d types.Datum, target *types.FieldType) int64 {
	switch target.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble:
		return int64(math.Round(d.GetFloat64()))
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return d.GetInt64()
	default:
		panic(fmt.Sprintf("unsupported auto-increment field type '%d'", target.GetType()))
	}
}

// Size returns the total size of the key-value pairs.
func (kvs *Pairs) Size() uint64 {
	size := uint64(0)
	for _, kv := range kvs.Pairs {
		size += uint64(len(kv.Key) + len(kv.Val))
	}
	return size
}

// ClassifyAndAppend separates the key-value pairs into data and index key-value pairs.
func (kvs *Pairs) ClassifyAndAppend(
	data *encode.Rows,
	dataChecksum *verification.KVChecksum,
	indices *encode.Rows,
	indexChecksum *verification.KVChecksum,
) {
	dataKVs := (*data).(*Pairs)
	indexKVs := (*indices).(*Pairs)

	for _, kv := range kvs.Pairs {
		if kv.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
			dataKVs.Pairs = append(dataKVs.Pairs, kv)
			dataChecksum.UpdateOne(kv)
		} else {
			indexKVs.Pairs = append(indexKVs.Pairs, kv)
			indexChecksum.UpdateOne(kv)
		}
	}

	// the related buf is shared, so we only need to set it into one of the kvs so it can be released
	if kvs.BytesBuf != nil {
		dataKVs.BytesBuf = kvs.BytesBuf
		dataKVs.MemBuf = kvs.MemBuf
		kvs.BytesBuf = nil
		kvs.MemBuf = nil
	}

	*data = dataKVs
	*indices = indexKVs
}

// Clear clears the key-value pairs.
func (kvs *Pairs) Clear() encode.Rows {
	if kvs.BytesBuf != nil {
		kvs.MemBuf.Recycle(kvs.BytesBuf)
		kvs.BytesBuf = nil
		kvs.MemBuf = nil
	}
	kvs.Pairs = kvs.Pairs[:0]
	return kvs
}
