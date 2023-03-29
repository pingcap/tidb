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
	"context"
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql" //nolint: goimports
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"golang.org/x/exp/slices"
)

type tableKVEncoder struct {
	*BaseKVEncoder
	metrics *metric.Metrics
}

func GetSession4test(encoder encode.Encoder) sessionctx.Context {
	return encoder.(*tableKVEncoder).SessionCtx
}

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
			expr, err := expression.RewriteAstExpr(se, col.GeneratedExpr, schema, names, true)
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
	slices.SortFunc(genCols, func(i, j GeneratedCol) bool {
		return cols[i.Index].Offset < cols[j.Index].Offset
	})
	return genCols, nil
}

func (kvcodec *tableKVEncoder) Close() {
	kvcodec.SessionCtx.Close()
	if kvcodec.metrics != nil {
		kvcodec.metrics.KvEncoderCounter.WithLabelValues("close").Inc()
	}
}

type KvPairs struct {
	Pairs    []common.KvPair
	BytesBuf *BytesBuf
	MemBuf   *MemBuf
}

// MakeRowsFromKvPairs converts a KvPair slice into a Rows instance. This is
// mainly used for testing only. The resulting Rows instance should only be used
// for the importer backend.
func MakeRowsFromKvPairs(pairs []common.KvPair) encode.Rows {
	return &KvPairs{Pairs: pairs}
}

// MakeRowFromKvPairs converts a KvPair slice into a Row instance. This is
// mainly used for testing only. The resulting Row instance should only be used
// for the importer backend.
func MakeRowFromKvPairs(pairs []common.KvPair) encode.Row {
	return &KvPairs{Pairs: pairs}
}

// KvPairsFromRows converts a Rows instance constructed from MakeRowsFromKvPairs
// back into a slice of KvPair. This method panics if the Rows is not
// constructed in such way.
// nolint:golint // kv.KvPairsFromRows sounds good.
func KvPairsFromRows(rows encode.Rows) []common.KvPair {
	return rows.(*KvPairs).Pairs
}

// KvPairsFromRow converts a Row instance constructed from MakeRowFromKvPairs
// back into a slice of KvPair. This method panics if the Row is not
// constructed in such way.
// nolint:golint // kv.KvPairsFromRow sounds good.
func KvPairsFromRow(row encode.Row) []common.KvPair {
	return row.(*KvPairs).Pairs
}

// Encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *tableKVEncoder) Encode(row []types.Datum, rowID int64, columnPermutation []int, offset int64) (encode.Row, error) {
	cols := kvcodec.Table.Cols()

	var value types.Datum
	var err error
	//nolint: prealloc
	var record []types.Datum

	if kvcodec.RecordCache != nil {
		record = kvcodec.RecordCache
	} else {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	meta := kvcodec.Table.Meta()
	for i, col := range cols {
		var theDatum *types.Datum = nil
		j := columnPermutation[i]
		if j >= 0 && j < len(row) {
			theDatum = &row[j]
		}
		value, err = kvcodec.GetActualDatum(rowID, i, theDatum)
		if err != nil {
			return nil, kvcodec.LogKVConvertFailed(row, j, col.ToInfo(), err)
		}

		record = append(record, value)

		if kvcodec.IsAutoRandomCol(col.ToInfo()) {
			shardFmt := autoid.NewShardIDFormat(&col.FieldType, meta.AutoRandomBits, meta.AutoRandomRangeBits)
			// this allocator is the same as the allocator in table importer, i.e. PanickingAllocators. below too.
			alloc := kvcodec.Table.Allocators(kvcodec.SessionCtx).Get(autoid.AutoRandomType)
			if err := alloc.Rebase(context.Background(), value.GetInt64()&shardFmt.IncrementalMask(), false); err != nil {
				return nil, errors.Trace(err)
			}
		}
		if IsAutoIncCol(col.ToInfo()) {
			alloc := kvcodec.Table.Allocators(kvcodec.SessionCtx).Get(autoid.AutoIncrementType)
			if err := alloc.Rebase(context.Background(), GetAutoRecordID(value, &col.FieldType), false); err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if common.TableHasAutoRowID(meta) {
		rowValue := rowID
		j := columnPermutation[len(cols)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.SessionCtx, row[j], ExtraHandleColumnInfo, false, false)
			rowValue = value.GetInt64()
		} else {
			rowID := kvcodec.AutoIDFn(rowID)
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, kvcodec.LogKVConvertFailed(row, j, ExtraHandleColumnInfo, err)
		}
		record = append(record, value)
		alloc := kvcodec.Table.Allocators(kvcodec.SessionCtx).Get(autoid.RowIDAllocType)
		if err := alloc.Rebase(context.Background(), rowValue, false); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(kvcodec.GenCols) > 0 {
		if errCol, err := kvcodec.EvalGeneratedColumns(record, cols); err != nil {
			return nil, kvcodec.LogEvalGenExprFailed(row, errCol, err)
		}
	}

	return kvcodec.Record2KV(record, row, rowID)
}

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
func GetActualDatum(encoder encode.Encoder, rowID int64, colIndex int, inputDatum *types.Datum) (types.Datum, error) {
	return encoder.(*tableKVEncoder).GetActualDatum(70, 0, inputDatum)
}

// get record value for auto-increment field
//
// See: https://github.com/pingcap/tidb/blob/47f0f15b14ed54fc2222f3e304e29df7b05e6805/executor/insert_common.go#L781-L852
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

func (kvs *KvPairs) Size() uint64 {
	size := uint64(0)
	for _, kv := range kvs.Pairs {
		size += uint64(len(kv.Key) + len(kv.Val))
	}
	return size
}

func (kvs *KvPairs) ClassifyAndAppend(
	data *encode.Rows,
	dataChecksum *verification.KVChecksum,
	indices *encode.Rows,
	indexChecksum *verification.KVChecksum,
) {
	dataKVs := (*data).(*KvPairs)
	indexKVs := (*indices).(*KvPairs)

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

func (kvs *KvPairs) SplitIntoChunks(splitSize int) []encode.Rows {
	if len(kvs.Pairs) == 0 {
		return nil
	}

	res := make([]encode.Rows, 0, 1)
	i := 0
	cumSize := 0
	for j, pair := range kvs.Pairs {
		size := len(pair.Key) + len(pair.Val)
		if i < j && cumSize+size > splitSize {
			res = append(res, &KvPairs{Pairs: kvs.Pairs[i:j]})
			i = j
			cumSize = 0
		}
		cumSize += size
	}

	if i == 0 {
		res = append(res, kvs)
	} else {
		res = append(res, &KvPairs{
			Pairs:    kvs.Pairs[i:],
			BytesBuf: kvs.BytesBuf,
			MemBuf:   kvs.MemBuf,
		})
	}
	return res
}

func (kvs *KvPairs) Clear() encode.Rows {
	if kvs.BytesBuf != nil {
		kvs.MemBuf.Recycle(kvs.BytesBuf)
		kvs.BytesBuf = nil
		kvs.MemBuf = nil
	}
	kvs.Pairs = kvs.Pairs[:0]
	return kvs
}
