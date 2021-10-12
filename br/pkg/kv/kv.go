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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

var extraHandleColumnInfo = model.NewExtraHandleColInfo()

// Iter abstract iterator method for Ingester.
type Iter interface {
	// Seek seek to specify position.
	// if key not found, seeks next key position in iter.
	Seek(key []byte) bool
	// Error return current error on this iter.
	Error() error
	// First moves this iter to the first key.
	First() bool
	// Last moves this iter to the last key.
	Last() bool
	// Valid check this iter reach the end.
	Valid() bool
	// Next moves this iter forward.
	Next() bool
	// Key represents current position pair's key.
	Key() []byte
	// Value represents current position pair's Value.
	Value() []byte
	// Close close this iter.
	Close() error
	// OpType represents operations of pair. currently we have two types.
	// 1. Put
	// 2. Delete
	OpType() sst.Pair_OP
}

// IterProducer produces iterator with given range.
type IterProducer interface {
	// Produce produces iterator with given range [start, end).
	Produce(start []byte, end []byte) Iter
}

// SimpleKVIterProducer represents kv iter producer.
type SimpleKVIterProducer struct {
	pairs Pairs
}

// NewSimpleKVIterProducer creates SimpleKVIterProducer.
func NewSimpleKVIterProducer(pairs Pairs) IterProducer {
	return &SimpleKVIterProducer{
		pairs: pairs,
	}
}

// Produce implements Iter.Producer.Produce.
func (p *SimpleKVIterProducer) Produce(start []byte, end []byte) Iter {
	startIndex := sort.Search(len(p.pairs), func(i int) bool {
		return bytes.Compare(start, p.pairs[i].Key) < 1
	})
	endIndex := sort.Search(len(p.pairs), func(i int) bool {
		return bytes.Compare(end, p.pairs[i].Key) < 1
	})
	if startIndex >= endIndex {
		log.Warn("produce failed due to start key is large than end key",
			zap.Binary("start", start), zap.Binary("end", end))
		return nil
	}
	return newSimpleKVIter(p.pairs[startIndex:endIndex])
}

// SimpleKVIter represents simple pair iterator.
// which is used for log restore.
type SimpleKVIter struct {
	index int
	pairs Pairs
}

// newSimpleKVIter creates SimpleKVIter.
func newSimpleKVIter(pairs Pairs) Iter {
	return &SimpleKVIter{
		index: -1,
		pairs: pairs,
	}
}

// Seek implements Iter.Seek.
func (s *SimpleKVIter) Seek(key []byte) bool {
	s.index = sort.Search(len(s.pairs), func(i int) bool {
		return bytes.Compare(key, s.pairs[i].Key) < 1
	})
	return s.index < len(s.pairs)
}

// Error implements Iter.Error.
func (s *SimpleKVIter) Error() error {
	return nil
}

// First implements Iter.First.
func (s *SimpleKVIter) First() bool {
	if len(s.pairs) == 0 {
		return false
	}
	s.index = 0
	return true
}

// Last implements Iter.Last.
func (s *SimpleKVIter) Last() bool {
	if len(s.pairs) == 0 {
		return false
	}
	s.index = len(s.pairs) - 1
	return true
}

// Valid implements Iter.Valid.
func (s *SimpleKVIter) Valid() bool {
	return s.index >= 0 && s.index < len(s.pairs)
}

// Next implements Iter.Next.
func (s *SimpleKVIter) Next() bool {
	s.index++
	return s.index < len(s.pairs)
}

// Key implements Iter.Key.
func (s *SimpleKVIter) Key() []byte {
	if s.index >= 0 && s.index < len(s.pairs) {
		return s.pairs[s.index].Key
	}
	return nil
}

// Value implements Iter.Value.
func (s *SimpleKVIter) Value() []byte {
	if s.index >= 0 && s.index < len(s.pairs) {
		return s.pairs[s.index].Val
	}
	return nil
}

// Close implements Iter.Close.
func (s *SimpleKVIter) Close() error {
	return nil
}

// OpType implements Iter.KeyIsDelete.
func (s *SimpleKVIter) OpType() sst.Pair_OP {
	if s.Valid() && s.pairs[s.index].IsDelete {
		return sst.Pair_Delete
	}
	return sst.Pair_Put
}

// Encoder encodes a row of SQL values into some opaque type which can be
// consumed by OpenEngine.WriteEncoded.
type Encoder interface {
	// Close the encoder.
	Close()

	// AddRecord encode encodes a row of SQL values into a backend-friendly format.
	AddRecord(
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, int, error)

	// RemoveRecord encode encodes a row of SQL delete values into a backend-friendly format.
	RemoveRecord(
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, int, error)
}

// Row represents a single encoded row.
type Row interface {
	// ClassifyAndAppend separates the data-like and index-like parts of the
	// encoded row, and appends these parts into the existing buffers and
	// checksums.
	ClassifyAndAppend(
		data *Pairs,
		dataChecksum *Checksum,
		indices *Pairs,
		indexChecksum *Checksum,
	)
}

type tableKVEncoder struct {
	tbl         table.Table
	se          *session
	recordCache []types.Datum
}

// NewTableKVEncoder creates the Encoder.
func NewTableKVEncoder(tbl table.Table, options *SessionOptions) Encoder {
	se := newSession(options)
	// Set CommonAddRecordCtx to session to reuse the slices and BufStore in AddRecord
	recordCtx := tables.NewCommonAddRecordCtx(len(tbl.Cols()))
	tables.SetAddRecordCtx(se, recordCtx)
	return &tableKVEncoder{
		tbl: tbl,
		se:  se,
	}
}

var kindStr = [...]string{
	types.KindNull:          "null",
	types.KindInt64:         "int64",
	types.KindUint64:        "uint64",
	types.KindFloat32:       "float32",
	types.KindFloat64:       "float64",
	types.KindString:        "string",
	types.KindBytes:         "bytes",
	types.KindBinaryLiteral: "binary",
	types.KindMysqlDecimal:  "decimal",
	types.KindMysqlDuration: "duration",
	types.KindMysqlEnum:     "enum",
	types.KindMysqlBit:      "bit",
	types.KindMysqlSet:      "set",
	types.KindMysqlTime:     "time",
	types.KindInterface:     "interface",
	types.KindMinNotNull:    "min",
	types.KindMaxValue:      "max",
	types.KindRaw:           "raw",
	types.KindMysqlJSON:     "json",
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface.
func zapRow(key string, row []types.Datum) zap.Field {
	return logutil.AbbreviatedArray(key, row, func(input interface{}) []string {
		row := input.([]types.Datum)
		vals := make([]string, 0, len(row))
		for _, datum := range row {
			kind := datum.Kind()
			var str string
			var err error
			switch kind {
			case types.KindNull:
				str = "NULL"
			case types.KindMinNotNull:
				str = "-inf"
			case types.KindMaxValue:
				str = "+inf"
			default:
				str, err = datum.ToString()
				if err != nil {
					vals = append(vals, err.Error())
					continue
				}
			}
			vals = append(vals,
				fmt.Sprintf("kind: %s, val: %s", kindStr[kind], redact.String(str)))
		}
		return vals
	})
}

// Pairs represents the slice of Pair.
type Pairs []Pair

// Close ...
func (kvcodec *tableKVEncoder) Close() {
}

// AddRecord encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *tableKVEncoder) AddRecord(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, int, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error

	record := kvcodec.recordCache
	if record == nil {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	isAutoRandom := false
	if kvcodec.tbl.Meta().PKIsHandle && kvcodec.tbl.Meta().ContainsAutoRandomBits() {
		isAutoRandom = true
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		isPk := mysql.HasPriKeyFlag(col.Flag)
		switch {
		case j >= 0 && j < len(row):
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		case isAutoIncCol:
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		default:
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}

		record = append(record, value)

		if isAutoRandom && isPk {
			typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[col.Tp] * 8)
			incrementalBits := typeBitsLength - kvcodec.tbl.Meta().AutoRandomBits
			hasSignBit := !mysql.HasUnsignedFlag(col.Flag)
			if hasSignBit {
				incrementalBits--
			}
			alloc := kvcodec.tbl.Allocators(kvcodec.se).Get(autoid.AutoRandomType)
			_ = alloc.Rebase(context.Background(), value.GetInt64()&((1<<incrementalBits)-1), false)
		}
		if isAutoIncCol {
			alloc := kvcodec.tbl.Allocators(kvcodec.se).Get(autoid.RowIDAllocType)
			_ = alloc.Rebase(context.Background(), getAutoRecordID(value, &col.FieldType), false)
		}
	}

	if TableHasAutoRowID(kvcodec.tbl.Meta()) {
		j := columnPermutation[len(cols)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], extraHandleColumnInfo, false, false)
		} else {
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		record = append(record, value)
		alloc := kvcodec.tbl.Allocators(kvcodec.se).Get(autoid.RowIDAllocType)
		_ = alloc.Rebase(context.Background(), value.GetInt64(), false)
	}
	_, err = kvcodec.tbl.AddRecord(kvcodec.se, record)
	if err != nil {
		log.Error("kv add Record failed",
			zapRow("originalRow", row),
			zapRow("convertedRow", record),
			zap.Error(err),
		)
		return nil, 0, errors.Trace(err)
	}

	pairs, size := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return Pairs(pairs), size, nil
}

// get record value for auto-increment field
//
// See: https://github.com/pingcap/tidb/blob/47f0f15b14ed54fc2222f3e304e29df7b05e6805/executor/insert_common.go#L781-L852
// TODO: merge this with pkg/lightning/backend/kv/sql2kv.go
func getAutoRecordID(d types.Datum, target *types.FieldType) int64 {
	switch target.Tp {
	case mysql.TypeFloat, mysql.TypeDouble:
		return int64(math.Round(d.GetFloat64()))
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return d.GetInt64()
	default:
		panic(fmt.Sprintf("unsupported auto-increment field type '%d'", target.Tp))
	}
}

// RemoveRecord encode a row of data into KV pairs.
func (kvcodec *tableKVEncoder) RemoveRecord(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, int, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error

	record := kvcodec.recordCache
	if record == nil {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		switch {
		case j >= 0 && j < len(row):
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		case isAutoIncCol:
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		default:
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		record = append(record, value)
	}
	err = kvcodec.tbl.RemoveRecord(kvcodec.se, kv.IntHandle(rowID), record)
	if err != nil {
		log.Error("kv remove record failed",
			zapRow("originalRow", row),
			zapRow("convertedRow", record),
			zap.Error(err),
		)
		return nil, 0, errors.Trace(err)
	}

	pairs, size := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return Pairs(pairs), size, nil
}

// ClassifyAndAppend split Pairs to data rows and index rows.
func (kvs Pairs) ClassifyAndAppend(
	data *Pairs,
	dataChecksum *Checksum,
	indices *Pairs,
	indexChecksum *Checksum,
) {
	dataKVs := *data
	indexKVs := *indices

	for _, kv := range kvs {
		if kv.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
			dataKVs = append(dataKVs, kv)
			dataChecksum.UpdateOne(kv)
		} else {
			indexKVs = append(indexKVs, kv)
			indexChecksum.UpdateOne(kv)
		}
	}

	*data = dataKVs
	*indices = indexKVs
}

// Clear resets the Pairs.
func (kvs Pairs) Clear() Pairs {
	return kvs[:0]
}

// NextKey return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func NextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	if tablecodec.IsRecordKey(key) {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		return tablecodec.EncodeRowKeyWithHandle(tableID, handle.Next())
	}

	// if key is an index, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}
