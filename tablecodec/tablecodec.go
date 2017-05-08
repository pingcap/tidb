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
// See the License for the specific language governing permissions and
// limitations under the License.

package tablecodec

import (
	"bytes"
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var (
	errInvalidKey         = terror.ClassXEval.New(codeInvalidKey, "invalid key")
	errInvalidRecordKey   = terror.ClassXEval.New(codeInvalidRecordKey, "invalid record key")
	errInvalidColumnCount = terror.ClassXEval.New(codeInvalidColumnCount, "invalid column count")
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

const (
	idLen           = 8
	prefixLen       = 1 + idLen /*tableID*/ + 2
	recordRowKeyLen = prefixLen + idLen /*handle*/
)

// TablePrefix returns table's prefix 't'.
func TablePrefix() []byte {
	return tablePrefix
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	buf := make([]byte, 0, recordRowKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle int64) kv.Key {
	buf := make([]byte, 0, recordRowKeyLen+idLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = codec.EncodeInt(buf, handle)
	return buf
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h int64) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+16)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)
	return buf
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle int64, err error) {
	k := key
	if !key.HasPrefix(tablePrefix) {
		return 0, 0, errInvalidRecordKey.Gen("invalid record key - %q", k)
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	if !key.HasPrefix(recordPrefixSep) {
		return 0, 0, errInvalidRecordKey.Gen("invalid record key - %q", k)
	}

	key = key[len(recordPrefixSep):]

	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	return
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		err = errInvalidKey.Gen("invalid key - %q", k)
		return
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if key.HasPrefix(recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !key.HasPrefix(indexPrefixSep) {
		err = errInvalidKey.Gen("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		return 0
	}
	key = key[len(tablePrefix):]
	_, tableID, _ := codec.DecodeInt(key)
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (int64, error) {
	_, handle, err := DecodeRecordKey(key)
	return handle, errors.Trace(err)
}

// EncodeValue encodes a go value to bytes.
func EncodeValue(raw types.Datum) ([]byte, error) {
	v, err := flatten(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := codec.EncodeValue(nil, v)
	return b, errors.Trace(err)
}

// EncodeRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
func EncodeRow(row []types.Datum, colIDs []int64) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	values := make([]types.Datum, 2*len(row))
	for i, c := range row {
		id := colIDs[i]
		idv := types.NewIntDatum(id)
		values[2*i] = idv
		fc, err := flatten(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[2*i+1] = fc
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	return codec.EncodeValue(nil, values...)
}

func flatten(data types.Datum) (types.Datum, error) {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		v, err := data.GetMysqlTime().ToPackedUint()
		return types.NewUintDatum(v), errors.Trace(err)
	case types.KindMysqlDuration:
		// for mysql time type
		data.SetInt64(int64(data.GetMysqlDuration().Duration))
		return data, nil
	case types.KindMysqlEnum:
		data.SetUint64(data.GetMysqlEnum().Value)
		return data, nil
	case types.KindMysqlSet:
		data.SetUint64(data.GetMysqlSet().Value)
		return data, nil
	case types.KindMysqlBit:
		data.SetUint64(data.GetMysqlBit().Value)
		return data, nil
	case types.KindMysqlHex:
		data.SetInt64(data.GetMysqlHex().Value)
		return data, nil
	default:
		return data, nil
	}
}

// DecodeValues decodes a byte slice into datums with column types.
func DecodeValues(data []byte, fts []*types.FieldType, inIndex bool) ([]types.Datum, error) {
	if len(data) == 0 {
		return nil, nil
	}
	values, err := codec.Decode(data, len(fts))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(values) > len(fts) {
		return nil, errInvalidColumnCount.Gen("invalid column count %d is less than value count %d", len(fts), len(values))
	}

	for i := range values {
		values[i], err = Unflatten(values[i], fts[i], inIndex)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return values, nil
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	colDatum, err := Unflatten(d, ft, false)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return colDatum, nil
}

// DecodeRow decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRow(b []byte, cols map[int64]*types.FieldType) (map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return nil, nil
	}
	row := make(map[int64]types.Datum, len(cols))
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = Unflatten(v, ft, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[id] = v
			cnt++
			if cnt == len(cols) {
				// Get enough data.
				break
			}
		}
	}
	return row, nil
}

// CutRowNew cuts encoded row into byte slices and return columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}

	var (
		cnt int
		b   []byte
		err error
	)
	row := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		offset, ok := colIDs[id]
		if ok {
			row[offset] = b
			cnt++
		}
	}
	return row, nil
}

// CutRow cuts encoded row into byte slices and return interested columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRow(data []byte, cols map[int64]*types.FieldType) (map[int64][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}
	row := make(map[int64][]byte, len(cols))
	cnt := 0
	var (
		b   []byte
		err error
	)
	for len(data) > 0 && cnt < len(cols) {
		// Get col id.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		_, ok := cols[id]
		if ok {
			row[id] = b
			cnt++
		}
	}
	return row, nil
}

// Unflatten converts a raw datum to a column datum.
func Unflatten(datum types.Datum, ft *types.FieldType, inIndex bool) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.Tp {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar,
		mysql.TypeString:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = ft.Tp
		t.Fsp = ft.Decimal
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration:
		dur := types.Duration{Duration: time.Duration(datum.GetInt64())}
		datum.SetValue(dur)
		return datum, nil
	case mysql.TypeEnum:
		enum, err := types.ParseEnumValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(enum)
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(set)
		return datum, nil
	case mysql.TypeBit:
		bit := types.Bit{Value: datum.GetUint64(), Width: ft.Flen}
		datum.SetValue(bit)
		return datum, nil
	}
	return datum, nil
}

// EncodeIndexSeekKey encodes an index value to kv.Key.
func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	key := make([]byte, 0, prefixLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// DecodeIndexKey decodes datums from an index key.
func DecodeIndexKey(key kv.Key) ([]types.Datum, error) {
	b := key[prefixLen+idLen:]
	return codec.Decode(b, 1)
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte)
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes table prefix with table ID.
func EncodeTablePrefix(tableID int64) kv.Key {
	var key kv.Key
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	if len(key) > recordRowKeyLen {
		return key[:recordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	tableStartKey := EncodeRowKeyWithHandle(tableID, math.MinInt64)
	tableEndKey := EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	startKey = codec.EncodeBytes(nil, tableStartKey)
	endKey = codec.EncodeBytes(nil, tableEndKey)
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	start := EncodeIndexSeekKey(tableID, indexID, nil)
	end := EncodeIndexSeekKey(tableID, indexID, []byte{255})
	startKey = codec.EncodeBytes(nil, start)
	endKey = codec.EncodeBytes(nil, end)
	return
}

type keyRangeSorter struct {
	ranges []kv.KeyRange
}

func (r *keyRangeSorter) Len() int {
	return len(r.ranges)
}

func (r *keyRangeSorter) Less(i, j int) bool {
	a := r.ranges[i]
	b := r.ranges[j]
	cmp := bytes.Compare(a.StartKey, b.StartKey)
	return cmp < 0
}

func (r *keyRangeSorter) Swap(i, j int) {
	r.ranges[i], r.ranges[j] = r.ranges[j], r.ranges[i]
}

const (
	codeInvalidRecordKey   = 4
	codeInvalidColumnCount = 5
	codeInvalidKey         = 6
)
