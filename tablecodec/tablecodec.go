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
	"encoding/binary"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var (
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
	buf = EncodeRecordKey(buf, handle)
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

// EncodeRowPR encode row data and column ids into a slice of byte.
// Row layout: data_offset | id1, len1, id2, len2 | data1, data2
func EncodeRowPR(row []types.Datum, colIDs []int64) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if len(row) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}

	var err error
	data := make([]byte, 0, 256)
	meta := make([]byte, 4, 256)
	for i, c := range row {
		length := len(data)
		data, err = codec.EncodeOne(data, c, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		length = len(data) - length

		meta = codec.EncodeVarint(meta, colIDs[i])
		meta = codec.EncodeVarint(meta, int64(length))
	}
	// data offset
	binary.BigEndian.PutUint32(meta[:], uint32(len(meta)))
	return append(meta, data...), nil
}

func flatten(data types.Datum) (types.Datum, error) {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		return types.NewUintDatum(data.GetMysqlTime().ToPackedUint()), nil
	case types.KindMysqlDuration:
		// for mysql time type
		data.SetInt64(int64(data.GetMysqlDuration().Duration))
		return data, nil
	case types.KindMysqlDecimal:
		data.SetString(data.GetMysqlDecimal().String())
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
	if data == nil {
		return nil, nil
	}
	values, err := codec.Decode(data)
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

// decodeRow decodes a row, store the result in map datum or cut.
// this code is shared by DecodeRow and CutRow, which have the same logic.
// when it's called by DecodeRow, datum is not nil and cur is nil.
// when it's called by CutRow, datum is nil and cur is not nil.
func decodeRow(b []byte, cols map[int64]*types.FieldType,
	datum map[int64]types.Datum, cut map[int64][]byte) error {
	dataOffset := binary.BigEndian.Uint32(b)
	meta := b[4:dataOffset]
	pos := int64(dataOffset)
	var (
		id     int64
		err    error
		length int64
		d      types.Datum
		cnt    int
	)

	for len(meta) > 0 && cnt < len(cols) {
		// Get col id.
		if meta, id, err = codec.DecodeVarint(meta); err != nil {
			return errors.Trace(err)
		}
		// Get col len.
		if meta, length, err = codec.DecodeVarint(meta); err != nil {
			return errors.Trace(err)
		}

		if ft, ok := cols[id]; ok {
			cnt++
			if cut != nil {
				cut[id] = b[pos : pos+length]
			} else {
				if _, d, err = codec.DecodeOne(b[pos:]); err != nil {
					return errors.Trace(err)
				}
				if d, err = Unflatten(d, ft, false); err != nil {
					return errors.Trace(err)
				}
				datum[id] = d
			}
		}

		pos += length
	}
	return nil
}

// DecodeRowPR decodes a byte slice into datums.
// Row layout: data_offset | id1, len1, id2, len2 | data1, data2
func DecodeRowPR(b []byte, cols map[int64]*types.FieldType) (map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return nil, nil
	}

	ret := make(map[int64]types.Datum, len(cols))
	err := decodeRow(b, cols, ret, nil)
	return ret, err
}

// CutRowPR cut encoded row into byte slices and return interested columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowPR(data []byte, cols map[int64]*types.FieldType) (map[int64][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}

	ret := make(map[int64][]byte, len(cols))
	err := decodeRow(data, cols, nil, ret)
	return ret, err
}

// CutRow cut encoded row into byte slices and return interested columns' byte slice.
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
		var t mysql.Time
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
		dur := mysql.Duration{Duration: time.Duration(datum.GetInt64())}
		datum.SetValue(dur)
		return datum, nil
	case mysql.TypeNewDecimal:
		if datum.Kind() == types.KindMysqlDecimal {
			if ft.Decimal >= 0 {
				dec := datum.GetMysqlDecimal().Truncate(int32(ft.Decimal))
				datum.SetMysqlDecimal(dec)
			}
			return datum, nil
		}
		dec, err := mysql.ParseDecimal(datum.GetString())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.Decimal >= 0 {
			dec = dec.Truncate(int32(ft.Decimal))
		}
		datum.SetValue(dec)
		return datum, nil
	case mysql.TypeEnum:
		enum, err := mysql.ParseEnumValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(enum)
		return datum, nil
	case mysql.TypeSet:
		set, err := mysql.ParseSetValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(set)
		return datum, nil
	case mysql.TypeBit:
		bit := mysql.Bit{Value: datum.GetUint64(), Width: ft.Flen}
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
	return codec.Decode(b)
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

// Record prefix is "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// Index prefix is "t[tableID]_i".
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
)

// EncodeRow3 encode row data and column ids into a slice of byte.
// Row layout: data_offset | id1, len1, id2, len2 | data1, data2
func EncodeRow3(row []types.Datum, colIDs []int64) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if len(row) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}

	var err error
	data := make([]byte, 0, 256)
	meta := make([]byte, 4, 256)
	for i, c := range row {
		length := len(data)
		data, err = codec.EncodeOne(data, c, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		length = len(data) - length
		meta = codec.EncodeVarint(meta, colIDs[i])
		meta = codec.EncodeVarint(meta, int64(length))
	}
	// data offset
	binary.BigEndian.PutUint32(meta[:], uint32(len(meta)))
	return append(meta, data...), nil
}

// DecodeRow3 decodes a byte slice into datums.
func DecodeRow3(b []byte, cols map[int64]*types.FieldType) (map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return nil, nil
	}

	dataOffset := binary.BigEndian.Uint32(b)
	pos := int64(dataOffset)
	meta := b[4:dataOffset]
	ret := make(map[int64]types.Datum, len(cols))
	var (
		id     int64
		err    error
		length int64
		d      types.Datum
	)
	for len(meta) > 0 && len(ret) < len(cols) {
		meta, id, err = codec.DecodeVarint(meta)
		if err != nil {
			return nil, errors.Trace(err)
		}
		meta, length, err = codec.DecodeVarint(meta)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if ft, ok := cols[id]; ok {
			_, d, err = codec.DecodeOne(b[pos:])
			if err != nil {
				return nil, errors.Trace(err)
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
			d, err = Unflatten(d, ft, false)
			ret[id] = d
		}

		pos += length
	}
	return ret, nil
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

// EncodeRow1 encode row data and column ids into a slice of byte.
// Row layout: meta_offset | data1, data2, data3... | meta
// meta: length | colID1, offset | colID2, offset | ...
func EncodeRow1(row []types.Datum, colIDs []int64) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}

	if len(row) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}

	meta := make([]int64, 2*len(colIDs)+1)
	meta[0] = int64(len(row))
	b := make([]byte, 4, 4+len(row)*4)
	for i, c := range row {
		fc, err := flatten(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offset := int64(len(b))
		meta[1+2*i] = colIDs[i]
		meta[2+2*i] = offset
		b, err = codec.EncodeOne(b, fc, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	metaOffset := uint32(len(b))

	for _, v := range meta {
		b = codec.EncodeVarint(b, v)
	}
	binary.BigEndian.PutUint32(b[0:], metaOffset)
	return b, nil
}

// EncodeRow2 encode row data and column ids into a slice of byte.
// Row layout: colID1, len1, value1, colID2, len2, value2, .....
func EncodeRow2(row []types.Datum, colIDs []int64) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if len(row) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}

	buf := make([]byte, 0, 64)

	var b []byte
	for i, c := range row {
		fc, err := flatten(c)
		if err != nil {
			return nil, errors.Trace(err)
		}

		x, err := codec.EncodeOne(buf, fc, false)
		if err != nil {
			return nil, errors.Trace(err)
		}

		b = codec.EncodeVarint(b, colIDs[i])
		b = codec.EncodeVarint(b, int64(len(x)))
		b = append(b, x...)
	}
	return b, nil
}

func decodeMeta(b []byte) ([]int64, error) {
	offset := binary.BigEndian.Uint32(b)
	data, count, err := codec.DecodeVarint(b[offset:])
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta := make([]int64, 2*count)
	for i := 0; i < int(2*count); i++ {
		data, meta[i], err = codec.DecodeVarint(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return meta, nil
}

// DecodeRow1 decodes a byte slice into datums.
// Row layout: meta_offset | data1, data2, data3... | meta
// meta: length | colID1, offset | colID2, offset | ...
func DecodeRow1(b []byte, cols map[int64]*types.FieldType) (map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return nil, nil
	}
	if len(b) < 4 {
		return nil, errors.New("insufficient bytes to decode value")
	}

	meta, err := decodeMeta(b)
	if err != nil {
		return nil, errors.Trace(err)
	}
	count := len(meta) / 2

	ret := make(map[int64]types.Datum, len(cols))
	for i := 0; i < int(count); i++ {
		colID := meta[2*i]
		offset := meta[2*i+1]
		ft, ok := cols[colID]
		if !ok {
			continue
		}
		_, d, err := codec.DecodeOne(b[offset:])
		if err != nil {
			return nil, errors.Trace(err)
		}
		d, err = Unflatten(d, ft, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret[colID] = d
	}
	return ret, nil
}

// DecodeRow2 decodes a byte slice into datums.
func DecodeRow2(b []byte, cols map[int64]*types.FieldType) (map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return nil, nil
	}

	var (
		err    error
		d      types.Datum
		colID  int64
		cnt    int
		length int64
	)
	row := make(map[int64]types.Datum, len(cols))
	for len(b) > 0 && len(row) < len(cols) {
		b, colID, err = codec.DecodeVarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b, length, err = codec.DecodeVarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if ft, ok := cols[colID]; !ok {
			// skip value
			b = b[length:]
		} else {
			b, d, err = codec.DecodeOne(b)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d, err = Unflatten(d, ft, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[colID] = d
		}
		cnt++
	}
	return row, nil
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
		cid  types.Datum
	)
	for len(b) > 0 {
		// Get col id.
		b, cid, err = codec.DecodeOne(b)
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
