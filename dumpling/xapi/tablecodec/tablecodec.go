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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
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

// EncodeColumnKey encodes the table id, row handle and columnID into a kv.Key
func EncodeColumnKey(tableID int64, handle int64, columnID int64) kv.Key {
	buf := make([]byte, 0, recordRowKeyLen+idLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = codec.EncodeInt(buf, handle)
	buf = codec.EncodeInt(buf, columnID)
	return buf
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (handle int64, err error) {
	k := key
	if !key.HasPrefix(tablePrefix) {
		return 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(tablePrefix):]
	// Table ID is not needed.
	key, _, err = codec.DecodeInt(key)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !key.HasPrefix(recordPrefixSep) {
		return 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(recordPrefixSep):]

	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return
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
		return nil, errors.Errorf("invalid column count %d is less than value count %d", len(fts), len(values))
	}
	if inIndex {
		// We don't need to unflatten index columns for now.
		return values, nil
	}

	for i := range values {
		values[i], err = unflatten(values[i], fts[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return values, nil
}

// unflatten converts a raw datum to a column datum.
func unflatten(datum types.Datum, ft *types.FieldType) (types.Datum, error) {
	if datum.Kind() == types.KindNull {
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
		err := t.Unmarshal(datum.GetBytes())
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
			return datum, nil
		}
		dec, err := mysql.ParseDecimal(datum.GetString())
		if err != nil {
			return datum, errors.Trace(err)
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

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, col *tipb.ColumnInfo) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	ft := &types.FieldType{
		Tp:      byte(col.GetTp()),
		Flen:    int(col.GetColumnLen()),
		Decimal: int(col.GetDecimal()),
		Elems:   col.Elems,
		Collate: mysql.Collations[uint8(col.GetCollation())],
	}
	colDatum, err := unflatten(d, ft)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return colDatum, nil
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

func columnToProto(c *model.ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  proto.Int64(c.ID),
		Collation: proto.Int32(collationToProto(c.FieldType.Collate)),
		ColumnLen: proto.Int32(int32(c.FieldType.Flen)),
		Decimal:   proto.Int32(int32(c.FieldType.Decimal)),
		Flag:      proto.Int32(int32(c.Flag)),
		Elems:     c.Elems,
	}
	t := int32(c.FieldType.Tp)
	pc.Tp = &t
	return pc
}

func collationToProto(c string) int32 {
	v, ok := mysql.CollationNames[c]
	if ok {
		return int32(v)
	}
	return int32(mysql.DefaultCollationID)
}

// ColumnsToProto converts a slice of model.ColumnInfo to a slice of tipb.ColumnInfo.
func ColumnsToProto(columns []*model.ColumnInfo, pkIsHandle bool) []*tipb.ColumnInfo {
	cols := make([]*tipb.ColumnInfo, 0, len(columns))
	for _, c := range columns {
		col := columnToProto(c)
		if pkIsHandle && mysql.HasPriKeyFlag(c.Flag) {
			col.PkHandle = proto.Bool(true)
		} else {
			col.PkHandle = proto.Bool(false)
		}
		cols = append(cols, col)
	}
	return cols
}

// ProtoColumnsToFieldTypes converts tipb column info slice to FieldTyps slice.
func ProtoColumnsToFieldTypes(pColumns []*tipb.ColumnInfo) []*types.FieldType {
	fields := make([]*types.FieldType, len(pColumns))
	for i, v := range pColumns {
		field := new(types.FieldType)
		field.Tp = byte(v.GetTp())
		field.Collate = mysql.Collations[byte(v.GetCollation())]
		field.Decimal = int(v.GetDecimal())
		field.Flen = int(v.GetColumnLen())
		field.Flag = uint(v.GetFlag())
		field.Elems = v.GetElems()
		fields[i] = field
	}
	return fields
}

// IndexToProto converts a model.IndexInfo to a tipb.IndexInfo.
func IndexToProto(t *model.TableInfo, idx *model.IndexInfo) *tipb.IndexInfo {
	pi := &tipb.IndexInfo{
		TableId: proto.Int64(t.ID),
		IndexId: proto.Int64(idx.ID),
		Unique:  proto.Bool(idx.Unique),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns))
	for _, c := range idx.Columns {
		cols = append(cols, columnToProto(t.Columns[c.Offset]))
	}
	pi.Columns = cols
	return pi
}

// EncodeTableRanges encodes table ranges into kv.KeyRanges.
func EncodeTableRanges(tid int64, rans []*tipb.KeyRange) []kv.KeyRange {
	keyRanges := make([]kv.KeyRange, 0, len(rans))
	for _, r := range rans {
		start := EncodeRowKey(tid, r.Low)
		end := EncodeRowKey(tid, r.High)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	return keyRanges
}

// EncodeIndexRanges encodes index ranges into kv.KeyRanges.
func EncodeIndexRanges(tid, idxID int64, rans []*tipb.KeyRange) []kv.KeyRange {
	keyRanges := make([]kv.KeyRange, 0, len(rans))
	for _, r := range rans {
		// Convert range to kv.KeyRange
		start := EncodeIndexSeekKey(tid, idxID, r.Low)
		end := EncodeIndexSeekKey(tid, idxID, r.High)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	return keyRanges
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
