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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablecodec

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/tikv/client-go/v2/tikv"
)

func TablePrefix() []byte {
	return tablePrefix
}

// MetaPrefix returns meta prefix 'm'.
func MetaPrefix() []byte {
	return metaPrefix
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	buf := make([]byte, 0, prefixLen+len(encodedHandle))
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle kv.Handle) kv.Key {
	return EncodeRowKey(tableID, handle.Encoded())
}

// CutRowKeyPrefix cuts the row key prefix.
func CutRowKeyPrefix(key kv.Key) []byte {
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h kv.Handle) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+h.Len())
	if ph, ok := h.(kv.PartitionHandle); ok {
		recordPrefix = GenTableRecordPrefix(ph.PartitionID)
	}
	buf = append(buf, recordPrefix...)
	buf = append(buf, h.Encoded()...)
	return buf
}

func hasTablePrefix(key kv.Key) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefixSep(key kv.Key) bool {
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle kv.Handle, err error) {
	if len(key) <= prefixLen {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}

	k := key
	if !hasTablePrefix(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[tablePrefixLength:]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	if !hasRecordPrefixSep(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[recordPrefixSepLength:]
	if len(key) == 8 {
		var intHandle int64
		key, intHandle, err = codec.DecodeInt(key)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		return tableID, kv.IntHandle(intHandle), nil
	}
	h, err := kv.NewCommonHandle(key)
	if err != nil {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q %v", k, err)
	}
	return tableID, h, nil
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
	k := key

	tableID, indexID, isRecord, err := DecodeKeyHead(key)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	if isRecord {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
		return 0, 0, nil, err
	}
	indexKey := key[prefixLen+idLen:]
	indexValues, err = DecodeValuesBytesToStrings(indexKey)
	if err != nil {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, err)
		return 0, 0, nil, err
	}
	return tableID, indexID, indexValues, nil
}

// DecodeValuesBytesToStrings decode the raw bytes to strings for each columns.
// FIXME: Without the schema information, we can only decode the raw kind of
// the column. For instance, MysqlTime is internally saved as uint64.
func DecodeValuesBytesToStrings(b []byte) ([]string, error) {
	var datumValues []string
	for len(b) > 0 {
		remain, d, e := codec.DecodeOne(b)
		if e != nil {
			return nil, e
		}
		str, e1 := d.ToString()
		if e1 != nil {
			return nil, e
		}
		datumValues = append(datumValues, str)
		b = remain
	}
	return datumValues, nil
}

// EncodeMetaKey encodes the key and field into meta key.
func EncodeMetaKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8+codec.EncodedBytesLength(len(field)))
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	ek = codec.EncodeBytes(ek, field)
	return ek
}

// EncodeMetaKeyPrefix encodes the key prefix into meta key
func EncodeMetaKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	return ek
}

// DecodeMetaKey decodes the key and get the meta key and meta field.
func DecodeMetaKey(ek kv.Key) (key []byte, field []byte, err error) {
	var tp uint64
	if !bytes.HasPrefix(ek, metaPrefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}
	ek = ek[metaPrefixLength:]
	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if structure.TypeFlag(tp) != structure.HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
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
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
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

// DecodeIndexID decodes indexID from the key.
// this method simply extract index id part, and no other checking.
// Caller should make sure the key is an index key.
func DecodeIndexID(key kv.Key) (int64, error) {
	key = key[len(tablePrefix)+8+len(indexPrefixSep):]

	_, indexID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return indexID, nil
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		// If the key is in API V2, then ignore the prefix
		_, k, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
		if err != nil {
			terror.Log(errors.Trace(err))
			return 0
		}
		key = k
		if !key.HasPrefix(tablePrefix) {
			return 0
		}
	}
	key = key[len(tablePrefix):]
	_, tableID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (kv.Handle, error) {
	// In the read path, remove the keyspace prefix
	// to ensure compatibility with the key parsing implemented in the mock.
	tempKey := rowcodec.RemoveKeyspacePrefix(key)

	if len(tempKey) < RecordRowKeyLen || !hasTablePrefix(tempKey) || !hasRecordPrefixSep(tempKey[prefixLen-2:]) {
		return kv.IntHandle(0), errInvalidKey.GenWithStack("invalid key - %q", tempKey)
	}
	if len(tempKey) == RecordRowKeyLen {
		u := binary.BigEndian.Uint64(tempKey[prefixLen:])
		return kv.IntHandle(codec.DecodeCmpUintToInt(u)), nil
	}
	return kv.NewCommonHandle(tempKey[prefixLen:])
}

// EncodeValue encodes a go value to bytes.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeValue(loc *time.Location, b []byte, raw types.Datum) ([]byte, error) {
	var v types.Datum
	err := flatten(loc, raw, &v)
	if err != nil {
		return nil, err
	}

	val, err := codec.EncodeValue(loc, b, v)

	return val, err
}

// EncodeRow encode row data and column ids into a slice of byte.
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum, checksum rowcodec.Checksum, e *rowcodec.Encoder) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if e.Enable {
		valBuf = valBuf[:0]
		return e.Encode(loc, colIDs, row, checksum, valBuf)
	}
	return EncodeOldRow(loc, row, colIDs, valBuf, values)
}

// EncodeOldRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeOldRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeOldRow will allocate it.
func EncodeOldRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Datum, len(row)*2)
	}
	for i, c := range row {
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(loc, c, &values[2*i+1])
		if err != nil {
			return valBuf, errors.Trace(err)
		}
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return append(valBuf, codec.NilFlag), nil
	}
	return codec.EncodeValue(loc, valBuf, values...)
}

func flatten(loc *time.Location, data types.Datum, ret *types.Datum) error {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && loc != nil && loc != time.UTC {
			err := t.ConvertTimeZone(loc, time.UTC)
			if err != nil {
				return errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.KindMysqlDuration:
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(types.StrictContext)
		if err != nil {
			return errors.Trace(err)
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	colDatum, err := Unflatten(d, ft, loc)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return colDatum, nil
}

// DecodeColumnValueWithDatum decodes data to an existing Datum according to the column info.
func DecodeColumnValueWithDatum(data []byte, ft *types.FieldType, loc *time.Location, result *types.Datum) error {
	var err error
	_, *result, err = codec.DecodeOne(data)
	if err != nil {
		return errors.Trace(err)
	}
	*result, err = Unflatten(*result, ft, loc)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DecodeRowWithMapNew decode a row to datum map.
func DecodeRowWithMapNew(b []byte, cols map[int64]*types.FieldType,
	loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}

	reqCols := make([]rowcodec.ColInfo, len(cols))
	var idx int
	for id, tp := range cols {
		reqCols[idx] = rowcodec.ColInfo{
			ID: id,
			Ft: tp,
		}
		idx++
	}
	rd := rowcodec.NewDatumMapDecoder(reqCols, loc)
	return rd.DecodeToDatumMap(b, row)
}

// DecodeRowWithMap decodes a byte slice into datums with an existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
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
			v, err = Unflatten(v, ft, loc)
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

// DecodeRowToDatumMap decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
// Default value columns, generated columns and handle columns are unprocessed.
func DecodeRowToDatumMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, error) {
	if !rowcodec.IsNewFormat(b) {
		return DecodeRowWithMap(b, cols, loc, nil)
	}
	return DecodeRowWithMapNew(b, cols, loc, nil)
}

// DecodeHandleToDatumMap decodes a handle into datum map.
func DecodeHandleToDatumMap(handle kv.Handle, handleColIDs []int64,
	cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if handle == nil || len(handleColIDs) == 0 {
		return row, nil
	}
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	for idx, id := range handleColIDs {
		ft, ok := cols[id]
		if !ok {
			continue
		}
		if types.NeedRestoredData(ft) {
			continue
		}
		d, err := decodeHandleToDatum(handle, ft, idx)
		if err != nil {
			return row, err
		}
		d, err = Unflatten(d, ft, loc)
		if err != nil {
			return row, err
		}
		if _, exists := row[id]; !exists {
			row[id] = d
		}
	}
	return row, nil
}

// decodeHandleToDatum decodes a handle to a specific column datum.
func decodeHandleToDatum(handle kv.Handle, ft *types.FieldType, idx int) (types.Datum, error) {
	var d types.Datum
	var err error
	if handle.IsInt() {
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			d = types.NewUintDatum(uint64(handle.IntValue()))
		} else {
			d = types.NewIntDatum(handle.IntValue())
		}
		return d, nil
	}
	// Decode common handle to Datum.
	_, d, err = codec.DecodeOne(handle.EncodedCol(idx))
	return d, err
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
		cid int64
	)
	row := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		data, cid, err = codec.CutColumnID(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		offset, ok := colIDs[cid]
		if ok {
			row[offset] = b
			cnt++
		}
	}
	return row, nil
}

// UnflattenDatums converts raw datums to column datums.
func UnflattenDatums(datums []types.Datum, fts []*types.FieldType, loc *time.Location) ([]types.Datum, error) {
	for i, datum := range datums {
		ft := fts[i]
		uDatum, err := Unflatten(datum, ft, loc)
		if err != nil {
			return datums, errors.Trace(err)
		}
		datums[i] = uDatum
	}
	return datums, nil
}

// Unflatten converts a raw datum to a column datum.
func Unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.GetType() {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		datum.SetString(datum.GetString(), ft.GetCollate())
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.GetType(), ft.GetDecimal())
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.GetType() == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return datum, errors.Trace(err)
			}
		}
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration: // duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: ft.GetDecimal()}
		datum.SetMysqlDuration(dur)
		return datum, nil
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		datum.SetMysqlEnum(enum, ft.GetCollate())
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlSet(set, ft.GetCollate())
		return datum, nil
	case mysql.TypeBit:
		val := datum.GetUint64()
		byteSize := (ft.GetFlen() + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return datum, nil
}
