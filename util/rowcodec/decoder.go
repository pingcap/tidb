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
// See the License for the specific language governing permissions and
// limitations under the License.

package rowcodec

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// decoder contains base util for decode row.
type decoder struct {
	row
	columns     []ColInfo
	handleColID int64
	loc         *time.Location
}

// NewDecoder creates a decoder.
func NewDecoder(columns []ColInfo, handleColID int64, loc *time.Location) *decoder {
	return &decoder{
		columns:     columns,
		handleColID: handleColID,
		loc:         loc,
	}
}

// ColInfo is used as column meta info for row decoder.
type ColInfo struct {
	ID         int64
	Tp         int32
	Flag       int32
	IsPKHandle bool

	Flen    int
	Decimal int
	Elems   []string
}

// DatumMapDecoder decodes the row to datum map.
type DatumMapDecoder struct {
	decoder
}

// NewDatumMapDecoder creates a DatumMapDecoder.
func NewDatumMapDecoder(columns []ColInfo, handleColID int64, loc *time.Location) *DatumMapDecoder {
	return &DatumMapDecoder{decoder{
		columns:     columns,
		handleColID: handleColID,
		loc:         loc,
	}}
}

// DecodeToDatumMap decodes byte slices to datum map.
func (decoder *DatumMapDecoder) DecodeToDatumMap(rowData []byte, handle int64, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(decoder.columns))
	}
	err := decoder.fromBytes(rowData)
	if err != nil {
		return nil, err
	}
	for _, col := range decoder.columns {
		if col.ID == decoder.handleColID {
			row[col.ID] = types.NewIntDatum(handle)
			continue
		}
		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			d, err := decoder.decodeColDatum(&col, colData)
			if err != nil {
				return nil, err
			}
			row[col.ID] = d
			continue
		}

		if isNil {
			var d types.Datum
			d.SetNull()
			row[col.ID] = d
			continue
		}
	}
	return row, nil
}

func (decoder *DatumMapDecoder) decodeColDatum(col *ColInfo, colData []byte) (types.Datum, error) {
	var d types.Datum
	switch byte(col.Tp) {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			d.SetUint64(decodeUint(colData))
		} else {
			d.SetInt64(decodeInt(colData))
		}
	case mysql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return d, err
		}
		d.SetFloat32(float32(fVal))
	case mysql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return d, err
		}
		d.SetFloat64(fVal)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetBytes(colData)
	case mysql.TypeNewDecimal:
		_, dec, _, _, err := codec.DecodeDecimal(colData)
		if err != nil {
			return d, err
		}
		d.SetMysqlDecimal(dec)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = uint8(col.Tp)
		t.Fsp = int8(col.Decimal)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return d, err
		}
		if byte(col.Tp) == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return d, err
			}
		}
		d.SetMysqlTime(t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = int8(col.Decimal)
		d.SetMysqlDuration(dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Elems, decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		d.SetMysqlEnum(enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Elems, decodeUint(colData))
		if err != nil {
			return d, err
		}
		d.SetMysqlSet(set)
	case mysql.TypeBit:
		byteSize := (col.Flen + 7) >> 3
		d.SetMysqlBit(types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		d.SetMysqlJSON(j)
	default:
		return d, errors.Errorf("unknown type %d", col.Tp)
	}
	return d, nil
}

// ChunkDecoder decodes the row to chunk.Chunk.
type ChunkDecoder struct {
	decoder
	defDatum func(i int) (types.Datum, error)
}

// NewChunkDecoder creates a NewChunkDecoder.
func NewChunkDecoder(columns []ColInfo, handleColID int64, defDatum func(i int) (types.Datum, error), loc *time.Location) *ChunkDecoder {
	return &ChunkDecoder{
		decoder: decoder{
			columns:     columns,
			handleColID: handleColID,
			loc:         loc,
		},
		defDatum: defDatum,
	}
}

// DecodeToChunk decodes a row to chunk.
func (decoder *ChunkDecoder) DecodeToChunk(rowData []byte, handle int64, chk *chunk.Chunk) error {
	err := decoder.fromBytes(rowData)
	if err != nil {
		return err
	}

	for colIdx, col := range decoder.columns {
		if col.ID == decoder.handleColID {
			chk.AppendInt64(colIdx, handle)
			continue
		}

		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			err := decoder.decodeColToChunk(colIdx, &col, colData, chk)
			if err != nil {
				return err
			}
			continue
		}

		if isNil {
			chk.AppendNull(colIdx)
			continue
		}

		if decoder.defDatum == nil {
			chk.AppendNull(colIdx)
			continue
		}

		defDatum, err := decoder.defDatum(colIdx)
		if err != nil {
			return err
		}

		chk.AppendDatum(colIdx, &defDatum)
	}
	return nil
}

func (decoder *ChunkDecoder) decodeColToChunk(colIdx int, col *ColInfo, colData []byte, chk *chunk.Chunk) error {
	switch byte(col.Tp) {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			chk.AppendUint64(colIdx, decodeUint(colData))
		} else {
			chk.AppendInt64(colIdx, decodeInt(colData))
		}
	case mysql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat32(colIdx, float32(fVal))
	case mysql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat64(colIdx, fVal)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(colIdx, colData)
	case mysql.TypeNewDecimal:
		_, dec, _, _, err := codec.DecodeDecimal(colData)
		if err != nil {
			return err
		}
		chk.AppendMyDecimal(colIdx, dec)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = uint8(col.Tp)
		t.Fsp = int8(col.Decimal)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return err
		}
		if byte(col.Tp) == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return err
			}
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = int8(col.Decimal)
		chk.AppendDuration(colIdx, dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Elems, decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Elems, decodeUint(colData))
		if err != nil {
			return err
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (col.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		chk.AppendJSON(colIdx, j)
	default:
		return errors.Errorf("unknown type %d", col.Tp)
	}
	return nil
}

// BytesDecoder decodes the row to old datums bytes.
type BytesDecoder struct {
	decoder
	defBytes func(i int) ([]byte, error)
}

// NewByteDecoder creates a BytesDecoder.
// defBytes: provided default value bytes in old datum format(flag+colData).
func NewByteDecoder(columns []ColInfo, handleColID int64, defBytes func(i int) ([]byte, error), loc *time.Location) *BytesDecoder {
	return &BytesDecoder{
		decoder: decoder{
			columns:     columns,
			handleColID: handleColID,
			loc:         loc,
		},
		defBytes: defBytes,
	}
}

// DecodeToBytes decodes raw byte slice to row data.
func (decoder *BytesDecoder) DecodeToBytes(outputOffset map[int64]int, handle int64, value []byte, cacheBytes []byte) ([][]byte, error) {
	var r row
	err := r.fromBytes(value)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(outputOffset))
	for i, col := range decoder.columns {
		tp := fieldType2Flag(byte(col.Tp), uint(col.Flag)&mysql.UnsignedFlag == 0)
		colID := col.ID
		offset := outputOffset[colID]
		if col.IsPKHandle || colID == model.ExtraHandleID {
			handleData := cacheBytes
			if mysql.HasUnsignedFlag(uint(col.Flag)) {
				handleData = append(handleData, UintFlag)
				handleData = codec.EncodeUint(handleData, uint64(handle))
			} else {
				handleData = append(handleData, IntFlag)
				handleData = codec.EncodeInt(handleData, handle)
			}
			values[offset] = handleData
			continue
		}

		idx, isNil, notFound := r.findColID(colID)
		if !notFound && !isNil {
			val := r.getData(idx)
			values[offset] = decoder.encodeOldDatum(tp, val)
			continue
		}

		if isNil {
			values[offset] = []byte{NilFlag}
			continue
		}

		if decoder.defBytes != nil {
			defVal, err := decoder.defBytes(i)
			if err != nil {
				return nil, err
			}
			if len(defVal) > 0 {
				values[offset] = defVal
				continue
			}
		}

		values[offset] = []byte{NilFlag}
	}
	return values, nil
}

func (decoder *BytesDecoder) encodeOldDatum(tp byte, val []byte) []byte {
	var buf []byte
	switch tp {
	case BytesFlag:
		buf = append(buf, CompactBytesFlag)
		buf = codec.EncodeCompactBytes(buf, val)
	case IntFlag:
		buf = append(buf, VarintFlag)
		buf = codec.EncodeVarint(buf, decodeInt(val))
	case UintFlag:
		buf = append(buf, VaruintFlag)
		buf = codec.EncodeUvarint(buf, decodeUint(val))
	default:
		buf = append(buf, tp)
		buf = append(buf, val...)
	}
	return buf
}

// fieldType2Flag transforms field type into kv type flag.
func fieldType2Flag(tp byte, signed bool) (flag byte) {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if signed {
			flag = IntFlag
		} else {
			flag = UintFlag
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		flag = FloatFlag
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		flag = BytesFlag
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp:
		flag = UintFlag
	case mysql.TypeDuration:
		flag = IntFlag
	case mysql.TypeNewDecimal:
		flag = DecimalFlag
	case mysql.TypeYear:
		flag = IntFlag
	case mysql.TypeEnum, mysql.TypeBit, mysql.TypeSet:
		flag = UintFlag
	case mysql.TypeJSON:
		flag = JSONFlag
	case mysql.TypeNull:
		flag = NilFlag
	default:
		panic(fmt.Sprintf("unknown field type %d", tp))
	}
	return
}
