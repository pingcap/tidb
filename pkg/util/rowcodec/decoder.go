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

package rowcodec

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// decoder contains base util for decode row.
type decoder struct {
	row
	columns      []ColInfo
	handleColIDs []int64
	loc          *time.Location
}

// NewDecoder creates a decoder.
func NewDecoder(columns []ColInfo, handleColIDs []int64, loc *time.Location) *decoder {
	return &decoder{
		columns:      columns,
		handleColIDs: handleColIDs,
		loc:          loc,
	}
}

// ColInfo is used as column meta info for row decoder.
type ColInfo struct {
	ID            int64
	IsPKHandle    bool
	VirtualGenCol bool
	Ft            *types.FieldType
}

// DatumMapDecoder decodes the row to datum map.
type DatumMapDecoder struct {
	decoder
}

// NewDatumMapDecoder creates a DatumMapDecoder.
func NewDatumMapDecoder(columns []ColInfo, loc *time.Location) *DatumMapDecoder {
	return &DatumMapDecoder{decoder{
		columns: columns,
		loc:     loc,
	}}
}

// DecodeToDatumMap decodes byte slices to datum map.
func (decoder *DatumMapDecoder) DecodeToDatumMap(rowData []byte, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(decoder.columns))
	}
	err := decoder.fromBytes(rowData)
	if err != nil {
		return nil, err
	}
	for i := range decoder.columns {
		col := &decoder.columns[i]
		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			d, err := decoder.decodeColDatum(col, colData)
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
	switch col.Ft.GetType() {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(col.Ft.GetFlag()) {
			d.SetUint64(decodeUint(colData))
		} else {
			d.SetInt64(decodeInt(colData))
		}
	case mysql.TypeYear:
		d.SetInt64(decodeInt(colData))
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
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString(string(colData), col.Ft.GetCollate())
	case mysql.TypeNewDecimal:
		_, dec, precision, frac, err := codec.DecodeDecimal(colData)
		if err != nil {
			return d, err
		}
		d.SetMysqlDecimal(dec)
		d.SetLength(precision)
		d.SetFrac(frac)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.SetType(col.Ft.GetType())
		t.SetFsp(col.Ft.GetDecimal())
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return d, err
		}
		if col.Ft.GetType() == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return d, err
			}
		}
		d.SetMysqlTime(t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = col.Ft.GetDecimal()
		d.SetMysqlDuration(dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Ft.GetElems(), decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		d.SetMysqlEnum(enum, col.Ft.GetCollate())
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Ft.GetElems(), decodeUint(colData))
		if err != nil {
			return d, err
		}
		d.SetMysqlSet(set, col.Ft.GetCollate())
	case mysql.TypeBit:
		byteSize := (col.Ft.GetFlen() + 7) >> 3
		d.SetMysqlBit(types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j types.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		d.SetMysqlJSON(j)
	case mysql.TypeTiDBVectorFloat32:
		v, _, err := types.ZeroCopyDeserializeVectorFloat32(colData)
		if err != nil {
			return d, err
		}
		d.SetVectorFloat32(v)
	default:
		return d, errors.Errorf("unknown type %d", col.Ft.GetType())
	}
	return d, nil
}

// ChunkDecoder decodes the row to chunk.Chunk.
type ChunkDecoder struct {
	decoder
	defDatum func(i int, chk *chunk.Chunk) error

	compiledCols       []compiledCol
	compiledColsInited bool

	// colMapping caches the column index in not-null columns array for decoder.columns[i].
	// It can skip binary search for stable not-null columns layout. The cached index
	// is validated on each row and falls back to findColID when layout changes.
	// -1 means not cached and needs to call findColID for each row.
	colMapping     []int
	mappingInited  bool
	mappingRowCols int
}

type compiledColKind uint8

const (
	compiledColOther compiledColKind = iota
	compiledColInt
	compiledColUint
	compiledColFloat32
	compiledColFloat64
	compiledColBytes
	compiledColTime
	compiledColDuration
)

type compiledCol struct {
	kind          compiledColKind
	tp            byte
	fsp           int
	needTZConvert bool
}

// NewChunkDecoder creates a NewChunkDecoder.
func NewChunkDecoder(columns []ColInfo, handleColIDs []int64, defDatum func(i int, chk *chunk.Chunk) error, loc *time.Location) *ChunkDecoder {
	return &ChunkDecoder{
		decoder: decoder{
			columns:      columns,
			handleColIDs: handleColIDs,
			loc:          loc,
		},
		defDatum: defDatum,
	}
}

func (decoder *ChunkDecoder) initCompiledCols() {
	if decoder.compiledColsInited {
		return
	}
	if cap(decoder.compiledCols) < len(decoder.columns) {
		decoder.compiledCols = make([]compiledCol, len(decoder.columns))
	} else {
		decoder.compiledCols = decoder.compiledCols[:len(decoder.columns)]
	}
	for i := range decoder.columns {
		col := &decoder.columns[i]
		if col.Ft == nil {
			decoder.compiledCols[i] = compiledCol{kind: compiledColOther}
			continue
		}
		tp := col.Ft.GetType()
		cc := compiledCol{kind: compiledColOther, tp: tp}
		switch tp {
		case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
			if mysql.HasUnsignedFlag(col.Ft.GetFlag()) {
				cc.kind = compiledColUint
			} else {
				cc.kind = compiledColInt
			}
		case mysql.TypeYear:
			cc.kind = compiledColInt
		case mysql.TypeFloat:
			cc.kind = compiledColFloat32
		case mysql.TypeDouble:
			cc.kind = compiledColFloat64
		case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			cc.kind = compiledColBytes
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			cc.kind = compiledColTime
			cc.fsp = col.Ft.GetDecimal()
			cc.needTZConvert = tp == mysql.TypeTimestamp && decoder.loc != nil
		case mysql.TypeDuration:
			cc.kind = compiledColDuration
			cc.fsp = col.Ft.GetDecimal()
		}
		decoder.compiledCols[i] = cc
	}
	decoder.compiledColsInited = true
}

// DecodeToChunk decodes a row to chunk.
func (decoder *ChunkDecoder) DecodeToChunk(rowData []byte, handle kv.Handle, chk *chunk.Chunk) error {
	err := decoder.fromBytes(rowData)
	if err != nil {
		return err
	}

	if !decoder.compiledColsInited {
		decoder.initCompiledCols()
	}

	// Build (or rebuild) column mapping cache for stable schema.
	rowCols := int(decoder.row.numNotNullCols) + int(decoder.row.numNullCols)
	if !decoder.mappingInited || decoder.mappingRowCols != rowCols {
		decoder.buildColMapping()
		decoder.mappingInited = true
		decoder.mappingRowCols = rowCols
	}

	for colIdx := range decoder.columns {
		col := &decoder.columns[colIdx]
		// fill the virtual column value after row calculation
		if col.VirtualGenCol {
			chk.AppendNull(colIdx)
			continue
		}
		if col.ID == model.ExtraRowChecksumID {
			chk.AppendNull(colIdx)
			continue
		}

		mappedIdx := decoder.colMapping[colIdx]
		if mappedIdx >= 0 && decoder.matchNotNullColID(mappedIdx, col.ID) {
			colData := decoder.getData(mappedIdx)
			err := decoder.decodeColToChunk(colIdx, col, colData, chk)
			if err != nil {
				return err
			}
			continue
		}

		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			colData := decoder.getData(idx)
			err := decoder.decodeColToChunk(colIdx, col, colData, chk)
			if err != nil {
				return err
			}
			continue
		}

		// Only try to decode handle when there is no corresponding column in the value.
		// This is because the information in handle may be incomplete in some cases.
		// For example, prefixed clustered index like 'primary key(col1(1))' only store the leftmost 1 char in the handle.
		if decoder.tryAppendHandleColumn(colIdx, col, handle, chk) {
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

		err := decoder.defDatum(colIdx, chk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (decoder *ChunkDecoder) buildColMapping() {
	if cap(decoder.colMapping) < len(decoder.columns) {
		decoder.colMapping = make([]int, len(decoder.columns))
	} else {
		decoder.colMapping = decoder.colMapping[:len(decoder.columns)]
	}
	for i := range decoder.colMapping {
		decoder.colMapping[i] = -1
	}

	for i := range decoder.columns {
		col := &decoder.columns[i]
		if col.VirtualGenCol || col.ID == model.ExtraRowChecksumID {
			continue
		}
		// Only attempt to cache columns declared NOT NULL. Nullable columns can move between
		// not-null and null segments, and other columns' NULL changes may also shift indices.
		if col.Ft == nil || !mysql.HasNotNullFlag(col.Ft.GetFlag()) {
			continue
		}
		idx, isNil, notFound := decoder.row.findColID(col.ID)
		if !notFound && !isNil {
			decoder.colMapping[i] = idx
		}
	}
}

func (decoder *ChunkDecoder) matchNotNullColID(idx int, colID int64) bool {
	if idx < 0 || idx >= int(decoder.row.numNotNullCols) {
		return false
	}
	if decoder.row.large() {
		return int64(decoder.row.colIDs32[idx]) == colID
	}
	return int64(decoder.row.colIDs[idx]) == colID
}

func (decoder *ChunkDecoder) tryAppendHandleColumn(colIdx int, col *ColInfo, handle kv.Handle, chk *chunk.Chunk) bool {
	if handle == nil {
		return false
	}
	if handle.IsInt() && col.ID == decoder.handleColIDs[0] {
		chk.AppendInt64(colIdx, handle.IntValue())
		return true
	}
	for i, id := range decoder.handleColIDs {
		if col.ID == id {
			if types.NeedRestoredData(col.Ft) {
				return false
			}
			coder := codec.NewDecoder(chk, decoder.loc)
			_, err := coder.DecodeOne(handle.EncodedCol(i), colIdx, col.Ft)
			return err == nil
		}
	}
	return false
}

func (decoder *ChunkDecoder) decodeColToChunk(colIdx int, col *ColInfo, colData []byte, chk *chunk.Chunk) error {
	cc := decoder.compiledCols[colIdx]
	switch cc.kind {
	case compiledColInt:
		chk.AppendInt64(colIdx, decodeInt(colData))
		return nil
	case compiledColUint:
		chk.AppendUint64(colIdx, decodeUint(colData))
		return nil
	case compiledColFloat32:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat32(colIdx, float32(fVal))
		return nil
	case compiledColFloat64:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat64(colIdx, fVal)
		return nil
	case compiledColBytes:
		chk.AppendBytes(colIdx, colData)
		return nil
	case compiledColTime:
		var t types.Time
		t.SetType(cc.tp)
		t.SetFsp(cc.fsp)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return err
		}
		if cc.needTZConvert && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return err
			}
		}
		chk.AppendTime(colIdx, t)
		return nil
	case compiledColDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = cc.fsp
		chk.AppendDuration(colIdx, dur)
		return nil
	}

	switch cc.tp {
	case mysql.TypeNewDecimal:
		_, dec, _, frac, err := codec.DecodeDecimal(colData)
		if err != nil {
			return err
		}
		if col.Ft.GetDecimal() != types.UnspecifiedLength && frac > col.Ft.GetDecimal() {
			to := new(types.MyDecimal)
			err := dec.Round(to, col.Ft.GetDecimal(), types.ModeHalfUp)
			if err != nil {
				return errors.Trace(err)
			}
			dec = to
		}
		chk.AppendMyDecimal(colIdx, dec)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(col.Ft.GetElems(), decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(col.Ft.GetElems(), decodeUint(colData))
		if err != nil {
			return err
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (col.Ft.GetFlen() + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j types.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		chk.AppendJSON(colIdx, j)
	case mysql.TypeTiDBVectorFloat32:
		v, _, err := types.ZeroCopyDeserializeVectorFloat32(colData)
		if err != nil {
			return err
		}
		chk.AppendVectorFloat32(colIdx, v)
	default:
		return errors.Errorf("unknown type %d", cc.tp)
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
func NewByteDecoder(columns []ColInfo, handleColIDs []int64, defBytes func(i int) ([]byte, error), loc *time.Location) *BytesDecoder {
	return &BytesDecoder{
		decoder: decoder{
			columns:      columns,
			handleColIDs: handleColIDs,
			loc:          loc,
		},
		defBytes: defBytes,
	}
}

func (decoder *BytesDecoder) decodeToBytesInternal(outputOffset map[int64]int, handle kv.Handle, value []byte, cacheBytes []byte) ([][]byte, error) {
	var r row
	err := r.fromBytes(value)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(outputOffset))
	for i := range decoder.columns {
		col := &decoder.columns[i]
		tp := fieldType2Flag(col.Ft.ArrayType().GetType(), col.Ft.GetFlag()&mysql.UnsignedFlag == 0)
		colID := col.ID
		offset := outputOffset[colID]
		idx, isNil, notFound := r.findColID(colID)
		if !notFound && !isNil {
			val := r.getData(idx)
			values[offset] = decoder.encodeOldDatum(tp, val)
			continue
		}

		// Only try to decode handle when there is no corresponding column in the value.
		// This is because the information in handle may be incomplete in some cases.
		// For example, prefixed clustered index like 'primary key(col1(1))' only store the leftmost 1 char in the handle.
		if decoder.tryDecodeHandle(values, offset, col, handle, cacheBytes) {
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

func (decoder *BytesDecoder) tryDecodeHandle(values [][]byte, offset int, col *ColInfo,
	handle kv.Handle, cacheBytes []byte) bool {
	if handle == nil {
		return false
	}
	if types.NeedRestoredData(col.Ft) {
		return false
	}
	if col.IsPKHandle || col.ID == model.ExtraHandleID {
		handleData := cacheBytes
		if mysql.HasUnsignedFlag(col.Ft.GetFlag()) {
			handleData = append(handleData, UintFlag)
			handleData = codec.EncodeUint(handleData, uint64(handle.IntValue()))
		} else {
			handleData = append(handleData, IntFlag)
			handleData = codec.EncodeInt(handleData, handle.IntValue())
		}
		values[offset] = handleData
		return true
	}
	var handleData []byte
	for i, hid := range decoder.handleColIDs {
		if col.ID == hid {
			handleData = append(handleData, handle.EncodedCol(i)...)
		}
	}
	if len(handleData) > 0 {
		values[offset] = handleData
		return true
	}
	return false
}

// DecodeToBytesNoHandle decodes raw byte slice to row data without handle.
func (decoder *BytesDecoder) DecodeToBytesNoHandle(outputOffset map[int64]int, value []byte) ([][]byte, error) {
	return decoder.decodeToBytesInternal(outputOffset, nil, value, nil)
}

// DecodeToBytes decodes raw byte slice to row data.
func (decoder *BytesDecoder) DecodeToBytes(outputOffset map[int64]int, handle kv.Handle, value []byte, cacheBytes []byte) ([][]byte, error) {
	return decoder.decodeToBytesInternal(outputOffset, handle, value, cacheBytes)
}

func (*BytesDecoder) encodeOldDatum(tp byte, val []byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(val))
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

// encodeOldDatumToArena is like encodeOldDatum but appends to a caller-provided arena
// instead of allocating a new slice. Returns the encoded sub-slice and the updated arena.
func encodeOldDatumToArena(tp byte, val []byte, arena []byte) (result []byte, newArena []byte) {
	start := len(arena)
	switch tp {
	case BytesFlag:
		arena = append(arena, CompactBytesFlag)
		arena = codec.EncodeCompactBytes(arena, val)
	case IntFlag:
		arena = append(arena, VarintFlag)
		arena = codec.EncodeVarint(arena, decodeInt(val))
	case UintFlag:
		arena = append(arena, VaruintFlag)
		arena = codec.EncodeUvarint(arena, decodeUint(val))
	default:
		arena = append(arena, tp)
		arena = append(arena, val...)
	}
	return arena[start:len(arena):len(arena)], arena
}

// DecodeToBytesNoHandleInto is like DecodeToBytesNoHandle but writes into a caller-provided
// values slice and arena instead of allocating new ones. The arena is used for encodeOldDatum
// allocations; caller should reset arena length between rows (arena = arena[:0]).
// The values slice is cleared and reused.
func (decoder *BytesDecoder) DecodeToBytesNoHandleInto(
	outputOffset map[int64]int, value []byte, values [][]byte, arena []byte,
) ([][]byte, []byte, error) {
	var r row
	err := r.fromBytes(value)
	if err != nil {
		return nil, arena, err
	}
	for i := range values {
		values[i] = nil
	}
	for i := range decoder.columns {
		col := &decoder.columns[i]
		tp := fieldType2Flag(col.Ft.ArrayType().GetType(), col.Ft.GetFlag()&mysql.UnsignedFlag == 0)
		colID := col.ID
		offset := outputOffset[colID]
		idx, isNil, notFound := r.findColID(colID)
		if !notFound && !isNil {
			val := r.getData(idx)
			values[offset], arena = encodeOldDatumToArena(tp, val, arena)
			continue
		}

		if isNil {
			values[offset] = []byte{NilFlag}
			continue
		}

		if decoder.defBytes != nil {
			defVal, err := decoder.defBytes(i)
			if err != nil {
				return nil, arena, err
			}
			if len(defVal) > 0 {
				values[offset] = defVal
				continue
			}
		}

		values[offset] = []byte{NilFlag}
	}
	return values, arena, nil
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
	case mysql.TypeTiDBVectorFloat32:
		flag = VectorFloat32Flag
	case mysql.TypeNull:
		flag = NilFlag
	default:
		panic(fmt.Sprintf("unknown field type %d", tp))
	}
	return
}
