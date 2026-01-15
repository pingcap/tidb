// Copyright 2018 PingCAP, Inc.
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

package chunk

import (
	"strconv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// RowSize is the size of `Row{}`
const RowSize = int64(unsafe.Sizeof(Row{}))

// Row represents a row of data, can be used to access values.
type Row struct {
	c   *Chunk
	idx int
}

// Chunk returns the Chunk which the row belongs to.
func (r Row) Chunk() *Chunk {
	return r.c
}

// IsEmpty returns true if the Row is empty.
func (r Row) IsEmpty() bool {
	return r == Row{}
}

// Idx returns the row index of Chunk.
func (r Row) Idx() int {
	return r.idx
}

// Len returns the number of values in the row.
func (r Row) Len() int {
	return r.c.NumCols()
}

// GetInt64 returns the int64 value with the colIdx.
func (r Row) GetInt64(colIdx int) int64 {
	return r.c.columns[colIdx].GetInt64(r.idx)
}

// GetUint64 returns the uint64 value with the colIdx.
func (r Row) GetUint64(colIdx int) uint64 {
	return r.c.columns[colIdx].GetUint64(r.idx)
}

// GetFloat32 returns the float32 value with the colIdx.
func (r Row) GetFloat32(colIdx int) float32 {
	return r.c.columns[colIdx].GetFloat32(r.idx)
}

// GetFloat64 returns the float64 value with the colIdx.
func (r Row) GetFloat64(colIdx int) float64 {
	return r.c.columns[colIdx].GetFloat64(r.idx)
}

// GetString returns the string value with the colIdx.
func (r Row) GetString(colIdx int) string {
	return r.c.columns[colIdx].GetString(r.idx)
}

// GetBytes returns the bytes value with the colIdx.
func (r Row) GetBytes(colIdx int) []byte {
	return r.c.columns[colIdx].GetBytes(r.idx)
}

// GetTime returns the Time value with the colIdx.
func (r Row) GetTime(colIdx int) types.Time {
	return r.c.columns[colIdx].GetTime(r.idx)
}

// GetDuration returns the Duration value with the colIdx.
func (r Row) GetDuration(colIdx int, fillFsp int) types.Duration {
	return r.c.columns[colIdx].GetDuration(r.idx, fillFsp)
}

func (r Row) getNameValue(colIdx int) (string, uint64) {
	return r.c.columns[colIdx].getNameValue(r.idx)
}

// GetEnum returns the Enum value with the colIdx.
func (r Row) GetEnum(colIdx int) types.Enum {
	return r.c.columns[colIdx].GetEnum(r.idx)
}

// GetSet returns the Set value with the colIdx.
func (r Row) GetSet(colIdx int) types.Set {
	return r.c.columns[colIdx].GetSet(r.idx)
}

// GetMyDecimal returns the MyDecimal value with the colIdx.
func (r Row) GetMyDecimal(colIdx int) *types.MyDecimal {
	return r.c.columns[colIdx].GetDecimal(r.idx)
}

// GetJSON returns the JSON value with the colIdx.
func (r Row) GetJSON(colIdx int) types.BinaryJSON {
	return r.c.columns[colIdx].GetJSON(r.idx)
}

// GetVectorFloat32 returns the VectorFloat32 value with the colIdx.
func (r Row) GetVectorFloat32(colIdx int) types.VectorFloat32 {
	return r.c.columns[colIdx].GetVectorFloat32(r.idx)
}

// GetDatumRow converts chunk.Row to types.DatumRow.
// Keep in mind that GetDatumRow has a reference to r.c, which is a chunk,
// this function works only if the underlying chunk is valid or unchanged.
func (r Row) GetDatumRow(fields []*types.FieldType) []types.Datum {
	datumRow := make([]types.Datum, r.c.NumCols())
	return r.GetDatumRowWithBuffer(fields, datumRow)
}

// GetDatumRowWithBuffer gets datum using the buffer datumRow.
func (r Row) GetDatumRowWithBuffer(fields []*types.FieldType, datumRow []types.Datum) []types.Datum {
	for colIdx := range datumRow {
		r.DatumWithBuffer(colIdx, fields[colIdx], &datumRow[colIdx])
	}
	return datumRow
}

// GetDatum implements the chunk.Row interface.
func (r Row) GetDatum(colIdx int, tp *types.FieldType) types.Datum {
	var d types.Datum
	r.DatumWithBuffer(colIdx, tp, &d)
	return d
}

// DatumWithBuffer gets datum using the buffer d.
func (r Row) DatumWithBuffer(colIdx int, tp *types.FieldType, d *types.Datum) {
	if r.IsNull(colIdx) {
		d.SetNull()
		return
	}
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(tp.GetFlag()) {
			d.SetUint64(r.GetUint64(colIdx))
		} else {
			d.SetInt64(r.GetInt64(colIdx))
		}
	case mysql.TypeYear:
		// FIXBUG: because insert type of TypeYear is definite int64, so we regardless of the unsigned flag.
		d.SetInt64(r.GetInt64(colIdx))
	case mysql.TypeFloat:
		d.SetFloat32(r.GetFloat32(colIdx))
	case mysql.TypeDouble:
		d.SetFloat64(r.GetFloat64(colIdx))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString(r.GetString(colIdx), tp.GetCollate())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		d.SetMysqlTime(r.GetTime(colIdx))
	case mysql.TypeDuration:
		duration := r.GetDuration(colIdx, tp.GetDecimal())
		d.SetMysqlDuration(duration)
	case mysql.TypeNewDecimal:
		dec := r.GetMyDecimal(colIdx)
		d.SetMysqlDecimal(dec)
		d.SetLength(tp.GetFlen())
		// If tp.decimal is unspecified(-1), we should set it to the real
		// fraction length of the decimal value, if not, the d.Frac will
		// be set to MAX_UINT16 which will cause unexpected BadNumber error
		// when encoding.
		if tp.GetDecimal() == types.UnspecifiedLength {
			d.SetFrac(int(dec.GetDigitsFrac()))
		} else {
			d.SetFrac(tp.GetDecimal())
		}
	case mysql.TypeEnum:
		d.SetMysqlEnum(r.GetEnum(colIdx), tp.GetCollate())
	case mysql.TypeSet:
		d.SetMysqlSet(r.GetSet(colIdx), tp.GetCollate())
	case mysql.TypeBit:
		d.SetMysqlBit(r.GetBytes(colIdx))
	case mysql.TypeJSON:
		d.SetMysqlJSON(r.GetJSON(colIdx))
	case mysql.TypeTiDBVectorFloat32:
		d.SetVectorFloat32(r.GetVectorFloat32(colIdx))
	}
}

// GetRawLen returns the byte length of a row
func (r Row) GetRawLen(colIdx int) int {
	return r.c.columns[colIdx].GetRawLength(r.idx)
}

// GetRaw returns the underlying raw bytes with the colIdx.
func (r Row) GetRaw(colIdx int) []byte {
	return r.c.columns[colIdx].GetRaw(r.idx)
}

// IsNull returns if the datum in the chunk.Row is null.
func (r Row) IsNull(colIdx int) bool {
	return r.c.columns[colIdx].IsNull(r.idx)
}

// CopyConstruct creates a new row and copies this row's data into it.
func (r Row) CopyConstruct() Row {
	newChk := renewWithCapacity(r.c, 1, 1)
	newChk.AppendRow(r)
	return newChk.GetRow(0)
}

// ToString returns all the values in a row.
func (r Row) ToString(ft []*types.FieldType) string {
	var buf []byte
	for colIdx := range r.Chunk().NumCols() {
		if r.IsNull(colIdx) {
			buf = append(buf, "NULL"...)
		} else {
			switch ft[colIdx].EvalType() {
			case types.ETInt:
				buf = strconv.AppendInt(buf, r.GetInt64(colIdx), 10)
			case types.ETString:
				switch ft[colIdx].GetType() {
				case mysql.TypeEnum:
					buf = append(buf, r.GetEnum(colIdx).String()...)
				case mysql.TypeSet:
					buf = append(buf, r.GetSet(colIdx).String()...)
				default:
					buf = append(buf, r.GetString(colIdx)...)
				}
			case types.ETDatetime, types.ETTimestamp:
				buf = append(buf, r.GetTime(colIdx).String()...)
			case types.ETDecimal:
				buf = append(buf, r.GetMyDecimal(colIdx).ToString()...)
			case types.ETDuration:
				buf = append(buf, r.GetDuration(colIdx, ft[colIdx].GetDecimal()).String()...)
			case types.ETJson:
				buf = append(buf, r.GetJSON(colIdx).String()...)
			case types.ETReal:
				switch ft[colIdx].GetType() {
				case mysql.TypeFloat:
					buf = strconv.AppendFloat(buf, float64(r.GetFloat32(colIdx)), 'f', -1, 32)
				case mysql.TypeDouble:
					buf = strconv.AppendFloat(buf, r.GetFloat64(colIdx), 'f', -1, 64)
				}
			case types.ETVectorFloat32:
				buf = append(buf, r.GetVectorFloat32(colIdx).String()...)
			}
		}
		if colIdx != r.Chunk().NumCols()-1 {
			buf = append(buf, ", "...)
		}
	}
	return string(buf)
}

// SerializeToBytesForOneColumn serializes value to bytes for one specified column
func (r Row) SerializeToBytesForOneColumn(typeCtx types.Context, ft *types.FieldType, colIdx int, collator collate.Collator) ([]byte, error) {
	// TODO(x) add comment to explain why we return this string for null
	nullValue := "#$^NULL@*^"
	if r.IsNull(colIdx) {
		return hack.Slice(nullValue), nil
	}

	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear, mysql.TypeDuration:
		return r.c.columns[colIdx].GetRaw(r.idx), nil
	case mysql.TypeFloat:
		f32s := r.c.columns[colIdx].Float32s()
		d := float64(f32s[r.idx])
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if d == 0 {
			d = 0
		}
		return unsafe.Slice((*byte)(unsafe.Pointer(&d)), sizeFloat64), nil
	case mysql.TypeDouble:
		f64s := r.c.columns[colIdx].Float64s()
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		f := f64s[r.idx]
		if f == 0 {
			f = 0
		}
		return unsafe.Slice((*byte)(unsafe.Pointer(&f)), sizeFloat64), nil
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return collator.ImmutableKey(string(hack.String(r.c.columns[colIdx].GetBytes(r.idx)))), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		ts := r.c.columns[colIdx].Times()
		v, err := ts[r.idx].ToPackedUint()
		if err != nil {
			return nil, err
		}
		return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64), nil
	case mysql.TypeNewDecimal:
		ds := r.c.columns[colIdx].Decimals()
		b, err := ds[r.idx].ToHashKey()
		if err != nil {
			return nil, err
		}
		return b, nil
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(ft.GetFlag()) {
			v := r.c.columns[colIdx].GetEnum(r.idx).Value
			return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64), nil
		} else {
			v := r.c.columns[colIdx].GetEnum(r.idx).Value
			str := ""
			if enum, err := types.ParseEnumValue(ft.GetElems(), v); err == nil {
				str = enum.Name
			}
			return collator.ImmutableKey(str), nil
		}
	case mysql.TypeSet:
		s, err := types.ParseSetValue(ft.GetElems(), r.c.columns[colIdx].GetSet(r.idx).Value)
		if err != nil {
			return nil, err
		}
		return collator.ImmutableKey(s.Name), nil
	case mysql.TypeBit:
		v, err1 := types.BinaryLiteral(r.c.columns[colIdx].GetBytes(r.idx)).ToInt(typeCtx)
		terror.Log(errors.Trace(err1))
		return unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64), nil
	case mysql.TypeJSON:
		jsonHashBuffer := make([]byte, 0)
		jsonHashBuffer = r.c.columns[colIdx].GetJSON(r.idx).HashValue(jsonHashBuffer)
		return jsonHashBuffer, nil
	case mysql.TypeNull:
		return hack.Slice(nullValue), nil
	default:
		return nil, errors.Errorf("unsupport column type for encode %d", ft.GetType())
	}
}
