// Copyright 2017 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestAppendRow(t *testing.T) {
	numCols := 6
	numRows := 10
	chk := newChunk(8, 8, 0, 0, 40, 0)
	strFmt := "%d.12345"
	for i := 0; i < numRows; i++ {
		chk.AppendNull(0)
		chk.AppendInt64(1, int64(i))
		str := fmt.Sprintf(strFmt, i)
		chk.AppendString(2, str)
		chk.AppendBytes(3, []byte(str))
		chk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		chk.AppendJSON(5, json.CreateBinary(str))
	}
	require.Equal(t, numCols, chk.NumCols())
	require.Equal(t, numRows, chk.NumRows())
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		require.Equal(t, int64(0), row.GetInt64(0))
		require.True(t, row.IsNull(0))
		require.Equal(t, int64(i), row.GetInt64(1))
		require.False(t, row.IsNull(1))
		str := fmt.Sprintf(strFmt, i)
		require.False(t, row.IsNull(2))
		require.Equal(t, str, row.GetString(2))
		require.False(t, row.IsNull(3))
		require.Equal(t, []byte(str), row.GetBytes(3))
		require.False(t, row.IsNull(4))
		require.Equal(t, str, row.GetMyDecimal(4).String())
		require.False(t, row.IsNull(5))
		require.Equal(t, str, string(row.GetJSON(5).GetString()))
	}

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		chk2.AppendRow(row)
	}
	for i := 0; i < numCols; i++ {
		col2, col1 := chk2.columns[i], chk.columns[i]
		col2.elemBuf, col1.elemBuf = nil, nil
		require.Equal(t, col1, col2)
	}

	// Test more types
	chk = newChunk(4, 8, 16, 16, 0, 0)
	f32Val := float32(1.2)
	chk.AppendFloat32(0, f32Val)
	f64Val := 1.3
	chk.AppendFloat64(1, f64Val)
	tVal := types.TimeFromDays(1)
	chk.AppendTime(2, tVal)
	durVal := types.Duration{Duration: time.Hour, Fsp: 6}
	chk.AppendDuration(3, durVal)
	enumVal := types.Enum{Name: "abc", Value: 100}
	chk.AppendEnum(4, enumVal)
	setVal := types.Set{Name: "def", Value: 101}
	chk.AppendSet(5, setVal)

	row := chk.GetRow(0)
	require.Equal(t, f32Val, row.GetFloat32(0))
	require.Equal(t, 0, row.GetTime(2).Compare(tVal))
	require.Equal(t, durVal.Duration, row.GetDuration(3, 0).Duration)
	require.Equal(t, enumVal, row.GetEnum(4))
	require.Equal(t, setVal, row.GetSet(5))

	// AppendPartialRow can append a row with different number of columns, useful for join.
	chk = newChunk(8, 8)
	chk2 = newChunk(8)
	chk2.AppendInt64(0, 1)
	chk2.AppendInt64(0, -1)
	chk.AppendPartialRow(0, chk2.GetRow(0))
	chk.AppendPartialRow(1, chk2.GetRow(0))
	require.Equal(t, int64(1), chk.GetRow(0).GetInt64(0))
	require.Equal(t, int64(1), chk.GetRow(0).GetInt64(1))
	require.Equal(t, 1, chk.NumRows())

	// AppendRowByColIdxs and AppendPartialRowByColIdxs can do projection from row.
	chk = newChunk(8, 8)
	row = MutRowFromValues(0, 1, 2, 3).ToRow()
	chk.AppendRowByColIdxs(row, []int{3})
	chk.AppendRowByColIdxs(row, []int{1})
	chk.AppendRowByColIdxs(row, []int{})
	require.Equal(t, []int64{3, 1}, chk.Column(0).Int64s())
	require.Equal(t, 3, chk.numVirtualRows)
	chk.AppendPartialRowByColIdxs(1, row, []int{2})
	chk.AppendPartialRowByColIdxs(1, row, []int{0})
	chk.AppendPartialRowByColIdxs(0, row, []int{1, 3})
	require.Equal(t, []int64{3, 1, 1}, chk.Column(0).Int64s())
	require.Equal(t, []int64{2, 0, 3}, chk.Column(1).Int64s())
	require.Equal(t, 3, chk.numVirtualRows)

	// Test Reset.
	chk = newChunk(0)
	chk.AppendString(0, "abcd")
	chk.Reset()
	chk.AppendString(0, "def")
	require.Equal(t, "def", chk.GetRow(0).GetString(0))
	require.Equal(t, 1, chk.NumRows())

	// Test float32
	chk = newChunk(4)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	require.Equal(t, float32(1), chk.GetRow(2).GetFloat32(0))
}

func TestAppendChunk(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeJSON))

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	require.NoError(t, err)

	src := NewChunkWithCapacity(fieldTypes, 32)
	dst := NewChunkWithCapacity(fieldTypes, 32)

	src.AppendFloat32(0, 12.8)
	src.AppendString(1, "abc")
	src.AppendJSON(2, jsonObj)
	src.AppendNull(0)
	src.AppendNull(1)
	src.AppendNull(2)

	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(dst, 2, 6)
	dst.Append(dst, 6, 6)

	require.Equal(t, 3, dst.NumCols())
	require.Equal(t, 12, dst.NumRows())

	col := dst.Column(0)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, 0, len(col.offsets))
	require.Equal(t, 4*12, len(col.data))
	require.Equal(t, 4, len(col.elemBuf))

	col = dst.Column(1)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}), fmt.Sprintf("%v", col.offsets))
	require.Equal(t, "abcabcabcabcabcabc", string(col.data))
	require.Equal(t, 0, len(col.elemBuf))

	col = dst.Column(2)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, 13, len(col.offsets))
	require.Equal(t, (len(jsonObj.Value)+int(unsafe.Sizeof(jsonObj.TypeCode)))*6, len(col.data))
	require.Equal(t, 0, len(col.elemBuf))
	for i := 0; i < 12; i += 2 {
		jsonElem := dst.GetRow(i).GetJSON(2)
		require.Zero(t, json.CompareBinary(jsonElem, jsonObj))
	}
}

func TestTruncateTo(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeJSON))

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	require.NoError(t, err)

	src := NewChunkWithCapacity(fieldTypes, 32)

	for i := 0; i < 8; i++ {
		src.AppendFloat32(0, 12.8)
		src.AppendString(1, "abc")
		src.AppendJSON(2, jsonObj)
		src.AppendNull(0)
		src.AppendNull(1)
		src.AppendNull(2)
	}

	require.Equal(t, 16, src.NumRows())
	src.TruncateTo(16)
	src.TruncateTo(16)
	require.Equal(t, 16, src.NumRows())
	src.TruncateTo(14)
	require.Equal(t, 14, src.NumRows())
	src.TruncateTo(12)
	require.Equal(t, 3, src.NumCols())
	require.Equal(t, 12, src.NumRows())

	col := src.Column(0)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, 0, len(col.offsets))
	require.Equal(t, 4*12, len(col.data))
	require.Equal(t, 4, len(col.elemBuf))

	col = src.Column(1)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}), fmt.Sprintf("%v", col.offsets))
	require.Equal(t, "abcabcabcabcabcabc", string(col.data))
	require.Equal(t, 0, len(col.elemBuf))

	col = src.Column(2)
	require.Equal(t, 12, col.length)
	require.Equal(t, 6, col.nullCount())
	require.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	require.Equal(t, 13, len(col.offsets))
	require.Equal(t, 150, len(col.data))
	require.Equal(t, (len(jsonObj.Value)+int(unsafe.Sizeof(jsonObj.TypeCode)))*6, len(col.data))
	require.Equal(t, 0, len(col.elemBuf))
	for i := 0; i < 12; i += 2 {
		row := src.GetRow(i)
		jsonElem := row.GetJSON(2)
		require.Zero(t, json.CompareBinary(jsonElem, jsonObj))
	}

	chk := NewChunkWithCapacity(fieldTypes[:1], 1)
	chk.AppendFloat32(0, 1.0)
	chk.AppendFloat32(0, 1.0)
	chk.TruncateTo(1)
	require.Equal(t, 1, chk.NumRows())
	chk.AppendNull(0)
	require.True(t, chk.GetRow(1).IsNull(0))
}

func TestChunkSizeControl(t *testing.T) {
	maxChunkSize := 10
	chk := New([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, maxChunkSize, maxChunkSize)
	require.Equal(t, maxChunkSize, chk.RequiredRows())

	for i := 0; i < maxChunkSize; i++ {
		chk.AppendInt64(0, 1)
	}
	maxChunkSize += maxChunkSize / 3
	chk.GrowAndReset(maxChunkSize)
	require.Equal(t, maxChunkSize, chk.RequiredRows())

	maxChunkSize2 := maxChunkSize + maxChunkSize/3
	chk2 := Renew(chk, maxChunkSize2)
	require.Equal(t, maxChunkSize2, chk2.RequiredRows())

	chk.Reset()
	for i := 1; i < maxChunkSize*2; i++ {
		chk.SetRequiredRows(i, maxChunkSize)
		require.Equal(t, mathutil.Min(maxChunkSize, i), chk.RequiredRows())
	}

	chk.SetRequiredRows(1, maxChunkSize).
		SetRequiredRows(2, maxChunkSize).
		SetRequiredRows(3, maxChunkSize)
	require.Equal(t, 3, chk.RequiredRows())

	chk.SetRequiredRows(-1, maxChunkSize)
	require.Equal(t, maxChunkSize, chk.RequiredRows())

	chk.SetRequiredRows(5, maxChunkSize)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	require.Equal(t, 4, chk.NumRows())
	require.False(t, chk.IsFull())

	chk.AppendInt64(0, 1)
	require.Equal(t, 5, chk.NumRows())
	require.True(t, chk.IsFull())

	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	require.Equal(t, 8, chk.NumRows())
	require.True(t, chk.IsFull())

	chk.SetRequiredRows(maxChunkSize, maxChunkSize)
	require.Equal(t, 8, chk.NumRows())
	require.False(t, chk.IsFull())
}

// newChunk creates a new chunk and initialize columns with element length.
// Positive len add a fixed length Column, otherwise adds an varlen Column.
func newChunk(elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.columns = append(chk.columns, newFixedLenColumn(l, 0))
		} else {
			chk.columns = append(chk.columns, newVarLenColumn(0))
		}
	}
	return chk
}

func newChunkWithInitCap(capacity int, elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.columns = append(chk.columns, newFixedLenColumn(l, capacity))
		} else {
			chk.columns = append(chk.columns, newVarLenColumn(capacity))
		}
	}
	return chk
}

func newAllTypes() []*types.FieldType {
	ret := []*types.FieldType{
		types.NewFieldType(mysql.TypeTiny),
		types.NewFieldType(mysql.TypeShort),
		types.NewFieldType(mysql.TypeInt24),
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeLonglong),
	}

	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlen(types.UnspecifiedLength)
	tp.SetDecimal(types.UnspecifiedLength)
	tp.SetFlag(mysql.UnsignedFlag)
	ret = append(ret, tp)

	ret = append(ret,
		types.NewFieldType(mysql.TypeYear),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeBlob),
		types.NewFieldType(mysql.TypeTinyBlob),
		types.NewFieldType(mysql.TypeMediumBlob),
		types.NewFieldType(mysql.TypeLongBlob),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeDatetime),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDuration),
		types.NewFieldType(mysql.TypeNewDecimal),
	)

	tp = types.NewFieldType(mysql.TypeSet)
	tp.SetFlen(types.UnspecifiedLength)
	tp.SetDecimal(types.UnspecifiedLength)
	tp.SetFlag(mysql.UnsignedFlag)
	tp.SetElems([]string{"a", "b"})
	ret = append(ret, tp)

	tp = types.NewFieldType(mysql.TypeEnum)
	tp.SetFlen(types.UnspecifiedLength)
	tp.SetDecimal(types.UnspecifiedLength)
	tp.SetFlag(mysql.UnsignedFlag)
	tp.SetElems([]string{"a", "b"})
	ret = append(ret, tp)

	ret = append(ret,
		types.NewFieldType(mysql.TypeBit),
		types.NewFieldType(mysql.TypeJSON),
	)

	return ret
}

func TestCompare(t *testing.T) {
	allTypes := newAllTypes()
	chunk := NewChunkWithCapacity(allTypes, 32)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			if mysql.HasUnsignedFlag(allTypes[i].GetFlag()) {
				chunk.AppendUint64(i, 0)
			} else {
				chunk.AppendInt64(i, -1)
			}
		case mysql.TypeFloat:
			chunk.AppendFloat32(i, 0)
		case mysql.TypeDouble:
			chunk.AppendFloat64(i, 0)
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			chunk.AppendString(i, "0")
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			chunk.AppendTime(i, types.TimeFromDays(2000*365))
		case mysql.TypeDuration:
			chunk.AppendDuration(i, types.ZeroDuration)
		case mysql.TypeNewDecimal:
			chunk.AppendMyDecimal(i, types.NewDecFromInt(0))
		case mysql.TypeSet:
			chunk.AppendSet(i, types.Set{Name: "a", Value: 0})
		case mysql.TypeEnum:
			chunk.AppendEnum(i, types.Enum{Name: "a", Value: 0})
		case mysql.TypeBit:
			chunk.AppendBytes(i, []byte{0})
		case mysql.TypeJSON:
			chunk.AppendJSON(i, json.CreateBinary(int64(0)))
		default:
			require.FailNow(t, "type not handled", allTypes[i].GetType())
		}
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			if mysql.HasUnsignedFlag(allTypes[i].GetFlag()) {
				chunk.AppendUint64(i, math.MaxUint64)
			} else {
				chunk.AppendInt64(i, 1)
			}
		case mysql.TypeFloat:
			chunk.AppendFloat32(i, 1)
		case mysql.TypeDouble:
			chunk.AppendFloat64(i, 1)
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			chunk.AppendString(i, "1")
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			chunk.AppendTime(i, types.TimeFromDays(2001*365))
		case mysql.TypeDuration:
			chunk.AppendDuration(i, types.Duration{Duration: time.Second})
		case mysql.TypeNewDecimal:
			chunk.AppendMyDecimal(i, types.NewDecFromInt(1))
		case mysql.TypeSet:
			chunk.AppendSet(i, types.Set{Name: "b", Value: 1})
		case mysql.TypeEnum:
			chunk.AppendEnum(i, types.Enum{Name: "b", Value: 1})
		case mysql.TypeBit:
			chunk.AppendBytes(i, []byte{1})
		case mysql.TypeJSON:
			chunk.AppendJSON(i, json.CreateBinary(int64(1)))
		default:
			require.FailNow(t, "type not handled", allTypes[i].GetType())
		}
	}
	rowNull := chunk.GetRow(0)
	rowSmall := chunk.GetRow(1)
	rowBig := chunk.GetRow(2)
	for i := 0; i < len(allTypes); i++ {
		cmpFunc := GetCompareFunc(allTypes[i])
		require.Equal(t, 0, cmpFunc(rowNull, i, rowNull, i))
		require.Equal(t, -1, cmpFunc(rowNull, i, rowSmall, i))
		require.Equal(t, 1, cmpFunc(rowSmall, i, rowNull, i))
		require.Equal(t, 0, cmpFunc(rowSmall, i, rowSmall, i))
		require.Equal(t, -1, cmpFunc(rowSmall, i, rowBig, i))
		require.Equal(t, 1, cmpFunc(rowBig, i, rowSmall, i))
		require.Equal(t, 0, cmpFunc(rowBig, i, rowBig, i))
	}
}

func TestCopyTo(t *testing.T) {
	allTypes := newAllTypes()
	chunk := NewChunkWithCapacity(allTypes, 101)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for k := 0; k < 100; k++ {
		for i := 0; i < len(allTypes); i++ {
			switch allTypes[i].GetType() {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
				if mysql.HasUnsignedFlag(allTypes[i].GetFlag()) {
					chunk.AppendUint64(i, uint64(k))
				} else {
					chunk.AppendInt64(i, int64(k))
				}
			case mysql.TypeFloat:
				chunk.AppendFloat32(i, float32(k))
			case mysql.TypeDouble:
				chunk.AppendFloat64(i, float64(k))
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
				chunk.AppendString(i, fmt.Sprintf("%v", k))
			case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
				chunk.AppendTime(i, types.TimeFromDays(2000*365+int64(k)))
			case mysql.TypeDuration:
				chunk.AppendDuration(i, types.Duration{Duration: time.Second * time.Duration(k), Fsp: types.DefaultFsp})
			case mysql.TypeNewDecimal:
				chunk.AppendMyDecimal(i, types.NewDecFromInt(int64(k)))
			case mysql.TypeSet:
				chunk.AppendSet(i, types.Set{Name: "a", Value: uint64(k)})
			case mysql.TypeEnum:
				chunk.AppendEnum(i, types.Enum{Name: "a", Value: uint64(k)})
			case mysql.TypeBit:
				chunk.AppendBytes(i, []byte{byte(k)})
			case mysql.TypeJSON:
				chunk.AppendJSON(i, json.CreateBinary(int64(k)))
			default:
				require.FailNow(t, "type not handled", allTypes[i].GetType())
			}
		}
	}

	ck1 := chunk.CopyConstruct()

	for k := 0; k < 101; k++ {
		row := chunk.GetRow(k)
		r1 := ck1.GetRow(k)
		for i := 0; i < len(allTypes); i++ {
			cmpFunc := GetCompareFunc(allTypes[i])
			require.Zero(t, cmpFunc(row, i, r1, i))
		}

	}
}

func TestGetDecimalDatum(t *testing.T) {
	datum := types.NewDatum(1.01)
	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decType.SetFlen(4)
	decType.SetDecimal(2)
	sc := new(stmtctx.StatementContext)
	decDatum, err := datum.ConvertTo(sc, decType)
	require.NoError(t, err)

	chk := NewChunkWithCapacity([]*types.FieldType{decType}, 32)
	chk.AppendMyDecimal(0, decDatum.GetMysqlDecimal())
	decFromChk := chk.GetRow(0).GetDatum(0, decType)
	require.Equal(t, decFromChk.Length(), decDatum.Length())
	require.Equal(t, decFromChk.Frac(), decDatum.Frac())
}

func TestChunkMemoryUsage(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeJSON))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDatetime))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDuration))

	initCap := 10
	chk := NewChunkWithCapacity(fieldTypes, initCap)

	// cap(c.nullBitmap) + cap(c.offsets)*8 + cap(c.data) + cap(c.elemBuf)
	colUsage := make([]int, len(fieldTypes))
	colUsage[0] = (initCap+7)>>3 + 0 + initCap*4 + 4
	colUsage[1] = (initCap+7)>>3 + (initCap+1)*8 + initCap*8 + 0
	colUsage[2] = (initCap+7)>>3 + (initCap+1)*8 + initCap*8 + 0
	colUsage[3] = (initCap+7)>>3 + 0 + initCap*sizeTime + sizeTime
	colUsage[4] = (initCap+7)>>3 + 0 + initCap*8 + 8

	expectedUsage := 0
	for i := range colUsage {
		expectedUsage += colUsage[i] + int(unsafe.Sizeof(*chk.columns[i]))
	}
	// empty chunk with initial capactiy
	require.Equal(t, int64(expectedUsage), chk.MemoryUsage())

	jsonObj, err := json.ParseBinaryFromString("1")
	require.NoError(t, err)

	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	// append one row, not exceed capacity
	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	require.Equal(t, int64(expectedUsage), chk.MemoryUsage())

	// append another row, only column 1 exceeds capacity
	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123111111111111111111111111111111111111111111111")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	colUsage[1] = (initCap+7)>>3 + (initCap+1)*8 + cap(chk.columns[1].data) + 0
	expectedUsage = 0
	for i := range colUsage {
		expectedUsage += colUsage[i] + int(unsafe.Sizeof(*chk.columns[i]))
	}
	require.Equal(t, int64(expectedUsage), chk.MemoryUsage())
}

func TestSwapColumn(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))

	// chk1: column1 refers to column0
	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.MakeRef(0, 1)
	chk1.AppendFloat32(2, 3)

	// chk2: column1 refers to column0
	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	chk2.AppendFloat32(0, 1)
	chk2.MakeRef(0, 1)
	chk2.AppendFloat32(2, 3)

	require.Same(t, chk1.Column(0), chk1.Column(1))
	require.Same(t, chk2.Column(0), chk2.Column(1))

	// swap preserves ref
	checkRef := func() {
		require.Same(t, chk1.Column(0), chk1.Column(1))
		require.NotSame(t, chk1.Column(0), chk2.Column(0))
		require.Same(t, chk2.Column(0), chk2.Column(1))
	}
	checkRef()

	// swap two chunk's columns
	require.NoError(t, chk1.SwapColumn(0, chk2, 0))
	checkRef()

	require.NoError(t, chk1.SwapColumn(0, chk2, 0))
	checkRef()

	// swap reference and referenced columns
	require.NoError(t, chk2.SwapColumn(1, chk2, 0))
	checkRef()

	// swap the same column in the same chunk
	require.NoError(t, chk2.SwapColumn(1, chk2, 1))
	checkRef()

	// swap reference and another column
	require.NoError(t, chk2.SwapColumn(1, chk2, 2))
	checkRef()

	require.NoError(t, chk2.SwapColumn(2, chk2, 0))
	checkRef()
}

func TestAppendSel(t *testing.T) {
	tll := types.NewFieldType(mysql.TypeLonglong)
	chk := NewChunkWithCapacity([]*types.FieldType{tll}, 1024)
	sel := make([]int, 0, 1024/2)
	for i := 0; i < 1024/2; i++ {
		chk.AppendInt64(0, int64(i))
		if i%2 == 0 {
			sel = append(sel, i)
		}
	}
	chk.SetSel(sel)
	require.Equal(t, 1024/2/2, chk.NumRows())
	chk.AppendInt64(0, int64(1))
	require.Equal(t, 1024/2/2+1, chk.NumRows())
	sel = chk.Sel()
	require.Equal(t, 1024/2, sel[len(sel)-1])
}

func TestMakeRefTo(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))

	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.AppendFloat32(1, 3)

	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	err := chk2.MakeRefTo(0, chk1, 1)
	require.NoError(t, err)

	err = chk2.MakeRefTo(1, chk1, 0)
	require.NoError(t, err)

	require.Same(t, chk1.Column(1), chk2.Column(0))
	require.Same(t, chk1.Column(0), chk2.Column(1))
}

func TestToString(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDouble))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeString))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDate))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeLonglong))

	chk := NewChunkWithCapacity(fieldTypes, 2)
	chk.AppendFloat32(0, float32(1))
	chk.AppendFloat64(1, 1.0)
	chk.AppendString(2, "1")
	chk.AppendTime(3, types.ZeroDate)
	chk.AppendInt64(4, 1)

	chk.AppendFloat32(0, float32(2))
	chk.AppendFloat64(1, 2.0)
	chk.AppendString(2, "2")
	chk.AppendTime(3, types.ZeroDatetime)
	chk.AppendInt64(4, 2)

	require.Equal(t, "1, 1, 1, 0000-00-00, 1\n2, 2, 2, 0000-00-00 00:00:00, 2\n", chk.ToString(fieldTypes))
}

func BenchmarkAppendInt(b *testing.B) {
	b.ReportAllocs()
	chk := newChunk(8)
	for i := 0; i < b.N; i++ {
		appendInt(chk)
	}
}

func appendInt(chk *Chunk) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendInt64(0, int64(i))
	}
}

func BenchmarkAppendString(b *testing.B) {
	b.ReportAllocs()
	chk := newChunk(0)
	for i := 0; i < b.N; i++ {
		appendString(chk)
	}
}

func appendString(chk *Chunk) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendString(0, "abcd")
	}
}

func BenchmarkAppendRow(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(8, 8, 0, 0)
	rowChk.AppendNull(0)
	rowChk.AppendInt64(1, 1)
	rowChk.AppendString(2, "abcd")
	rowChk.AppendBytes(3, []byte("abcd"))

	chk := newChunk(8, 8, 0, 0)
	for i := 0; i < b.N; i++ {
		appendRow(chk, rowChk.GetRow(0))
	}
}

func appendRow(chk *Chunk, row Row) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendRow(row)
	}
}

func BenchmarkAppendBytes1024(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 1024)
	}
}

func BenchmarkAppendBytes512(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 512)
	}
}

func BenchmarkAppendBytes256(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 256)
	}
}

func BenchmarkAppendBytes128(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 128)
	}
}

func BenchmarkAppendBytes64(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 64)
	}
}

func BenchmarkAppendBytes32(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 32)
	}
}

func BenchmarkAppendBytes16(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 16)
	}
}

func BenchmarkAppendBytes8(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 8)
	}
}

func BenchmarkAppendBytes4(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 4)
	}
}

func BenchmarkAppendBytes2(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 2)
	}
}

func BenchmarkAppendBytes1(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 1)
	}
}

func appendBytes(chk *Chunk, bs []byte, times int) {
	chk.Reset()
	for i := 0; i < times; i++ {
		chk.AppendBytes(0, bs)
	}
}

func BenchmarkAccess(b *testing.B) {
	b.StopTimer()
	rowChk := newChunk(8)
	for i := 0; i < 8192; i++ {
		rowChk.AppendInt64(0, math.MaxUint16)
	}
	b.StartTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		for j := 0; j < 8192; j++ {
			sum += rowChk.GetRow(j).GetInt64(0)
		}
	}
	fmt.Println(sum)
}

func BenchmarkChunkMemoryUsage(b *testing.B) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDatetime))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDuration))

	initCap := 10
	chk := NewChunkWithCapacity(fieldTypes, initCap)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	for i := 0; i < initCap; i++ {
		chk.AppendFloat64(0, 123.123)
		chk.AppendString(1, "123")
		chk.AppendTime(2, timeObj)
		chk.AppendDuration(3, durationObj)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chk.MemoryUsage()
	}
}

type seqNumberGenerateExec struct {
	seq          int
	genCountSize int
}

func (x *seqNumberGenerateExec) Next(chk *Chunk, resize bool) {
	if resize {
		chk.GrowAndReset(1024)
	} else {
		chk.Reset()
	}
	for chk.NumRows() < chk.Capacity() {
		x.seq++
		if x.seq > x.genCountSize {
			break
		}
		chk.AppendInt64(0, 1)
	}
}

type benchChunkGrowCase struct {
	tag        string
	reuse      bool
	newReset   bool
	cntPerCall int
	initCap    int
	maxCap     int
}

func (b *benchChunkGrowCase) String() string {
	var buff bytes.Buffer
	if b.reuse {
		buff.WriteString("renew,")
	} else {
		buff.WriteString("reset,")
	}
	buff.WriteString("cntPerCall:" + strconv.Itoa(b.cntPerCall) + ",")
	buff.WriteString("cap from:" + strconv.Itoa(b.initCap) + " to " + strconv.Itoa(b.maxCap) + ",")
	if b.tag != "" {
		buff.WriteString("[" + b.tag + "]")
	}
	return buff.String()
}

func BenchmarkChunkGrowSuit(b *testing.B) {
	tests := []benchChunkGrowCase{
		{reuse: true, newReset: false, cntPerCall: 10000000, initCap: 1024, maxCap: 1024},
		{reuse: true, newReset: false, cntPerCall: 10000000, initCap: 32, maxCap: 32},
		{reuse: true, newReset: true, cntPerCall: 10000000, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: false, newReset: false, cntPerCall: 10000000, initCap: 1024, maxCap: 1024},
		{reuse: false, newReset: false, cntPerCall: 10000000, initCap: 32, maxCap: 32},
		{reuse: false, newReset: true, cntPerCall: 10000000, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: true, newReset: false, cntPerCall: 10, initCap: 1024, maxCap: 1024},
		{reuse: true, newReset: false, cntPerCall: 10, initCap: 32, maxCap: 32},
		{reuse: true, newReset: true, cntPerCall: 10, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: false, newReset: false, cntPerCall: 10, initCap: 1024, maxCap: 1024},
		{reuse: false, newReset: false, cntPerCall: 10, initCap: 32, maxCap: 32},
		{reuse: false, newReset: true, cntPerCall: 10, initCap: 32, maxCap: 1024, tag: "grow"},
	}
	for _, test := range tests {
		b.Run(test.String(), benchmarkChunkGrow(test))
	}
}

func benchmarkChunkGrow(t benchChunkGrowCase) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		chk := New([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, t.initCap, t.maxCap)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := &seqNumberGenerateExec{genCountSize: t.cntPerCall}
			for {
				e.Next(chk, t.newReset)
				if chk.NumRows() == 0 {
					break
				}
				if !t.reuse {
					if t.newReset {
						chk = Renew(chk, t.maxCap)
					} else {
						chk = New([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, t.initCap, t.maxCap)
					}
				}
			}
		}
	}
}

func TestAppendRows(t *testing.T) {
	numCols := 6
	numRows := 10
	chk := newChunk(8, 8, 0, 0, 40, 0)
	strFmt := "%d.12345"
	for i := 0; i < numRows; i++ {
		chk.AppendNull(0)
		chk.AppendInt64(1, int64(i))
		str := fmt.Sprintf(strFmt, i)
		chk.AppendString(2, str)
		chk.AppendBytes(3, []byte(str))
		chk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		chk.AppendJSON(5, json.CreateBinary(str))
	}
	require.Equal(t, numCols, chk.NumCols())
	require.Equal(t, numRows, chk.NumRows())

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	require.Equal(t, numCols, chk.NumCols())
	rows := make([]Row, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = chk.GetRow(i)
	}
	chk2.AppendRows(rows)
	for i := 0; i < numRows; i++ {
		row := chk2.GetRow(i)
		require.Equal(t, int64(0), row.GetInt64(0))
		require.True(t, row.IsNull(0))
		require.Equal(t, int64(i), row.GetInt64(1))
		str := fmt.Sprintf(strFmt, i)
		require.False(t, row.IsNull(2))
		require.Equal(t, str, row.GetString(2))
		require.False(t, row.IsNull(3))
		require.Equal(t, []byte(str), row.GetBytes(3))
		require.False(t, row.IsNull(4))
		require.Equal(t, str, row.GetMyDecimal(4).String())
		require.False(t, row.IsNull(5))
		require.Equal(t, str, string(row.GetJSON(5).GetString()))
	}
}

func BenchmarkBatchAppendRows(b *testing.B) {
	b.ReportAllocs()
	numRows := 4096
	rowChk := newChunk(8, 8, 0, 0)
	for i := 0; i < numRows; i++ {
		rowChk.AppendNull(0)
		rowChk.AppendInt64(1, 1)
		rowChk.AppendString(2, "abcd")
		rowChk.AppendBytes(3, []byte("abcd"))
	}
	chk := newChunk(8, 8, 0, 0)
	type testCaseConf struct {
		batchSize int
	}
	testCaseConfs := []testCaseConf{
		{batchSize: 10},
		{batchSize: 100},
		{batchSize: 500},
		{batchSize: 1000},
		{batchSize: 1500},
		{batchSize: 2000},
		{batchSize: 3000},
		{batchSize: 4000},
	}
	for _, conf := range testCaseConfs {
		b.Run(fmt.Sprintf("row-%d", conf.batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				chk.Reset()
				for j := 0; j < conf.batchSize; j++ {
					chk.AppendRow(rowChk.GetRow(j))
				}
			}
		})
		b.ResetTimer()
		b.Run(fmt.Sprintf("column-%d", conf.batchSize), func(b *testing.B) {
			rows := make([]Row, conf.batchSize)
			for i := 0; i < conf.batchSize; i++ {
				rows[i] = rowChk.GetRow(i)
			}
			for i := 0; i < b.N; i++ {
				chk.Reset()
				chk.AppendRows(rows)
			}
		})
	}
}

func BenchmarkAppendRows(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(8, 8, 0, 0)

	for i := 0; i < 4096; i++ {
		rowChk.AppendNull(0)
		rowChk.AppendInt64(1, 1)
		rowChk.AppendString(2, "abcd")
		rowChk.AppendBytes(3, []byte("abcd"))
	}

	type testCaseConf struct {
		batchSize int
	}
	testCaseConfs := []testCaseConf{
		{batchSize: 2},
		{batchSize: 8},
		{batchSize: 16},
		{batchSize: 100},
		{batchSize: 1000},
		{batchSize: 4000},
	}

	chk := newChunk(8, 8, 0, 0)
	for _, conf := range testCaseConfs {
		b.ResetTimer()
		b.Run(fmt.Sprintf("row-%d", conf.batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				chk.Reset()
				for j := 0; j < conf.batchSize; j++ {
					chk.AppendRow(rowChk.GetRow(j))
				}
			}
		})
		b.ResetTimer()
		b.Run(fmt.Sprintf("column-%d", conf.batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				chk.Reset()
				chk.Append(rowChk, 0, conf.batchSize)
			}
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(0, 0)

	for i := 0; i < 4096; i++ {
		rowChk.AppendString(0, "abcd")
		rowChk.AppendBytes(1, []byte("abcd"))
	}

	type testCaseConf struct {
		batchSize int
	}
	testCaseConfs := []testCaseConf{
		{batchSize: 2},
		{batchSize: 8},
		{batchSize: 16},
		{batchSize: 100},
		{batchSize: 1000},
		{batchSize: 4000},
	}

	chk := newChunk(0, 0)
	for _, conf := range testCaseConfs {
		b.ResetTimer()
		b.Run(fmt.Sprintf("column-%d", conf.batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				chk.Reset()
				chk.Append(rowChk, 0, conf.batchSize)
			}
		})
	}
}
