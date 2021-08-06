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
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	path, _ := os.MkdirTemp("", "oom-use-tmp-storage")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})
	_ = os.RemoveAll(path) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(path, 0755)
	check.TestingT(t)
}

var _ = check.Suite(&testChunkSuite{})

type testChunkSuite struct{}

func TestAppendRow(t *testing.T) {
	t.Parallel()

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
	assert.Equal(t, numCols, chk.NumCols())
	assert.Equal(t, numRows, chk.NumRows())
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		assert.True(t, row.IsNull(0))
		assert.False(t, row.IsNull(1))
		assert.False(t, row.IsNull(2))
		assert.False(t, row.IsNull(3))
		assert.False(t, row.IsNull(4))
		assert.False(t, row.IsNull(5))

		assert.Equal(t, int64(0), row.GetInt64(0))
		assert.Equal(t, int64(i), row.GetInt64(1))
		str := fmt.Sprintf(strFmt, i)
		assert.Equal(t, str, row.GetString(2))
		assert.Equal(t, []byte(str), row.GetBytes(3))
		assert.Equal(t, str, row.GetMyDecimal(4).String())
		assert.Equal(t, str, string(row.GetJSON(5).GetString()))
	}

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		chk2.AppendRow(row)
	}
	for i := 0; i < numCols; i++ {
		col2, col1 := chk2.columns[i], chk.columns[i]
		col2.elemBuf, col1.elemBuf = nil, nil
		assert.Equal(t, col1, col2)
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
	assert.Equal(t, f32Val, row.GetFloat32(0))
	assert.Equal(t, 0, row.GetTime(2).Compare(tVal))
	assert.Equal(t, durVal.Duration, row.GetDuration(3, 0).Duration)
	assert.Equal(t, enumVal, row.GetEnum(4))
	assert.Equal(t, setVal, row.GetSet(5))

	// AppendPartialRow can append a row with different number of columns, useful for join.
	chk = newChunk(8, 8)
	row = MutRowFromValues(1).ToRow()
	chk.AppendPartialRow(0, row)
	chk.AppendPartialRow(1, row)
	assert.Equal(t, int64(1), chk.GetRow(0).GetInt64(0))
	assert.Equal(t, int64(1), chk.GetRow(0).GetInt64(1))
	assert.Equal(t, 1, chk.NumRows())

	// AppendRowByColIdxs and AppendPartialRowByColIdxs can do projection from row.
	chk = newChunk(8, 8)
	row = MutRowFromValues(0, 1, 2, 3).ToRow()
	chk.AppendRowByColIdxs(row, []int{3})
	chk.AppendRowByColIdxs(row, []int{1})
	chk.AppendRowByColIdxs(row, []int{})
	assert.Equal(t, []int64{3, 1}, chk.Column(0).Int64s())
	assert.Equal(t, 3, chk.numVirtualRows)
	chk.AppendPartialRowByColIdxs(1, row, []int{2})
	chk.AppendPartialRowByColIdxs(1, row, []int{0})
	chk.AppendPartialRowByColIdxs(0, row, []int{1, 3})
	assert.Equal(t, []int64{3, 1, 1}, chk.Column(0).Int64s())
	assert.Equal(t, []int64{2, 0, 3}, chk.Column(1).Int64s())
	assert.Equal(t, 3, chk.numVirtualRows)

	// Test Reset.
	chk = newChunk(0)
	chk.AppendString(0, "abcd")
	chk.Reset()
	chk.AppendString(0, "def")
	assert.Equal(t, "def", chk.GetRow(0).GetString(0))
	assert.Equal(t, 1, chk.NumRows())
}

func TestAppendChunk(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})

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

	assert.Equal(t, 3, dst.NumCols())
	assert.Equal(t, 12, dst.NumRows())

	col := dst.Column(0)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, 0, len(col.offsets))
	assert.Equal(t, 4*12, len(col.data))
	assert.Equal(t, 4, len(col.elemBuf))

	col = dst.Column(1)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}), fmt.Sprintf("%v", col.offsets))
	assert.Equal(t, "abcabcabcabcabcabc", string(col.data))
	assert.Equal(t, 0, len(col.elemBuf))

	col = dst.Column(2)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, 13, len(col.offsets))
	assert.Equal(t, (len(jsonObj.Value)+int(unsafe.Sizeof(jsonObj.TypeCode)))*6, len(col.data))
	assert.Equal(t, 0, len(col.elemBuf))
	for i := 0; i < 12; i += 2 {
		jsonElem := col.GetJSON(i)
		assert.Zero(t, json.CompareBinary(jsonElem, jsonObj))
	}
}

func TestTruncateTo(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})

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

	assert.Equal(t, 16, src.NumRows())
	src.TruncateTo(16)
	src.TruncateTo(16)
	assert.Equal(t, 16, src.NumRows())
	src.TruncateTo(14)
	assert.Equal(t, 14, src.NumRows())
	src.TruncateTo(12)
	assert.Equal(t, 3, src.NumCols())
	assert.Equal(t, 12, src.NumRows())

	col := src.Column(0)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, 0, len(col.offsets))
	assert.Equal(t, 4*12, len(col.data))
	assert.Equal(t, 4, len(col.elemBuf))

	col = src.Column(1)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}), fmt.Sprintf("%v", col.offsets))
	assert.Equal(t, "abcabcabcabcabcabc", string(col.data))
	assert.Equal(t, 0, len(col.elemBuf))

	col = src.Column(2)
	assert.Equal(t, 12, col.length)
	assert.Equal(t, 6, col.nullCount())
	assert.Equal(t, string([]byte{0b1010101, 0b0000101}), string(col.nullBitmap))
	assert.Equal(t, 13, len(col.offsets))
	assert.Equal(t, (len(jsonObj.Value)+int(unsafe.Sizeof(jsonObj.TypeCode)))*6, len(col.data))
	assert.Equal(t, 0, len(col.elemBuf))
	for i := 0; i < 12; i += 2 {
		jsonElem := col.GetJSON(i)
		assert.Zero(t, json.CompareBinary(jsonElem, jsonObj))
	}

	chk := NewChunkWithCapacity(fieldTypes[:1], 1)
	chk.AppendFloat32(0, 1.0)
	chk.AppendFloat32(0, 1.0)
	chk.TruncateTo(1)
	assert.Equal(t, 1, chk.NumRows())
	chk.AppendNull(0)
	assert.True(t, chk.GetRow(1).IsNull(0))
}

func (s *testChunkSuite) TestChunkSizeControl(c *check.C) {
	maxChunkSize := 10
	chk := New([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, maxChunkSize, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	for i := 0; i < maxChunkSize; i++ {
		chk.AppendInt64(0, 1)
	}
	maxChunkSize += maxChunkSize / 3
	chk.GrowAndReset(maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	maxChunkSize2 := maxChunkSize + maxChunkSize/3
	chk2 := Renew(chk, maxChunkSize2)
	c.Assert(chk2.RequiredRows(), check.Equals, maxChunkSize2)

	chk.Reset()
	for i := 1; i < maxChunkSize*2; i++ {
		chk.SetRequiredRows(i, maxChunkSize)
		c.Assert(chk.RequiredRows(), check.Equals, mathutil.Min(maxChunkSize, i))
	}

	chk.SetRequiredRows(1, maxChunkSize).
		SetRequiredRows(2, maxChunkSize).
		SetRequiredRows(3, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, 3)

	chk.SetRequiredRows(-1, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	chk.SetRequiredRows(5, maxChunkSize)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 4)
	c.Assert(chk.IsFull(), check.IsFalse)

	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 5)
	c.Assert(chk.IsFull(), check.IsTrue)

	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 8)
	c.Assert(chk.IsFull(), check.IsTrue)

	chk.SetRequiredRows(maxChunkSize, maxChunkSize)
	c.Assert(chk.NumRows(), check.Equals, 8)
	c.Assert(chk.IsFull(), check.IsFalse)
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

func newChunkWithInitCap(cap int, elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.columns = append(chk.columns, newFixedLenColumn(l, cap))
		} else {
			chk.columns = append(chk.columns, newVarLenColumn(cap))
		}
	}
	return chk
}

var allTypes = []*types.FieldType{
	types.NewFieldType(mysql.TypeTiny),
	types.NewFieldType(mysql.TypeShort),
	types.NewFieldType(mysql.TypeInt24),
	types.NewFieldType(mysql.TypeLong),
	types.NewFieldType(mysql.TypeLonglong),
	{
		Tp:      mysql.TypeLonglong,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
	},
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
	{
		Tp:      mysql.TypeSet,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	{
		Tp:      mysql.TypeEnum,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	types.NewFieldType(mysql.TypeBit),
	types.NewFieldType(mysql.TypeJSON),
}

func TestCompare(t *testing.T) {
	t.Parallel()

	chunk := NewChunkWithCapacity(allTypes, 32)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			if mysql.HasUnsignedFlag(allTypes[i].Flag) {
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
			t.FailNow()
		}
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			if mysql.HasUnsignedFlag(allTypes[i].Flag) {
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
			t.FailNow()
		}
	}
	rowNull := chunk.GetRow(0)
	rowSmall := chunk.GetRow(1)
	rowBig := chunk.GetRow(2)
	for i := 0; i < len(allTypes); i++ {
		cmpFunc := GetCompareFunc(allTypes[i])
		assert.Equal(t, 0, cmpFunc(rowNull, i, rowNull, i))
		assert.Equal(t, 0, cmpFunc(rowSmall, i, rowSmall, i))
		assert.Equal(t, 0, cmpFunc(rowBig, i, rowBig, i))

		assert.Equal(t, -1, cmpFunc(rowNull, i, rowSmall, i))
		assert.Equal(t, -1, cmpFunc(rowNull, i, rowBig, i))
		assert.Equal(t, -1, cmpFunc(rowSmall, i, rowBig, i))

		assert.Equal(t, 1, cmpFunc(rowSmall, i, rowNull, i))
		assert.Equal(t, 1, cmpFunc(rowBig, i, rowNull, i))
		assert.Equal(t, 1, cmpFunc(rowBig, i, rowSmall, i))
	}
}

func TestCopyTo(t *testing.T) {
	t.Parallel()

	chunk := NewChunkWithCapacity(allTypes, 101)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for k := 0; k < 100; k++ {
		for i := 0; i < len(allTypes); i++ {
			switch allTypes[i].Tp {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
				if mysql.HasUnsignedFlag(allTypes[i].Flag) {
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
				t.FailNow()
			}
		}
	}

	ck1 := chunk.CopyConstruct()

	for k := 0; k < 101; k++ {
		row := chunk.GetRow(k)
		r1 := ck1.GetRow(k)
		for i := 0; i < len(allTypes); i++ {
			cmpFunc := GetCompareFunc(allTypes[i])
			assert.Zero(t, cmpFunc(row, i, r1, i))
		}

	}
}

func TestGetDecimalDatum(t *testing.T) {
	t.Parallel()

	datum := types.NewDatum(1.01)
	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decType.Flen = 4
	decType.Decimal = 2
	sc := new(stmtctx.StatementContext)
	decDatum, err := datum.ConvertTo(sc, decType)
	require.NoError(t, err)

	chk := NewChunkWithCapacity([]*types.FieldType{decType}, 32)
	chk.AppendMyDecimal(0, decDatum.GetMysqlDecimal())
	decFromChk := chk.GetRow(0).GetDatum(0, decType)
	assert.Equal(t, decFromChk.Length(), decDatum.Length())
	assert.Equal(t, decFromChk.Frac(), decDatum.Frac())
}

func TestChunkMemoryUsage(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

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
	assert.Equal(t, int64(expectedUsage), chk.MemoryUsage())

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

	assert.Equal(t, int64(expectedUsage), chk.MemoryUsage())

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
	assert.Equal(t, int64(expectedUsage), chk.MemoryUsage())
}

func TestSwapColumn(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})

	// chk1: column1 refers to column0
	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.MakeRef(0, 1)
	chk1.AppendFloat32(2, 3)
	row1 := chk1.GetRow(0)

	// chk2: column1 refers to column0
	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	chk2.AppendFloat32(0, 12)
	chk2.MakeRef(0, 1)
	chk2.AppendFloat32(2, 32)
	row2 := chk2.GetRow(0)

	// swap preserves ref
	checkRef := func() {
		assert.Same(t, chk1.Column(0), chk1.Column(1))
		assert.Same(t, chk2.Column(0), chk2.Column(1))
		assert.NotSame(t, chk2.Column(0), chk1.Column(0))
		assert.NotSame(t, chk1.Column(0), chk1.Column(2))
		assert.NotSame(t, chk2.Column(0), chk2.Column(2))
	}
	checkRef()

	// swap two chunk's columns
	require.NoError(t, chk1.SwapColumn(0, chk2, 0))
	checkRef()
	assert.Equal(t, row1.GetFloat32(0), float32(12))
	assert.Equal(t, row1.GetFloat32(1), float32(12))
	assert.Equal(t, row2.GetFloat32(0), float32(1))
	assert.Equal(t, row2.GetFloat32(1), float32(1))

	require.NoError(t, chk1.SwapColumn(0, chk2, 0))
	checkRef()

	// swap reference and referenced columns
	require.NoError(t, chk2.SwapColumn(1, chk2, 0))
	checkRef()
	assert.Equal(t, row2.GetFloat32(0), float32(12))
	assert.Equal(t, row2.GetFloat32(1), float32(12))
	assert.Equal(t, row2.GetFloat32(2), float32(32))

	// swap src = dst
	require.NoError(t, chk2.SwapColumn(1, chk2, 1))
	checkRef()
	assert.Equal(t, row2.GetFloat32(0), float32(12))
	assert.Equal(t, row2.GetFloat32(1), float32(12))
	assert.Equal(t, row2.GetFloat32(2), float32(32))

	// swap reference and another column
	require.NoError(t, chk2.SwapColumn(1, chk2, 2))
	checkRef()
	assert.Equal(t, row2.GetFloat32(0), float32(32))
	assert.Equal(t, row2.GetFloat32(1), float32(32))
	assert.Equal(t, row2.GetFloat32(2), float32(12))

	require.NoError(t, chk2.SwapColumn(2, chk2, 0))
	checkRef()
	assert.Equal(t, row2.GetFloat32(0), float32(12))
	assert.Equal(t, row2.GetFloat32(1), float32(12))
	assert.Equal(t, row2.GetFloat32(2), float32(32))
}

func TestAppendSel(t *testing.T) {
	t.Parallel()

	tll := &types.FieldType{Tp: mysql.TypeLonglong}
	chk := NewChunkWithCapacity([]*types.FieldType{tll}, 1024)
	sel := make([]int, 0, 1024/2)
	for i := 0; i < 1024/2; i++ {
		chk.AppendInt64(0, int64(i))
		if i%2 == 0 {
			sel = append(sel, i)
		}
	}
	chk.SetSel(sel)
	assert.Equal(t, 1024/2/2, chk.NumRows())
	chk.AppendInt64(0, int64(1))
	assert.Equal(t, 1024/2/2+1, chk.NumRows())
	sel = chk.Sel()
	assert.Equal(t, 1024/2, sel[len(sel)-1])

}

func TestMakeRefTo(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})

	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.AppendFloat32(1, 3)

	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	err := chk2.MakeRefTo(0, chk1, 1)
	require.NoError(t, err)

	err = chk2.MakeRefTo(1, chk1, 0)
	require.NoError(t, err)

	assert.Same(t, chk1.Column(1), chk2.Column(0))
	assert.Same(t, chk1.Column(0), chk2.Column(1))
}

func TestToString(t *testing.T) {
	t.Parallel()

	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDouble})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeString})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDate})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeLonglong})

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

	assert.Equal(t, "1, 1, 1, 0000-00-00, 1\n2, 2, 2, 0000-00-00 00:00:00, 2\n", chk.ToString(fieldTypes))
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
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

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
	for chk.NumRows() < chk.RequiredRows() {
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
		chk := New([]*types.FieldType{{Tp: mysql.TypeLong}}, t.initCap, t.maxCap)
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
						chk = New([]*types.FieldType{{Tp: mysql.TypeLong}}, t.initCap, t.maxCap)
					}
				}
			}
		}
	}
}

func (s *testChunkSuite) TestAppendRows(c *check.C) {
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
	c.Assert(chk.NumCols(), check.Equals, numCols)
	c.Assert(chk.NumRows(), check.Equals, numRows)

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	c.Assert(chk.NumCols(), check.Equals, numCols)
	rows := make([]Row, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = chk.GetRow(i)
	}
	chk2.AppendRows(rows)
	for i := 0; i < numRows; i++ {
		row := chk2.GetRow(i)
		c.Assert(row.GetInt64(0), check.Equals, int64(0))
		c.Assert(row.IsNull(0), check.IsTrue)
		c.Assert(row.GetInt64(1), check.Equals, int64(i))
		str := fmt.Sprintf(strFmt, i)
		c.Assert(row.IsNull(2), check.IsFalse)
		c.Assert(row.GetString(2), check.Equals, str)
		c.Assert(row.IsNull(3), check.IsFalse)
		c.Assert(row.GetBytes(3), check.BytesEquals, []byte(str))
		c.Assert(row.IsNull(4), check.IsFalse)
		c.Assert(row.GetMyDecimal(4).String(), check.Equals, str)
		c.Assert(row.IsNull(5), check.IsFalse)
		c.Assert(string(row.GetJSON(5).GetString()), check.Equals, str)
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
