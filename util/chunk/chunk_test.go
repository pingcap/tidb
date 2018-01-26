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
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testChunkSuite{})

type testChunkSuite struct{}

func (s *testChunkSuite) TestChunk(c *check.C) {
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
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
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
		c.Assert(hack.String(row.GetJSON(5).GetString()), check.Equals, str)
	}

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		chk2.AppendRow(row)
	}
	for i := 0; i < numCols; i++ {
		col2, col1 := chk2.columns[i], chk.columns[i]
		col2.elemBuf, col1.elemBuf = nil, nil
		c.Assert(col2, check.DeepEquals, col1)
	}

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
	c.Assert(row.GetFloat32(0), check.Equals, f32Val)
	c.Assert(row.GetTime(2).Compare(tVal), check.Equals, 0)
	c.Assert(row.GetDuration(3), check.DeepEquals, durVal)
	c.Assert(row.GetEnum(4), check.DeepEquals, enumVal)
	c.Assert(row.GetSet(5), check.DeepEquals, setVal)

	// AppendPartialRow can be different number of columns, useful for join.
	chk = newChunk(8, 8)
	chk2 = newChunk(8)
	chk2.AppendInt64(0, 1)
	chk2.AppendInt64(0, -1)
	chk.AppendPartialRow(0, chk2.GetRow(0))
	chk.AppendPartialRow(1, chk2.GetRow(0))
	c.Assert(chk.GetRow(0).GetInt64(0), check.Equals, int64(1))
	c.Assert(chk.GetRow(0).GetInt64(1), check.Equals, int64(1))
	c.Assert(chk.NumRows(), check.Equals, 1)

	// Test Reset.
	chk = newChunk(0)
	chk.AppendString(0, "abcd")
	chk.Reset()
	chk.AppendString(0, "def")
	c.Assert(chk.GetRow(0).GetString(0), check.Equals, "def")

	// Test float32
	chk = newChunk(4)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	c.Assert(chk.GetRow(2).GetFloat32(0), check.Equals, float32(1))
}

func (s *testChunkSuite) TestAppend(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	c.Assert(err, check.IsNil)

	src := NewChunk(fieldTypes)
	dst := NewChunk(fieldTypes)

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

	c.Assert(len(dst.columns), check.Equals, 3)

	c.Assert(dst.columns[0].length, check.Equals, 12)
	c.Assert(dst.columns[0].nullCount, check.Equals, 6)
	c.Assert(string(dst.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(dst.columns[0].offsets), check.Equals, 0)
	c.Assert(len(dst.columns[0].data), check.Equals, 4*12)
	c.Assert(len(dst.columns[0].elemBuf), check.Equals, 4)

	c.Assert(dst.columns[1].length, check.Equals, 12)
	c.Assert(dst.columns[1].nullCount, check.Equals, 6)
	c.Assert(string(dst.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(string(dst.columns[1].offsets), check.Equals, string([]int32{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}))
	c.Assert(string(dst.columns[1].data), check.Equals, "abcabcabcabcabcabc")
	c.Assert(len(dst.columns[1].elemBuf), check.Equals, 0)

	c.Assert(dst.columns[2].length, check.Equals, 12)
	c.Assert(dst.columns[2].nullCount, check.Equals, 6)
	c.Assert(string(dst.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(dst.columns[2].offsets), check.Equals, 13)
	c.Assert(len(dst.columns[2].data), check.Equals, 150)
	c.Assert(len(dst.columns[2].elemBuf), check.Equals, 0)
	for i := 0; i < 12; i += 2 {
		jsonElem := dst.GetRow(i).GetJSON(2)
		cmpRes := json.CompareBinary(jsonElem, jsonObj)
		c.Assert(cmpRes, check.Equals, 0)
	}
}

func (s *testChunkSuite) TestTruncateTo(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	c.Assert(err, check.IsNil)

	src := NewChunk(fieldTypes)

	for i := 0; i < 8; i++ {
		src.AppendFloat32(0, 12.8)
		src.AppendString(1, "abc")
		src.AppendJSON(2, jsonObj)
		src.AppendNull(0)
		src.AppendNull(1)
		src.AppendNull(2)
	}

	src.TruncateTo(16)
	src.TruncateTo(16)
	src.TruncateTo(14)
	src.TruncateTo(12)
	c.Assert(len(src.columns), check.Equals, 3)

	c.Assert(src.columns[0].length, check.Equals, 12)
	c.Assert(src.columns[0].nullCount, check.Equals, 6)
	c.Assert(string(src.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x55}))
	c.Assert(len(src.columns[0].offsets), check.Equals, 0)
	c.Assert(len(src.columns[0].data), check.Equals, 4*12)
	c.Assert(len(src.columns[0].elemBuf), check.Equals, 4)

	c.Assert(src.columns[1].length, check.Equals, 12)
	c.Assert(src.columns[1].nullCount, check.Equals, 6)
	c.Assert(string(src.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x55}))
	c.Assert(string(src.columns[1].offsets), check.Equals, string([]int32{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}))
	c.Assert(string(src.columns[1].data), check.Equals, "abcabcabcabcabcabc")
	c.Assert(len(src.columns[1].elemBuf), check.Equals, 0)

	c.Assert(src.columns[2].length, check.Equals, 12)
	c.Assert(src.columns[2].nullCount, check.Equals, 6)
	c.Assert(string(src.columns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x55}))
	c.Assert(len(src.columns[2].offsets), check.Equals, 13)
	c.Assert(len(src.columns[2].data), check.Equals, 150)
	c.Assert(len(src.columns[2].elemBuf), check.Equals, 0)
	for i := 0; i < 12; i += 2 {
		row := src.GetRow(i)
		jsonElem := row.GetJSON(2)
		cmpRes := json.CompareBinary(jsonElem, jsonObj)
		c.Assert(cmpRes, check.Equals, 0)
	}
}

// newChunk creates a new chunk and initialize columns with element length.
// 0 adds an varlen column, positive len add a fixed length column, negative len adds a interface column.
func newChunk(elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.addFixedLenColumn(l, 0)
		} else {
			chk.addVarLenColumn(0)
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

func (s *testChunkSuite) TestCompare(c *check.C) {
	chunk := NewChunk(allTypes)
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
			c.FailNow()
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
			c.FailNow()
		}
	}
	rowNull := chunk.GetRow(0)
	rowSmall := chunk.GetRow(1)
	rowBig := chunk.GetRow(2)
	for i := 0; i < len(allTypes); i++ {
		cmpFunc := GetCompareFunc(allTypes[i])
		c.Assert(cmpFunc(rowNull, i, rowNull, i), check.Equals, 0)
		c.Assert(cmpFunc(rowNull, i, rowSmall, i), check.Equals, -1)
		c.Assert(cmpFunc(rowSmall, i, rowNull, i), check.Equals, 1)
		c.Assert(cmpFunc(rowSmall, i, rowSmall, i), check.Equals, 0)
		c.Assert(cmpFunc(rowSmall, i, rowBig, i), check.Equals, -1, check.Commentf("%d", allTypes[i].Tp))
		c.Assert(cmpFunc(rowBig, i, rowSmall, i), check.Equals, 1)
		c.Assert(cmpFunc(rowBig, i, rowBig, i), check.Equals, 0)
	}
}

func (s *testChunkSuite) TestGetDecimalDatum(c *check.C) {
	datum := types.NewDatum(1.01)
	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decType.Flen = 4
	decType.Decimal = 2
	sc := new(stmtctx.StatementContext)
	decDatum, err := datum.ConvertTo(sc, decType)
	c.Assert(err, check.IsNil)
	chk := NewChunk([]*types.FieldType{decType})
	chk.AppendMyDecimal(0, decDatum.GetMysqlDecimal())
	decFromChk := chk.GetRow(0).GetDatum(0, decType)
	c.Assert(decDatum.Length(), check.Equals, decFromChk.Length())
	c.Assert(decDatum.Frac(), check.Equals, decFromChk.Frac())
}

func (s *testChunkSuite) TestChunkMemoryUsage(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

	initCap := 10
	chk := NewChunkWithCapacity(fieldTypes, initCap)

	//cap(c.nullBitmap) + cap(c.offsets)*4 + cap(c.data) + cap(c.elemBuf)
	colUsage := make([]int, len(fieldTypes))
	colUsage[0] = initCap>>3 + 0 + initCap*4 + 4
	colUsage[1] = initCap>>3 + (initCap+1)*4 + initCap*4 + 0
	colUsage[2] = initCap>>3 + (initCap+1)*4 + initCap*4 + 0
	colUsage[3] = initCap>>3 + 0 + initCap*16 + 16
	colUsage[4] = initCap>>3 + 0 + initCap*16 + 16

	expectedUsage := 0
	for i := range colUsage {
		expectedUsage += colUsage[i] + int(unsafe.Sizeof(*chk.columns[i]))
	}
	memUsage := chk.MemoryUsage()
	c.Assert(memUsage, check.Equals, int64(expectedUsage))

	jsonObj, err := json.ParseBinaryFromString("1")
	c.Assert(err, check.IsNil)
	timeObj := types.Time{Time: types.FromGoTime(time.Now()), Fsp: 0, Type: mysql.TypeDatetime}
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	memUsage = chk.MemoryUsage()
	c.Assert(memUsage, check.Equals, int64(expectedUsage))

	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123111111111111111111111111111111111111111111111")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	memUsage = chk.MemoryUsage()
	colUsage[1] = initCap>>3 + (initCap+1)*4 + cap(chk.columns[1].data) + 0
	expectedUsage = 0
	for i := range colUsage {
		expectedUsage += colUsage[i] + int(unsafe.Sizeof(*chk.columns[i]))
	}
	c.Assert(memUsage, check.Equals, int64(expectedUsage))
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
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 1024)
	}
}

func BenchmarkAppendBytes512(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 512)
	}
}

func BenchmarkAppendBytes256(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 256)
	}
}

func BenchmarkAppendBytes128(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 128)
	}
}

func BenchmarkAppendBytes64(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 64)
	}
}

func BenchmarkAppendBytes32(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 32)
	}
}

func BenchmarkAppendBytes16(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 16)
	}
}

func BenchmarkAppendBytes8(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 8)
	}
}

func BenchmarkAppendBytes4(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 4)
	}
}

func BenchmarkAppendBytes2(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 2)
	}
}

func BenchmarkAppendBytes1(b *testing.B) {
	chk := NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeString)})
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
