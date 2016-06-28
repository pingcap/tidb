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

package table

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testColumnSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testColumnSuite struct{}

func (s *testColumnSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	col := &Column{
		model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeTiny),
			State:     model.StatePublic,
		},
	}
	col.Flen = 2
	col.Decimal = 1
	col.Charset = mysql.DefaultCharset
	col.Collate = mysql.DefaultCollationName
	col.Flag |= mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag

	cs := col.String()
	c.Assert(len(cs), Greater, 0)

	col.Tp = mysql.TypeEnum
	col.Flag = 0
	col.Elems = []string{"a", "b"}

	c.Assert(col.GetTypeDesc(), Equals, "enum('a','b')")

	col.Elems = []string{"'a'", "b"}
	c.Assert(col.GetTypeDesc(), Equals, "enum('''a''','b')")

	col.Tp = mysql.TypeFloat
	col.Flen = 8
	col.Decimal = -1
	c.Assert(col.GetTypeDesc(), Equals, "float")

	col.Decimal = 1
	c.Assert(col.GetTypeDesc(), Equals, "float(8,1)")

	col.Tp = mysql.TypeDatetime
	col.Decimal = 6
	c.Assert(col.GetTypeDesc(), Equals, "datetime(6)")

	col.Decimal = 0
	c.Assert(col.GetTypeDesc(), Equals, "datetime")

	col.Decimal = -1
	c.Assert(col.GetTypeDesc(), Equals, "datetime")
}

func (s *testColumnSuite) TestFind(c *C) {
	defer testleak.AfterTest(c)()
	cols := []*Column{
		newCol("a"),
		newCol("b"),
		newCol("c"),
	}
	FindCols(cols, []string{"a"})
	FindCols(cols, []string{"d"})
	cols[0].Flag |= mysql.OnUpdateNowFlag
	FindOnUpdateCols(cols)
}

func (s *testColumnSuite) TestCheck(c *C) {
	defer testleak.AfterTest(c)()
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag
	cols := []*Column{col, col}
	CheckOnce(cols)
	cols = cols[:1]
	CheckNotNull(cols, types.MakeDatums(nil))
	cols[0].Flag |= mysql.NotNullFlag
	CheckNotNull(cols, types.MakeDatums(nil))
}

func (s *testColumnSuite) TestDesc(c *C) {
	defer testleak.AfterTest(c)()
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag | mysql.NotNullFlag | mysql.PriKeyFlag
	NewColDesc(col)
	col.Flag = mysql.MultipleKeyFlag
	NewColDesc(col)
	ColDescFieldNames(false)
	ColDescFieldNames(true)
}

func (s *testColumnSuite) TestGetZeroValue(c *C) {
	cases := []struct {
		ft    *types.FieldType
		value types.Datum
	}{
		{
			types.NewFieldType(mysql.TypeLong),
			types.NewIntDatum(0),
		},
		{
			&types.FieldType{
				Tp:   mysql.TypeLonglong,
				Flag: mysql.UnsignedFlag,
			},
			types.NewUintDatum(0),
		},
		{
			types.NewFieldType(mysql.TypeFloat),
			types.NewFloat32Datum(0),
		},
		{
			types.NewFieldType(mysql.TypeDouble),
			types.NewFloat64Datum(0),
		},
		{
			types.NewFieldType(mysql.TypeNewDecimal),
			types.NewDecimalDatum(mysql.NewDecimalFromInt(0, 0)),
		},
		{
			types.NewFieldType(mysql.TypeVarchar),
			types.NewStringDatum(""),
		},
		{
			types.NewFieldType(mysql.TypeBlob),
			types.NewBytesDatum([]byte{}),
		},
		{
			types.NewFieldType(mysql.TypeDuration),
			types.NewDurationDatum(mysql.ZeroDuration),
		},
		{
			types.NewFieldType(mysql.TypeDatetime),
			types.NewDatum(mysql.ZeroDatetime),
		},
		{
			types.NewFieldType(mysql.TypeTimestamp),
			types.NewDatum(mysql.ZeroTimestamp),
		},
		{
			types.NewFieldType(mysql.TypeDate),
			types.NewDatum(mysql.ZeroDate),
		},
		{
			types.NewFieldType(mysql.TypeBit),
			types.NewDatum(mysql.Bit{Value: 0, Width: mysql.MinBitWidth}),
		},
		{
			types.NewFieldType(mysql.TypeSet),
			types.NewDatum(mysql.Set{}),
		},
	}
	for _, ca := range cases {
		colInfo := &model.ColumnInfo{FieldType: *ca.ft}
		zv := getZeroValue(colInfo)
		c.Assert(zv.Kind(), Equals, ca.value.Kind())
		cmp, err := zv.CompareDatum(ca.value)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testColumnSuite) TestGetDefaultValue(c *C) {
	colInfo := &model.ColumnInfo{
		FieldType:    *types.NewFieldType(mysql.TypeLong),
		State:        model.StatePublic,
		DefaultValue: 1.0,
	}
	val, ok, err := GetColDefaultValue(nil, colInfo)
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	c.Assert(val.Kind(), Equals, types.KindInt64)
	c.Assert(val.GetInt64(), Equals, int64(1))
}

func newCol(name string) *Column {
	return &Column{
		model.ColumnInfo{
			Name:  model.NewCIStr(name),
			State: model.StatePublic,
		},
	}
}
