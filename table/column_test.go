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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testTableSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	col := ToColumn(&model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeTiny),
		State:     model.StatePublic,
	})
	col.Flen = 2
	col.Decimal = 1
	col.Charset = mysql.DefaultCharset
	col.Collate = mysql.DefaultCollationName
	col.Flag |= mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag

	c.Assert(col.GetTypeDesc(), Equals, "tinyint(2,1) UNSIGNED")
	col.ToInfo()
	tbInfo := &model.TableInfo{}
	c.Assert(col.IsPKHandleColumn(tbInfo), Equals, false)
	tbInfo.PKIsHandle = true
	col.Flag |= mysql.PriKeyFlag
	c.Assert(col.IsPKHandleColumn(tbInfo), Equals, true)

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

func (s *testTableSuite) TestFind(c *C) {
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

func (s *testTableSuite) TestCheck(c *C) {
	defer testleak.AfterTest(c)()
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag
	cols := []*Column{col, col}
	CheckOnce(cols)
	cols = cols[:1]
	CheckNotNull(cols, types.MakeDatums(nil))
	cols[0].Flag |= mysql.NotNullFlag
	CheckNotNull(cols, types.MakeDatums(nil))
	CheckOnce([]*Column{})
}

func (s *testTableSuite) TestDesc(c *C) {
	defer testleak.AfterTest(c)()
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag | mysql.NotNullFlag | mysql.PriKeyFlag
	NewColDesc(col)
	col.Flag = mysql.MultipleKeyFlag
	NewColDesc(col)
	col.Flag = mysql.UniqueKeyFlag | mysql.OnUpdateNowFlag
	desc := NewColDesc(col)
	c.Assert(desc.Extra, Equals, "on update CURRENT_TIMESTAMP")
	col.Flag = 0
	col.GeneratedExprString = "test"
	col.GeneratedStored = true
	desc = NewColDesc(col)
	c.Assert(desc.Extra, Equals, "STORED GENERATED")
	col.GeneratedStored = false
	desc = NewColDesc(col)
	c.Assert(desc.Extra, Equals, "VIRTUAL GENERATED")
	ColDescFieldNames(false)
	ColDescFieldNames(true)
}

func (s *testTableSuite) TestGetZeroValue(c *C) {
	tests := []struct {
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
			types.NewDecimalDatum(types.NewDecFromInt(0)),
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
			types.NewDurationDatum(types.ZeroDuration),
		},
		{
			types.NewFieldType(mysql.TypeDatetime),
			types.NewDatum(types.ZeroDatetime),
		},
		{
			types.NewFieldType(mysql.TypeTimestamp),
			types.NewDatum(types.ZeroTimestamp),
		},
		{
			types.NewFieldType(mysql.TypeDate),
			types.NewDatum(types.ZeroDate),
		},
		{
			types.NewFieldType(mysql.TypeBit),
			types.NewDatum(types.Bit{Value: 0, Width: types.MinBitWidth}),
		},
		{
			types.NewFieldType(mysql.TypeSet),
			types.NewDatum(types.Set{}),
		},
		{
			types.NewFieldType(mysql.TypeEnum),
			types.NewDatum(types.Enum{}),
		},
	}
	sc := new(variable.StatementContext)
	for _, tt := range tests {
		colInfo := &model.ColumnInfo{FieldType: *tt.ft}
		zv := GetZeroValue(colInfo)
		c.Assert(zv.Kind(), Equals, tt.value.Kind())
		cmp, err := zv.CompareDatum(sc, tt.value)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testTableSuite) TestCastValue(c *C) {
	ctx := mock.NewContext()
	colInfo := model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeLong),
		State:     model.StatePublic,
	}
	colInfo.Charset = mysql.UTF8Charset
	val, err := CastValue(ctx, types.Datum{}, &colInfo)
	c.Assert(err, Equals, nil)
	c.Assert(val.GetInt64(), Equals, int64(0))

	val, err = CastValue(ctx, types.NewDatum("test"), &colInfo)
	c.Assert(err, Not(Equals), nil)
	c.Assert(val.GetInt64(), Equals, int64(0))

	col := ToColumn(&model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeTiny),
		State:     model.StatePublic,
	})

	err = CastValues(ctx, []types.Datum{types.NewDatum("test")}, []*Column{col}, false)
	c.Assert(err, NotNil)
	err = CastValues(ctx, []types.Datum{types.NewDatum("test")}, []*Column{col}, true)
	c.Assert(err, IsNil)

	colInfoS := model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeString),
		State:     model.StatePublic,
	}
	val, err = CastValue(ctx, types.NewDatum("test"), &colInfoS)
	c.Assert(err, IsNil)
	c.Assert(val, NotNil)
}

func (s *testTableSuite) TestGetDefaultValue(c *C) {
	ctx := mock.NewContext()
	zeroTimestamp := types.ZeroTimestamp
	zeroTimestamp.TimeZone = ctx.GetSessionVars().GetTimeZone()
	tests := []struct {
		colInfo *model.ColumnInfo
		strict  bool
		val     types.Datum
		err     error
	}{
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
				OriginDefaultValue: 1.0,
				DefaultValue:       1.0,
			},
			false,
			types.NewIntDatum(1),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
			},
			false,
			types.NewIntDatum(0),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp: mysql.TypeLonglong,
				},
			},
			false,
			types.Datum{},
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:    mysql.TypeEnum,
					Flag:  mysql.NotNullFlag,
					Elems: []string{"abc", "def"},
				},
			},
			false,
			types.NewStringDatum("abc"),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.TimestampFlag,
				},
				OriginDefaultValue: "0000-00-00 00:00:00",
				DefaultValue:       "0000-00-00 00:00:00",
			},
			false,
			types.NewDatum(zeroTimestamp),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
			},
			true,
			types.NewDatum(zeroTimestamp),
			errNoDefaultValue,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag | mysql.AutoIncrementFlag,
				},
			},
			true,
			types.Datum{},
			nil,
		},
	}

	for _, tt := range tests {
		ctx.GetSessionVars().StrictSQLMode = tt.strict
		val, err := GetColDefaultValue(ctx, tt.colInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		c.Assert(val, DeepEquals, tt.val)
	}

	for _, tt := range tests {
		ctx.GetSessionVars().StrictSQLMode = tt.strict
		val, err := GetColOriginDefaultValue(ctx, tt.colInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		c.Assert(val, DeepEquals, tt.val)
	}

}

func newCol(name string) *Column {
	return ToColumn(&model.ColumnInfo{
		Name:  model.NewCIStr(name),
		State: model.StatePublic,
	})
}
