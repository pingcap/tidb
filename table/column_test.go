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

package table

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	col := ToColumn(&model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeTiny),
		State:     model.StatePublic,
	})
	col.Flen = 2
	col.Decimal = 1
	col.Charset = mysql.DefaultCharset
	col.Collate = mysql.DefaultCollationName
	col.Flag |= mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag

	require.Equal(t, "tinyint(2) unsigned zerofill", col.GetTypeDesc())
	col.ToInfo()
	tbInfo := &model.TableInfo{}
	require.False(t, col.IsPKHandleColumn(tbInfo))
	tbInfo.PKIsHandle = true
	col.Flag |= mysql.PriKeyFlag
	require.True(t, col.IsPKHandleColumn(tbInfo))

	cs := col.String()
	require.Greater(t, len(cs), 0)

	col.Tp = mysql.TypeEnum
	col.Flag = 0
	col.Elems = []string{"a", "b"}

	require.Equal(t, "enum('a','b')", col.GetTypeDesc())

	col.Elems = []string{"'a'", "b"}
	require.Equal(t, "enum('''a''','b')", col.GetTypeDesc())

	col.Tp = mysql.TypeFloat
	col.Flen = 8
	col.Decimal = -1
	require.Equal(t, "float", col.GetTypeDesc())

	col.Decimal = 1
	require.Equal(t, "float(8,1)", col.GetTypeDesc())

	col.Tp = mysql.TypeDatetime
	col.Decimal = 6
	require.Equal(t, "datetime(6)", col.GetTypeDesc())

	col.Decimal = 0
	require.Equal(t, "datetime", col.GetTypeDesc())

	col.Decimal = -1
	require.Equal(t, "datetime", col.GetTypeDesc())
}

func TestFind(t *testing.T) {
	cols := []*Column{
		newCol("a"),
		newCol("b"),
		newCol("c"),
	}
	c, s := FindCols(cols, []string{"a"}, true)
	require.Equal(t, cols[:1], c)
	require.Equal(t, "", s)

	c1, s1 := FindCols(cols, []string{"d"}, true)
	require.Nil(t, c1)
	require.Equal(t, "d", s1)

	cols[0].Flag |= mysql.OnUpdateNowFlag
	c2 := FindOnUpdateCols(cols)
	require.Equal(t, cols[:1], c2)
}

func TestCheck(t *testing.T) {
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag
	cols := []*Column{col, col}
	err := CheckOnce(cols)
	require.Error(t, err)
	cols = cols[:1]
	err = CheckNotNull(cols, types.MakeDatums(nil))
	require.NoError(t, err)
	cols[0].Flag |= mysql.NotNullFlag
	err = CheckNotNull(cols, types.MakeDatums(nil))
	require.Error(t, err)
	err = CheckOnce([]*Column{})
	require.NoError(t, err)
}

func TestHandleBadNull(t *testing.T) {
	col := newCol("a")
	sc := new(stmtctx.StatementContext)
	d := types.Datum{}
	err := col.HandleBadNull(&d, sc)
	require.NoError(t, err)
	cmp, err := d.Compare(sc, &types.Datum{}, collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	col.Flag |= mysql.NotNullFlag
	err = col.HandleBadNull(&types.Datum{}, sc)
	require.Error(t, err)

	sc.BadNullAsWarning = true
	err = col.HandleBadNull(&types.Datum{}, sc)
	require.NoError(t, err)
}

func TestDesc(t *testing.T) {
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag | mysql.NotNullFlag | mysql.PriKeyFlag
	NewColDesc(col)
	col.Flag = mysql.MultipleKeyFlag
	NewColDesc(col)
	col.Flag = mysql.UniqueKeyFlag | mysql.OnUpdateNowFlag
	desc := NewColDesc(col)
	require.Equal(t, "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", desc.Extra)
	col.Flag = 0
	col.GeneratedExprString = "test"
	col.GeneratedStored = true
	desc = NewColDesc(col)
	require.Equal(t, "STORED GENERATED", desc.Extra)
	col.GeneratedStored = false
	desc = NewColDesc(col)
	require.Equal(t, "VIRTUAL GENERATED", desc.Extra)
	ColDescFieldNames(false)
	ColDescFieldNames(true)
}

func TestGetZeroValue(t *testing.T) {
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
			types.NewStringDatum(""),
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
			types.NewMysqlBitDatum(types.ZeroBinaryLiteral),
		},
		{
			types.NewFieldType(mysql.TypeSet),
			types.NewDatum(types.Set{}),
		},
		{
			types.NewFieldType(mysql.TypeEnum),
			types.NewDatum(types.Enum{}),
		},
		{
			&types.FieldType{
				Tp:      mysql.TypeString,
				Flen:    2,
				Charset: charset.CharsetBin,
				Collate: charset.CollationBin,
			},
			types.NewDatum(make([]byte, 2)),
		},
		{
			&types.FieldType{
				Tp:      mysql.TypeString,
				Flen:    2,
				Charset: charset.CharsetUTF8MB4,
				Collate: charset.CollationBin,
			},
			types.NewDatum(""),
		},
		{
			types.NewFieldType(mysql.TypeJSON),
			types.NewDatum(json.CreateBinary(nil)),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%+v", tt.ft), func(t *testing.T) {
			colInfo := &model.ColumnInfo{FieldType: *tt.ft}
			zv := GetZeroValue(colInfo)
			require.Equal(t, tt.value.Kind(), zv.Kind())
			cmp, err := zv.Compare(sc, &tt.value, collate.GetCollator(tt.ft.Collate))
			require.NoError(t, err)
			require.Equal(t, 0, cmp)
		})
	}
}

func TestCastValue(t *testing.T) {
	ctx := mock.NewContext()
	colInfo := model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeLong),
		State:     model.StatePublic,
	}
	colInfo.Charset = mysql.UTF8Charset
	val, err := CastValue(ctx, types.Datum{}, &colInfo, false, false)
	require.NoError(t, err)
	require.Equal(t, int64(0), val.GetInt64())

	val, err = CastValue(ctx, types.NewDatum("test"), &colInfo, false, false)
	require.Error(t, err)
	require.Equal(t, int64(0), val.GetInt64())

	colInfoS := model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeString),
		State:     model.StatePublic,
	}
	val, err = CastValue(ctx, types.NewDatum("test"), &colInfoS, false, false)
	require.NoError(t, err)
	require.NotNil(t, val)

	colInfoS.Charset = mysql.UTF8Charset
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x8c, 0x80}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.Charset = mysql.UTF8Charset
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x8c, 0x80}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.Charset = mysql.UTF8MB4Charset
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x80}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.Charset = mysql.UTF8MB4Charset
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x80}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.Charset = charset.CharsetASCII
	_, err = CastValue(ctx, types.NewDatum([]byte{0x32, 0xf0}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.Charset = charset.CharsetASCII
	_, err = CastValue(ctx, types.NewDatum([]byte{0x32, 0xf0}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.Charset = charset.CharsetUTF8MB4
	colInfoS.Collate = "utf8mb4_general_ci"
	val, err = CastValue(ctx, types.NewBinaryLiteralDatum([]byte{0xE5, 0xA5, 0xBD}), &colInfoS, false, false)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", val.Collation())
	val, err = CastValue(ctx, types.NewBinaryLiteralDatum([]byte{0xE5, 0xA5, 0xBD, 0x81}), &colInfoS, false, false)
	require.Error(t, err, "[table:1366]Incorrect string value '\\x81' for column ''")
	require.Equal(t, "utf8mb4_general_ci", val.Collation())
	val, err = CastValue(ctx, types.NewDatum([]byte{0xE5, 0xA5, 0xBD, 0x81}), &colInfoS, false, false)
	require.Error(t, err, "[table:1366]Incorrect string value '\\x81' for column ''")
	require.Equal(t, "utf8mb4_general_ci", val.Collation())
}

func TestGetDefaultValue(t *testing.T) {
	var nilDt types.Datum
	nilDt.SetNull()
	ctx := mock.NewContext()
	zeroTimestamp := types.ZeroTimestamp
	timestampValue := types.NewTime(types.FromDate(2019, 5, 6, 12, 48, 49, 0), mysql.TypeTimestamp, types.DefaultFsp)
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
					Tp:      mysql.TypeEnum,
					Flag:    mysql.NotNullFlag,
					Elems:   []string{"abc", "def"},
					Collate: mysql.DefaultCollationName,
				},
			},
			false,
			types.NewMysqlEnumDatum(types.Enum{Name: "abc", Value: 1}),
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
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.TimestampFlag,
				},
				OriginDefaultValue: timestampValue.String(),
				DefaultValue:       timestampValue.String(),
			},
			true,
			types.NewDatum(timestampValue),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.TimestampFlag,
				},
				OriginDefaultValue: "not valid date",
				DefaultValue:       "not valid date",
			},
			true,
			types.NewDatum(zeroTimestamp),
			errGetDefaultFailed,
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
			ErrNoDefaultValue,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag | mysql.AutoIncrementFlag,
				},
			},
			true,
			types.NewIntDatum(0),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
				DefaultIsExpr: true,
				DefaultValue:  "1",
			},
			false,
			nilDt,
			nil,
		},
	}

	exp := expression.EvalAstExpr
	expression.EvalAstExpr = func(sctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error) {
		return types.NewIntDatum(1), nil
	}
	defer func() {
		expression.EvalAstExpr = exp
	}()

	for _, tt := range tests {
		ctx.GetSessionVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetColDefaultValue(ctx, tt.colInfo)
		if err != nil {
			require.Errorf(t, tt.err, "%v", err)
			continue
		}
		if tt.colInfo.DefaultIsExpr {
			require.Equal(t, types.NewIntDatum(1), val)
		} else {
			require.Equal(t, tt.val, val)
		}
	}

	for _, tt := range tests {
		ctx.GetSessionVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetColOriginDefaultValue(ctx, tt.colInfo)
		if err != nil {
			require.Errorf(t, tt.err, "%v", err)
			continue
		}
		if !tt.colInfo.DefaultIsExpr {
			require.Equal(t, tt.val, val)
		}
	}
}

func newCol(name string) *Column {
	return ToColumn(&model.ColumnInfo{
		Name:  model.NewCIStr(name),
		State: model.StatePublic,
	})
}
