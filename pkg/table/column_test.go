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
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	col := ToColumn(&model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeTiny),
		State:     model.StatePublic,
	})
	col.SetFlen(2)
	col.SetDecimal(1)
	col.SetCharset(mysql.DefaultCharset)
	col.SetCollate(mysql.DefaultCollationName)
	col.AddFlag(mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag)

	require.Equal(t, "tinyint(2) unsigned zerofill", col.GetTypeDesc())
	col.ToInfo()
	tbInfo := &model.TableInfo{}
	require.False(t, col.IsPKHandleColumn(tbInfo))
	tbInfo.PKIsHandle = true
	col.AddFlag(mysql.PriKeyFlag)
	require.True(t, col.IsPKHandleColumn(tbInfo))

	cs := col.String()
	require.Greater(t, len(cs), 0)

	col.SetType(mysql.TypeEnum)
	col.SetFlag(0)
	col.SetElems([]string{"a", "b"})

	require.Equal(t, "enum('a','b')", col.GetTypeDesc())

	col.SetElems([]string{"'a'", "b"})
	require.Equal(t, "enum('''a''','b')", col.GetTypeDesc())

	col.SetType(mysql.TypeFloat)
	col.SetFlen(8)
	col.SetDecimal(-1)
	require.Equal(t, "float", col.GetTypeDesc())

	col.SetDecimal(1)
	require.Equal(t, "float(8,1)", col.GetTypeDesc())

	col.SetType(mysql.TypeDatetime)
	col.SetDecimal(6)
	require.Equal(t, "datetime(6)", col.GetTypeDesc())

	col.SetDecimal(0)
	require.Equal(t, "datetime", col.GetTypeDesc())

	col.SetDecimal(-1)
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

	cols[0].AddFlag(mysql.OnUpdateNowFlag)
	c2 := FindOnUpdateCols(cols)
	require.Equal(t, cols[:1], c2)
}

// checkNotNull checks if row has nil value set to a column with NotNull flag set.
func checkNotNull(cols []*Column, row []types.Datum) error {
	for _, c := range cols {
		if err := c.CheckNotNull(&row[c.Offset], 0); err != nil {
			return err
		}
	}
	return nil
}

func TestCheck(t *testing.T) {
	col := newCol("a")
	col.SetFlag(mysql.AutoIncrementFlag)
	cols := []*Column{col, col}
	err := CheckOnce(cols)
	require.Error(t, err)
	cols = cols[:1]
	err = checkNotNull(cols, types.MakeDatums(nil))
	require.NoError(t, err)
	cols[0].AddFlag(mysql.NotNullFlag)
	err = checkNotNull(cols, types.MakeDatums(nil))
	require.Error(t, err)
	err = CheckOnce([]*Column{})
	require.NoError(t, err)
}

func TestHandleBadNull(t *testing.T) {
	col := newCol("a")
	sc := stmtctx.NewStmtCtx()
	d := types.Datum{}
	err := col.HandleBadNull(sc.ErrCtx(), &d, 0)
	require.NoError(t, err)
	cmp, err := d.Compare(sc.TypeCtx(), &types.Datum{}, collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	col.AddFlag(mysql.NotNullFlag)
	err = col.HandleBadNull(sc.ErrCtx(), &types.Datum{}, 0)
	require.Error(t, err)

	var levels errctx.LevelMap
	levels[errctx.ErrGroupBadNull] = errctx.LevelWarn
	sc.SetErrLevels(levels)
	err = col.HandleBadNull(sc.ErrCtx(), &types.Datum{}, 0)
	require.NoError(t, err)
}

func TestDesc(t *testing.T) {
	col := newCol("a")
	col.SetFlag(mysql.AutoIncrementFlag | mysql.NotNullFlag | mysql.PriKeyFlag)
	NewColDesc(col)
	col.SetFlag(mysql.MultipleKeyFlag)
	NewColDesc(col)
	col.SetFlag(mysql.UniqueKeyFlag | mysql.OnUpdateNowFlag)
	desc := NewColDesc(col)
	require.Equal(t, "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", desc.Extra)
	col.SetFlag(0)
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
	tp1 := &types.FieldType{}
	tp1.SetType(mysql.TypeLonglong)
	tp1.SetFlag(mysql.UnsignedFlag)

	tp2 := &types.FieldType{}
	tp2.SetType(mysql.TypeString)
	tp2.SetFlen(2)
	tp2.SetCharset(charset.CharsetBin)
	tp2.SetCollate(charset.CollationBin)

	tp3 := &types.FieldType{}
	tp3.SetType(mysql.TypeString)
	tp3.SetFlen(2)
	tp3.SetCharset(charset.CharsetUTF8MB4)
	tp3.SetCollate(charset.CollationBin)

	tests := []struct {
		ft    *types.FieldType
		value types.Datum
	}{
		{
			types.NewFieldType(mysql.TypeLong),
			types.NewIntDatum(0),
		},
		{
			tp1,
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
			tp2,
			types.NewDatum(make([]byte, 2)),
		},
		{
			tp3,
			types.NewDatum(""),
		},
		{
			types.NewFieldType(mysql.TypeJSON),
			types.NewDatum(types.CreateBinaryJSON(nil)),
		},
	}
	sc := stmtctx.NewStmtCtx()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%+v", tt.ft), func(t *testing.T) {
			colInfo := &model.ColumnInfo{FieldType: *tt.ft}
			zv := GetZeroValue(colInfo)
			require.Equal(t, tt.value.Kind(), zv.Kind())
			cmp, err := zv.Compare(sc.TypeCtx(), &tt.value, collate.GetCollator(tt.ft.GetCollate()))
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
	colInfo.SetCharset(mysql.UTF8Charset)
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

	colInfoS.SetCharset(mysql.UTF8Charset)
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x8c, 0x80}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.SetCharset(mysql.UTF8Charset)
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x8c, 0x80}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.SetCharset(mysql.UTF8MB4Charset)
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x80}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.SetCharset(mysql.UTF8MB4Charset)
	_, err = CastValue(ctx, types.NewDatum([]byte{0xf0, 0x9f, 0x80}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.SetCharset(charset.CharsetASCII)
	_, err = CastValue(ctx, types.NewDatum([]byte{0x32, 0xf0}), &colInfoS, false, false)
	require.Error(t, err)

	colInfoS.SetCharset(charset.CharsetASCII)
	_, err = CastValue(ctx, types.NewDatum([]byte{0x32, 0xf0}), &colInfoS, false, true)
	require.NoError(t, err)

	colInfoS.SetCharset(charset.CharsetUTF8MB4)
	colInfoS.SetCollate("utf8mb4_general_ci")
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
	tz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	ctx.ResetSessionAndStmtTimeZone(tz)
	zeroTimestamp := types.ZeroTimestamp
	timestampValue := types.NewTime(types.FromDate(2019, 5, 6, 12, 48, 49, 0), mysql.TypeTimestamp, types.DefaultFsp)
	timestampValueUTC := types.NewTime(timestampValue.CoreTime(), timestampValue.Type(), timestampValue.Fsp())
	err = timestampValueUTC.ConvertTimeZone(tz, time.UTC)
	require.NoError(t, err)

	tp0 := types.FieldType{}
	tp0.SetType(mysql.TypeLonglong)

	tp1 := types.FieldType{}
	tp1.SetType(mysql.TypeLonglong)
	tp1.SetFlag(mysql.NotNullFlag)

	tp2 := types.FieldType{}
	tp2.SetType(mysql.TypeEnum)
	tp2.SetFlag(mysql.NotNullFlag)
	tp2.SetElems([]string{"abc", "def"})
	tp2.SetCollate(mysql.DefaultCollationName)

	tp3 := types.FieldType{}
	tp3.SetType(mysql.TypeTimestamp)
	tp3.SetFlag(mysql.TimestampFlag)

	tp4 := types.FieldType{}
	tp4.SetType(mysql.TypeLonglong)
	tp4.SetFlag(mysql.NotNullFlag | mysql.AutoIncrementFlag)

	tests := []struct {
		colInfo *model.ColumnInfo
		strict  bool
		val     types.Datum
		err     error
	}{
		{
			&model.ColumnInfo{
				FieldType:          tp1,
				OriginDefaultValue: 1.0,
				DefaultValue:       1.0,
			},
			false,
			types.NewIntDatum(1),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: tp1,
			},
			false,
			types.NewIntDatum(0),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: tp0,
			},
			false,
			types.Datum{},
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: tp2,
			},
			false,
			types.NewMysqlEnumDatum(types.Enum{Name: "abc", Value: 1}),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType:          tp3,
				OriginDefaultValue: "0000-00-00 00:00:00",
				DefaultValue:       "0000-00-00 00:00:00",
			},
			false,
			types.NewDatum(zeroTimestamp),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType:          tp3,
				OriginDefaultValue: timestampValueUTC.String(),
				DefaultValue:       timestampValueUTC.String(),
				Version:            model.ColumnInfoVersion2,
			},
			true,
			types.NewDatum(timestampValue),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType:          tp3,
				OriginDefaultValue: "not valid date",
				DefaultValue:       "not valid date",
			},
			true,
			types.NewDatum(zeroTimestamp),
			errGetDefaultFailed,
		},
		{
			&model.ColumnInfo{
				FieldType: tp1,
			},
			true,
			types.NewDatum(zeroTimestamp),
			ErrNoDefaultValue,
		},
		{
			&model.ColumnInfo{
				FieldType: tp4,
			},
			true,
			types.NewIntDatum(0),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType:     tp1,
				DefaultIsExpr: true,
				DefaultValue:  "1",
			},
			false,
			nilDt,
			nil,
		},
	}

	exp := expression.EvalSimpleAst
	expression.EvalSimpleAst = func(sctx expression.BuildContext, expr ast.ExprNode) (types.Datum, error) {
		return types.NewIntDatum(1), nil
	}
	defer func() {
		expression.EvalSimpleAst = exp
	}()

	defaultMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	require.True(t, defaultMode.HasStrictMode())
	for _, tt := range tests {
		sc := ctx.GetSessionVars().StmtCtx
		if tt.strict {
			ctx.GetSessionVars().SQLMode = defaultMode
		} else {
			ctx.GetSessionVars().SQLMode = mysql.DelSQLMode(defaultMode, mysql.ModeStrictAllTables|mysql.ModeStrictTransTables)
		}
		levels := sc.ErrLevels()
		levels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !tt.strict)
		sc.SetErrLevels(levels)
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
		sc := ctx.GetSessionVars().StmtCtx
		if tt.strict {
			ctx.GetSessionVars().SQLMode = defaultMode
		} else {
			ctx.GetSessionVars().SQLMode = mysql.DelSQLMode(defaultMode, mysql.ModeStrictAllTables|mysql.ModeStrictTransTables)
		}
		levels := sc.ErrLevels()
		levels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !tt.strict)
		sc.SetErrLevels(levels)
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
