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

package decoder_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestRowDecoder(t *testing.T) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
	c3 := &model.ColumnInfo{ID: 3, Name: model.NewCIStr("c3"), State: model.StatePublic, Offset: 2, FieldType: *types.NewFieldType(mysql.TypeNewDecimal)}
	c4 := &model.ColumnInfo{ID: 4, Name: model.NewCIStr("c4"), State: model.StatePublic, Offset: 3, FieldType: *types.NewFieldType(mysql.TypeTimestamp)}
	c5 := &model.ColumnInfo{ID: 5, Name: model.NewCIStr("c5"), State: model.StatePublic, Offset: 4, FieldType: *types.NewFieldType(mysql.TypeDuration), OriginDefaultValue: "02:00:02"}
	c6 := &model.ColumnInfo{ID: 6, Name: model.NewCIStr("c6"), State: model.StatePublic, Offset: 5, FieldType: *types.NewFieldType(mysql.TypeTimestamp), GeneratedExprString: "c4+c5"}
	c7 := &model.ColumnInfo{ID: 7, Name: model.NewCIStr("c7"), State: model.StatePublic, Offset: 6, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c7.AddFlag(mysql.PriKeyFlag)

	cols := []*model.ColumnInfo{c1, c2, c3, c4, c5, c6, c7}

	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: true}
	tbl := tables.MockTableFromMeta(tblInfo)

	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	decodeColsMap := make(map[int64]decoder.Column, len(cols))
	decodeColsMap2 := make(map[int64]decoder.Column, len(cols))
	for _, col := range tbl.Cols() {
		tpExpr := decoder.Column{
			Col: col,
		}
		decodeColsMap2[col.ID] = tpExpr
		if col.GeneratedExprString != "" {
			expr, err := expression.ParseSimpleExprCastWithTableInfo(ctx, col.GeneratedExprString, tblInfo, &col.FieldType)
			require.NoError(t, err)
			tpExpr.GenExpr = expr
		}
		decodeColsMap[col.ID] = tpExpr
	}
	de := decoder.NewRowDecoder(tbl, tbl.Cols(), decodeColsMap)
	deWithNoGenCols := decoder.NewRowDecoder(tbl, tbl.Cols(), decodeColsMap2)

	time1 := types.NewTime(types.FromDate(2019, 01, 01, 8, 01, 01, 0), mysql.TypeTimestamp, types.DefaultFsp)
	t1 := types.NewTimeDatum(time1)
	d1 := types.NewDurationDatum(types.Duration{
		Duration: time.Hour + time.Second,
	})

	time2, err := time1.Add(sc, d1.GetMysqlDuration())
	require.Nil(t, err)
	t2 := types.NewTimeDatum(time2)

	time3, err := time1.Add(sc, types.Duration{Duration: time.Hour*2 + time.Second*2})
	require.Nil(t, err)
	t3 := types.NewTimeDatum(time3)

	testRows := []struct {
		cols   []int64
		input  []types.Datum
		output []types.Datum
	}{
		{
			[]int64{cols[0].ID, cols[1].ID, cols[2].ID, cols[3].ID, cols[4].ID},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1)), t1, d1},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1)), t1, d1, t2},
		},
		{
			[]int64{cols[0].ID, cols[1].ID, cols[2].ID, cols[3].ID},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1)), t1},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1)), t1, types.NewDatum(nil), t3},
		},
		{
			[]int64{cols[0].ID, cols[1].ID, cols[2].ID, cols[3].ID, cols[4].ID},
			[]types.Datum{types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil)},
			[]types.Datum{types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil)},
		},
	}
	rd := rowcodec.Encoder{Enable: true}
	for i, row := range testRows {
		// test case for pk is unsigned.
		if i > 0 {
			c7.AddFlag(mysql.UnsignedFlag)
		}
		bs, err := tablecodec.EncodeRow(sc, row.input, row.cols, nil, nil, &rd)
		require.NoError(t, err)
		require.NotNil(t, bs)

		r, err := de.DecodeAndEvalRowWithMap(ctx, kv.IntHandle(i), bs, time.UTC, nil)
		require.Nil(t, err)
		// Last column is primary-key column, and the table primary-key is handle, then the primary-key value won't be
		// stored in raw data, but store in the raw key.
		// So when decode, we can't get the primary-key value from raw data, then ignore check the value of the
		// last primary key column.
		for i, col := range cols[:len(cols)-1] {
			v, ok := r[col.ID]
			if ok {
				equal, err1 := v.Compare(sc, &row.output[i], collate.GetBinaryCollator())
				require.Nil(t, err1)
				require.Equal(t, 0, equal)
			} else {
				// use default value.
				require.True(t, col.DefaultValue != "")
			}
		}
		// test decode with no generated column.
		r2, err := deWithNoGenCols.DecodeAndEvalRowWithMap(ctx, kv.IntHandle(i), bs, time.UTC, nil)
		require.Nil(t, err)
		for k, v := range r2 {
			v1, ok := r[k]
			require.True(t, ok)
			equal, err1 := v.Compare(sc, &v1, collate.GetBinaryCollator())
			require.Nil(t, err1)
			require.Equal(t, 0, equal)
		}
	}
}

func TestClusterIndexRowDecoder(t *testing.T) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
	c3 := &model.ColumnInfo{ID: 3, Name: model.NewCIStr("c3"), State: model.StatePublic, Offset: 2, FieldType: *types.NewFieldType(mysql.TypeNewDecimal)}
	c1.AddFlag(mysql.PriKeyFlag)
	c2.AddFlag(mysql.PriKeyFlag)
	pk := &model.IndexInfo{ID: 1, Name: model.NewCIStr("primary"), State: model.StatePublic, Primary: true, Columns: []*model.IndexColumn{
		{Name: model.NewCIStr("c1"), Offset: 0},
		{Name: model.NewCIStr("c2"), Offset: 1},
	}}

	cols := []*model.ColumnInfo{c1, c2, c3}

	tblInfo := &model.TableInfo{ID: 1, Columns: cols, Indices: []*model.IndexInfo{pk}, IsCommonHandle: true, CommonHandleVersion: 1}
	tbl := tables.MockTableFromMeta(tblInfo)

	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	decodeColsMap := make(map[int64]decoder.Column, len(cols))
	for _, col := range tbl.Cols() {
		tpExpr := decoder.Column{
			Col: col,
		}
		decodeColsMap[col.ID] = tpExpr
	}
	de := decoder.NewRowDecoder(tbl, tbl.Cols(), decodeColsMap)

	testRows := []struct {
		cols   []int64
		input  []types.Datum
		output []types.Datum
	}{
		{
			[]int64{cols[0].ID, cols[1].ID, cols[2].ID},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1))},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1))},
		},
	}
	rd := rowcodec.Encoder{Enable: true}
	for _, row := range testRows {
		bs, err := tablecodec.EncodeRow(sc, row.input, row.cols, nil, nil, &rd)
		require.NoError(t, err)
		require.NotNil(t, bs)

		r, err := de.DecodeAndEvalRowWithMap(ctx, testutil.MustNewCommonHandle(t, 100, "abc"), bs, time.UTC, nil)
		require.Nil(t, err)

		for i, col := range cols {
			v, ok := r[col.ID]
			require.True(t, ok)
			equal, err1 := v.Compare(sc, &row.output[i], collate.GetBinaryCollator())
			require.Nil(t, err1)
			require.Equal(t, 0, equal)
		}
	}
}
