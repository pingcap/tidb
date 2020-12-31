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
// See the License for the specific language governing permissions and
// limitations under the License.

package decoder_test

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	_ "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDecoderSuite{})

type testDecoderSuite struct{}

func (s *testDecoderSuite) TestRowDecoder(c *C) {
	defer testleak.AfterTest(c)()
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
	c3 := &model.ColumnInfo{ID: 3, Name: model.NewCIStr("c3"), State: model.StatePublic, Offset: 2, FieldType: *types.NewFieldType(mysql.TypeNewDecimal)}
	c4 := &model.ColumnInfo{ID: 4, Name: model.NewCIStr("c4"), State: model.StatePublic, Offset: 3, FieldType: *types.NewFieldType(mysql.TypeTimestamp)}
	c5 := &model.ColumnInfo{ID: 5, Name: model.NewCIStr("c5"), State: model.StatePublic, Offset: 4, FieldType: *types.NewFieldType(mysql.TypeDuration), OriginDefaultValue: "02:00:02"}
	c6 := &model.ColumnInfo{ID: 6, Name: model.NewCIStr("c6"), State: model.StatePublic, Offset: 5, FieldType: *types.NewFieldType(mysql.TypeTimestamp), GeneratedExprString: "c4+c5"}
	c7 := &model.ColumnInfo{ID: 7, Name: model.NewCIStr("c7"), State: model.StatePublic, Offset: 6, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c7.Flag |= mysql.PriKeyFlag

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
			c.Assert(err, IsNil)
			tpExpr.GenExpr = expr
		}
		decodeColsMap[col.ID] = tpExpr
	}
	de := decoder.NewRowDecoder(tbl, tbl.Cols(), decodeColsMap)
	deWithNoGenCols := decoder.NewRowDecoder(tbl, tbl.Cols(), decodeColsMap2)

	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	time1 := types.NewTime(types.FromDate(2019, 01, 01, 8, 01, 01, 0), mysql.TypeTimestamp, types.DefaultFsp)
	t1 := types.NewTimeDatum(time1)
	d1 := types.NewDurationDatum(types.Duration{
		Duration: time.Hour + time.Second,
	})

	time2, err := time1.Add(sc, d1.GetMysqlDuration())
	c.Assert(err, IsNil)
	err = time2.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
	t2 := types.NewTimeDatum(time2)

	time3, err := time1.Add(sc, types.Duration{Duration: time.Hour*2 + time.Second*2})
	c.Assert(err, IsNil)
	err = time3.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
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
			c7.Flag |= mysql.UnsignedFlag
		}
		bs, err := tablecodec.EncodeRow(sc, row.input, row.cols, nil, nil, &rd)
		c.Assert(err, IsNil)
		c.Assert(bs, NotNil)

		r, err := de.DecodeAndEvalRowWithMap(ctx, kv.IntHandle(i), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)
		// Last column is primary-key column, and the table primary-key is handle, then the primary-key value won't be
		// stored in raw data, but store in the raw key.
		// So when decode, we can't get the primary-key value from raw data, then ignore check the value of the
		// last primary key column.
		for i, col := range cols[:len(cols)-1] {
			v, ok := r[col.ID]
			if ok {
				equal, err1 := v.CompareDatum(sc, &row.output[i])
				c.Assert(err1, IsNil)
				c.Assert(equal, Equals, 0)
			} else {
				// use default value.
				c.Assert(col.DefaultValue != "", IsTrue)
			}
		}
		// test decode with no generated column.
		r2, err := deWithNoGenCols.DecodeAndEvalRowWithMap(ctx, kv.IntHandle(i), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)
		for k, v := range r2 {
			v1, ok := r[k]
			c.Assert(ok, IsTrue)
			equal, err1 := v.CompareDatum(sc, &v1)
			c.Assert(err1, IsNil)
			c.Assert(equal, Equals, 0)
		}
	}
}

func (s *testDecoderSuite) TestClusterIndexRowDecoder(c *C) {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
	c3 := &model.ColumnInfo{ID: 3, Name: model.NewCIStr("c3"), State: model.StatePublic, Offset: 2, FieldType: *types.NewFieldType(mysql.TypeNewDecimal)}
	c1.Flag |= mysql.PriKeyFlag
	c2.Flag |= mysql.PriKeyFlag
	pk := &model.IndexInfo{ID: 1, Name: model.NewCIStr("primary"), State: model.StatePublic, Primary: true, Columns: []*model.IndexColumn{
		{Name: model.NewCIStr("c1"), Offset: 0},
		{Name: model.NewCIStr("c2"), Offset: 1},
	}}

	cols := []*model.ColumnInfo{c1, c2, c3}

	tblInfo := &model.TableInfo{ID: 1, Columns: cols, Indices: []*model.IndexInfo{pk}, IsCommonHandle: true}
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

	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		c.Assert(bs, NotNil)

		r, err := de.DecodeAndEvalRowWithMap(ctx, testutil.MustNewCommonHandle(c, 100, "abc"), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)

		for i, col := range cols {
			v, ok := r[col.ID]
			c.Assert(ok, IsTrue)
			equal, err1 := v.CompareDatum(sc, &row.output[i])
			c.Assert(err1, IsNil)
			c.Assert(equal, Equals, 0)
		}
	}
}
