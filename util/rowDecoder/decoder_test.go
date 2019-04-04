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
// See the License for the specific language governing permissions and
// limitations under the License.

package decoder

import (
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
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
	dependenciesMap := make(map[string]struct{})
	dependenciesMap["c1"] = struct{}{}
	dependenciesMap["c3"] = struct{}{}
	c4 := &model.ColumnInfo{ID: 4, Name: model.NewCIStr("c4"), State: model.StatePublic, Offset: 3, FieldType: *types.NewFieldType(mysql.TypeLonglong), GeneratedExprString: "c1+c5", GeneratedStored: false}
	c5 := &model.ColumnInfo{ID: 5, Name: model.NewCIStr("c5"), State: model.StatePublic, Offset: 4, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c5.Flag |= mysql.PriKeyFlag

	cols := []*model.ColumnInfo{c1, c2, c3, c4, c5}
	encodeCols := []*model.ColumnInfo{c1, c2, c3}

	tblInfo := &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: true}
	tbl := tables.MockTableFromMeta(tblInfo)

	ctx := mock.NewContext()
	decodeColsMap := make(map[int64]Column, len(cols))
	for i := range tbl.Cols() {
		col := tbl.Cols()[i]
		tpExpr := Column{
			Col: col,
		}
		if col.GeneratedExprString != "" {
			expr, err := expression.ParseSimpleExprCastWithTableInfo(ctx, col.GeneratedExprString, tblInfo, &col.FieldType)
			c.Assert(err, IsNil)
			tpExpr.GenExpr = expr
		}
		decodeColsMap[col.ID] = tpExpr
	}
	de := NewRowDecoder(tbl, decodeColsMap)

	testRows := []struct {
		input  []types.Datum
		output []types.Datum
	}{
		{
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1))},
			[]types.Datum{types.NewIntDatum(100), types.NewBytesDatum([]byte("abc")), types.NewDecimalDatum(types.NewDecFromInt(1)), types.NewIntDatum(100), types.NewIntDatum(1)},
		},

		{
			[]types.Datum{types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil)},
			[]types.Datum{types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil), types.NewIntDatum(1)},
		},
	}
	// Encode
	encodeColIDs := make([]int64, 0, 3)
	for _, col := range encodeCols {
		encodeColIDs = append(encodeColIDs, col.ID)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for i, row := range testRows {
		bs, err := tablecodec.EncodeRow(sc, row.input, encodeColIDs, nil, nil)
		c.Assert(err, IsNil)
		c.Assert(bs, NotNil)

		r, err := de.DecodeAndEvalRowWithMap(ctx, int64(i), bs, time.UTC, time.UTC, nil)
		c.Assert(err, IsNil)
		for i, col := range cols[:len(cols)-1] {
			v, ok := r[col.ID]
			fmt.Printf("%v\n", i)
			c.Assert(ok, IsTrue)
			equal, err1 := v.CompareDatum(sc, &row.output[i])
			c.Assert(err1, IsNil)
			c.Assert(equal, Equals, 0)
		}
	}
}
