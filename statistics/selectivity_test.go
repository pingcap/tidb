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

package statistics_test

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

const eps = 1e-9

var _ = Suite(&testSelectivitySuite{})

type testSelectivitySuite struct {
}

// generateIntDatum will generate a datum slice, every dimension is begin from 0, end with num - 1.
// If dimension is x, num is y, the total number of datum is y^x. And This slice is sorted.
func (s *testSelectivitySuite) generateIntDatum(dimension, num int) ([]types.Datum, error) {
	len := int(math.Pow(float64(num), float64(dimension)))
	ret := make([]types.Datum, len)
	if dimension == 1 {
		for i := 0; i < num; i++ {
			ret[i] = types.NewIntDatum(int64(i))
		}
	} else {
		// In this way, we can guarantee the datum is in order.
		for i := 0; i < len; i++ {
			data := make([]types.Datum, dimension)
			j := i
			for k := 0; k < dimension; k++ {
				data[dimension-k-1].SetInt64(int64(j % num))
				j = j / num
			}
			bytes, err := codec.EncodeKey(nil, data...)
			if err != nil {
				return nil, err
			}
			ret[i].SetBytes(bytes)
		}
	}
	return ret, nil
}

// mockStatsHistogram will create a statistics.Histogram, of which the data is uniform distribution.
func mockStatsHistogram(id int64, values []types.Datum, repeat int64) *statistics.Histogram {
	ndv := len(values)
	histogram := &statistics.Histogram{
		ID:      id,
		NDV:     int64(ndv),
		Buckets: make([]statistics.Bucket, ndv),
	}
	for i := 0; i < ndv; i++ {
		histogram.Buckets[i].Repeats = repeat
		histogram.Buckets[i].Count = repeat * int64(i+1)
		histogram.Buckets[i].UpperBound = values[i]
	}
	return histogram
}

func mockStatsTable(tbl *model.TableInfo, rowCount int64) *statistics.Table {
	statsTbl := &statistics.Table{
		TableID: tbl.ID,
		Count:   rowCount,
		Columns: make(map[int64]*statistics.Column, len(tbl.Columns)),
		Indices: make(map[int64]*statistics.Index, len(tbl.Indices)),
	}
	return statsTbl
}

func (s *testSelectivitySuite) TestSelectivity(c *C) {
	store, dom, err := newStoreWithBootstrap()
	defer func() {
		dom.Close()
		store.Close()
	}()
	c.Assert(err, IsNil)

	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b int, c int, d int, e int, index idx_cd(c, d), index idx_de(d, e))")

	is := dom.InfoSchema()
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl := tb.Meta()

	// mock the statistic table
	statsTbl := mockStatsTable(tbl, 540)

	// Set the value of columns' histogram.
	colValues, _ := s.generateIntDatum(1, 54)
	for i := 1; i <= 5; i++ {
		statsTbl.Columns[int64(i)] = &statistics.Column{Histogram: *mockStatsHistogram(int64(i), colValues, 10), Info: tbl.Columns[i-1]}
	}

	// Set the value of two indices' histograms.
	idxValues, err := s.generateIntDatum(2, 3)
	c.Assert(err, IsNil)
	statsTbl.Indices[1] = &statistics.Index{Histogram: *mockStatsHistogram(1, idxValues, 60), Info: tbl.Indices[0]}
	statsTbl.Indices[2] = &statistics.Index{Histogram: *mockStatsHistogram(2, idxValues, 60), Info: tbl.Indices[1]}

	tests := []struct {
		exprs       string
		selectivity float64
	}{
		{
			exprs:       "a > 0 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and b > 1 and a < 2",
			selectivity: 0.01783264746,
		},
		{
			exprs:       "a >= 1 and c > 1 and a < 2",
			selectivity: 0.00617283950,
		},
		{
			exprs:       "a >= 1 and c >= 1 and a < 2",
			selectivity: 0.01234567901,
		},
		{
			exprs:       "d = 0 and e = 1",
			selectivity: 0.11111111111,
		},
		{
			exprs:       "b > 1",
			selectivity: 0.96296296296,
		},
		{
			exprs:       "a > 1 and b < 2 and c > 3 and d < 4 and e > 5",
			selectivity: 0.00352249826,
		},
	}
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprs
		comment := Commentf("for %s", tt.exprs)
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprs))
		c.Assert(stmts, HasLen, 1)
		err = plan.ResolveName(stmts[0], is, ctx)

		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, tt.exprs))
		var sel *plan.Selection
		for _, child := range p.Children() {
			p, ok := child.(*plan.Selection)
			if ok {
				sel = p
				break
			}
		}
		c.Assert(sel, NotNil, comment)
		ratio, err := statsTbl.Selectivity(ctx, sel.Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))

		statsTbl.Count *= 10
		ratio, err = statsTbl.Selectivity(ctx, sel.Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))
		statsTbl.Count /= 10
	}
}
