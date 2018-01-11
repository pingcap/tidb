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
	"os"
	"runtime/pprof"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

const eps = 1e-9

var _ = Suite(&testSelectivitySuite{})

type testSelectivitySuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (suite *testSelectivitySuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap(0)
	c.Assert(err, IsNil)
	suite.dom = dom
	suite.store = store
}

func (suite *testSelectivitySuite) TearDownSuite(c *C) {
	suite.dom.Close()
	suite.store.Close()
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
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		// In this way, we can guarantee the datum is in order.
		for i := 0; i < len; i++ {
			data := make([]types.Datum, dimension)
			j := i
			for k := 0; k < dimension; k++ {
				data[dimension-k-1].SetInt64(int64(j % num))
				j = j / num
			}
			bytes, err := codec.EncodeKey(sc, nil, data...)
			if err != nil {
				return nil, err
			}
			ret[i].SetBytes(bytes)
		}
	}
	return ret, nil
}

// mockStatsHistogram will create a statistics.Histogram, of which the data is uniform distribution.
func mockStatsHistogram(id int64, values []types.Datum, repeat int64, tp *types.FieldType) *statistics.Histogram {
	ndv := len(values)
	histogram := statistics.NewHistogram(id, int64(ndv), 0, 0, tp, ndv)
	for i := 0; i < ndv; i++ {
		histogram.AppendBucket(&values[i], &values[i], repeat*int64(i+1), repeat)
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

func (s *testSelectivitySuite) prepareSelectivity(testKit *testkit.TestKit, c *C) *statistics.Table {
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b int, c int, d int, e int, index idx_cd(c, d), index idx_de(d, e))")

	is := s.dom.InfoSchema()
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl := tb.Meta()

	// mock the statistic table
	statsTbl := mockStatsTable(tbl, 540)

	// Set the value of columns' histogram.
	colValues, _ := s.generateIntDatum(1, 54)
	for i := 1; i <= 5; i++ {
		statsTbl.Columns[int64(i)] = &statistics.Column{Histogram: *mockStatsHistogram(int64(i), colValues, 10, types.NewFieldType(mysql.TypeLonglong)), Info: tbl.Columns[i-1]}
	}

	// Set the value of two indices' histograms.
	idxValues, err := s.generateIntDatum(2, 3)
	c.Assert(err, IsNil)
	tp := types.NewFieldType(mysql.TypeBlob)
	statsTbl.Indices[1] = &statistics.Index{Histogram: *mockStatsHistogram(1, idxValues, 60, tp), Info: tbl.Indices[0]}
	statsTbl.Indices[2] = &statistics.Index{Histogram: *mockStatsHistogram(2, idxValues, 60, tp), Info: tbl.Indices[1]}
	return statsTbl
}

func (s *testSelectivitySuite) TestSelectivity(c *C) {
	testKit := testkit.NewTestKit(c, s.store)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.dom.InfoSchema()

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
		err = plan.Preprocess(ctx, stmts[0], is, false)
		c.Assert(err, IsNil, comment)
		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, tt.exprs))
		ratio, err := statsTbl.Selectivity(ctx, p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection).Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))

		statsTbl.Count *= 10
		ratio, err = statsTbl.Selectivity(ctx, p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection).Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))
		statsTbl.Count /= 10
	}
}

func BenchmarkSelectivity(b *testing.B) {
	c := &C{}
	s := &testSelectivitySuite{}
	s.SetUpSuite(c)
	defer s.TearDownSuite(c)

	testKit := testkit.NewTestKit(c, s.store)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.dom.InfoSchema()
	exprs := "a > 1 and b < 2 and c > 3 and d < 4 and e > 5"
	sql := "select * from t where " + exprs
	comment := Commentf("for %s", exprs)
	ctx := testKit.Se.(context.Context)
	stmts, err := tidb.Parse(ctx, sql)
	c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, exprs))
	c.Assert(stmts, HasLen, 1)
	err = plan.Preprocess(ctx, stmts[0], is, false)
	c.Assert(err, IsNil, comment)
	p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
	c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, exprs))

	file, _ := os.Create("cpu.profile")
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := statsTbl.Selectivity(ctx, p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection).Conditions)
			c.Assert(err, IsNil)
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}
