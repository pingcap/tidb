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
	"context"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const eps = 1e-9

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	store kv.Storage
	do    *domain.Domain
	hook  *logHook
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	s.registerHook()
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	c.Assert(s.store.Close(), IsNil)
	testleak.AfterTest(c)()
}

func (s *testStatsSuite) registerHook() {
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.hook = &logHook{r.Core, ""}
	lg := zap.New(s.hook)
	log.ReplaceGlobals(lg, r)
}

type logHook struct {
	zapcore.Core
	results string
}

func (h *logHook) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	message := entry.Message
	if idx := strings.Index(message, "[stats"); idx != -1 {
		h.results = h.results + message
		for _, f := range fields {
			h.results = h.results + ", " + f.Key + "=" + h.field2String(f)
		}
	}
	return nil
}

func (h *logHook) field2String(field zapcore.Field) string {
	switch field.Type {
	case zapcore.StringType:
		return field.String
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Uint32Type:
		return fmt.Sprintf("%v", field.Integer)
	case zapcore.Float64Type:
		return fmt.Sprintf("%v", math.Float64frombits(uint64(field.Integer)))
	case zapcore.StringerType:
		return field.Interface.(fmt.Stringer).String()
	}
	return "not support"
}

func (h *logHook) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain.RunAutoAnalyze = false
	do, err := session.BootstrapSession(store)
	do.SetStatsUpdating(true)
	return store, do, errors.Trace(err)
}

func cleanEnv(c *C, store kv.Storage, do *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	do.StatsHandle().Clear()
}

// generateIntDatum will generate a datum slice, every dimension is begin from 0, end with num - 1.
// If dimension is x, num is y, the total number of datum is y^x. And This slice is sorted.
func (s *testStatsSuite) generateIntDatum(dimension, num int) ([]types.Datum, error) {
	length := int(math.Pow(float64(num), float64(dimension)))
	ret := make([]types.Datum, length)
	if dimension == 1 {
		for i := 0; i < num; i++ {
			ret[i] = types.NewIntDatum(int64(i))
		}
	} else {
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		// In this way, we can guarantee the datum is in order.
		for i := 0; i < length; i++ {
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
	histogram := statistics.NewHistogram(id, int64(ndv), 0, 0, tp, ndv, 0)
	for i := 0; i < ndv; i++ {
		histogram.AppendBucket(&values[i], &values[i], repeat*int64(i+1), repeat)
	}
	return histogram
}

func mockStatsTable(tbl *model.TableInfo, rowCount int64) *statistics.Table {
	histColl := statistics.HistColl{
		PhysicalID:     tbl.ID,
		HavePhysicalID: true,
		Count:          rowCount,
		Columns:        make(map[int64]*statistics.Column, len(tbl.Columns)),
		Indices:        make(map[int64]*statistics.Index, len(tbl.Indices)),
	}
	statsTbl := &statistics.Table{
		HistColl: histColl,
	}
	return statsTbl
}

func (s *testStatsSuite) prepareSelectivity(testKit *testkit.TestKit, c *C) *statistics.Table {
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b int, c int, d int, e int, index idx_cd(c, d), index idx_de(d, e))")

	is := s.do.InfoSchema()
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl := tb.Meta()

	// mock the statistic table
	statsTbl := mockStatsTable(tbl, 540)

	// Set the value of columns' histogram.
	colValues, err := s.generateIntDatum(1, 54)
	c.Assert(err, IsNil)
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

func (s *testStatsSuite) TestSelectivity(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.do.InfoSchema()

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
			selectivity: 0,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprs
		comment := Commentf("for %s", tt.exprs)
		sctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(sctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprs))
		c.Assert(stmts, HasLen, 1)

		err = plannercore.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, comment)
		p, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, tt.exprs))

		sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		histColl := statsTbl.GenerateHistCollFromColumnInfo(ds.Columns, ds.Schema().Columns)

		ratio, _, err := histColl.Selectivity(sctx, sel.Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))

		histColl.Count *= 10
		ratio, _, err = histColl.Selectivity(sctx, sel.Conditions)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))
	}
}

// TestDiscreteDistribution tests the estimation for discrete data distribution. This is more common when the index
// consists several columns, and the first column has small NDV.
func (s *testStatsSuite) TestDiscreteDistribution(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a char(10), b int, key idx(a, b))")
	for i := 0; i < 499; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values ('cn', %d)", i))
	}
	for i := 0; i < 10; i++ {
		testKit.MustExec("insert into t values ('tw', 0)")
	}
	testKit.MustExec("analyze table t")
	testKit.MustQuery("explain select * from t where a = 'tw' and b < 0").Check(testkit.Rows(
		"IndexReader_6 0.00 root index:IndexScan_5",
		"└─IndexScan_5 0.00 cop table:t, index:a, b, range:[\"tw\" -inf,\"tw\" 0), keep order:false"))
}

func (s *testStatsSuite) TestSelectCombinedLowBound(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(id int auto_increment, kid int, pid int, primary key(id), key(kid, pid))")
	testKit.MustExec("insert into t (kid, pid) values (1,2), (1,3), (1,4),(1, 11), (1, 12), (1, 13), (1, 14), (2, 2), (2, 3), (2, 4)")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("explain select * from t where kid = 1").Check(testkit.Rows(
		"IndexReader_6 7.00 root index:IndexScan_5",
		"└─IndexScan_5 7.00 cop table:t, index:kid, pid, range:[1,1], keep order:false"))
}

func getRange(start, end int64) []*ranger.Range {
	ran := &ranger.Range{
		LowVal:  []types.Datum{types.NewIntDatum(start)},
		HighVal: []types.Datum{types.NewIntDatum(end)},
	}
	return []*ranger.Range{ran}
}

func (s *testStatsSuite) TestEstimationForUnknownValues(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, key idx(a, b))")
	testKit.MustExec("analyze table t")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+10, i+10))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := h.GetTableStats(table.Meta())

	sc := &stmtctx.StatementContext{}
	colID := table.Meta().Columns[0].ID
	count, err := statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(30, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2.0)

	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 4.2)

	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(9, math.MaxInt64))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 4.2)

	idxID := table.Meta().Indices[0].ID
	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(30, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.2)

	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2.2)

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values (null, null)")
	testKit.MustExec("analyze table t")
	table, err = s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl = h.GetTableStats(table.Meta())

	colID = table.Meta().Columns[0].ID
	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(1, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(a int, b int, index idx(b))")
	testKit.MustExec("insert into t values (1,1)")
	testKit.MustExec("analyze table t")
	table, err = s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl = h.GetTableStats(table.Meta())

	colID = table.Meta().Columns[0].ID
	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(2, 2))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)

	idxID = table.Meta().Indices[0].ID
	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(2, 2))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestEstimationUniqueKeyEqualConds(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, unique key(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7)")
	testKit.MustExec("analyze table t")
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := s.do.StatsHandle().GetTableStats(table.Meta())

	sc := &stmtctx.StatementContext{}
	idxID := table.Meta().Indices[0].ID
	count, err := statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(7, 7))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(6, 6))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	colID := table.Meta().Columns[0].ID
	count, err = statsTbl.GetRowCountByIntColumnRanges(sc, colID, getRange(7, 7))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	count, err = statsTbl.GetRowCountByIntColumnRanges(sc, colID, getRange(6, 6))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)
}

func (s *testStatsSuite) TestPrimaryKeySelectivity(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a char(10) primary key, b int)")
	testKit.MustQuery(`explain select * from t where a > "t"`).Check(testkit.Rows(
		"IndexLookUp_10 3333.33 root ",
		"├─IndexScan_8 3333.33 cop table:t, index:a, range:(\"t\",+inf], keep order:false, stats:pseudo",
		"└─TableScan_9 3333.33 cop table:t, keep order:false, stats:pseudo"))

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(a int primary key, b int)")
	testKit.MustQuery(`explain select * from t where a > 1`).Check(testkit.Rows(
		"TableReader_6 3333.33 root data:TableScan_5",
		"└─TableScan_5 3333.33 cop table:t, range:(1,+inf], keep order:false, stats:pseudo"))
}

func BenchmarkSelectivity(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpSuite(c)
	defer s.TearDownSuite(c)

	testKit := testkit.NewTestKit(c, s.store)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.do.InfoSchema()
	exprs := "a > 1 and b < 2 and c > 3 and d < 4 and e > 5"
	sql := "select * from t where " + exprs
	comment := Commentf("for %s", exprs)
	sctx := testKit.Se.(sessionctx.Context)
	stmts, err := session.Parse(sctx, sql)
	c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, exprs))
	c.Assert(stmts, HasLen, 1)
	err = plannercore.Preprocess(sctx, stmts[0], is)
	c.Assert(err, IsNil, comment)
	p, err := plannercore.BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
	c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, exprs))

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := statsTbl.Selectivity(sctx, p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection).Conditions)
			c.Assert(err, IsNil)
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}

func (s *testStatsSuite) TestColumnIndexNullEstimation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	testKit.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null);")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	testKit.MustQuery(`explain select b from t where b is null`).Check(testkit.Rows(
		"IndexReader_6 4.00 root index:IndexScan_5",
		"└─IndexScan_5 4.00 cop table:t, index:b, range:[NULL,NULL], keep order:false",
	))
	testKit.MustQuery(`explain select b from t where b is not null`).Check(testkit.Rows(
		"IndexReader_6 1.00 root index:IndexScan_5",
		"└─IndexScan_5 1.00 cop table:t, index:b, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select b from t where b is null or b > 3`).Check(testkit.Rows(
		"IndexReader_6 4.00 root index:IndexScan_5",
		"└─IndexScan_5 4.00 cop table:t, index:b, range:[NULL,NULL], (3,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select b from t use index(idx_b)`).Check(testkit.Rows(
		"IndexReader_5 5.00 root index:IndexScan_4",
		"└─IndexScan_4 5.00 cop table:t, index:b, range:[NULL,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select b from t where b < 4`).Check(testkit.Rows(
		"IndexReader_6 1.00 root index:IndexScan_5",
		"└─IndexScan_5 1.00 cop table:t, index:b, range:[-inf,4), keep order:false",
	))
	// Make sure column stats has been loaded.
	testKit.MustExec(`explain select * from t where a is null`)
	c.Assert(h.LoadNeededHistograms(), IsNil)
	testKit.MustQuery(`explain select * from t where a is null`).Check(testkit.Rows(
		"TableReader_7 1.00 root data:Selection_6",
		"└─Selection_6 1.00 cop isnull(test.t.a)",
		"  └─TableScan_5 5.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select * from t where a is not null`).Check(testkit.Rows(
		"TableReader_7 4.00 root data:Selection_6",
		"└─Selection_6 4.00 cop not(isnull(test.t.a))",
		"  └─TableScan_5 5.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select * from t where a is null or a > 3`).Check(testkit.Rows(
		"TableReader_7 2.00 root data:Selection_6",
		"└─Selection_6 2.00 cop or(isnull(test.t.a), gt(test.t.a, 3))",
		"  └─TableScan_5 5.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select * from t`).Check(testkit.Rows(
		"TableReader_5 5.00 root data:TableScan_4",
		"└─TableScan_4 5.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery(`explain select * from t where a < 4`).Check(testkit.Rows(
		"TableReader_7 3.00 root data:Selection_6",
		"└─Selection_6 3.00 cop lt(test.t.a, 4)",
		"  └─TableScan_5 5.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
}

func (s *testStatsSuite) TestSelectivityGreedyAlgo(c *C) {
	nodes := make([]*statistics.StatsNode, 3)
	nodes[0] = statistics.MockStatsNode(1, 3, 2)
	nodes[1] = statistics.MockStatsNode(2, 5, 2)
	nodes[2] = statistics.MockStatsNode(3, 9, 2)

	// Sets should not overlap on mask, so only nodes[0] is chosen.
	usedSets := statistics.GetUsableSetsByGreedy(nodes)
	c.Assert(len(usedSets), Equals, 1)
	c.Assert(usedSets[0].ID, Equals, int64(1))

	nodes[0], nodes[1] = nodes[1], nodes[0]
	// Sets chosen should be stable, so the returned node is still the one with ID 1.
	usedSets = statistics.GetUsableSetsByGreedy(nodes)
	c.Assert(len(usedSets), Equals, 1)
	c.Assert(usedSets[0].ID, Equals, int64(1))
}
