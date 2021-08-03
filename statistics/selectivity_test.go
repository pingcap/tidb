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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const eps = 1e-9

var _ = SerialSuites(&testStatsSuite{})

type testStatsSuite struct {
	store    kv.Storage
	do       *domain.Domain
	hook     *logHook
	testData testutil.TestData
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	s.registerHook()
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.testData, err = testutil.LoadTestSuiteData("testdata", "stats_suite")
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	c.Assert(s.store.Close(), IsNil)
	testleak.AfterTest(c)()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testStatsSuite) registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
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
	store, err := mockstore.NewMockStore()
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

	longExpr := "0 < a and a = 1 "
	for i := 1; i < 64; i++ {
		longExpr += fmt.Sprintf(" and a > %d ", i)
	}
	tests := []struct {
		exprs                    string
		selectivity              float64
		selectivityAfterIncrease float64
	}{
		{
			exprs:                    "a > 0 and a < 2",
			selectivity:              0.01851851851,
			selectivityAfterIncrease: 0.01851851851,
		},
		{
			exprs:                    "a >= 1 and a < 2",
			selectivity:              0.01851851851,
			selectivityAfterIncrease: 0.01851851851,
		},
		{
			exprs:                    "a >= 1 and b > 1 and a < 2",
			selectivity:              0.01783264746,
			selectivityAfterIncrease: 0.01851851852,
		},
		{
			exprs:                    "a >= 1 and c > 1 and a < 2",
			selectivity:              0.00617283950,
			selectivityAfterIncrease: 0.00617283950,
		},
		{
			exprs:                    "a >= 1 and c >= 1 and a < 2",
			selectivity:              0.01234567901,
			selectivityAfterIncrease: 0.01234567901,
		},
		{
			exprs:                    "d = 0 and e = 1",
			selectivity:              0.11111111111,
			selectivityAfterIncrease: 0.11111111111,
		},
		{
			exprs:                    "b > 1",
			selectivity:              0.96296296296,
			selectivityAfterIncrease: 1,
		},
		{
			exprs:                    "a > 1 and b < 2 and c > 3 and d < 4 and e > 5",
			selectivity:              0,
			selectivityAfterIncrease: 0,
		},
		{
			exprs:                    longExpr,
			selectivity:              0.001,
			selectivityAfterIncrease: 0.001,
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

		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		c.Assert(err, IsNil, comment)
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], ret.InfoSchema)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, tt.exprs))

		sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		histColl := statsTbl.GenerateHistCollFromColumnInfo(ds.Columns, ds.Schema().Columns)

		ratio, _, err := histColl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))

		histColl.Count *= 10
		ratio, _, err = histColl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivityAfterIncrease) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivityAfterIncrease, ratio))
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
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestSelectCombinedLowBound(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(id int auto_increment, kid int, pid int, primary key(id), key(kid, pid))")
	testKit.MustExec("insert into t (kid, pid) values (1,2), (1,3), (1,4),(1, 11), (1, 12), (1, 13), (1, 14), (2, 2), (2, 3), (2, 4)")
	testKit.MustExec("analyze table t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func getRange(start, end int64) []*ranger.Range {
	ran := &ranger.Range{
		LowVal:  []types.Datum{types.NewIntDatum(start)},
		HighVal: []types.Datum{types.NewIntDatum(end)},
	}
	return []*ranger.Range{ran}
}

func (s *testStatsSuite) TestOutOfRangeEstimation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int unsigned)")
	for i := 0; i < 3000; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%v)", i/5+300)) // [300, 900)
	}
	testKit.MustExec("analyze table t with 2000 samples")

	h := s.do.StatsHandle()
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := h.GetTableStats(table.Meta())
	sc := &stmtctx.StatementContext{}
	col := statsTbl.Columns[table.Meta().Columns[0].ID]
	count, err := col.GetColumnRowCount(sc, getRange(900, 900), statsTbl.Count, false)
	c.Assert(err, IsNil)
	// Because the ANALYZE collect data by random sampling, so the result is not an accurate value.
	// so we use a range here.
	c.Assert(count < 5.5, IsTrue, Commentf("expected: around 5.0, got: %v", count))
	c.Assert(count > 4.5, IsTrue, Commentf("expected: around 5.0, got: %v", count))

	var input []struct {
		Start int64
		End   int64
	}
	var output []struct {
		Start int64
		End   int64
		Count float64
	}
	s.testData.GetTestCases(c, &input, &output)
	increasedTblRowCount := int64(float64(statsTbl.Count) * 1.5)
	for i, ran := range input {
		count, err = col.GetColumnRowCount(sc, getRange(ran.Start, ran.End), increasedTblRowCount, false)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].Start = ran.Start
			output[i].End = ran.End
			output[i].Count = count
		})
		c.Assert(count < output[i].Count*1.2, IsTrue, Commentf("for [%v, %v], needed: around %v, got: %v", ran.Start, ran.End, output[i].Count, count))
		c.Assert(count > output[i].Count*0.8, IsTrue, Commentf("for [%v, %v], needed: around %v, got: %v", ran.Start, ran.End, output[i].Count, count))
	}
}

func (s *testStatsSuite) TestEstimationForUnknownValues(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, key idx(a, b))")
	testKit.MustExec("set @@tidb_analyze_version=1")
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
	c.Assert(count, Equals, 0.2)

	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 7.2)

	count, err = statsTbl.GetRowCountByColumnRanges(sc, colID, getRange(9, math.MaxInt64))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 7.2)

	idxID := table.Meta().Indices[0].ID
	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(30, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.1)

	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 7.0)

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
	testKit.MustExec("analyze table t with 4 cmsketch width, 1 cmsketch depth;")
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
	testKit.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	testKit.MustExec("create table t(a char(10) primary key, b int)")
	var input, output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				testKit.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				if j == len(ts)-1 {
					output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
			}
		}
	}
}

func BenchmarkSelectivity(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpSuite(c)
	defer s.TearDownSuite(c)

	testKit := testkit.NewTestKit(c, s.store)
	statsTbl := s.prepareSelectivity(testKit, c)
	exprs := "a > 1 and b < 2 and c > 3 and d < 4 and e > 5"
	sql := "select * from t where " + exprs
	comment := Commentf("for %s", exprs)
	sctx := testKit.Se.(sessionctx.Context)
	stmts, err := session.Parse(sctx, sql)
	c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, exprs))
	c.Assert(stmts, HasLen, 1)
	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
	c.Assert(err, IsNil, comment)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), sctx, stmts[0], ret.InfoSchema)
	c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, exprs))

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer func() {
		err := file.Close()
		c.Assert(err, IsNil)
	}()
	err = pprof.StartCPUProfile(file)
	c.Assert(err, IsNil)

	b.Run("Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := statsTbl.Selectivity(sctx, p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection).Conditions, nil)
			c.Assert(err, IsNil)
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}

func (s *testStatsSuite) TestStatsVer2(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_analyze_version=2")

	testKit.MustExec("drop table if exists tint")
	testKit.MustExec("create table tint(a int, b int, c int, index singular(a), index multi(b, c))")
	testKit.MustExec("insert into tint values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8)")
	testKit.MustExec("analyze table tint with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists tdouble")
	testKit.MustExec("create table tdouble(a double, b double, c double, index singular(a), index multi(b, c))")
	testKit.MustExec("insert into tdouble values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8)")
	testKit.MustExec("analyze table tdouble with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists tdecimal")
	testKit.MustExec("create table tdecimal(a decimal(40, 20), b decimal(40, 20), c decimal(40, 20), index singular(a), index multi(b, c))")
	testKit.MustExec("insert into tdecimal values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8)")
	testKit.MustExec("analyze table tdecimal with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists tstring")
	testKit.MustExec("create table tstring(a varchar(64), b varchar(64), c varchar(64), index singular(a), index multi(b, c))")
	testKit.MustExec("insert into tstring values ('1', '1', '1'), ('2', '2', '2'), ('3', '3', '3'), ('4', '4', '4'), ('5', '5', '5'), ('6', '6', '6'), ('7', '7', '7'), ('8', '8', '8')")
	testKit.MustExec("analyze table tstring with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists tdatetime")
	testKit.MustExec("create table tdatetime(a datetime, b datetime, c datetime, index singular(a), index multi(b, c))")
	testKit.MustExec("insert into tdatetime values ('2001-01-01', '2001-01-01', '2001-01-01'), ('2001-01-02', '2001-01-02', '2001-01-02'), ('2001-01-03', '2001-01-03', '2001-01-03'), ('2001-01-04', '2001-01-04', '2001-01-04')")
	testKit.MustExec("analyze table tdatetime with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists tprefix")
	testKit.MustExec("create table tprefix(a varchar(64), b varchar(64), index prefixa(a(2)))")
	testKit.MustExec("insert into tprefix values ('111', '111'), ('222', '222'), ('333', '333'), ('444', '444'), ('555', '555'), ('666', '666')")
	testKit.MustExec("analyze table tprefix with 2 topn, 3 buckets")

	// test with clustered index
	testKit.MustExec("drop table if exists ct1")
	testKit.MustExec("create table ct1 (a int, pk varchar(10), primary key(pk) clustered)")
	testKit.MustExec("insert into ct1 values (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6'), (7, '7'), (8, '8')")
	testKit.MustExec("analyze table ct1 with 2 topn, 3 buckets")

	testKit.MustExec("drop table if exists ct2")
	testKit.MustExec("create table ct2 (a int, b int, c int, primary key(a, b) clustered)")
	testKit.MustExec("insert into ct2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8)")
	testKit.MustExec("analyze table ct2 with 2 topn, 3 buckets")

	rows := testKit.MustQuery("select stats_ver from mysql.stats_histograms").Rows()
	for _, r := range rows {
		// ensure statsVer = 2
		c.Assert(fmt.Sprintf("%v", r[0]), Equals, "2")
	}

	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestTopNOutOfHist(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_analyze_version=2")

	testKit.MustExec("drop table if exists topn_before_hist")
	testKit.MustExec("create table topn_before_hist(a int, index idx(a))")
	testKit.MustExec("insert into topn_before_hist values(1), (1), (1), (1), (3), (3), (4), (5), (6)")
	testKit.MustExec("analyze table topn_before_hist with 2 topn, 3 buckets")

	testKit.MustExec("create table topn_after_hist(a int, index idx(a))")
	testKit.MustExec("insert into topn_after_hist values(2), (2), (3), (4), (5), (7), (7), (7), (7)")
	testKit.MustExec("analyze table topn_after_hist with 2 topn, 3 buckets")

	testKit.MustExec("create table topn_before_hist_no_index(a int)")
	testKit.MustExec("insert into topn_before_hist_no_index values(1), (1), (1), (1), (3), (3), (4), (5), (6)")
	testKit.MustExec("analyze table topn_before_hist_no_index with 2 topn, 3 buckets")

	testKit.MustExec("create table topn_after_hist_no_index(a int)")
	testKit.MustExec("insert into topn_after_hist_no_index values(2), (2), (3), (4), (5), (7), (7), (7), (7)")
	testKit.MustExec("analyze table topn_after_hist_no_index with 2 topn, 3 buckets")

	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
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
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 5; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
	// Make sure column stats has been loaded.
	testKit.MustExec(`explain select * from t where a is null`)
	c.Assert(h.LoadNeededHistograms(), IsNil)
	for i := 5; i < len(input); i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestUniqCompEqualEst(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, primary key(a, b))")
	testKit.MustExec("insert into t values(1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8),(1,9),(1,10)")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 1; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
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

func (s *testStatsSuite) TestCollationColumnEstimate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t values('aaa'), ('bbb'), ('AAA'), ('BBB')")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < len(input); i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

// TestDNFCondSelectivity tests selectivity calculation with DNF conditions covered by using independence assumption.
func (s *testStatsSuite) TestDNFCondSelectivity(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)

	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, d int)")
	testKit.MustExec("insert into t value(1,5,4,4),(3,4,1,8),(4,2,6,10),(6,7,2,5),(7,1,4,9),(8,9,8,3),(9,1,9,1),(10,6,6,2)")
	testKit.MustExec("alter table t add index (b)")
	testKit.MustExec("alter table t add index (d)")
	testKit.MustExec(`analyze table t`)

	ctx := context.Background()
	h := s.do.StatsHandle()
	tb, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tb.Meta()
	statsTbl := h.GetTableStats(tblInfo)

	var (
		input  []string
		output []struct {
			SQL         string
			Selectivity float64
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		sctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(sctx, tt)
		c.Assert(err, IsNil, Commentf("error %v, for sql %s", err, tt))
		c.Assert(stmts, HasLen, 1)

		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		c.Assert(err, IsNil, Commentf("error %v, for sql %s", err, tt))
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], ret.InfoSchema)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, sql %s", err, tt))

		sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		histColl := statsTbl.GenerateHistCollFromColumnInfo(ds.Columns, ds.Schema().Columns)

		ratio, _, err := histColl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt))
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Selectivity = ratio
		})
		c.Assert(math.Abs(ratio-output[i].Selectivity) < eps, IsTrue,
			Commentf("for %s, needed: %v, got: %v", tt, output[i].Selectivity, ratio))
	}

	// Test issue 19981
	testKit.MustExec("select * from t where _tidb_rowid is null or _tidb_rowid > 7")

	// Test issue 22134
	// Information about column n will not be in stats immediately after this SQL executed.
	// If we don't have a check against this, DNF condition could lead to infinite recursion in Selectivity().
	testKit.MustExec("alter table t add column n timestamp;")
	testKit.MustExec("select * from t where n = '2000-01-01' or n = '2000-01-02';")
}

func (s *testStatsSuite) TestIndexEstimationCrossValidate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a,b))")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3), (2, 2)")
	tk.MustExec("analyze table t")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/statistics/table/mockQueryBytesMaxUint64", `return(100000)`), IsNil)
	tk.MustQuery("explain select * from t where a = 1 and b = 2").Check(testkit.Rows(
		"IndexReader_6 1.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 1.00 cop[tikv] table:t, index:a(a, b) range:[1 2,1 2], keep order:false"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/statistics/table/mockQueryBytesMaxUint64"), IsNil)

	// Test issue 22466
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b int, key b(b))")
	tk.MustExec("insert into t2 values(1, 1), (2, 2), (3, 3), (4, 4), (5,5)")
	// This line of select will mark column b stats as needed, and an invalid(empty) stats for column b
	// will be loaded at the next analyze line, this will trigger the bug.
	tk.MustQuery("select * from t2 where b=2")
	tk.MustExec("analyze table t2 index b")
	tk.MustQuery("explain select * from t2 where b=2").Check(testkit.Rows(
		"TableReader_7 1.00 root  data:Selection_6",
		"└─Selection_6 1.00 cop[tikv]  eq(test.t2.b, 2)",
		"  └─TableFullScan_5 5.00 cop[tikv] table:t2 keep order:false"))
}

func (s *testStatsSuite) TestRangeStepOverflow(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (col datetime)")
	tk.MustExec("insert into t values('3580-05-26 07:16:48'),('4055-03-06 22:27:16'),('4862-01-26 07:16:54')")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t")
	// Trigger the loading of column stats.
	tk.MustQuery("select * from t where col between '8499-1-23 2:14:38' and '9961-7-23 18:35:26'").Check(testkit.Rows())
	c.Assert(h.LoadNeededHistograms(), IsNil)
	// Must execute successfully after loading the column stats.
	tk.MustQuery("select * from t where col between '8499-1-23 2:14:38' and '9961-7-23 18:35:26'").Check(testkit.Rows())
}

func (s *testStatsSuite) TestSmallRangeEstimation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int)")
	for i := 0; i < 400; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%v), (%v), (%v)", i, i, i)) // [0, 400)
	}
	testKit.MustExec("analyze table t with 0 topn")

	h := s.do.StatsHandle()
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := h.GetTableStats(table.Meta())
	sc := &stmtctx.StatementContext{}
	col := statsTbl.Columns[table.Meta().Columns[0].ID]

	var input []struct {
		Start int64
		End   int64
	}
	var output []struct {
		Start int64
		End   int64
		Count float64
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ran := range input {
		count, err := col.GetColumnRowCount(sc, getRange(ran.Start, ran.End), statsTbl.Count, false)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].Start = ran.Start
			output[i].End = ran.End
			output[i].Count = count
		})
		c.Assert(math.Abs(count-output[i].Count) < eps, IsTrue, Commentf("for [%v, %v], needed: around %v, got: %v", ran.Start, ran.End, output[i].Count, count))
	}
}
