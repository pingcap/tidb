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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestAnalyzePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("analyze table t")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi := table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	do, err := session.GetDomain(s.store)
	c.Assert(err, IsNil)
	handle := do.StatsHandle()
	for _, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Assert(len(statsTbl.Columns), Equals, 3)
		c.Assert(len(statsTbl.Indices), Equals, 1)
		for _, col := range statsTbl.Columns {
			c.Assert(col.Len(), Greater, 0)
		}
		for _, idx := range statsTbl.Indices {
			c.Assert(idx.Len(), Greater, 0)
		}
	}

	tk.MustExec("drop table t")
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("alter table t analyze partition p0")
	is = executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi = table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	for i, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		if i == 0 {
			c.Assert(statsTbl.Pseudo, IsFalse)
			c.Assert(len(statsTbl.Columns), Equals, 3)
			c.Assert(len(statsTbl.Indices), Equals, 1)
		} else {
			c.Assert(statsTbl.Pseudo, IsTrue)
		}
	}
}

func (s *testSuite1) TestAnalyzeParameters(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	tk.MustExec("analyze table t")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 20)

	tk.MustExec("analyze table t with 4 buckets")
	tbl = s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 4)
}

func (s *testSuite1) TestAnalyzeTooLongColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze table t")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 0)
	c.Assert(tbl.Columns[1].TotColSize, Equals, int64(65559))
}

func (s *testSuite1) TestAnalyzeFastSample(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	var dom *domain.Domain
	session.SetStatsLease(0)
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	executor.MaxSampleSize = 20
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	// construct 5 regions split by {12, 24, 36, 48}
	splitKeys := generateTableSplitKeyForInt(tid, []int{12, 24, 36, 48})
	manipulateCluster(cluster, splitKeys)

	var pkCol *model.ColumnInfo
	var colsInfo []*model.ColumnInfo
	var indicesInfo []*model.IndexInfo
	for _, col := range tblInfo.Columns {
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		} else {
			colsInfo = append(colsInfo, col)
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	mockExec := &executor.AnalyzeTestFastExec{
		Ctx:             tk.Se.(sessionctx.Context),
		PKInfo:          pkCol,
		ColsInfo:        colsInfo,
		IdxsInfo:        indicesInfo,
		Concurrency:     1,
		PhysicalTableID: tbl.(table.PhysicalTable).GetPhysicalID(),
		TblInfo:         tblInfo,
	}
	err = mockExec.TestFastSample()
	c.Assert(err, IsNil)
	vals := make([][]string, 0)
	c.Assert(len(mockExec.Collectors), Equals, 3)
	for i := 0; i < 2; i++ {
		vals = append(vals, make([]string, 0))
		c.Assert(len(mockExec.Collectors[i].Samples), Equals, 20)
		for j := 0; j < 20; j++ {
			s, err := mockExec.Collectors[i].Samples[j].Value.ToString()
			c.Assert(err, IsNil)
			vals[i] = append(vals[i], s)
		}
	}
	c.Assert(fmt.Sprintln(vals), Equals, "[[0 4 6 9 10 11 12 14 17 24 25 29 30 34 35 44 52 54 57 58] [0 4 6 9 10 11 12 14 17 24 25 29 30 34 35 44 52 54 57 58]]\n")
}

func (s *testSuite1) TestFastAnalyze(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	var dom *domain.Domain
	session.SetStatsLease(0)
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	executor.MaxSampleSize = 1000
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	for i := 0; i < 3000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID

	// construct 5 regions split by {600, 1200, 1800, 2400}
	splitKeys := generateTableSplitKeyForInt(tid, []int{600, 1200, 1800, 2400})
	manipulateCluster(cluster, splitKeys)

	tk.MustExec("analyze table t with 5 buckets")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	sTbl := fmt.Sprintln(tbl)
	matched := false
	if sTbl == "Table:39 Count:3000\n"+
		"column:1 ndv:3000 totColSize:0\n"+
		"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
		"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
		"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
		"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
		"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n"+
		"column:2 ndv:3000 totColSize:0\n"+
		"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
		"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
		"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
		"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
		"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n"+
		"index:1 ndv:3000\n"+
		"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
		"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
		"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
		"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
		"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n" ||
		sTbl == "Table:39 Count:3000\n"+
			"column:2 ndv:3000 totColSize:0\n"+
			"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
			"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
			"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
			"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
			"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n"+
			"column:1 ndv:3000 totColSize:0\n"+
			"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
			"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
			"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
			"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
			"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n"+
			"index:1 ndv:3000\n"+
			"num: 603 lower_bound: 6 upper_bound: 612 repeats: 1\n"+
			"num: 603 lower_bound: 621 upper_bound: 1205 repeats: 1\n"+
			"num: 603 lower_bound: 1207 upper_bound: 1830 repeats: 1\n"+
			"num: 603 lower_bound: 1831 upper_bound: 2387 repeats: 1\n"+
			"num: 588 lower_bound: 2390 upper_bound: 2997 repeats: 1\n" {
		matched = true
	}
	c.Assert(matched, Equals, true)
}

func (s *testSuite1) TestAnalyzeIncremental(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b))")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  idx 1 0 1 1 1 1"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
	tk.MustExec("analyze incremental table t index")
	// Result should not change.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
}
