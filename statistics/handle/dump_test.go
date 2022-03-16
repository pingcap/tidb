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

package handle_test

import (
	"fmt"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStatsSuite) TestConversion(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index c on t(a,b)")
	tk.MustExec("insert into t(a,b) values (3, 1),(2, 1),(1, 10)")
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t(a,b) values (1, 1),(3, 1),(5, 10)")
	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)

	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	c.Assert(err, IsNil)
	loadTbl, err := handle.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, jsonTbl)
	c.Assert(err, IsNil)

	tbl := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTbl, tbl)

	cleanEnv(c, s.store, s.do)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.Assert(h.Update(is), IsNil)
		wg.Done()
	}()
	err = h.LoadStatsFromJSON(is, jsonTbl)
	wg.Wait()
	c.Assert(err, IsNil)
	loadTblInStorage := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTblInStorage, tbl)
}

func (s *testStatsSuite) TestDumpPartitions(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d)`, i, i))
	}
	tk.MustExec("analyze table t")
	is := s.do.InfoSchema()
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo, nil)
	c.Assert(err, IsNil)
	pi := tableInfo.GetPartitionInfo()
	originTables := make([]*statistics.Table, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		originTables = append(originTables, h.GetPartitionStats(tableInfo, def.ID))
	}

	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	h.Clear()

	err = h.LoadStatsFromJSON(s.do.InfoSchema(), jsonTbl)
	c.Assert(err, IsNil)
	for i, def := range pi.Definitions {
		t := h.GetPartitionStats(tableInfo, def.ID)
		assertTableEqual(c, originTables[i], t)
	}
}

func (s *testStatsSuite) TestDumpAlteredTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	h := s.do.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() { h.SetLease(oriLease) }()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t drop column a")
	table, err := s.do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = h.DumpStatsToJSON("test", table.Meta(), nil)
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TestDumpCMSketchWithTopN(c *C) {
	// Just test if we can store and recover the Top N elements stored in database.
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int)")
	testKit.MustExec("insert into t values (1),(3),(4),(2),(5)")
	testKit.MustExec("analyze table t")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	c.Assert(h.Update(is), IsNil)

	// Insert 30 fake data
	fakeData := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		fakeData = append(fakeData, []byte(fmt.Sprintf("%01024d", i)))
	}
	cms, _, _ := statistics.NewCMSketchWithTopN(5, 2048, fakeData, 20, 100)

	stat := h.GetTableStats(tableInfo)
	err = h.SaveStatsToStorage(tableInfo.ID, 1, 0, &stat.Columns[tableInfo.Columns[0].ID].Histogram, cms, 1)
	c.Assert(err, IsNil)
	c.Assert(h.Update(is), IsNil)

	stat = h.GetTableStats(tableInfo)
	cmsFromStore := stat.Columns[tableInfo.Columns[0].ID].CMSketch
	c.Assert(cmsFromStore, NotNil)
	c.Check(cms.Equal(cmsFromStore), IsTrue)

	jsonTable, err := h.DumpStatsToJSON("test", tableInfo, nil)
	c.Check(err, IsNil)
	err = h.LoadStatsFromJSON(is, jsonTable)
	c.Check(err, IsNil)
	stat = h.GetTableStats(tableInfo)
	cmsFromJSON := stat.Columns[tableInfo.Columns[0].ID].CMSketch.Copy()
	c.Check(cms.Equal(cmsFromJSON), IsTrue)
}

func (s *testStatsSuite) TestDumpPseudoColumns(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a))")
	// Force adding an pseudo tables in stats cache.
	testKit.MustQuery("select * from t")
	testKit.MustExec("analyze table t index idx")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	_, err = h.DumpStatsToJSON("test", tbl.Meta(), nil)
<<<<<<< HEAD
	c.Assert(err, IsNil)
=======
	require.NoError(t, err)
}

func TestDumpExtendedStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,5),(2,4),(3,3),(4,2),(5,1)")
	h := dom.StatsHandle()
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")

	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tbl := h.GetTableStats(tableInfo.Meta())
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)
	loadTbl, err := handle.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, jsonTbl)
	require.NoError(t, err)
	requireTableEqual(t, loadTbl, tbl)

	cleanStats(tk, dom)
	wg := util.WaitGroupWrapper{}
	wg.Run(func() {
		require.Nil(t, h.Update(is))
	})
	err = h.LoadStatsFromJSON(is, jsonTbl)
	wg.Wait()
	require.NoError(t, err)
	loadTblInStorage := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTblInStorage, tbl)
}

func TestDumpVer2Stats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("insert into t value(1, 'aaa'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc')")
	// mark column stats as needed
	tk.MustExec("select * from t where a = 3")
	tk.MustExec("select * from t where b = 'bbb'")
	tk.MustExec("alter table t add index single(a)")
	tk.MustExec("alter table t add index multi(a, b)")
	tk.MustExec("analyze table t with 2 topn")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	storageTbl, err := h.TableStatsFromStorage(tableInfo.Meta(), tableInfo.Meta().ID, false, 0)
	require.NoError(t, err)

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)

	jsonBytes, err := json.MarshalIndent(dumpJSONTable, "", " ")
	require.NoError(t, err)

	loadJSONTable := &handle.JSONTable{}
	err = json.Unmarshal(jsonBytes, loadJSONTable)
	require.NoError(t, err)

	loadTbl, err := handle.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, loadJSONTable)
	require.NoError(t, err)

	// assert that a statistics.Table from storage dumped into JSON text and then unmarshalled into a statistics.Table keeps unchanged
	requireTableEqual(t, loadTbl, storageTbl)

	// assert that this statistics.Table is the same as the one in stats cache
	statsCacheTbl := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTbl, statsCacheTbl)

	err = h.LoadStatsFromJSON(is, loadJSONTable)
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsCacheTbl = h.GetTableStats(tableInfo.Meta())
	// assert that after the JSONTable above loaded into storage then updated into the stats cache,
	// the statistics.Table in the stats cache is the same as the unmarshalled statistics.Table
	requireTableEqual(t, statsCacheTbl, loadTbl)
}

func TestLoadStatsForNewCollation(t *testing.T) {
	// This test is almost the same as TestDumpVer2Stats, except that: b varchar(10) => b varchar(3) collate utf8mb4_unicode_ci
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(3) collate utf8mb4_unicode_ci)")
	tk.MustExec("insert into t value(1, 'aaa'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc')")
	// mark column stats as needed
	tk.MustExec("select * from t where a = 3")
	tk.MustExec("select * from t where b = 'bbb'")
	tk.MustExec("alter table t add index single(a)")
	tk.MustExec("alter table t add index multi(a, b)")
	tk.MustExec("analyze table t with 2 topn")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	storageTbl, err := h.TableStatsFromStorage(tableInfo.Meta(), tableInfo.Meta().ID, false, 0)
	require.NoError(t, err)

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)

	jsonBytes, err := json.MarshalIndent(dumpJSONTable, "", " ")
	require.NoError(t, err)

	loadJSONTable := &handle.JSONTable{}
	err = json.Unmarshal(jsonBytes, loadJSONTable)
	require.NoError(t, err)

	loadTbl, err := handle.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, loadJSONTable)
	require.NoError(t, err)

	// assert that a statistics.Table from storage dumped into JSON text and then unmarshalled into a statistics.Table keeps unchanged
	requireTableEqual(t, loadTbl, storageTbl)

	// assert that this statistics.Table is the same as the one in stats cache
	statsCacheTbl := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTbl, statsCacheTbl)

	err = h.LoadStatsFromJSON(is, loadJSONTable)
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsCacheTbl = h.GetTableStats(tableInfo.Meta())
	// assert that after the JSONTable above loaded into storage then updated into the stats cache,
	// the statistics.Table in the stats cache is the same as the unmarshalled statistics.Table
	requireTableEqual(t, statsCacheTbl, loadTbl)
}

func TestJSONTableToBlocks(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("insert into t value(1, 'aaa'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc')")
	// mark column stats as needed
	tk.MustExec("select * from t where a = 3")
	tk.MustExec("select * from t where b = 'bbb'")
	tk.MustExec("alter table t add index single(a)")
	tk.MustExec("alter table t add index multi(a, b)")
	tk.MustExec("analyze table t with 2 topn")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)
	jsOrigin, _ := json.Marshal(dumpJSONTable)

	blockSize := 30
	js, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)
	dumpJSONBlocks, err := handle.JSONTableToBlocks(js, blockSize)
	require.NoError(t, err)
	jsConverted, err := handle.BlocksToJSONTable(dumpJSONBlocks)
	require.NoError(t, err)
	jsonStr, err := json.Marshal(jsConverted)
	require.NoError(t, err)
	require.JSONEq(t, string(jsOrigin), string(jsonStr))
>>>>>>> 128dc1682... statistics: fix load stat for new collation column (#33146)
}
