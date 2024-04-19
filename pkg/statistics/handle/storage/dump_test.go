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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/internal"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func requireTableEqual(t *testing.T, a *statistics.Table, b *statistics.Table) {
	require.Equal(t, b.RealtimeCount, a.RealtimeCount)
	require.Equal(t, b.ModifyCount, a.ModifyCount)
	require.Equal(t, len(b.Columns), len(a.Columns))
	for i := range a.Columns {
		require.True(t, statistics.HistogramEqual(&a.Columns[i].Histogram, &b.Columns[i].Histogram, false))
		if a.Columns[i].CMSketch == nil {
			require.Nil(t, b.Columns[i].CMSketch)
		} else {
			require.True(t, a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch))
		}
		// The nil case has been considered in (*TopN).Equal() so we don't need to consider it here.
		require.Truef(t, a.Columns[i].TopN.Equal(b.Columns[i].TopN), "%v, %v", a.Columns[i].TopN, b.Columns[i].TopN)
	}
	require.Equal(t, len(b.Indices), len(a.Indices))
	for i := range a.Indices {
		require.True(t, statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false))
		if a.Indices[i].CMSketch == nil {
			require.Nil(t, b.Indices[i].CMSketch)
		} else {
			require.True(t, a.Indices[i].CMSketch.Equal(b.Indices[i].CMSketch))
		}
		require.True(t, a.Indices[i].TopN.Equal(b.Indices[i].TopN))
	}
	require.True(t, internal.IsSameExtendedStats(a.ExtendedStats, b.ExtendedStats))
}

func cleanStats(tk *testkit.TestKit, do *domain.Domain) {
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	tk.MustExec("delete from mysql.stats_extended")
	tk.MustExec("delete from mysql.stats_fm_sketch")
	tk.MustExec("delete from mysql.column_stats_usage")
	do.StatsHandle().Clear()
}

func TestConversion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index c on t(a,b)")
	tk.MustExec("insert into t(a,b) values (3, 1),(2, 1),(1, 10)")
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t(a,b) values (1, 1),(3, 1),(5, 10)")
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.Nil(t, h.DumpStatsDeltaToKV(true))
	require.Nil(t, h.Update(is))

	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)
	loadTbl, err := storage.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, jsonTbl)
	require.NoError(t, err)

	tbl := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTbl, tbl)
	cleanStats(tk, dom)
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		require.Nil(t, h.Update(is))
	})
	err = h.LoadStatsFromJSON(context.Background(), is, jsonTbl, 0)
	wg.Wait()
	require.NoError(t, err)
	loadTblInStorage := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTblInStorage, tbl)
}

func getStatsJSON(t *testing.T, dom *domain.Domain, db, tableName string) *handleutil.JSONTable {
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.Nil(t, h.Update(is))
	table, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(tableName))
	require.NoError(t, err)
	tableInfo := table.Meta()
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo, nil, true)
	require.NoError(t, err)
	return jsonTbl
}

func persistStats(ctx context.Context, t *testing.T, dom *domain.Domain, db, tableName string, persist statstypes.PersistFunc) {
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.Nil(t, h.Update(is))
	table, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(tableName))
	require.NoError(t, err)
	tableInfo := table.Meta()
	err = h.PersistStatsBySnapshot(ctx, "test", tableInfo, math.MaxUint64, persist)
	require.NoError(t, err)
}

func TestDumpGlobalStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t values (1), (2)")
	tk.MustExec("analyze table t")

	// global-stats is not existed
	stats := getStatsJSON(t, dom, "test", "t")
	require.NotNil(t, stats.Partitions["p0"])
	require.NotNil(t, stats.Partitions["p1"])
	require.Nil(t, stats.Partitions[handleutil.TiDBGlobalStats])

	// global-stats is existed
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t")
	stats = getStatsJSON(t, dom, "test", "t")
	require.NotNil(t, stats.Partitions["p0"])
	require.NotNil(t, stats.Partitions["p1"])
	require.NotNil(t, stats.Partitions[handleutil.TiDBGlobalStats])
}

func TestLoadGlobalStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t values (1), (2)")
	tk.MustExec("analyze table t")
	globalStats := getStatsJSON(t, dom, "test", "t")

	// remove all statistics
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	dom.StatsHandle().Clear()
	clearedStats := getStatsJSON(t, dom, "test", "t")
	require.Equal(t, 0, len(clearedStats.Partitions))

	// load global-stats back
	require.Nil(t, dom.StatsHandle().LoadStatsFromJSON(context.Background(), dom.InfoSchema(), globalStats, 0))
	loadedStats := getStatsJSON(t, dom, "test", "t")
	require.Equal(t, 3, len(loadedStats.Partitions)) // p0, p1, global
}

func TestLoadPartitionStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 8")
	vals := make([]string, 0, 5000)
	for i := 0; i < 5000; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
	tk.MustExec("analyze table t")

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	jsonTbl, err := dom.StatsHandle().DumpStatsToJSON("test", tableInfo, nil, true)
	require.NoError(t, err)
	pi := tableInfo.GetPartitionInfo()
	originPartStats := make([]*statistics.Table, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		originPartStats = append(originPartStats, dom.StatsHandle().GetPartitionStats(tableInfo, def.ID))
	}
	originGlobalStats := dom.StatsHandle().GetTableStats(tableInfo)

	// remove all statistics
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	dom.StatsHandle().Clear()
	clearedStats := getStatsJSON(t, dom, "test", "t")
	require.Equal(t, 0, len(clearedStats.Partitions))

	// load stats back
	require.Nil(t, dom.StatsHandle().LoadStatsFromJSON(context.Background(), dom.InfoSchema(), jsonTbl, 0))

	// compare
	for i, def := range pi.Definitions {
		newPartStats := dom.StatsHandle().GetPartitionStats(tableInfo, def.ID)
		requireTableEqual(t, originPartStats[i], newPartStats)
	}
	requireTableEqual(t, originGlobalStats, dom.StatsHandle().GetTableStats(tableInfo))
}

func TestLoadPartitionStatsErrPanic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	val := runtime.GOMAXPROCS(1)
	defer func() {
		runtime.GOMAXPROCS(val)
	}()
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 8")
	vals := make([]string, 0, 5000)
	for i := 0; i < 5000; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
	tk.MustExec("analyze table t")

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	jsonTbl, err := dom.StatsHandle().DumpStatsToJSON("test", tableInfo, nil, true)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), storage.TestLoadStatsErr{}, func(tableInfo *model.TableInfo, physicalID int64, jsonTbl *handleutil.JSONTable) error {
		return errors.New("ERROR")
	})
	err = dom.StatsHandle().LoadStatsFromJSON(ctx, dom.InfoSchema(), jsonTbl, 0)
	require.ErrorContains(t, err, "ERROR")
	ctx = context.WithValue(context.Background(), storage.TestLoadStatsErr{}, func(tableInfo *model.TableInfo, physicalID int64, jsonTbl *handleutil.JSONTable) error {
		panic("PANIC")
	})
	err = dom.StatsHandle().LoadStatsFromJSON(ctx, dom.InfoSchema(), jsonTbl, 0)
	require.ErrorContains(t, err, "PANIC") // recover panic as an error
}

func TestDumpPartitions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.Nil(t, h.Update(is))

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo, nil, true)
	require.NoError(t, err)
	pi := tableInfo.GetPartitionInfo()
	originTables := make([]*statistics.Table, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		originTables = append(originTables, h.GetPartitionStats(tableInfo, def.ID))
	}

	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	h.Clear()

	err = h.LoadStatsFromJSON(context.Background(), dom.InfoSchema(), jsonTbl, 0)
	require.NoError(t, err)
	for i, def := range pi.Definitions {
		tt := h.GetPartitionStats(tableInfo, def.ID)
		requireTableEqual(t, originTables[i], tt)
	}
}

func TestDumpAlteredTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() { h.SetLease(oriLease) }()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t drop column a")
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	_, err = h.DumpStatsToJSON("test", table.Meta(), nil, true)
	require.NoError(t, err)
}

func TestDumpCMSketchWithTopN(t *testing.T) {
	// Just test if we can store and recover the Top N elements stored in database.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int)")
	testKit.MustExec("insert into t values (1),(3),(4),(2),(5)")
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	require.Nil(t, h.Update(is))

	// Insert 30 fake data
	fakeData := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		fakeData = append(fakeData, []byte(fmt.Sprintf("%01024d", i)))
	}
	cms, _, _, _ := statistics.NewCMSketchAndTopN(5, 2048, fakeData, 20, 100)

	stat := h.GetTableStats(tableInfo)
	err = h.SaveStatsToStorage(tableInfo.ID, 1, 0, 0, &stat.Columns[tableInfo.Columns[0].ID].Histogram, cms, nil, statistics.Version2, 1, false, handleutil.StatsMetaHistorySourceLoadStats)
	require.NoError(t, err)
	require.Nil(t, h.Update(is))

	stat = h.GetTableStats(tableInfo)
	cmsFromStore := stat.Columns[tableInfo.Columns[0].ID].CMSketch
	require.NotNil(t, cmsFromStore)
	require.True(t, cms.Equal(cmsFromStore))

	jsonTable, err := h.DumpStatsToJSON("test", tableInfo, nil, true)
	require.NoError(t, err)
	err = h.LoadStatsFromJSON(context.Background(), is, jsonTable, 0)
	require.NoError(t, err)
	stat = h.GetTableStats(tableInfo)
	cmsFromJSON := stat.Columns[tableInfo.Columns[0].ID].CMSketch.Copy()
	require.True(t, cms.Equal(cmsFromJSON))
}

func TestDumpPseudoColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a))")
	// Force adding an pseudo tables in stats cache.
	testKit.MustQuery("select * from t")
	testKit.MustExec("analyze table t index idx")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	h := dom.StatsHandle()
	_, err = h.DumpStatsToJSON("test", tbl.Meta(), nil, true)
	require.NoError(t, err)
}

func TestDumpExtendedStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,5),(2,4),(3,3),(4,2),(5,1)")
	h := dom.StatsHandle()
	require.Nil(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")

	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tbl := h.GetTableStats(tableInfo.Meta())
	jsonTbl, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)
	loadTbl, err := storage.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, jsonTbl)
	require.NoError(t, err)
	requireTableEqual(t, loadTbl, tbl)

	cleanStats(tk, dom)
	wg := util.WaitGroupWrapper{}
	wg.Run(func() {
		require.Nil(t, h.Update(is))
	})
	err = h.LoadStatsFromJSON(context.Background(), is, jsonTbl, 0)
	wg.Wait()
	require.NoError(t, err)
	loadTblInStorage := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTblInStorage, tbl)
}

func TestDumpVer2Stats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)

	jsonBytes, err := json.MarshalIndent(dumpJSONTable, "", " ")
	require.NoError(t, err)

	loadJSONTable := &handleutil.JSONTable{}
	err = json.Unmarshal(jsonBytes, loadJSONTable)
	require.NoError(t, err)

	loadTbl, err := storage.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, loadJSONTable)
	require.NoError(t, err)

	// assert that a statistics.Table from storage dumped into JSON text and then unmarshalled into a statistics.Table keeps unchanged
	requireTableEqual(t, loadTbl, storageTbl)

	// assert that this statistics.Table is the same as the one in stats cache
	statsCacheTbl := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTbl, statsCacheTbl)

	err = h.LoadStatsFromJSON(context.Background(), is, loadJSONTable, 0)
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsCacheTbl = h.GetTableStats(tableInfo.Meta())
	// assert that after the JSONTable above loaded into storage then updated into the stats cache,
	// the statistics.Table in the stats cache is the same as the unmarshalled statistics.Table
	requireTableEqual(t, statsCacheTbl, loadTbl)
}

func TestLoadStatsForNewCollation(t *testing.T) {
	// This test is almost the same as TestDumpVer2Stats, except that: b varchar(10) => b varchar(3) collate utf8mb4_unicode_ci
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(3) collate utf8mb4_unicode_ci)")
	tk.MustExec("insert into t value(1, 'aaa'), (1, 'aaa'), (3, 'aab'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc'), (7, 'Ste')")
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

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)

	jsonBytes, err := json.MarshalIndent(dumpJSONTable, "", " ")
	require.NoError(t, err)

	loadJSONTable := &handleutil.JSONTable{}
	err = json.Unmarshal(jsonBytes, loadJSONTable)
	require.NoError(t, err)

	loadTbl, err := storage.TableStatsFromJSON(tableInfo.Meta(), tableInfo.Meta().ID, loadJSONTable)
	require.NoError(t, err)

	// assert that a statistics.Table from storage dumped into JSON text and then unmarshalled into a statistics.Table keeps unchanged
	requireTableEqual(t, loadTbl, storageTbl)

	// assert that this statistics.Table is the same as the one in stats cache
	statsCacheTbl := h.GetTableStats(tableInfo.Meta())
	requireTableEqual(t, loadTbl, statsCacheTbl)

	err = h.LoadStatsFromJSON(context.Background(), is, loadJSONTable, 0)
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	statsCacheTbl = h.GetTableStats(tableInfo.Meta())
	// assert that after the JSONTable above loaded into storage then updated into the stats cache,
	// the statistics.Table in the stats cache is the same as the unmarshalled statistics.Table
	requireTableEqual(t, statsCacheTbl, loadTbl)
}

func TestJSONTableToBlocks(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)
	jsOrigin, _ := json.Marshal(dumpJSONTable)

	blockSize := 30
	js, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)
	dumpJSONBlocks, err := storage.JSONTableToBlocks(js, blockSize)
	require.NoError(t, err)
	jsConverted, err := storage.BlocksToJSONTable(dumpJSONBlocks)
	require.NoError(t, err)
	jsonStr, err := json.Marshal(jsConverted)
	require.NoError(t, err)
	require.JSONEq(t, string(jsOrigin), string(jsonStr))
}

func TestLoadStatsFromOldVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(b))")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.Update(is))

	statsJSONFromOldVersion := `{
 "database_name": "test",
 "table_name": "t",
 "columns": {
  "a": {
   "histogram": {
    "ndv": 0
   },
   "cm_sketch": null,
   "null_count": 0,
   "tot_col_size": 256,
   "last_update_version": 440735055846047747,
   "correlation": 0
  },
  "b": {
   "histogram": {
    "ndv": 0
   },
   "cm_sketch": null,
   "null_count": 0,
   "tot_col_size": 256,
   "last_update_version": 440735055846047747,
   "correlation": 0
  }
 },
 "indices": {
  "idx": {
   "histogram": {
    "ndv": 0
   },
   "cm_sketch": null,
   "null_count": 0,
   "tot_col_size": 0,
   "last_update_version": 440735055846047747,
   "correlation": 0
  }
 },
 "count": 256,
 "modify_count": 256,
 "partitions": null
}`
	jsonTbl := &handleutil.JSONTable{}
	require.NoError(t, json.Unmarshal([]byte(statsJSONFromOldVersion), jsonTbl))
	require.NoError(t, h.LoadStatsFromJSON(context.Background(), is, jsonTbl, 0))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	statsTbl := h.GetTableStats(tbl.Meta())
	for _, col := range statsTbl.Columns {
		require.False(t, col.IsStatsInitialized())
	}
	for _, idx := range statsTbl.Indices {
		require.False(t, idx.IsStatsInitialized())
	}
}

func TestPersistStats(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	createTable := `CREATE TABLE t1 (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	tk.MustExec("CREATE TABLE t2 (a int, b int, primary key(a), index idx(b))")
	for i := 1; i < 21; i++ {
		tk.MustExec("insert into t1 values (?, ?)", i, i)
		tk.MustExec("insert into t2 values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")

	statsCnt := 0
	persistStats(ctx, t, dom, "test", "t1", func(ctx context.Context, jsonTable *handleutil.JSONTable, physicalID int64) error {
		require.True(t, physicalID > 0)
		require.NotNil(t, jsonTable)
		statsCnt += 1
		return nil
	})
	require.Equal(t, statsCnt, 5)
	statsCnt = 0
	persistStats(ctx, t, dom, "test", "t2", func(ctx context.Context, jsonTable *handleutil.JSONTable, physicalID int64) error {
		require.True(t, physicalID > 0)
		require.NotNil(t, jsonTable)
		statsCnt += 1
		return nil
	})
	require.Equal(t, statsCnt, 1)
}
