// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestRecordHistoryStatsAfterAnalyze(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats")
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")

	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// 1. switch off the tidb_enable_historical_stats, and there is no records in table `mysql.stats_history`
	rows := tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table t with 2 topn")
	rows = tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	// 2. switch on the tidb_enable_historical_stats and do analyze
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("analyze table t with 2 topn")
	// dump historical stats
	hsWorker := dom.GetHistoricalStatsWorker()
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.Nil(t, err)
	rows = tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.GreaterOrEqual(t, num, 1)

	// 3. dump current stats json
	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil, true)
	require.NoError(t, err)
	jsOrigin, _ := json.Marshal(dumpJSONTable)

	// 4. get the historical stats json
	rows = tk.MustQuery(fmt.Sprintf("select * from mysql.stats_history where table_id = '%d' and create_time = ("+
		"select create_time from mysql.stats_history where table_id = '%d' order by create_time desc limit 1) "+
		"order by seq_no", tableInfo.Meta().ID, tableInfo.Meta().ID)).Rows()
	num = len(rows)
	require.GreaterOrEqual(t, num, 1)
	data := make([][]byte, num)
	for i, row := range rows {
		data[i] = []byte(row[1].(string))
	}
	jsonTbl, err := storage.BlocksToJSONTable(data)
	require.NoError(t, err)
	jsCur, err := json.Marshal(jsonTbl)
	require.NoError(t, err)
	// 5. historical stats must be equal to the current stats
	require.JSONEq(t, string(jsOrigin), string(jsCur))
}

func TestRecordHistoryStatsMetaAfterAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("analyze table test.t")

	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// 1. switch off the tidb_enable_historical_stats, and there is no record in table `mysql.stats_meta_history`
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Check(testkit.Rows("0"))
	// insert demo tuples, and there is no record either.
	insertNums := 5
	for i := 0; i < insertNums; i++ {
		tk.MustExec("insert into test.t (a,b) values (1,1), (2,2), (3,3)")
		err := h.DumpStatsDeltaToKV(false)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Check(testkit.Rows("0"))

	// 2. switch on the tidb_enable_historical_stats and insert tuples to produce count/modifyCount delta change.
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")

	for i := 0; i < insertNums; i++ {
		tk.MustExec("insert into test.t (a,b) values (1,1), (2,2), (3,3)")
		err := h.DumpStatsDeltaToKV(false)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("18 18", "21 21", "24 24", "27 27", "30 30"))
	tk.MustQuery(fmt.Sprintf("select distinct source from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Sort().Check(testkit.Rows("flush stats"))

	// assert delete
	tk.MustExec("delete from test.t where test.t.a = 1")
	err = h.DumpStatsDeltaToKV(true)
	require.NoError(t, err)
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = '%d'", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("40 20"))
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time desc limit 1", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("40 20"))

	// assert update
	tk.MustExec("update test.t set test.t.b = 4 where test.t.a = 2")
	err = h.DumpStatsDeltaToKV(true)
	require.NoError(t, err)
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = '%d'", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("50 20"))
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time desc limit 1", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("50 20"))
}

func TestGCHistoryStatsAfterDropTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("analyze table test.t")
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// dump historical stats
	h := dom.StatsHandle()
	hsWorker := dom.GetHistoricalStatsWorker()
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.Nil(t, err)

	// assert the records of history stats table
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d' order by create_time",
		tableInfo.Meta().ID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'",
		tableInfo.Meta().ID)).Check(testkit.Rows("1"))
	// drop the table and gc stats
	tk.MustExec("drop table t")
	is = dom.InfoSchema()
	h.GCStats(is, 0)

	// assert stats_history tables delete the record of dropped table
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d' order by create_time",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
}

func TestAssertHistoricalStatsAfterAlterTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10),c int, KEY `idx` (`c`))")
	tk.MustExec("analyze table test.t")
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// dump historical stats
	h := dom.StatsHandle()
	hsWorker := dom.GetHistoricalStatsWorker()
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.Nil(t, err)

	time.Sleep(1 * time.Second)
	snapshot := oracle.GoTimeToTS(time.Now())
	jsTable, _, err := h.DumpHistoricalStatsBySnapshot("test", tableInfo.Meta(), snapshot)
	require.NoError(t, err)
	require.NotNil(t, jsTable)
	require.NotEqual(t, jsTable.Version, uint64(0))
	originVersion := jsTable.Version

	// assert historical stats non-change after drop column
	tk.MustExec("alter table t drop column b")
	h.GCStats(is, 0)
	snapshot = oracle.GoTimeToTS(time.Now())
	jsTable, _, err = h.DumpHistoricalStatsBySnapshot("test", tableInfo.Meta(), snapshot)
	require.NoError(t, err)
	require.NotNil(t, jsTable)
	require.Equal(t, jsTable.Version, originVersion)

	// assert historical stats non-change after drop index
	tk.MustExec("alter table t drop index idx")
	h.GCStats(is, 0)
	snapshot = oracle.GoTimeToTS(time.Now())
	jsTable, _, err = h.DumpHistoricalStatsBySnapshot("test", tableInfo.Meta(), snapshot)
	require.NoError(t, err)
	require.NotNil(t, jsTable)
	require.Equal(t, jsTable.Version, originVersion)
}

func TestGCOutdatedHistoryStats(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("analyze table test.t")
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// dump historical stats
	h := dom.StatsHandle()
	hsWorker := dom.GetHistoricalStatsWorker()
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.Nil(t, err)

	// assert the records of history stats table
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d' order by create_time",
		tableInfo.Meta().ID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'",
		tableInfo.Meta().ID)).Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_historical_stats_duration = '1s'")
	duration := variable.HistoricalStatsDuration.Load()
	fmt.Println(duration.String())
	time.Sleep(2 * time.Second)
	err = dom.StatsHandle().ClearOutdatedHistoryStats()
	require.NoError(t, err)
	// assert the records of history stats table
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d' order by create_time",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
}

func TestPartitionTableHistoricalStats(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, index idx(b))
PARTITION BY RANGE ( a ) (
PARTITION p0 VALUES LESS THAN (6)
)`)
	tk.MustExec("delete from mysql.stats_history")

	tk.MustExec("analyze table test.t")
	// dump historical stats
	h := dom.StatsHandle()
	hsWorker := dom.GetHistoricalStatsWorker()

	// assert global table and partition table be dumped
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err := hsWorker.DumpHistoricalStats(tblID, h)
	require.NoError(t, err)
	tblID = hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.NoError(t, err)
	tk.MustQuery("select count(*) from mysql.stats_history").Check(testkit.Rows("2"))
}

func TestDumpHistoricalStatsByTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, index idx(b))
PARTITION BY RANGE ( a ) (
PARTITION p0 VALUES LESS THAN (6)
)`)
	// dump historical stats
	h := dom.StatsHandle()

	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl)

	// dump historical stats
	hsWorker := dom.GetHistoricalStatsWorker()
	// only partition p0 stats will be dumped in static mode
	tblID := hsWorker.GetOneHistoricalStatsTable()
	require.NotEqual(t, tblID, -1)
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.NoError(t, err)
	tblID = hsWorker.GetOneHistoricalStatsTable()
	require.Equal(t, tblID, int64(-1))

	time.Sleep(1 * time.Second)
	snapshot := oracle.GoTimeToTS(time.Now())
	jsTable, _, err := h.DumpHistoricalStatsBySnapshot("test", tbl.Meta(), snapshot)
	require.NoError(t, err)
	require.NotNil(t, jsTable)
	// only has p0 stats
	require.NotNil(t, jsTable.Partitions["p0"])
	require.Nil(t, jsTable.Partitions[util.TiDBGlobalStats])

	// change static to dynamic then assert
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("analyze table t")
	require.NoError(t, err)
	// global and p0's stats will be dumped
	tblID = hsWorker.GetOneHistoricalStatsTable()
	require.NotEqual(t, tblID, -1)
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.NoError(t, err)
	tblID = hsWorker.GetOneHistoricalStatsTable()
	require.NotEqual(t, tblID, -1)
	err = hsWorker.DumpHistoricalStats(tblID, h)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	snapshot = oracle.GoTimeToTS(time.Now())
	jsTable, _, err = h.DumpHistoricalStatsBySnapshot("test", tbl.Meta(), snapshot)
	require.NoError(t, err)
	require.NotNil(t, jsTable)
	// has both global and p0 stats
	require.NotNil(t, jsTable.Partitions["p0"])
	require.NotNil(t, jsTable.Partitions[util.TiDBGlobalStats])
}

func TestDumpHistoricalStatsFallback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, index idx(b))
PARTITION BY RANGE ( a ) (
PARTITION p0 VALUES LESS THAN (6)
)`)
	// dump historical stats
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl)

	// dump historical stats
	hsWorker := dom.GetHistoricalStatsWorker()
	tblID := hsWorker.GetOneHistoricalStatsTable()
	// assert no historical stats task generated
	require.Equal(t, tblID, int64(-1))
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	h := dom.StatsHandle()
	jt, _, err := h.DumpHistoricalStatsBySnapshot("test", tbl.Meta(), oracle.GoTimeToTS(time.Now()))
	require.NoError(t, err)
	require.NotNil(t, jt)
	require.False(t, jt.IsHistoricalStats)
}
