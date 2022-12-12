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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestRecordHistoryStatsAfterAnalyze(t *testing.T) {
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
	jsonTbl, err := handle.BlocksToJSONTable(data)
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
		err := h.DumpStatsDeltaToKV(handle.DumpDelta)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Check(testkit.Rows("0"))

	// 2. switch on the tidb_enable_historical_stats and insert tuples to produce count/modifyCount delta change.
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")

	for i := 0; i < insertNums; i++ {
		tk.MustExec("insert into test.t (a,b) values (1,1), (2,2), (3,3)")
		err := h.DumpStatsDeltaToKV(handle.DumpDelta)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("18 18", "21 21", "24 24", "27 27", "30 30"))

	// assert delete
	tk.MustExec("delete from test.t where test.t.a = 1")
	err = h.DumpStatsDeltaToKV(handle.DumpAll)
	require.NoError(t, err)
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = '%d' order by create_time desc", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("40 20"))
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time desc limit 1", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("40 20"))

	// assert update
	tk.MustExec("update test.t set test.t.b = 4 where test.t.a = 2")
	err = h.DumpStatsDeltaToKV(handle.DumpAll)
	require.NoError(t, err)
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = '%d' order by create_time desc", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("50 20"))
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time desc limit 1", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("50 20"))
}

func TestGCHistoryStatsAfterDropTable(t *testing.T) {
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
	h.GCStats(is, 0)

	// assert stats_history tables delete the record of dropped table
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d' order by create_time",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'",
		tableInfo.Meta().ID)).Check(testkit.Rows("0"))
}
