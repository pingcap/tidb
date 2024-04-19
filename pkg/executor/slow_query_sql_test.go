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
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryWithoutSlowLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = "tidb-slow-not-exist.log"
	newCfg.Instance.SlowThreshold = math.MaxUint64
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", newCfg.Log.SlowQueryFile))
	tk.MustQuery("select query from information_schema.slow_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.slow_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
}

func TestSlowQuerySensitiveQuery(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec("drop user if exists user_sensitive;")
	tk.MustExec("create user user_sensitive identified by '123456789';")
	tk.MustExec("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustExec("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query from `information_schema`.`slow_query` " +
		"where (query like 'set password%' or query like 'create user%' or query like 'alter user%') " +
		"and query like '%user_sensitive%' order by query;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***};",
			"create user {user_sensitive@% password = ***};",
			"set password for user user_sensitive@%;",
		))
}

func TestSlowQueryNonPrepared(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		tk.MustExec("set tidb_redact_log=0;")
	}()

	tk.MustExec(`use test`)
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec("set tidb_slow_log_threshold=0;")

	tk.MustExec(`select * from t where a<1`)
	tk.MustExec(`select * from t where a<2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
	tk.MustExec(`select * from t where a<3`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustQuery(`select prepared, plan_from_cache, query from information_schema.slow_query where query like '%select * from t where a%' order by query`).Check(testkit.Rows(
		`0 0 select * from t where a<1;`,
		`0 1 select * from t where a<2;`,
		`0 0 select * from t where a<3;`))
}

func TestSlowQueryMisc(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		tk.MustExec("set tidb_redact_log=0;")
	}()

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec(`prepare mystmt1 from 'select sleep(?), 1';`)
	tk.MustExec("SET @num = 0.01;")
	tk.MustExec("execute mystmt1 using @num;")
	tk.MustQuery("SELECT Query FROM `information_schema`.`slow_query` " +
		"where query like 'select%sleep%' order by time desc limit 1").
		Check(testkit.Rows("select sleep(?), 1 [arguments: 0.01];"))

	tk.MustExec("set tidb_redact_log=1;")
	tk.MustExec(`prepare mystmt2 from 'select sleep(?), 2';`)
	tk.MustExec("execute mystmt2 using @num;")
	tk.MustQuery("SELECT Query FROM `information_schema`.`slow_query` " +
		"where query like 'select%sleep%' order by time desc limit 1").
		Check(testkit.Rows("select `sleep` ( ? ) , ?;"))

	// Test 3 kinds of stale-read query.
	tk.MustExec("create table test.t_stale_read (a int)")
	time.Sleep(time.Second + time.Millisecond*10)
	tk.MustExec("set tidb_redact_log=0;")
	tk.MustExec("set @@tidb_read_staleness='-1'")
	tk.MustQuery("select a from test.t_stale_read")
	tk.MustExec("set @@tidb_read_staleness='0'")
	t1 := time.Now()
	tk.MustQuery(fmt.Sprintf("select a from test.t_stale_read as of timestamp '%s'", t1.Format("2006-1-2 15:04:05")))
	tk.MustExec(fmt.Sprintf("start transaction read only as of timestamp '%v'", t1.Format("2006-1-2 15:04:05")))
	tk.MustQuery("select a from test.t_stale_read")
	tk.MustExec("commit")
	require.Len(t, tk.MustQuery("SELECT query, txn_start_ts  FROM `information_schema`.`slow_query` "+
		"where (query = 'select a from test.t_stale_read;' or query like 'select a from test.t_stale_read as of timestamp %') and Txn_start_ts > 0").Rows(), 3)
}

func TestLogSlowLogIndex(t *testing.T) {
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = f.Name()
	})
	require.NoError(t, logutil.InitLogger(config.GetGlobalConfig().Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int,index idx(a));")
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustQuery("select * from t use index (idx) where a in (1) union select * from t use index (idx) where a in (2,3);")
	tk.MustExec("set tidb_slow_log_threshold=300;")
	tk.MustQuery("select index_names from `information_schema`.`slow_query` " +
		"where query like 'select%union%' limit 1").
		Check(testkit.Rows("[t:idx]"))
}

func TestSlowQuerySessionAlias(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		tk.MustExec("set tidb_redact_log=0;")
	}()

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec("set @@tidb_session_alias='alias123'")
	tk.MustQuery("select sleep(0.0123);")
	tk.MustQuery("select Session_alias from `information_schema`.`slow_query` " +
		"where Query='select sleep(0.0123);' limit 1").
		Check(testkit.Rows("alias123"))

	tk.MustExec("set @@tidb_session_alias='alias中文'")
	tk.MustQuery("select sleep(0.0456);")
	tk.MustQuery("select Session_alias from `information_schema`.`slow_query` " +
		"where Query='select sleep(0.0456);' limit 1").
		Check(testkit.Rows("alias中文"))
}

func TestSlowQuery(t *testing.T) {
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	_, err = f.WriteString(`
# Time: 2020-10-13T20:08:13.970563+08:00
# Plan_digest: 0368dd12858f813df842c17bcb37ca0e8858b554479bebcd78da1f8c14ad12d0
select * from t;
# Time: 2020-10-16T20:08:13.970563+08:00
# Plan_digest: 0368dd12858f813df842c17bcb37ca0e8858b554479bebcd78da1f8c14ad12d0
select * from t;
# Time: 2022-04-21T14:44:54.103041447+08:00
# Txn_start_ts: 432674816242745346
# Query_time: 59.251052432
# Parse_time: 0
# Compile_time: 21.36997765
# Rewrite_time: 2.107040149
# Optimize_time: 12.866449698
# Wait_TS: 1.485568827
# Cop_time: 8.619838386 Request_count: 1 Total_keys: 1 Rocksdb_block_cache_hit_count: 3
# Index_names: [bind_info:time_index]
# Is_internal: true
# Digest: caf0da652413a857b1ded77811703043e52753ca8a466e20e89c6b74d9662783
# Stats: bind_info:pseudo
# Num_cop_tasks: 1
# Cop_proc_avg: 0 Cop_proc_addr: 172.16.6.173:40161
# Cop_wait_avg: 0 Cop_wait_addr: 172.16.6.173:40161
# Mem_max: 186
# Prepared: false
# Plan_from_cache: false
# Plan_from_binding: false
# Has_more_results: false
# KV_total: 4.032247202
# PD_total: 0.108570401
# Backoff_total: 0
# Write_sql_response_total: 0
# Result_rows: 0
# Succ: true
# IsExplicitTxn: false
# Plan: tidb_decode_plan('8gW4MAkxNF81CTAJMzMzMy4zMwlteXNxbC5iaW5kX2luZm8udXBkYXRlX3RpbWUsIG06HQAMY3JlYQ0ddAkwCXRpbWU6MTkuM3MsIGxvb3BzOjEJMCBCeXRlcxEIIAoxCTMwXzEzCRlxFTkINy40GTkYLCAJMTg2IAk9OE4vQQoyCTQ3XzExCTFfMBWsFHRhYmxlOhWsHCwgaW5kZXg6AYgAXwULCCh1cBW+OCksIHJhbmdlOigwMDAwLQUDDCAwMDoFAwAuARSgMDAsK2luZl0sIGtlZXAgb3JkZXI6ZmFsc2UsIHN0YXRzOnBzZXVkbwkN6wg5LjYysgDAY29wX3Rhc2s6IHtudW06IDEsIG1heDogNS4wNnMsIHByb2Nfa2V5czogMCwgcnBjXxEmAQwBtRw6IDQuMDVzLAFKSHJfY2FjaGVfaGl0X3JhdGlvOiABphh9LCB0aWt2CWgAewU1ADA5Nlh9LCBzY2FuX2RldGFpbDoge3RvdGFsXwF6CGVzcxl9RhcAFF9zaXplOgGZCRwAawWogDEsIHJvY2tzZGI6IHtkZWxldGVfc2tpcHBlZF9jb3VudAUyCGtleUoWAAxibG9jIQsZxw0yFDMsIHJlYS5BAAUPCGJ5dAGBKfMYfX19CU4vQQEEIfoQNV8xMgly+gGCsgEgCU4vQQlOL0EK')
# Plan_digest: c338c3017eb2e4980cb49c8f804fea1fb7c1104aede2385f12909cdd376799b3
SELECT original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source  FROM mysql.bind_info WHERE update_time > '0000-00-00 00:00:00' ORDER BY update_time, create_time;
`)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	executor.ParseSlowLogBatchSize = 1
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		executor.ParseSlowLogBatchSize = 64
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@time_zone='+08:00'")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2020-10-16 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2019-10-13 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("2"))
	// Cover tidb issue 34320
	tk.MustQuery("select count(plan_digest) from `information_schema`.`slow_query` where time > '2019-10-13 20:08:13' and time < now();").Check(testkit.Rows("3"))
	tk.MustQuery("select count(plan_digest) from `information_schema`.`slow_query` where time > '2022-04-29 17:50:00'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time < '2010-01-02 15:04:05'").Check(testkit.Rows("0"))
}

func TestIssue37066(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int, primary key (a) clustered);")
	tk.MustExec("create table t2(a int, b int, primary key (a) nonclustered);")
	tk.MustExec("create table t3(a varchar(10), b varchar(10), c int, primary key (a) clustered, index ib(b), index ic(c));")
	tk.MustExec("create table t4(a varchar(10), b int, primary key (a) nonclustered);")

	cases := []string{
		"select * from t1 where a = 10",
		"select * from t2 where a = 10",
		"select * from t3 where a = 'abc'",
		"select * from t4 where a = 'abc'",
		"select * from t1 where a in (10, 11, 12)",
		"select * from t2 where a in (10, 11, 12)",
		"select * from t3 where a in ('abc', 'bcd', 'cde')",
		"select * from t4 where a in ('abc', 'bcd', 'cde')",
	}
	// For now, we keep the consistency between the index_names column and the result of EXPLAIN.
	// And what's the best behavior is still to be discussed.
	results := []string{
		"",
		"[t2:PRIMARY]",
		"[t3:PRIMARY]",
		"[t4:PRIMARY]",
		"",
		"[t2:PRIMARY]",
		"[t3:PRIMARY]",
		"[t4:PRIMARY]",
	}

	for i, c := range cases {
		tk.MustQuery(c)
		result1 := testdata.ConvertRowsToStrings(tk.MustQuery("select index_names from information_schema.slow_query " +
			`where query = "` + c + `;"` +
			"limit 1;").Rows())
		result2 := testdata.ConvertRowsToStrings(tk.MustQuery("select index_names from information_schema.statements_summary " +
			`where QUERY_SAMPLE_TEXT like "%` + c + `%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
			"limit 1;").Rows())
		// assert result1
		require.Len(t, result1, 1)
		res1 := result1[0]
		require.Equal(t, results[i], res1)
		// assert result2
		require.Len(t, result2, 1)
		res2 := result2[0]
		if res1 == "" {
			require.Equal(t, res2, "<nil>")
		} else {
			require.Equal(t, res1, "["+res2+"]")
		}
	}
}

func TestWarningsInSlowQuery(t *testing.T) {
	// Prepare the slow log
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, e int, f int, g int, h set('11', '22', '33')," +
		"primary key (a), unique key c_d_e (c, d, e), unique key f (f), unique key f_g (f, g), key g (g))")
	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		SQL    string
		Result string
	}
	slowQuerySuiteData.LoadTestCases(t, &input, &output)
	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		if len(test) < 6 || test[:6] != "select" {
			tk.MustExec(test)
		} else {
			tk.MustQuery(test)
		}
		res := testdata.ConvertRowsToStrings(
			tk.MustQuery("select warnings from information_schema.slow_query " +
				`where query = "` + test + `;" ` +
				"order by time desc limit 1").Rows(),
		)
		require.Lenf(t, res, 1, comment)

		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Result = res[0]
		})
		require.Equal(t, output[i].Result, res[0])
	}
}
