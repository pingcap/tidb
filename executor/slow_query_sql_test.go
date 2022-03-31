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

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryWithoutSlowLog(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = "tidb-slow-not-exist.log"
	newCfg.Log.SlowThreshold = math.MaxUint64
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	tk.MustQuery("select query from information_schema.slow_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.slow_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
}

func TestSlowQuerySensitiveQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

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

func TestSlowQueryPrepared(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		tk.MustExec("set tidb_redact_log=0;")
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

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
}

func TestLogSlowLogIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = f.Name()
	})
	require.NoError(t, logutil.InitLogger(config.GetGlobalConfig().Log.ToLogConfig()))

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

func TestSlowQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	_, err = f.WriteString(`
# Time: 2020-10-13T20:08:13.970563+08:00
select * from t;
# Time: 2020-10-16T20:08:13.970563+08:00
select * from t;
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

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2020-10-16 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2019-10-13 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("2"))
}
