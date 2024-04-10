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

package clustertablestest

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newTestKitWithRoot(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func newTestKitWithPlanCache(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{PreparedPlanCache: plannercore.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false)})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.RefreshConnectionID()
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func TestInfoSchemaFieldValue(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists numschema, timeschema")
	tk.MustExec("create table numschema(i int(2), f float(4,2), d decimal(4,3))")
	tk.MustExec("create table timeschema(d date, dt datetime(3), ts timestamp(3), t time(4), y year(4))")
	tk.MustExec("create table strschema(c char(3), c2 varchar(3), b blob(3), t text(3))")
	tk.MustExec("create table floatschema(a float, b double(7, 3))")

	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='numschema'").
		Check(testkit.Rows("<nil> <nil> 2 0 <nil>", "<nil> <nil> 4 2 <nil>", "<nil> <nil> 4 3 <nil>")) // FIXME: for mysql first one will be "<nil> <nil> 10 0 <nil>"
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='timeschema'").
		Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 4", "<nil> <nil> <nil> <nil> <nil>"))
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='strschema'").
		Check(testkit.Rows("3 12 <nil> <nil> <nil>", "3 12 <nil> <nil> <nil>", "255 255 <nil> <nil> <nil>", "255 1020 <nil> <nil> <nil>"))
	tk.MustQuery("select NUMERIC_SCALE from information_schema.COLUMNS where table_name='floatschema'").
		Check(testkit.Rows("<nil>", "3"))

	// Test for auto increment ID.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int auto_increment primary key, d int)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("1"))
	tk.MustExec("insert into t(c, d) values(1, 1)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))

	tk.MustQuery("show create table t").Check(
		testkit.Rows("" +
			"t CREATE TABLE `t` (\n" +
			"  `c` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002"))

	// Test auto_increment for table without auto_increment column
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("<nil>"))

	tk.MustExec("create user xxx")

	// Test for length of enum and set
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t ( s set('a','bc','def','ghij') default NULL, e1 enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))")
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's'").Check(
		testkit.Rows("s 13"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 'S'").Check(
		testkit.Rows("s 13"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's2'").Check(
		testkit.Rows("s2 30"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 'e1'").Check(
		testkit.Rows("e1 4"))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))

	tk1.MustQuery("select distinct(table_schema) from information_schema.tables").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Fix issue 9836
	sm := &testkit.MockSessionManager{PS: make([]*util.ProcessInfo, 0)}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
		StmtCtx: tk.Session().GetSessionVars().StmtCtx,
	})
	tk.Session().SetSessionManager(sm)
	tk.MustQuery("SELECT user,host,command FROM information_schema.processlist;").Check(testkit.Rows("root 127.0.0.1 Query"))

	// Test for all system tables `TABLE_TYPE` is `SYSTEM VIEW`.
	rows1 := tk.MustQuery("select count(*) from information_schema.tables where table_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA');").Rows()
	rows2 := tk.MustQuery("select count(*) from information_schema.tables where table_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA') and  table_type = 'SYSTEM VIEW';").Rows()
	require.Equal(t, rows2, rows1)
	// Test for system table default value
	tk.MustQuery("show create table information_schema.PROCESSLIST").Check(
		testkit.Rows("" +
			"PROCESSLIST CREATE TABLE `PROCESSLIST` (\n" +
			"  `ID` bigint(21) unsigned NOT NULL DEFAULT '0',\n" +
			"  `USER` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `HOST` varchar(64) NOT NULL DEFAULT '',\n" +
			"  `DB` varchar(64) DEFAULT NULL,\n" +
			"  `COMMAND` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `TIME` int(7) NOT NULL DEFAULT '0',\n" +
			"  `STATE` varchar(7) DEFAULT NULL,\n" +
			"  `INFO` longtext DEFAULT NULL,\n" +
			"  `DIGEST` varchar(64) DEFAULT '',\n" +
			"  `MEM` bigint(21) unsigned DEFAULT NULL,\n" +
			"  `DISK` bigint(21) unsigned DEFAULT NULL,\n" +
			"  `TxnStart` varchar(64) NOT NULL DEFAULT '',\n" +
			"  `RESOURCE_GROUP` varchar(32) NOT NULL DEFAULT '',\n" +
			"  `SESSION_ALIAS` varchar(64) NOT NULL DEFAULT ''\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table information_schema.cluster_log").Check(
		testkit.Rows("" +
			"CLUSTER_LOG CREATE TABLE `CLUSTER_LOG` (\n" +
			"  `TIME` varchar(32) DEFAULT NULL,\n" +
			"  `TYPE` varchar(64) DEFAULT NULL,\n" +
			"  `INSTANCE` varchar(64) DEFAULT NULL,\n" +
			"  `LEVEL` varchar(8) DEFAULT NULL,\n" +
			"  `MESSAGE` longtext DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestSomeTables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)
	sm := &testkit.MockSessionManager{PS: make([]*util.ProcessInfo, 0)}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:                1,
		User:              "user-1",
		Host:              "localhost",
		Port:              "",
		DB:                "information_schema",
		Command:           byte(1),
		Digest:            "abc1",
		State:             1,
		Info:              "do something",
		StmtCtx:           tk.Session().GetSessionVars().StmtCtx,
		ResourceGroupName: "rg1",
		SessionAlias:      "alias1",
	})
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:                2,
		User:              "user-2",
		Host:              "localhost",
		Port:              "",
		DB:                "test",
		Command:           byte(2),
		Digest:            "abc2",
		State:             2,
		Info:              strings.Repeat("x", 101),
		StmtCtx:           tk.Session().GetSessionVars().StmtCtx,
		ResourceGroupName: "rg2",
	})
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:                3,
		User:              "user-3",
		Host:              "127.0.0.1",
		Port:              "12345",
		DB:                "test",
		Command:           byte(2),
		Digest:            "abc3",
		State:             1,
		Info:              "check port",
		StmtCtx:           tk.Session().GetSessionVars().StmtCtx,
		ResourceGroupName: "rg3",
		SessionAlias:      "中文alias",
	})
	tk.Session().SetSessionManager(sm)
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0  rg1 alias1", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s abc2 0 0  rg2 ", "autocommit", strings.Repeat("x", 101)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s abc3 0 0  rg3 中文alias", "in transaction", "check port"),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s", "in transaction", "check port"),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s", "in transaction", "check port"),
		))

	sm = &testkit.MockSessionManager{PS: make([]*util.ProcessInfo, 0)}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:                1,
		User:              "user-1",
		Host:              "localhost",
		DB:                "information_schema",
		Command:           byte(1),
		Digest:            "abc1",
		State:             1,
		ResourceGroupName: "rg1",
	})
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:                2,
		User:              "user-2",
		Host:              "localhost",
		Command:           byte(2),
		Digest:            "abc2",
		State:             2,
		Info:              strings.Repeat("x", 101),
		CurTxnStartTS:     410090409861578752,
		ResourceGroupName: "rg2",
		SessionAlias:      "alias3",
	})
	tk.Session().SetSessionManager(sm)
	tk.Session().GetSessionVars().TimeZone = time.UTC
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0  rg1 ", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752) rg2 alias3", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where db is null;").Check(
		testkit.Rows(
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752) rg2 alias3", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where Info is null;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0  rg1 ", "in transaction", "<nil>"),
		))
}

func TestTableRowIDShardingInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS `sharding_info_test_db`")
	tk.MustExec("CREATE DATABASE `sharding_info_test_db`")

	assertShardingInfo := func(tableName string, expectInfo any) {
		querySQL := fmt.Sprintf("select tidb_row_id_sharding_info from information_schema.tables where table_schema = 'sharding_info_test_db' and table_name = '%s'", tableName)
		info := tk.MustQuery(querySQL).Rows()[0][0]
		if expectInfo == nil {
			require.Equal(t, "<nil>", info)
		} else {
			require.Equal(t, expectInfo, info)
		}
	}
	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t1` (a int)")
	assertShardingInfo("t1", "NOT_SHARDED")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t2` (a int key)")
	assertShardingInfo("t2", "NOT_SHARDED(PK_IS_HANDLE)")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t3` (a int) SHARD_ROW_ID_BITS=4")
	assertShardingInfo("t3", "SHARD_BITS=4")

	tk.MustExec("CREATE VIEW `sharding_info_test_db`.`tv` AS select 1")
	assertShardingInfo("tv", nil)

	testFunc := func(dbName string, expectInfo any) {
		tableInfo := model.TableInfo{}

		info := infoschema.GetShardingInfo(model.NewCIStr(dbName), &tableInfo)
		require.Equal(t, expectInfo, info)
	}

	testFunc("information_schema", nil)
	testFunc("mysql", nil)
	testFunc("performance_schema", nil)
	testFunc("uucc", "NOT_SHARDED")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t4` (a bigint key clustered auto_random)")
	assertShardingInfo("t4", "PK_AUTO_RANDOM_BITS=5")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t5` (a bigint key clustered auto_random(1))")
	assertShardingInfo("t5", "PK_AUTO_RANDOM_BITS=1")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t6` (a bigint key clustered auto_random(2, 32))")
	assertShardingInfo("t6", "PK_AUTO_RANDOM_BITS=2, RANGE BITS=32")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t7` (a bigint key clustered auto_random(5, 64))")
	assertShardingInfo("t7", "PK_AUTO_RANDOM_BITS=5")

	tk.MustExec("DROP DATABASE `sharding_info_test_db`")
}

func TestSlowQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	// Prepare slow log file.
	slowLogFileName := "tidb_slow.log"
	internal.PrepareSlowLogfile(t, slowLogFileName)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()
	expectedRes := [][]any{
		{"2019-02-12 19:33:56.571953",
			"406315658548871171",
			"root",
			"localhost",
			"6",
			"",
			"57",
			"0.12",
			"4.895492",
			"0.4",
			"0.2",
			"0.000000003",
			"2",
			"0.000000002",
			"0.00000001",
			"0.000000003",
			"0.19",
			"0.21",
			"0.01",
			"0",
			"0.18",
			"[txnLock]",
			"0.03",
			"0",
			"15",
			"480",
			"1",
			"8",
			"0.3824278",
			"0.161",
			"0.101",
			"0.092",
			"1.71",
			"1",
			"100001",
			"100000",
			"100",
			"10",
			"10",
			"10",
			"100",
			"test",
			"",
			"0",
			"42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772",
			"t1:1,t2:2",
			"0.1",
			"0.2",
			"0.03",
			"127.0.0.1:20160",
			"0.05",
			"0.6",
			"0.8",
			"0.0.0.0:20160",
			"70724",
			"65536",
			"0",
			"0",
			"0",
			"0",
			"10",
			"",
			"",
			"0",
			"1",
			"0",
			"0",
			"1",
			"0",
			"0",
			"default",
			"0",
			"0",
			"0",
			"abcd",
			"60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4",
			"",
			"update t set i = 2;",
			"select * from t_slim;",
		},
		{"2021-09-08 14:39:54.506967",
			"427578666238083075",
			"root",
			"172.16.0.0",
			"40507",
			"alias123",
			"0",
			"0",
			"25.571605962",
			"0.002923536",
			"0.006800973",
			"0.002100764",
			"0",
			"0",
			"0",
			"0.000015801",
			"25.542014572",
			"0",
			"0.002294647",
			"0.000605473",
			"12.483",
			"[tikvRPC regionMiss tikvRPC regionMiss regionMiss]",
			"0",
			"0",
			"624",
			"172064",
			"60",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"0",
			"rtdb",
			"",
			"0",
			"124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc",
			"",
			"0",
			"0",
			"0",
			"",
			"0",
			"0",
			"0",
			"",
			"856544",
			"0",
			"86.635049185",
			"0.015486658",
			"100.054",
			"0",
			"0",
			"",
			"",
			"0",
			"1",
			"0",
			"0",
			"0",
			"0",
			"0",
			"rg1",
			"96.66703066666668",
			"3182.424414062492",
			"0",
			"",
			"",
			"",
			"",
			"INSERT INTO ...;",
		},
	}

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", slowLogFileName))
	tk.MustExec("set time_zone = '+08:00';")
	re := tk.MustQuery("select * from information_schema.slow_query")
	re.Check(expectedRes)

	tk.MustExec("set time_zone = '+00:00';")
	re = tk.MustQuery("select * from information_schema.slow_query")
	expectedRes[0][0] = "2019-02-12 11:33:56.571953"
	expectedRes[1][0] = "2021-09-08 06:39:54.506967"
	re.Check(expectedRes)

	// Test for long query.
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = f.Write([]byte(`
# Time: 2019-02-13T19:33:56.571953+08:00
`))
	require.NoError(t, err)
	sql := "select * from "
	for len(sql) < 5000 {
		sql += "abcdefghijklmnopqrstuvwxyz_1234567890_qwertyuiopasdfghjklzxcvbnm"
	}
	sql += ";"
	_, err = f.Write([]byte(sql))
	require.NoError(t, err)
	re = tk.MustQuery("select query from information_schema.slow_query order by time desc limit 1")
	rows := re.Rows()
	require.Equal(t, sql, rows[0][0])
}

func TestTableIfHasColumn(t *testing.T) {
	columnName := variable.SlowLogHasMoreResults
	store := testkit.CreateMockStore(t)
	slowLogFileName := "tidb-table-has-column-slow.log"
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = slowLogFileName
	})
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte(`# Time: 2019-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Has_more_results: true
INSERT INTO ...;
`))
	require.NoError(t, f.Close())
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()
	tk := testkit.NewTestKit(t, store)

	// check schema
	tk.MustQuery(`select COUNT(*) from information_schema.columns
WHERE table_name = 'slow_query' and column_name = '` + columnName + `'`).
		Check(testkit.Rows("1"))

	// check select
	tk.MustQuery(`select ` + columnName +
		` from information_schema.slow_query`).Check(testkit.Rows("1"))
}

func TestReloadDropDatabase(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_dbs")
	tk.MustExec("use test_dbs")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create table t3 (a int)")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	t2, err := is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tk.MustExec("drop database test_dbs")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	_, err = is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	require.True(t, terror.ErrorEqual(infoschema.ErrTableNotExists, err))
	_, ok := is.TableByID(t2.Meta().ID)
	require.False(t, ok)
}

func TestSystemSchemaID(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)

	uniqueIDMap := make(map[int64]string)
	checkSystemSchemaTableID(t, dom, "information_schema", autoid.InformationSchemaDBID, 1, 10000, uniqueIDMap)
	checkSystemSchemaTableID(t, dom, "performance_schema", autoid.PerformanceSchemaDBID, 10000, 20000, uniqueIDMap)
	checkSystemSchemaTableID(t, dom, "metrics_schema", autoid.MetricSchemaDBID, 20000, 30000, uniqueIDMap)
}

func checkSystemSchemaTableID(t *testing.T, dom *domain.Domain, dbName string, dbID, start, end int64, uniqueIDMap map[int64]string) {
	is := dom.InfoSchema()
	require.NotNil(t, is)
	db, ok := is.SchemaByName(model.NewCIStr(dbName))
	require.True(t, ok)
	require.Equal(t, dbID, db.ID)
	// Test for information_schema table id.
	tables := is.SchemaTables(model.NewCIStr(dbName))
	require.Greater(t, len(tables), 0)
	for _, tbl := range tables {
		tid := tbl.Meta().ID
		require.Greaterf(t, tid&autoid.SystemSchemaIDFlag, int64(0), "table name is %v", tbl.Meta().Name)
		require.Greaterf(t, tid&^autoid.SystemSchemaIDFlag, start, "table name is %v", tbl.Meta().Name)
		require.Lessf(t, tid&^autoid.SystemSchemaIDFlag, end, "table name is %v", tbl.Meta().Name)

		name, ok := uniqueIDMap[tid]
		require.Falsef(t, ok, "schema id of %v is duplicate with %v, both is %v", name, tbl.Meta().Name, tid)
		uniqueIDMap[tid] = tbl.Meta().Name.O
	}
}

func TestSelectHiddenColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS `test_hidden`;")
	tk.MustExec("CREATE DATABASE `test_hidden`;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE hidden (a int , b int, c int);")
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("3"))
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("hidden"))
	require.NoError(t, err)
	colInfo := tb.Meta().Columns
	// Set column b to hidden
	colInfo[1].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("0"))
	// Set column b to visible
	colInfo[1].Hidden = false
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("1"))
	// Set a, b ,c to hidden
	colInfo[0].Hidden = true
	colInfo[1].Hidden = true
	colInfo[2].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("0"))
}

func TestFormatVersion(t *testing.T) {
	// Test for defaultVersions.
	versions := []struct {
		version  string
		expected string
		userset  bool
	}{
		// default versions
		{"5.7.25-TiDB-None", "None", true},
		{"5.7.25-TiDB-8.0.18", "8.0.18", true},
		{"5.7.25-TiDB-8.0.18-beta.1", "8.0.18-beta.1", true},
		{"5.7.25-TiDB-v4.0.0-beta-446-g5268094af", "4.0.0-beta-446-g5268094af", true},
		{"5.7.25-TiDB-", "", true},
		{"5.7.25-TiDB-v4.0.0-TiDB-446", "4.0.0-TiDB-446", true},
		// userset
		{"8.0.18", "8.0.18", false},
		{"5.7.25-TiDB", "5.7.25-TiDB", false},
		{"8.0.18-TiDB-4.0.0-beta.1", "8.0.18-TiDB-4.0.0-beta.1", false},
	}
	for _, tt := range versions {
		version := infoschema.FormatTiDBVersion(tt.version, tt.userset)
		require.Equal(t, tt.expected, version)
	}
}

func TestFormatStoreServerVersion(t *testing.T) {
	versions := []string{"v4.0.12", "4.0.12", "v5.0.1"}
	resultVersion := []string{"4.0.12", "4.0.12", "5.0.1"}

	for i, versionString := range versions {
		require.Equal(t, infoschema.FormatStoreServerVersion(versionString), resultVersion[i])
	}
}

// TestStmtSummaryTable Test statements_summary.
func TestStmtSummaryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // affect est-rows in this UT

	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustQuery("select column_comment from information_schema.columns " +
		"where table_name='STATEMENTS_SUMMARY' and column_name='STMT_TYPE'",
	).Check(testkit.Rows("Statement type"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = newTestKitWithRoot(t, store)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // affect est-rows in this UT

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t    values(2, 'b')")
	tk.MustExec("insert into t VALUES(3, 'c')")
	tk.MustExec("/**/insert into t values(4, 'd')")

	sql := "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text " +
		"from information_schema.statements_summary " +
		"where digest_text like 'insert into `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test point get.
	tk.MustExec("drop table if exists p")
	tk.MustExec("create table p(a int primary key, b int)")
	for i := 1; i < 3; i++ {
		tk.MustQuery("select b from p where a=1")
		expectedResult := fmt.Sprintf("%d \tid         \ttask\testRows\toperator info\n\tPoint_Get_1\troot\t1      \ttable:p, handle:1 %s", i, "test.p")
		// Also make sure that the plan digest is not empty
		sql = "select exec_count, plan, table_names from information_schema.statements_summary " +
			"where digest_text like 'select `b` from `p`%' and plan_digest != ''"
		tk.MustQuery(sql).Check(testkit.Rows(expectedResult))
	}

	// Point get another database.
	tk.MustQuery("select variable_value from mysql.tidb where variable_name = 'system_tz'")
	// Test for Encode plan cache.
	p1 := tk.Session().GetSessionVars().StmtCtx.GetEncodedPlan()
	require.Greater(t, len(p1), 0)
	rows := tk.MustQuery("select tidb_decode_plan('" + p1 + "');").Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, 1, len(rows[0]))
	require.Regexp(t, "\n.*Point_Get.*table.tidb, index.PRIMARY.VARIABLE_NAME", rows[0][0])

	sql = "select table_names from information_schema.statements_summary " +
		"where digest_text like 'select `variable_value`%' and `schema_name`='test'"
	tk.MustQuery(sql).Check(testkit.Rows("mysql.tidb"))

	// Test `create database`.
	tk.MustExec("create database if not exists test")
	// Test for Encode plan cache.
	p2 := tk.Session().GetSessionVars().StmtCtx.GetEncodedPlan()
	require.Equal(t, "", p2)
	tk.MustQuery(`select table_names
			from information_schema.statements_summary
			where digest_text like 'create database%' and schema_name='test'`,
	).Check(testkit.Rows("<nil>"))

	// Test SELECT.
	const failpointName = "github.com/pingcap/tidb/pkg/planner/core/mockPlanRowCount"
	require.NoError(t, failpoint.Enable(failpointName, "return(100)"))
	defer func() { require.NoError(t, failpoint.Disable(failpointName)) }()
	tk.MustQuery("select * from t where a=2")

	// sum_cop_task_num is always 0 if tidb_enable_collect_execution_info disabled
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                       \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10           \troot     \t100    \t\n" +
		"\t├─IndexRangeScan_8(Build)\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9(Probe)\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

	// select ... order by
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text
		from information_schema.statements_summary
		order by exec_count desc limit 1`,
	).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test different plans with same digest.
	require.NoError(t, failpoint.Enable(failpointName, "return(1000)"))
	tk.MustQuery("select * from t where a=3")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows(
		"Select test test.t t:k 2 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                       \ttask     \testRows\toperator info\n" +
			"\tIndexLookUp_10           \troot     \t100    \t\n" +
			"\t├─IndexRangeScan_8(Build)\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
			"\t└─TableRowIDScan_9(Probe)\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustExec("set global tidb_enable_stmt_summary = false")
	defer tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new session to test
	tk = newTestKitWithRoot(t, store)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // affect est-rows in this UT

	// This statement shouldn't be summarized.
	tk.MustQuery("select * from t where a=2")

	// The table should be cleared.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary`,
	).Check(testkit.Rows())

	tk.MustExec("SET GLOBAL tidb_enable_stmt_summary = on")
	// It should work immediately.
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("commit")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text " +
		"from information_schema.statements_summary " +
		"where digest_text like 'insert into `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.t <nil> 1 0 0 0 0 0 0 0 0 0 1 insert into t values(1, 'a') "))
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text
		from information_schema.statements_summary
		where digest_text='commit'`,
	).Check(testkit.Rows("Commit test <nil> <nil> 1 0 0 0 0 0 2 2 1 1 0 commit insert into t values(1, 'a')"))

	tk.MustQuery("select * from t where a=2")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                       \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10           \troot     \t1000   \t\n" +
		"\t├─IndexRangeScan_8(Build)\tcop[tikv]\t1000   \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9(Probe)\tcop[tikv]\t1000   \ttable:t, keep order:false, stats:pseudo"))

	// Disable it in global scope.
	tk.MustExec("set global tidb_enable_stmt_summary = false")

	// Create a new session to test.
	tk = newTestKitWithRoot(t, store)

	// Statement summary is disabled.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary`,
	).Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_stmt_summary = on")
	tk.MustExec("set global tidb_stmt_summary_history_size = 24")
}

func TestStmtSummaryTablePrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t")

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))
	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	// Create a new user to test statements summary table privilege
	tk.MustExec("drop user if exists 'test_user'@'localhost'")
	tk.MustExec("create user 'test_user'@'localhost'")
	defer tk.MustExec("drop user if exists 'test_user'@'localhost'")
	tk.MustExec("grant select on test.t to 'test_user'@'localhost'")
	tk.MustExec("select * from t where a=1")
	result := tk.MustQuery("select * from information_schema.statements_summary where digest_text like 'select * from `t`%'")
	require.Equal(t, 1, len(result.Rows()))
	result = tk.MustQuery("select *	from information_schema.statements_summary_history	where digest_text like 'select * from `t`%'")
	require.Equal(t, 1, len(result.Rows()))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.Session().Auth(&auth.UserIdentity{
		Username:     "test_user",
		Hostname:     "localhost",
		AuthUsername: "test_user",
		AuthHostname: "localhost",
	}, nil, nil, nil)

	result = tk1.MustQuery("select * from information_schema.statements_summary where digest_text like 'select * from `t`%'")
	// Ordinary users can not see others' records
	require.Equal(t, 0, len(result.Rows()))
	result = tk1.MustQuery("select *	from information_schema.statements_summary_history where digest_text like 'select * from `t`%'")
	require.Equal(t, 0, len(result.Rows()))
	tk1.MustExec("select * from t where b=1")
	result = tk1.MustQuery("select *	from information_schema.statements_summary	where digest_text like 'select * from `t`%'")
	// Ordinary users can see his own records
	require.Equal(t, 1, len(result.Rows()))
	result = tk1.MustQuery("select *	from information_schema.statements_summary_history	where digest_text like 'select * from `t`%'")
	require.Equal(t, 1, len(result.Rows()))

	tk.MustExec("grant process on *.* to 'test_user'@'localhost'")
	result = tk1.MustQuery("select *	from information_schema.statements_summary	where digest_text like 'select * from `t`%'")
	// Users with 'PROCESS' privileges can query all records.
	require.Equal(t, 2, len(result.Rows()))
	result = tk1.MustQuery("select *	from information_schema.statements_summary_history	where digest_text like 'select * from `t`%'")
	require.Equal(t, 2, len(result.Rows()))
}

func TestCapturePrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t1")

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))
	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	// Create a new user to test statements summary table privilege
	tk.MustExec("drop user if exists 'test_user'@'localhost'")
	tk.MustExec("create user 'test_user'@'localhost'")
	defer tk.MustExec("drop user if exists 'test_user'@'localhost'")
	tk.MustExec("grant select on test.t1 to 'test_user'@'localhost'")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.Session().Auth(&auth.UserIdentity{
		Username:     "test_user",
		Hostname:     "localhost",
		AuthUsername: "test_user",
		AuthHostname: "localhost",
	}, nil, nil, nil)

	rows = tk1.MustQuery("show global bindings").Rows()
	// Ordinary users can not see others' records
	require.Len(t, rows, 0)
	tk1.MustExec("select * from t1 where b=1")
	tk1.MustExec("select * from t1 where b=1")
	tk1.MustExec("admin capture bindings")
	rows = tk1.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)

	tk.MustExec("grant all on *.* to 'test_user'@'localhost'")
	tk1.MustExec("admin capture bindings")
	rows = tk1.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
}

// TestStmtSummaryInternalQuery Test statements_summary_history.
func TestStmtSummaryInternalQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// We use the sql binding evolve to check the internal query summary.
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines = 1")
	tk.MustExec("create global binding for select * from t where t.a = 1 using select * from t ignore index(k) where t.a = 1")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))
}

// TestSimpleStmtSummaryEvictedCount test stmtSummaryEvictedCount
func TestSimpleStmtSummaryEvictedCount(t *testing.T) {
	store := testkit.CreateMockStore(t)

	now := time.Now().Unix()
	interval := int64(1800)
	beginTimeForCurInterval := now - now%interval
	tk := newTestKitWithPlanCache(t, store)
	tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval = %v", interval))

	// no evict happens now, evicted count should be empty
	tk.MustQuery("select count(*) from information_schema.statements_summary_evicted;").Check(testkit.Rows("0"))

	// clean up side effects
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")
	defer tk.MustExec("set global tidb_stmt_summary_refresh_interval = 1800")

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	// statements summary evicted is also disabled when set tidb_enable_stmt_summary to off
	tk.MustQuery("select count(*) from information_schema.statements_summary_evicted;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	// first sql
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	// second sql
	tk.MustQuery("show databases;")
	// query `evicted table` is also a SQL, passing it leads to the eviction of the previous SQLs.
	tk.MustQuery("select * from `information_schema`.`STATEMENTS_SUMMARY_EVICTED`;").
		Check(testkit.Rows(
			fmt.Sprintf("%s %s %v",
				time.Unix(beginTimeForCurInterval, 0).Format(time.DateTime),
				time.Unix(beginTimeForCurInterval+interval, 0).Format(time.DateTime),
				int64(2)),
		))

	// test too much intervals
	tk.MustExec("use test;")
	tk.MustExec(fmt.Sprintf("set @@global.tidb_stmt_summary_refresh_interval=%v", interval))
	tk.MustExec("set @@global.tidb_enable_stmt_summary=0")
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	historySize := 24
	fpPath := "github.com/pingcap/tidb/pkg/util/stmtsummary/mockTimeForStatementsSummary"
	for i := int64(0); i < 100; i++ {
		err := failpoint.Enable(fpPath, fmt.Sprintf(`return("%v")`, time.Now().Unix()+interval*i))
		if err != nil {
			panic(err.Error())
		}
		tk.MustExec(fmt.Sprintf("create table if not exists th%v (p bigint key, q int);", i))
	}
	err := failpoint.Disable(fpPath)
	if err != nil {
		panic(err.Error())
	}
	tk.MustQuery("select count(*) from information_schema.statements_summary_evicted;").
		Check(testkit.Rows(fmt.Sprintf("%v", historySize)))

	// test discrete intervals
	tk.MustExec("set @@global.tidb_enable_stmt_summary=0")
	tk.MustExec("set @@global.tidb_stmt_summary_max_stmt_count=1;")
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	for i := int64(0); i < 3; i++ {
		tk.MustExec(fmt.Sprintf("select count(*) from th%v", i))
	}
	err = failpoint.Enable(fpPath, fmt.Sprintf(`return("%v")`, time.Now().Unix()+2*interval))
	if err != nil {
		panic(err.Error())
	}
	for i := int64(0); i < 3; i++ {
		tk.MustExec(fmt.Sprintf("select count(*) from th%v", i))
	}
	tk.MustQuery("select count(*) from information_schema.statements_summary_evicted;").Check(testkit.Rows("2"))
	tk.MustQuery("select BEGIN_TIME from information_schema.statements_summary_evicted;").
		Check(testkit.
			Rows(time.Unix(beginTimeForCurInterval+2*interval, 0).Format(time.DateTime),
				time.Unix(beginTimeForCurInterval, 0).Format(time.DateTime)))
	require.NoError(t, failpoint.Disable(fpPath))
	// TODO: Add more tests.
}

func TestStmtSummaryEvictedPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)

	interval := int64(1800)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval=%v;", interval))
	tk.MustExec("create database point_get;")
	tk.MustExec("use point_get;")
	for i := 0; i < 6; i++ {
		tk.MustExec(fmt.Sprintf("create table if not exists th%v ("+
			"p bigint key,"+
			"q int);", i))
	}

	tk.MustExec("set @@global.tidb_enable_stmt_summary=0;")
	tk.MustExec("set @@global.tidb_stmt_summary_max_stmt_count=5;")
	defer tk.MustExec("set @@global.tidb_stmt_summary_max_stmt_count=100;")
	// first SQL
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1;")

	for i := int64(0); i < 1000; i++ {
		// six SQLs
		tk.MustExec(fmt.Sprintf("select p from th%v where p=2333;", i%6))
	}
	tk.MustQuery("select EVICTED_COUNT from information_schema.statements_summary_evicted;").
		Check(testkit.Rows("7"))

	tk.MustExec("set @@global.tidb_enable_stmt_summary=0;")
	tk.MustQuery("select count(*) from information_schema.statements_summary_evicted;").
		Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1;")
}

func TestServerInfoResolveLoopBackAddr(t *testing.T) {
	nodes := []infoschema.ServerInfo{
		{Address: "127.0.0.1:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "0.0.0.0:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "localhost:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "0.0.0.0:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "127.0.0.1:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "localhost:10080"},
	}
	for i := range nodes {
		nodes[i].ResolveLoopBackAddr()
	}
	for _, n := range nodes {
		require.Equal(t, "192.168.130.22:4000", n.Address)
		require.Equal(t, "192.168.130.22:10080", n.StatusAddr)
	}
}

func TestInfoSchemaClientErrors(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)

	tk.MustExec("FLUSH CLIENT_ERRORS_SUMMARY")

	errno.IncrementError(1365, "root", "localhost")
	errno.IncrementError(1365, "infoschematest", "localhost")
	errno.IncrementError(1365, "root", "localhost")

	tk.MustExec("CREATE USER 'infoschematest'@'localhost'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "infoschematest", Hostname: "localhost"}, nil, nil, nil))

	err := tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_global")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation", err.Error())

	err = tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_by_host")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation", err.Error())

	tk.MustQuery("SELECT error_number, error_count, warning_count FROM information_schema.client_errors_summary_by_user ORDER BY error_number").Check(testkit.Rows("1365 1 0"))

	err = tk.ExecToErr("FLUSH CLIENT_ERRORS_SUMMARY")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the RELOAD privilege(s) for this operation", err.Error())
}

func TestTiDBTrx(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)
	tk.MustExec("drop table if exists test_tidb_trx")
	tk.MustExec("create table test_tidb_trx(i int)")
	// Execute the statement once so that the statement will be collected into statements_summary and able to be found
	// by digest.
	tk.MustExec("update test_tidb_trx set i = i + 1")
	_, digest := parser.NormalizeDigest("update test_tidb_trx set i = i + 1")
	sm := &testkit.MockSessionManager{TxnInfo: make([]*txninfo.TxnInfo, 2)}
	memDBTracker := memory.NewTracker(memory.LabelForMemDB, -1)
	memDBTracker.Consume(19)
	tk.Session().GetSessionVars().MemDBFootprint = memDBTracker

	t1 := time.Date(2021, 5, 7, 4, 56, 48, 1000000, time.UTC)
	t2 := time.Date(2021, 5, 20, 13, 16, 35, 778000000, time.UTC)

	sm.TxnInfo[0] = &txninfo.TxnInfo{
		StartTS:          oracle.GoTimeToTS(t1),
		CurrentSQLDigest: digest.String(),
		State:            txninfo.TxnIdle,
		EntriesCount:     1,
		ConnectionID:     2,
		Username:         "root",
		CurrentDB:        "test",
	}

	blockTime2 := time.Date(2021, 05, 20, 13, 18, 30, 123456000, time.Local)
	sm.TxnInfo[1] = &txninfo.TxnInfo{
		StartTS:          oracle.GoTimeToTS(t2),
		CurrentSQLDigest: "",
		AllSQLDigests:    []string{"sql1", "sql2", digest.String()},
		State:            txninfo.TxnLockAcquiring,
		ConnectionID:     10,
		Username:         "user1",
		CurrentDB:        "db1",
	}
	sm.TxnInfo[1].BlockStartTime.Valid = true
	sm.TxnInfo[1].BlockStartTime.Time = blockTime2
	tk.Session().SetSessionManager(sm)

	tk.MustQuery(`select ID,
	START_TIME,
	CURRENT_SQL_DIGEST,
	CURRENT_SQL_DIGEST_TEXT,
	STATE,
	WAITING_START_TIME,
	MEM_BUFFER_KEYS,
	MEM_BUFFER_BYTES,
	SESSION_ID,
	USER,
	DB,
	ALL_SQL_DIGESTS,
	RELATED_TABLE_IDS
	from information_schema.TIDB_TRX`).Check(testkit.Rows(
		"424768545227014144 "+t1.Local().Format(types.TimeFSPFormat)+" "+digest.String()+" update `test_tidb_trx` set `i` = `i` + ? Idle <nil> 1 19 2 root test [] ",
		"425070846483628032 "+t2.Local().Format(types.TimeFSPFormat)+" <nil> <nil> LockWaiting "+
			// `WAITING_START_TIME` will not be affected by time_zone, it is in memory and we assume that the system time zone will not change.
			blockTime2.Format(types.TimeFSPFormat)+
			" 0 19 10 user1 db1 [\"sql1\",\"sql2\",\""+digest.String()+"\"] "))

	rows := tk.MustQuery(`select WAITING_TIME from information_schema.TIDB_TRX where WAITING_TIME is not null`)
	require.Len(t, rows.Rows(), 1)

	// Test the all_sql_digests column can be directly passed to the tidb_decode_sql_digests function.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal"))
	}()
	tk.MustQuery("select tidb_decode_sql_digests(all_sql_digests) from information_schema.tidb_trx").Check(testkit.Rows(
		"[]",
		"[null,null,\"update `test_tidb_trx` set `i` = `i` + ?\"]"))
}

func TestTiDBTrxSummary(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := newTestKitWithRoot(t, store)
	tk.MustExec("drop table if exists test_tidb_trx")
	tk.MustExec("create table test_tidb_trx(i int)")
	_, beginDigest := parser.NormalizeDigest("begin")
	_, digest := parser.NormalizeDigest("update test_tidb_trx set i = i + 1")
	_, commitDigest := parser.NormalizeDigest("commit")
	txninfo.Recorder.Clean()
	txninfo.Recorder.SetMinDuration(500 * time.Millisecond)
	defer txninfo.Recorder.SetMinDuration(2147483647)
	txninfo.Recorder.ResizeSummaries(128)
	defer txninfo.Recorder.ResizeSummaries(0)
	tk.MustExec("begin")
	tk.MustExec("update test_tidb_trx set i = i + 1")
	time.Sleep(1 * time.Second)
	tk.MustExec("update test_tidb_trx set i = i + 1")
	tk.MustExec("commit")
	// it is possible for TRX_SUMMARY to have other rows (due to parallel execution of tests)
	for _, row := range tk.MustQuery("select * from information_schema.TRX_SUMMARY;").Rows() {
		// so we just look for the row we are looking for
		if row[0] == "1bb679108d0012a8" {
			require.Equal(t, strings.TrimSpace(row[1].(string)), "[\""+beginDigest.String()+"\",\""+digest.String()+"\",\""+digest.String()+"\",\""+commitDigest.String()+"\"]")
			return
		}
	}
	t.Fatal("cannot find the expected row")
}

func TestAttributes(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// test the failpoint for testing
	fpName := "github.com/pingcap/tidb/pkg/executor/mockOutputOfAttributes"
	tk := newTestKitWithRoot(t, store)
	tk.MustQuery("select * from information_schema.attributes").Check(testkit.Rows())

	require.NoError(t, failpoint.Enable(fpName, "return"))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	tk.MustQuery(`select * from information_schema.attributes`).Check(testkit.Rows(
		`schema/test/test_label key-range "merge_option=allow" [7480000000000000ff395f720000000000fa, 7480000000000000ff3a5f720000000000fa]`,
	))
}

func TestMemoryUsageAndOpsHistory(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner"))
	}()
	gctuner.GlobalMemoryLimitTuner.Start()
	defer func() {
		time.Sleep(1200 * time.Millisecond) // Wait tuning finished.
	}()
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	tk.MustExec("set global tidb_server_memory_limit=512<<20")
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom=off")
	dom, err := session.GetDomain(store)
	require.Nil(t, err)
	go dom.ServerMemoryLimitHandle().SetSessionManager(tk.Session().GetSessionManager()).Run()
	// OOM
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	for i := 0; i < 9; i++ {
		tk.MustExec("insert into t select * from t;")
	}

	var tmp string
	var ok bool
	var beginTime = time.Now().Format(types.TimeFormat)
	err = tk.QueryToErr("explain analyze select * from t t1 join t t2 join t t3 on t1.a=t2.a and t1.a=t3.a order by t1.a")
	var endTime = time.Now().Format(types.TimeFormat)
	require.NotNil(t, err)
	// Check Memory Table
	rows := tk.MustQuery("select * from INFORMATION_SCHEMA.MEMORY_USAGE").Rows()
	require.Len(t, rows, 1)
	row := rows[0]
	require.Len(t, row, 11)
	require.Equal(t, row[0], strconv.FormatUint(memory.GetMemTotalIgnoreErr(), 10)) // MEMORY_TOTAL
	require.Equal(t, row[1], "536870912")                                           // MEMORY_LIMIT
	require.Greater(t, row[2], "0")                                                 // MEMORY_CURRENT
	tmp, ok = row[3].(string)                                                       // MEMORY_MAX_USED
	require.Equal(t, ok, true)
	val, err := strconv.ParseUint(tmp, 10, 64)
	require.Nil(t, err)
	require.Greater(t, val, uint64(536870912))

	tmp, ok = row[4].(string) // CURRENT_OPS
	require.Equal(t, ok, true)
	if tmp != "null" && tmp != "shrink" {
		require.Fail(t, "CURRENT_OPS get wrong value")
	}
	require.GreaterOrEqual(t, row[5], beginTime) // SESSION_KILL_LAST
	require.LessOrEqual(t, row[5], endTime)
	require.Greater(t, row[6], "0")              // SESSION_KILL_TOTAL
	require.GreaterOrEqual(t, row[7], beginTime) // GC_LAST
	require.LessOrEqual(t, row[7], endTime)
	require.Greater(t, row[8], "0") // GC_TOTAL
	require.Equal(t, row[9], "0")   // DISK_USAGE
	require.Equal(t, row[10], "0")  // QUERY_FORCE_DISK

	rows = tk.MustQuery("select * from INFORMATION_SCHEMA.MEMORY_USAGE_OPS_HISTORY").Rows()
	require.Greater(t, len(rows), 0)
	row = rows[len(rows)-1]
	require.Len(t, row, 12)
	require.GreaterOrEqual(t, row[0], beginTime) // TIME
	require.LessOrEqual(t, row[0], endTime)
	require.Equal(t, row[1], "SessionKill") // OPS
	require.Equal(t, row[2], "536870912")   // MEMORY_LIMIT
	tmp, ok = row[3].(string)               // MEMORY_CURRENT
	require.Equal(t, ok, true)
	val, err = strconv.ParseUint(tmp, 10, 64)
	require.Nil(t, err)
	require.Greater(t, val, uint64(536870912))

	require.Greater(t, row[4], "0")                                                                                              // PROCESSID
	require.Greater(t, row[5], "0")                                                                                              // MEM
	require.Equal(t, row[6], "0")                                                                                                // DISK
	require.Equal(t, row[7], "")                                                                                                 // CLIENT
	require.Equal(t, row[8], "test")                                                                                             // DB
	require.Equal(t, row[9], "")                                                                                                 // USER
	require.Equal(t, row[10], "e3237ec256015a3566757e0c2742507cd30ae04e4cac2fbc14d269eafe7b067b")                                // SQL_DIGEST
	require.Equal(t, row[11], "explain analyze select * from t t1 join t t2 join t t3 on t1.a=t2.a and t1.a=t3.a order by t1.a") // SQL_TEXT
}

func TestAddFieldsForBinding(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key(a))")
	tk.MustExec("select /*+ ignore_index(t, a)*/ * from t where a = 1")
	planDigest := "4e3159169cc63c14b139a4e7d72eae1759875c9a9581f94bb2079aae961189cb"
	rows := tk.MustQuery(fmt.Sprintf("select stmt_type, prepared, sample_user, schema_name, query_sample_text, charset, collation, plan_hint, digest_text "+
		"from information_schema.cluster_statements_summary where plan_digest = '%s'", planDigest)).Rows()

	require.Equal(t, rows[0][0], "Select")
	require.Equal(t, rows[0][1], "0")
	require.Equal(t, rows[0][2], "root")
	require.Equal(t, rows[0][3], "test")
	require.Equal(t, rows[0][4], "select /*+ ignore_index(t, a)*/ * from t where a = 1")
	require.Equal(t, rows[0][5], "utf8mb4")
	require.Equal(t, rows[0][6], "utf8mb4_bin")
	require.Equal(t, rows[0][7], "use_index(@`sel_1` `test`.`t` ), ignore_index(`t` `a`)")
	require.Equal(t, rows[0][8], "select * from `t` where `a` = ?")
}

func TestClusterInfoTime(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("SELECT START_TIME+1 FROM information_schema.CLUSTER_INFO")
	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Nil(t, warnings)
}
