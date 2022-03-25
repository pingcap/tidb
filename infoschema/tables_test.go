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

package infoschema_test

import (
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/stretchr/testify/require"
)

func newTestKitWithRoot(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	return tk
}

func newTestKitWithPlanCache(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64)})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.RefreshConnectionID()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	return tk
}

func TestInfoSchemaFieldValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil))

	tk1.MustQuery("select distinct(table_schema) from information_schema.tables").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Fix issue 9836
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 1), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
		StmtCtx: tk.Session().GetSessionVars().StmtCtx,
	}
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
			"  `TxnStart` varchar(64) NOT NULL DEFAULT ''\n" +
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

func TestCharacterSetCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// Test charset/collation in information_schema.COLUMNS table.
	tk.MustExec("DROP DATABASE IF EXISTS charset_collate_test")
	tk.MustExec("CREATE DATABASE charset_collate_test; USE charset_collate_test")

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec(`CREATE TABLE charset_collate_col_test(
		c_int int,
		c_float float,
		c_bit bit,
		c_bool bool,
		c_char char(1) charset ascii collate ascii_bin,
		c_nchar national char(1) charset ascii collate ascii_bin,
		c_binary binary,
		c_varchar varchar(1) charset ascii collate ascii_bin,
		c_nvarchar national varchar(1) charset ascii collate ascii_bin,
		c_varbinary varbinary(1),
		c_year year,
		c_date date,
		c_time time,
		c_datetime datetime,
		c_timestamp timestamp,
		c_blob blob,
		c_tinyblob tinyblob,
		c_mediumblob mediumblob,
		c_longblob longblob,
		c_text text charset ascii collate ascii_bin,
		c_tinytext tinytext charset ascii collate ascii_bin,
		c_mediumtext mediumtext charset ascii collate ascii_bin,
		c_longtext longtext charset ascii collate ascii_bin,
		c_json json,
		c_enum enum('1') charset ascii collate ascii_bin,
		c_set set('1') charset ascii collate ascii_bin
	)`)

	tk.MustQuery(`SELECT column_name, character_set_name, collation_name
					FROM information_schema.COLUMNS
					WHERE table_schema = "charset_collate_test" AND table_name = "charset_collate_col_test"
					ORDER BY column_name`,
	).Check(testkit.Rows(
		"c_binary <nil> <nil>",
		"c_bit <nil> <nil>",
		"c_blob <nil> <nil>",
		"c_bool <nil> <nil>",
		"c_char ascii ascii_bin",
		"c_date <nil> <nil>",
		"c_datetime <nil> <nil>",
		"c_enum ascii ascii_bin",
		"c_float <nil> <nil>",
		"c_int <nil> <nil>",
		"c_json <nil> <nil>",
		"c_longblob <nil> <nil>",
		"c_longtext ascii ascii_bin",
		"c_mediumblob <nil> <nil>",
		"c_mediumtext ascii ascii_bin",
		"c_nchar ascii ascii_bin",
		"c_nvarchar ascii ascii_bin",
		"c_set ascii ascii_bin",
		"c_text ascii ascii_bin",
		"c_time <nil> <nil>",
		"c_timestamp <nil> <nil>",
		"c_tinyblob <nil> <nil>",
		"c_tinytext ascii ascii_bin",
		"c_varbinary <nil> <nil>",
		"c_varchar ascii ascii_bin",
		"c_year <nil> <nil>",
	))
	tk.MustExec("DROP DATABASE charset_collate_test")
}

func TestCurrentTimestampAsDefault(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("DROP DATABASE IF EXISTS default_time_test")
	tk.MustExec("CREATE DATABASE default_time_test; USE default_time_test")

	tk.MustExec(`CREATE TABLE default_time_table(
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_varchar_default varchar(20) default "current_timestamp",
					c_varchar_default_3 varchar(20) default "current_timestamp(3)",
					c_varchar_default_on_update datetime default current_timestamp on update current_timestamp,
					c_varchar_default_on_update_fsp datetime(3) default current_timestamp(3) on update current_timestamp(3),
					c_varchar_default_with_case varchar(20) default "cUrrent_tImestamp"
				);`)

	tk.MustQuery(`SELECT column_name, column_default, extra
					FROM information_schema.COLUMNS
					WHERE table_schema = "default_time_test" AND table_name = "default_time_table"
					ORDER BY column_name`,
	).Check(testkit.Rows(
		"c_datetime <nil> ",
		"c_datetime_default CURRENT_TIMESTAMP ",
		"c_datetime_default_2 CURRENT_TIMESTAMP(2) ",
		"c_timestamp <nil> ",
		"c_timestamp_default CURRENT_TIMESTAMP ",
		"c_timestamp_default_3 CURRENT_TIMESTAMP(3) ",
		"c_varchar_default current_timestamp ",
		"c_varchar_default_3 current_timestamp(3) ",
		"c_varchar_default_on_update CURRENT_TIMESTAMP DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
		"c_varchar_default_on_update_fsp CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
		"c_varchar_default_with_case cUrrent_tImestamp ",
	))
	tk.MustExec("DROP DATABASE default_time_test")
}

type mockSessionManager struct {
	processInfoMap map[uint64]*util.ProcessInfo
	txnInfo        []*txninfo.TxnInfo
}

func (sm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	return sm.txnInfo
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) Kill(_ uint64, _ bool) {}

func (sm *mockSessionManager) KillAllConnections() {}

func (sm *mockSessionManager) UpdateTLSConfig(_ *tls.Config) {}

func (sm *mockSessionManager) ServerID() uint64 { return 1 }

func TestSomeTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		Port:    "",
		DB:      "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
		Info:    "do something",
		StmtCtx: tk.Session().GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:      2,
		User:    "user-2",
		Host:    "localhost",
		Port:    "",
		DB:      "test",
		Command: byte(2),
		Digest:  "abc2",
		State:   2,
		Info:    strings.Repeat("x", 101),
		StmtCtx: tk.Session().GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[3] = &util.ProcessInfo{
		ID:      3,
		User:    "user-3",
		Host:    "127.0.0.1",
		Port:    "12345",
		DB:      "test",
		Command: byte(2),
		Digest:  "abc3",
		State:   1,
		Info:    "check port",
		StmtCtx: tk.Session().GetSessionVars().StmtCtx,
	}
	tk.Session().SetSessionManager(sm)
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s abc2 0 0 ", "autocommit", strings.Repeat("x", 101)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s abc3 0 0 ", "in transaction", "check port"),
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

	sm = &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		DB:      "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:            2,
		User:          "user-2",
		Host:          "localhost",
		Command:       byte(2),
		Digest:        "abc2",
		State:         2,
		Info:          strings.Repeat("x", 101),
		CurTxnStartTS: 410090409861578752,
	}
	tk.Session().SetSessionManager(sm)
	tk.Session().GetSessionVars().TimeZone = time.UTC
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
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
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where Info is null;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "<nil>"),
		))
}

func prepareSlowLogfile(t *testing.T, slowLogFileName string) {
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte(`# Time: 2019-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Conn_ID: 6
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 4.895492
# Parse_time: 0.4
# Compile_time: 0.2
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# LockKeys_time: 1.71 Request_count: 1 Prewrite_time: 0.19 Wait_prewrite_binlog_time: 0.21 Commit_time: 0.01 Commit_backoff_time: 0.18 Backoff_types: [txnLock] Resolve_lock_time: 0.03 Write_keys: 15 Write_size: 480 Prewrite_region: 1 Txn_retry: 8
# Cop_time: 0.3824278 Process_time: 0.161 Request_count: 1 Total_keys: 100001 Process_keys: 100000
# Rocksdb_delete_skipped_count: 100 Rocksdb_key_skipped_count: 10 Rocksdb_block_cache_hit_count: 10 Rocksdb_block_read_count: 10 Rocksdb_block_read_byte: 100
# Wait_time: 0.101
# Backoff_time: 0.092
# DB: test
# Is_internal: false
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Result_rows: 10
# Succ: true
# Plan: abcd
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 2;
select * from t_slim;
# Time: 2021-09-08T14:39:54.506967433+08:00
# Txn_start_ts: 427578666238083075
# User@Host: root[root] @ 172.16.0.0 [172.16.0.0]
# Conn_ID: 40507
# Query_time: 25.571605962
# Parse_time: 0.002923536
# Compile_time: 0.006800973
# Rewrite_time: 0.002100764
# Optimize_time: 0
# Wait_TS: 0.000015801
# Prewrite_time: 25.542014572 Commit_time: 0.002294647 Get_commit_ts_time: 0.000605473 Commit_backoff_time: 12.483 Backoff_types: [tikvRPC regionMiss tikvRPC regionMiss regionMiss] Write_keys: 624 Write_size: 172064 Prewrite_region: 60
# DB: rtdb
# Is_internal: false
# Digest: 124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc
# Num_cop_tasks: 0
# Mem_max: 856544
# Prepared: false
# Plan_from_cache: false
# Plan_from_binding: false
# Has_more_results: false
# KV_total: 86.635049185
# PD_total: 0.015486658
# Backoff_total: 100.054
# Write_sql_response_total: 0
# Succ: true
INSERT INTO ...;
`))
	require.NoError(t, f.Close())
	require.NoError(t, err)
}

func TestTableRowIDShardingInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS `sharding_info_test_db`")
	tk.MustExec("CREATE DATABASE `sharding_info_test_db`")

	assertShardingInfo := func(tableName string, expectInfo interface{}) {
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

	testFunc := func(dbName string, expectInfo interface{}) {
		dbInfo := model.DBInfo{Name: model.NewCIStr(dbName)}
		tableInfo := model.TableInfo{}

		info := infoschema.GetShardingInfo(&dbInfo, &tableInfo)
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

	tk.MustExec("DROP DATABASE `sharding_info_test_db`")
}

func TestSlowQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Prepare slow log file.
	slowLogFileName := "tidb_slow.log"
	prepareSlowLogfile(t, slowLogFileName)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", slowLogFileName))
	tk.MustExec("set time_zone = '+08:00';")
	re := tk.MustQuery("select * from information_schema.slow_query")
	re.Check(testkit.RowsWithSep("|", "2019-02-12 19:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|100|10|10|10|100|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|0|0|0|0|10||0|1|0|0|1|0|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|update t set i = 2;|select * from t_slim;",
		"2021-09-08|14:39:54.506967|427578666238083075|root|172.16.0.0|40507|0|0|25.571605962|0.002923536|0.006800973|0.002100764|0|0|0|0.000015801|25.542014572|0|0.002294647|0.000605473|12.483|[tikvRPC regionMiss tikvRPC regionMiss regionMiss]|0|0|624|172064|60|0|0|0|0|0|0|0|0|0|0|0|0|0|0|rtdb||0|124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc||0|0|0||0|0|0||856544|0|86.635049185|0.015486658|100.054|0|0||0|1|0|0|0|0||||INSERT INTO ...;",
	))
	tk.MustExec("set time_zone = '+00:00';")
	re = tk.MustQuery("select * from information_schema.slow_query")
	re.Check(testkit.RowsWithSep("|", "2019-02-12 11:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|100|10|10|10|100|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|0|0|0|0|10||0|1|0|0|1|0|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|update t set i = 2;|select * from t_slim;",
		"2021-09-08|06:39:54.506967|427578666238083075|root|172.16.0.0|40507|0|0|25.571605962|0.002923536|0.006800973|0.002100764|0|0|0|0.000015801|25.542014572|0|0.002294647|0.000605473|12.483|[tikvRPC regionMiss tikvRPC regionMiss regionMiss]|0|0|624|172064|60|0|0|0|0|0|0|0|0|0|0|0|0|0|0|rtdb||0|124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc||0|0|0||0|0|0||856544|0|86.635049185|0.015486658|100.054|0|0||0|1|0|0|0|0||||INSERT INTO ...;",
	))

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

func TestColumnStatistics(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from information_schema.column_statistics").Check(testkit.Rows())
}

func TestReloadDropDatabase(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	_, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	defaultVersions := []string{"5.7.25-TiDB-None", "5.7.25-TiDB-8.0.18", "5.7.25-TiDB-8.0.18-beta.1", "5.7.25-TiDB-v4.0.0-beta-446-g5268094af"}
	defaultRes := []string{"None", "8.0.18", "8.0.18-beta.1", "4.0.0-beta"}
	for i, v := range defaultVersions {
		version := infoschema.FormatTiDBVersion(v, true)
		require.Equal(t, defaultRes[i], version)
	}

	// Test for versions user set.
	versions := []string{"8.0.18", "5.7.25-TiDB", "8.0.18-TiDB-4.0.0-beta.1"}
	res := []string{"8.0.18", "5.7.25-TiDB", "8.0.18-TiDB-4.0.0-beta.1"}
	for i, v := range versions {
		version := infoschema.FormatTiDBVersion(v, false)
		require.Equal(t, res[i], version)
	}

	versions = []string{"v4.0.12", "4.0.12", "v5.0.1"}
	resultVersion := []string{"4.0.12", "4.0.12", "5.0.1"}

	for i, versionString := range versions {
		require.Equal(t, infoschema.FormatStoreServerVersion(versionString), resultVersion[i])
	}
}

// Test statements_summary.
func TestStmtSummaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)

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
	const failpointName = "github.com/pingcap/tidb/planner/core/mockPlanRowCount"
	require.NoError(t, failpoint.Enable(failpointName, "return(100)"))
	defer func() { require.NoError(t, failpoint.Disable(failpointName)) }()
	tk.MustQuery("select * from t where a=2")

	// sum_cop_task_num is always 0 if tidb_enable_collect_execution_info disabled
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10    \troot     \t100    \t\n" +
		"\t├─IndexRangeScan_8\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

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
		"Select test test.t t:k 2 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
			"\tIndexLookUp_10    \troot     \t100    \t\n" +
			"\t├─IndexRangeScan_8\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
			"\t└─TableRowIDScan_9\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustExec("set global tidb_enable_stmt_summary = false")
	defer tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new session to test
	tk = newTestKitWithRoot(t, store)

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
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 0 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10    \troot     \t1000   \t\n" +
		"\t├─IndexRangeScan_8\tcop[tikv]\t1000   \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9\tcop[tikv]\t1000   \ttable:t, keep order:false, stats:pseudo"))

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	}, nil, nil)

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	}, nil, nil)

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

func TestIssue18845(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'user18845'@'localhost';`)
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "user18845",
		Hostname:     "localhost",
		AuthUsername: "user18845",
		AuthHostname: "localhost",
	}, nil, nil)
	tk.MustQuery(`select count(*) from information_schema.columns;`)
}

// Test statements_summary_history.
func TestStmtSummaryInternalQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

	// Test Internal

	// Create a new session to test.
	tk = newTestKitWithRoot(t, store)

	tk.MustExec("select * from t where t.a = 1")
	tk.MustQuery(`select exec_count, digest_text
		from information_schema.statements_summary
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows())

	// Enable internal query and evolve baseline.
	tk.MustExec("set global tidb_stmt_summary_internal_query = 1")
	defer tk.MustExec("set global tidb_stmt_summary_internal_query = false")

	// Create a new session to test.
	tk = newTestKitWithRoot(t, store)

	tk.MustExec("admin flush bindings")
	tk.MustExec("admin evolve bindings")

	// `exec_count` may be bigger than 1 because other cases are also running.
	sql := "select digest_text " +
		"from information_schema.statements_summary " +
		"where digest_text like \"select `original_sql` , `bind_sql` , `default_db` , status%\""
	tk.MustQuery(sql).Check(testkit.Rows(
		"select `original_sql` , `bind_sql` , `default_db` , status , `create_time` , `update_time` , charset , " +
			"collation , source from `mysql` . `bind_info` where `update_time` > ? order by `update_time` , `create_time`"))

	// Test for issue #21642.
	tk.MustQuery(`select tidb_version()`)
	rows := tk.MustQuery("select plan from information_schema.statements_summary where digest_text like \"select `tidb_version`%\"").Rows()
	require.Contains(t, rows[0][0].(string), "Projection")
}

// Test error count and warning count.
func TestStmtSummaryErrorCount(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists stmt_summary_test")
	tk.MustExec("create table stmt_summary_test(id int primary key)")
	tk.MustExec("insert into stmt_summary_test values(1)")
	_, err := tk.Exec("insert into stmt_summary_test values(1)")
	require.Error(t, err)

	sql := "select exec_count, sum_errors, sum_warnings from information_schema.statements_summary where digest_text like \"insert into `stmt_summary_test`%\""
	tk.MustQuery(sql).Check(testkit.Rows("2 1 0"))

	tk.MustExec("insert ignore into stmt_summary_test values(1)")
	sql = "select exec_count, sum_errors, sum_warnings from information_schema.statements_summary where digest_text like \"insert ignore into `stmt_summary_test`%\""
	tk.MustQuery(sql).Check(testkit.Rows("1 0 1"))
}

func TestStmtSummaryPreparedStatements(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select ?'")
	tk.MustExec("set @number=1")
	tk.MustExec("execute stmt using @number")

	tk.MustQuery(`select exec_count
		from information_schema.statements_summary
		where digest_text like "prepare%"`).Check(testkit.Rows())
	tk.MustQuery(`select exec_count
		from information_schema.statements_summary
		where digest_text like "select ?"`).Check(testkit.Rows("1"))
}

func TestStmtSummarySensitiveQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("drop user if exists user_sensitive;")
	tk.MustExec("create user user_sensitive identified by '123456789';")
	tk.MustExec("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustExec("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query_sample_text from `information_schema`.`STATEMENTS_SUMMARY` " +
		"where query_sample_text like '%user_sensitive%' and " +
		"(query_sample_text like 'set password%' or query_sample_text like 'create user%' or query_sample_text like 'alter user%') " +
		"order by query_sample_text;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***}",
			"create user {user_sensitive@% password = ***}",
			"set password for user user_sensitive@%",
		))
}

// test stmtSummaryEvictedCount
func TestSimpleStmtSummaryEvictedCount(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
				time.Unix(beginTimeForCurInterval, 0).Format("2006-01-02 15:04:05"),
				time.Unix(beginTimeForCurInterval+interval, 0).Format("2006-01-02 15:04:05"),
				int64(2)),
		))

	// test too much intervals
	tk.MustExec("use test;")
	tk.MustExec(fmt.Sprintf("set @@global.tidb_stmt_summary_refresh_interval=%v", interval))
	tk.MustExec("set @@global.tidb_enable_stmt_summary=0")
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	historySize := 24
	fpPath := "github.com/pingcap/tidb/util/stmtsummary/mockTimeForStatementsSummary"
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
			Rows(time.Unix(beginTimeForCurInterval+2*interval, 0).Format("2006-01-02 15:04:05"),
				time.Unix(beginTimeForCurInterval, 0).Format("2006-01-02 15:04:05")))
	require.NoError(t, failpoint.Disable(fpPath))
	// TODO: Add more tests.
}

func TestStmtSummaryEvictedPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestStmtSummaryTableOther(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	interval := int64(1800)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval=%v", interval))
	tk.MustExec("set global tidb_enable_stmt_summary=0")
	tk.MustExec("set global tidb_enable_stmt_summary=1")
	// set stmt size to 1
	// first sql
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count=1")
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count=100")
	// second sql, evict first sql from stmt_summary
	tk.MustExec("show databases;")
	// third sql, evict second sql from stmt_summary
	tk.MustQuery("SELECT DIGEST_TEXT, DIGEST FROM `INFORMATION_SCHEMA`.`STATEMENTS_SUMMARY`;").
		Check(testkit.Rows(
			// digest in cache
			// "show databases ;"
			"show databases ; dcd020298c5f79e8dc9d63b3098083601614a04a52db458738347d15ea5712a1",
			// digest evicted
			" <nil>",
		))
	// forth sql, evict third sql from stmt_summary
	tk.MustQuery("SELECT SCHEMA_NAME FROM `INFORMATION_SCHEMA`.`STATEMENTS_SUMMARY`;").
		Check(testkit.Rows(
			// digest in cache
			"test", // select xx from yy;
			// digest evicted
			"<nil>",
		))
}

func TestStmtSummaryHistoryTableOther(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)
	// disable refreshing summary
	interval := int64(9999)
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")
	tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval = %v", interval))
	defer tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval = %v", 1800))

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	// first sql
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count=1")
	// second sql, evict first sql from stmt_summary
	tk.MustExec("show databases;")
	// third sql, evict second sql from stmt_summary
	tk.MustQuery("SELECT DIGEST_TEXT, DIGEST FROM `INFORMATION_SCHEMA`.`STATEMENTS_SUMMARY_HISTORY`;").
		Check(testkit.Rows(
			// digest in cache
			// "show databases ;"
			"show databases ; dcd020298c5f79e8dc9d63b3098083601614a04a52db458738347d15ea5712a1",
			// digest evicted
			" <nil>",
		))
	// forth sql, evict third sql from stmt_summary
	tk.MustQuery("SELECT SCHEMA_NAME FROM `INFORMATION_SCHEMA`.`STATEMENTS_SUMMARY_HISTORY`;").
		Check(testkit.Rows(
			// digest in cache
			"test", // select xx from yy;
			// digest evicted
			"<nil>",
		))
}

func TestPerformanceSchemaforPlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	tk := newTestKitWithPlanCache(t, store)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustExec("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("0 0"))
	tk.MustExec("execute stmt")
	tk.MustExec("execute stmt")
	tk.MustExec("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("3 1"))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)

	tk.MustExec("FLUSH CLIENT_ERRORS_SUMMARY")

	errno.IncrementError(1365, "root", "localhost")
	errno.IncrementError(1365, "infoschematest", "localhost")
	errno.IncrementError(1365, "root", "localhost")

	tk.MustExec("CREATE USER 'infoschematest'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "infoschematest", Hostname: "localhost"}, nil, nil))

	err := tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_global")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation", err.Error())

	err = tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_by_host")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation", err.Error())

	tk.MustQuery("SELECT error_number, error_count, warning_count FROM information_schema.client_errors_summary_by_user ORDER BY error_number").Check(testkit.Rows("1365 1 0"))

	err = tk.ExecToErr("FLUSH CLIENT_ERRORS_SUMMARY")
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the RELOAD privilege(s) for this operation", err.Error())
}

func TestTiDBTrx(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)
	tk.MustExec("drop table if exists test_tidb_trx")
	tk.MustExec("create table test_tidb_trx(i int)")
	// Execute the statement once so that the statement will be collected into statements_summary and able to be found
	// by digest.
	tk.MustExec("update test_tidb_trx set i = i + 1")
	_, digest := parser.NormalizeDigest("update test_tidb_trx set i = i + 1")
	sm := &mockSessionManager{nil, make([]*txninfo.TxnInfo, 2)}
	sm.txnInfo[0] = &txninfo.TxnInfo{
		StartTS:          424768545227014155,
		CurrentSQLDigest: digest.String(),
		State:            txninfo.TxnIdle,
		EntriesCount:     1,
		EntriesSize:      19,
		ConnectionID:     2,
		Username:         "root",
		CurrentDB:        "test",
	}
	blockTime2 := time.Date(2021, 05, 20, 13, 18, 30, 123456000, time.Local)
	sm.txnInfo[1] = &txninfo.TxnInfo{
		StartTS:          425070846483628033,
		CurrentSQLDigest: "",
		AllSQLDigests:    []string{"sql1", "sql2", digest.String()},
		State:            txninfo.TxnLockWaiting,
		ConnectionID:     10,
		Username:         "user1",
		CurrentDB:        "db1",
	}
	sm.txnInfo[1].BlockStartTime.Valid = true
	sm.txnInfo[1].BlockStartTime.Time = blockTime2
	tk.Session().SetSessionManager(sm)

	tk.MustQuery("select * from information_schema.TIDB_TRX;").Check(testkit.Rows(
		"424768545227014155 2021-05-07 12:56:48.001000 "+digest.String()+" update `test_tidb_trx` set `i` = `i` + ? Idle <nil> 1 19 2 root test []",
		"425070846483628033 2021-05-20 21:16:35.778000 <nil> <nil> LockWaiting 2021-05-20 13:18:30.123456 0 0 10 user1 db1 [\"sql1\",\"sql2\",\""+digest.String()+"\"]"))

	// Test the all_sql_digests column can be directly passed to the tidb_decode_sql_digests function.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/sqlDigestRetrieverSkipRetrieveGlobal", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/sqlDigestRetrieverSkipRetrieveGlobal"))
	}()
	tk.MustQuery("select tidb_decode_sql_digests(all_sql_digests) from information_schema.tidb_trx").Check(testkit.Rows(
		"[]",
		"[null,null,\"update `test_tidb_trx` set `i` = `i` + ?\"]"))
}

func TestInfoSchemaDeadlockPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := newTestKitWithRoot(t, store)
	tk.MustExec("create user 'testuser'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil))
	err := tk.QueryToErr("select * from information_schema.deadlocks")
	require.Error(t, err)
	require.Equal(t, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation", err.Error())

	tk = newTestKitWithRoot(t, store)
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil))
	_ = tk.MustQuery("select * from information_schema.deadlocks")
}

func TestAttributes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	// test the failpoint for testing
	fpName := "github.com/pingcap/tidb/executor/mockOutputOfAttributes"
	tk := newTestKitWithRoot(t, store)
	tk.MustQuery("select * from information_schema.attributes").Check(testkit.Rows())

	require.NoError(t, failpoint.Enable(fpName, "return"))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	tk.MustQuery(`select * from information_schema.attributes`).Check(testkit.Rows(
		`schema/test/test_label key-range "merge_option=allow" [7480000000000000ff395f720000000000fa, 7480000000000000ff3a5f720000000000fa]`,
	))
}

func TestReferentialConstraints(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE DATABASE referconstraints")
	tk.MustExec("use referconstraints")

	tk.MustExec("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)")
	tk.MustExec("CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, t1_id INT DEFAULT NULL, INDEX (t1_id), CONSTRAINT `fk_to_t1` FOREIGN KEY (`t1_id`) REFERENCES `t1` (`id`))")

	tk.MustQuery(`SELECT * FROM information_schema.referential_constraints WHERE table_name='t2'`).Check(testkit.Rows("def referconstraints fk_to_t1 def referconstraints PRIMARY NONE NO ACTION NO ACTION t2 t1"))
}
