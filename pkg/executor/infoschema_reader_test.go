// Copyright 2020 PingCAP, Inc.
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
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestInspectionTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	instances := []string{
		"pd,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,0",
		"tidb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,1001",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash,0",
		"tiproxy,127.0.0.1:6000,127.0.0.1:3380,mock-version,mock-githash,0",
		"ticdc,127.0.0.1:8300,127.0.0.1:8301,mock-version,mock-githash,0",
		"tso,127.0.0.1:3379,127.0.0.1:3379,mock-version,mock-githash,0",
		"scheduling,127.0.0.1:4379,127.0.0.1:4379,mock-version,mock-githash,0",
	}
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tiproxy 127.0.0.1:6000 127.0.0.1:3380 mock-version mock-githash 0",
		"ticdc 127.0.0.1:8300 127.0.0.1:8301 mock-version mock-githash 0",
		"tso 127.0.0.1:3379 127.0.0.1:3379 mock-version mock-githash 0",
		"scheduling 127.0.0.1:4379 127.0.0.1:4379 mock-version mock-githash 0",
	))

	// enable inspection mode
	inspectionTableCache := map[string]variable.TableSnapshot{}
	tk.Session().GetSessionVars().InspectionTableCache = inspectionTableCache
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tiproxy 127.0.0.1:6000 127.0.0.1:3380 mock-version mock-githash 0",
		"ticdc 127.0.0.1:8300 127.0.0.1:8301 mock-version mock-githash 0",
		"tso 127.0.0.1:3379 127.0.0.1:3379 mock-version mock-githash 0",
		"scheduling 127.0.0.1:4379 127.0.0.1:4379 mock-version mock-githash 0",
	))
	require.NoError(t, inspectionTableCache["cluster_info"].Err)
	require.Len(t, inspectionTableCache["cluster_info"].Rows, 7)

	// check whether is obtain data from cache at the next time
	inspectionTableCache["cluster_info"].Rows[0][0].SetString("modified-pd", mysql.DefaultCollationName)
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		"modified-pd 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tidb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 1001",
		"tikv 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash 0",
		"tiproxy 127.0.0.1:6000 127.0.0.1:3380 mock-version mock-githash 0",
		"ticdc 127.0.0.1:8300 127.0.0.1:8301 mock-version mock-githash 0",
		"tso 127.0.0.1:3379 127.0.0.1:3379 mock-version mock-githash 0",
		"scheduling 127.0.0.1:4379 127.0.0.1:4379 mock-version mock-githash 0",
	))
	tk.Session().GetSessionVars().InspectionTableCache = nil
}

func TestUserPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// test the privilege of new user for information_schema.table_constraints
	tk.MustExec("create user constraints_tester")
	constraintsTester := testkit.NewTestKit(t, store)
	constraintsTester.MustExec("use information_schema")
	require.NoError(t, constraintsTester.Session().Auth(&auth.UserIdentity{
		Username: "constraints_tester",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS WHERE TABLE_NAME != 'CLUSTER_SLOW_QUERY';").Check([][]any{})

	// test the privilege of user with privilege of mysql.gc_delete_range for information_schema.table_constraints
	tk.MustExec("CREATE ROLE r_gc_delete_range ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.gc_delete_range TO r_gc_delete_range;")
	tk.MustExec("GRANT r_gc_delete_range TO constraints_tester;")
	constraintsTester.MustExec("set role r_gc_delete_range")
	rows := constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Rows()
	require.Greater(t, len(rows), 0)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='tables_priv';").Check([][]any{})

	// test the privilege of new user for information_schema
	tk.MustExec("create user tester1")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use information_schema")
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{
		Username: "tester1",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	tk1.MustQuery("select * from information_schema.STATISTICS WHERE TABLE_NAME != 'CLUSTER_SLOW_QUERY';").Check([][]any{})

	// test the privilege of user with some privilege for information_schema
	tk.MustExec("create user tester2")
	tk.MustExec("CREATE ROLE r_columns_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.columns_priv TO r_columns_priv;")
	tk.MustExec("GRANT r_columns_priv TO tester2;")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use information_schema")
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{
		Username: "tester2",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	tk2.MustExec("set role r_columns_priv")
	rows = tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='columns_priv' and COLUMN_NAME='Host';").Rows()
	require.Greater(t, len(rows), 0)
	tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='tables_priv' and COLUMN_NAME='Host';").Check(
		[][]any{})

	// test the privilege of user with all privilege for information_schema
	tk.MustExec("create user tester3")
	tk.MustExec("CREATE ROLE r_all_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO r_all_priv;")
	tk.MustExec("GRANT r_all_priv TO tester3;")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use information_schema")
	require.NoError(t, tk3.Session().Auth(&auth.UserIdentity{
		Username: "tester3",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	tk3.MustExec("set role r_all_priv")
	rows = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='columns_priv' and COLUMN_NAME='Host';").Rows()
	require.Greater(t, len(rows), 0)
	rows = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='tables_priv' and COLUMN_NAME='Host';").Rows()
	require.Greater(t, len(rows), 0)
}

func TestDataForTableStatsField(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	h := dom.StatsHandle()
	h.Clear()
	is := dom.InfoSchema()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int, e char(5), index idx(e))")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("0 0 0 0"))
	tk.MustExec(`insert into t(c, d, e) values(1, 2, "c"), (2, 3, "d"), (3, 4, "e")`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3 18 54 6"))
	tk.MustExec(`insert into t(c, d, e) values(4, 5, "f")`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("4 18 72 8"))
	tk.MustExec("delete from t where c >= 3")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 18 36 4"))
	tk.MustExec("delete from t where c=3")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 18 36 4"))

	// Test partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16))`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec(`insert into t(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e")`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3 18 54 6"))
}

func TestPartitionsTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	h := dom.StatsHandle()
	h.Clear()
	is := dom.InfoSchema()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	testkit.WithPruneMode(tk, variable.Static, func() {
		tk.MustExec("DROP TABLE IF EXISTS `test_partitions`;")
		tk.MustExec(`CREATE TABLE test_partitions (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
		require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
		tk.MustExec(`insert into test_partitions(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)

		tk.MustQuery("select PARTITION_NAME, PARTITION_DESCRIPTION from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"p0 6]\n" +
				"[p1 11]\n" +
				"[p2 16"))

		tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"0 0 0 0]\n" +
				"[0 0 0 0]\n" +
				"[0 0 0 0"))
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where table_name='test_partitions';").Check(
			testkit.Rows("" +
				"1 18 18 2]\n" +
				"[1 18 18 2]\n" +
				"[1 18 18 2"))
	})

	// Test for table has no partitions.
	tk.MustExec("DROP TABLE IF EXISTS `test_partitions_1`;")
	tk.MustExec(`CREATE TABLE test_partitions_1 (a int, b int, c varchar(5), primary key(a), index idx(c));`)
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec(`insert into test_partitions_1(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	tk.MustQuery("select PARTITION_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH from information_schema.PARTITIONS where table_name='test_partitions_1';").Check(
		testkit.Rows("<nil> 3 18 54 6"))

	tk.MustExec("DROP TABLE IF EXISTS `test_partitions`;")
	tk.MustExec(`CREATE TABLE test_partitions1 (id int, b int, c varchar(5), primary key(id), index idx(c)) PARTITION BY RANGE COLUMNS(id) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions1';").Check(testkit.Rows("p0 RANGE COLUMNS `id`", "p1 RANGE COLUMNS `id`", "p2 RANGE COLUMNS `id`"))
	tk.MustExec("DROP TABLE test_partitions1")

	tk.MustExec(`CREATE TABLE test_partitions (id int, b int, c varchar(5), primary key(id,b), index idx(c)) PARTITION BY RANGE COLUMNS(id,b) (PARTITION p0 VALUES LESS THAN (6,1), PARTITION p1 VALUES LESS THAN (11,9), PARTITION p2 VALUES LESS THAN (16,MAXVALUE))`)
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 RANGE COLUMNS `id`,`b`", "p1 RANGE COLUMNS `id`,`b`", "p2 RANGE COLUMNS `id`,`b`"))
	tk.MustExec("DROP TABLE test_partitions")

	tk.MustExec(`create table test_partitions (a varchar(255), b int, c datetime) partition by list columns (b,a) (partition p0 values in ((1,"1"), (3,"3")), partition p1 values in ((2, "2")))`)
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions'").Check(testkit.Rows("p0 LIST COLUMNS `b`,`a`", "p1 LIST COLUMNS `b`,`a`"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a varchar(3)) partition by list columns (a) (partition p0 values in ('1'), partition p1 values in ('2'))")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions'").Check(testkit.Rows("p0 LIST COLUMNS `a`", "p1 LIST COLUMNS `a`"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (2));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST `a`", "p1 LIST `a`"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a date) partition by list (year(a)) (partition p0 values in (1), partition p1 values in (2));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST YEAR(`a`)", "p1 LIST YEAR(`a`)"))
	tk.MustExec("drop table test_partitions")

	tk.MustExec("create table test_partitions (a bigint, b date) partition by list columns (a,b) (partition p0 values in ((1,'2020-09-28'),(1,'2020-09-29')));")
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where table_name = 'test_partitions';").Check(testkit.Rows("p0 LIST COLUMNS `a`,`b`"))
	pid, err := strconv.Atoi(tk.MustQuery("select TIDB_PARTITION_ID from information_schema.partitions where table_name = 'test_partitions';").Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, pid, 0)
	tk.MustExec("drop table test_partitions")
}

func TestForAnalyzeStatus(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	analyzeStatusTable := "CREATE TABLE `ANALYZE_STATUS` (\n" +
		"  `TABLE_SCHEMA` varchar(64) DEFAULT NULL,\n" +
		"  `TABLE_NAME` varchar(64) DEFAULT NULL,\n" +
		"  `PARTITION_NAME` varchar(64) DEFAULT NULL,\n" +
		"  `JOB_INFO` longtext DEFAULT NULL,\n" +
		"  `PROCESSED_ROWS` bigint(64) unsigned DEFAULT NULL,\n" +
		"  `START_TIME` datetime DEFAULT NULL,\n" +
		"  `END_TIME` datetime DEFAULT NULL,\n" +
		"  `STATE` varchar(64) DEFAULT NULL,\n" +
		"  `FAIL_REASON` longtext DEFAULT NULL,\n" +
		"  `INSTANCE` varchar(512) DEFAULT NULL,\n" +
		"  `PROCESS_ID` bigint(64) unsigned DEFAULT NULL,\n" +
		"  `REMAINING_SECONDS` varchar(512) DEFAULT NULL,\n" +
		"  `PROGRESS` double(22,6) DEFAULT NULL,\n" +
		"  `ESTIMATED_TOTAL_ROWS` bigint(64) unsigned DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	tk.MustQuery("show create table information_schema.analyze_status").Check(testkit.Rows("ANALYZE_STATUS " + analyzeStatusTable))
	tk.MustExec("delete from mysql.analyze_jobs")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists analyze_test")
	tk.MustExec("create table analyze_test (a int, b int, index idx(a))")
	tk.MustExec("insert into analyze_test values (1,2),(3,4)")

	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check([][]any{})
	tk.MustExec("analyze table analyze_test")
	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check(testkit.Rows("analyze_test"))

	// test the privilege of new user for information_schema.analyze_status
	tk.MustExec("create user analyze_tester")
	analyzeTester := testkit.NewTestKit(t, store)
	analyzeTester.MustExec("use information_schema")
	require.NoError(t, analyzeTester.Session().Auth(&auth.UserIdentity{
		Username: "analyze_tester",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	analyzeTester.MustQuery("show analyze status").Check([][]any{})
	analyzeTester.MustQuery("select * from information_schema.ANALYZE_STATUS;").Check([][]any{})

	// test the privilege of user with privilege of test.t1 for information_schema.analyze_status
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,2),(3,4)")
	tk.MustExec("analyze table t1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"")) // 1 note.
	require.NoError(t, dom.StatsHandle().LoadNeededHistograms())
	tk.MustExec("CREATE ROLE r_t1 ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test.t1 TO r_t1;")
	tk.MustExec("GRANT r_t1 TO analyze_tester;")
	analyzeTester.MustExec("set role r_t1")
	rows := tk.MustQuery("select * from information_schema.analyze_status where TABLE_NAME='t1'").Sort().Rows()
	require.Greater(t, len(rows), 0)
	for _, row := range rows {
		require.Len(t, row, 14) // test length of row
		// test `End_time` field
		str, ok := row[6].(string)
		require.True(t, ok)
		_, err := time.Parse(time.DateTime, str)
		require.NoError(t, err)
	}
	rows2 := tk.MustQuery("show analyze status where TABLE_NAME='t1'").Sort().Rows()
	require.Equal(t, len(rows), len(rows2))
	for i, row := range rows {
		for j, r := range row {
			require.Equal(t, r, rows2[i][j])
		}
	}
}

func TestForServersInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("select * from information_schema.TIDB_SERVERS_INFO").Rows()
	require.Len(t, rows, 1)

	info, err := infosync.GetServerInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, info.ID, rows[0][0])
	require.Equal(t, info.IP, rows[0][1])
	require.Equal(t, strconv.FormatInt(int64(info.Port), 10), rows[0][2])
	require.Equal(t, strconv.FormatInt(int64(info.StatusPort), 10), rows[0][3])
	require.Equal(t, info.Lease, rows[0][4])
	require.Equal(t, info.Version, rows[0][5])
	require.Equal(t, info.GitHash, rows[0][6])
	require.Equal(t, info.BinlogStatus, rows[0][7])
	require.Equal(t, stringutil.BuildStringFromLabels(info.Labels), rows[0][8])
}

func TestTiFlashSystemTableWithTiFlashV620(t *testing.T) {
	instances := []string{
		"tiflash,127.0.0.1:3933,127.0.0.1:7777,,",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,,",
	}
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockStoreServerInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	mocker := newGetTiFlashSystemTableRequestMocker(t)
	mocker.MockQuery(`SELECT * FROM system.dt_segments LIMIT 0, 1024`, func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error) {
		require.EqualValues(t, req.Sql, "SELECT * FROM system.dt_segments LIMIT 0, 1024")
		data, err := os.ReadFile("testdata/tiflash_v620_dt_segments.json")
		require.NoError(t, err)
		return &kvrpcpb.TiFlashSystemTableResponse{
			Data: data,
		}, nil
	})
	mocker.MockQuery(`SELECT * FROM system.dt_tables LIMIT 0, 1024`, func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error) {
		require.EqualValues(t, req.Sql, "SELECT * FROM system.dt_tables LIMIT 0, 1024")
		data, err := os.ReadFile("testdata/tiflash_v620_dt_tables.json")
		require.NoError(t, err)
		return &kvrpcpb.TiFlashSystemTableResponse{
			Data: data,
		}, nil
	})

	store := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from information_schema.TIFLASH_SEGMENTS;").Check(testkit.Rows(
		"db_1 t_10 mysql tables_priv 10 0 1 [-9223372036854775808,9223372036854775807) <nil> 0 0 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 0 2032 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 127.0.0.1:3933",
		"db_1 t_8 mysql db 8 0 1 [-9223372036854775808,9223372036854775807) <nil> 0 0 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 0 2032 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 127.0.0.1:3933",
		"db_2 t_70 test segment 70 0 1 [01,FA) <nil> 30511 50813627 0.6730359542460096 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 3578860 409336 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> 127.0.0.1:3933",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("select * from information_schema.TIFLASH_TABLES;").Check(testkit.Rows(
		"db_1 t_10 mysql tables_priv 10 0 1 0 0 0 <nil> 0 <nil> 0 <nil> <nil> 0 0 0 0 0 0 <nil> <nil> <nil> 0 0 0 0 <nil> <nil> 0 <nil> <nil> <nil> <nil> 0 <nil> <nil> <nil> 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_1 t_8 mysql db 8 0 1 0 0 0 <nil> 0 <nil> 0 <nil> <nil> 0 0 0 0 0 0 <nil> <nil> <nil> 0 0 0 0 <nil> <nil> 0 <nil> <nil> <nil> <nil> 0 <nil> <nil> <nil> 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_2 t_70 test segment 70 0 1 102000 169873868 0 0 0 <nil> 0 <nil> <nil> 0 102000 169873868 0 0 0 <nil> <nil> <nil> 1 102000 169873868 43867622 102000 169873868 0 <nil> <nil> <nil> <nil> 13 13 7846.153846153846 13067220.615384616 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestTiFlashSystemTableWithTiFlashV630(t *testing.T) {
	instances := []string{
		"tiflash,127.0.0.1:3933,127.0.0.1:7777,,",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,,",
	}
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockStoreServerInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	mocker := newGetTiFlashSystemTableRequestMocker(t)
	mocker.MockQuery(`SELECT * FROM system.dt_segments LIMIT 0, 1024`, func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error) {
		require.EqualValues(t, req.Sql, "SELECT * FROM system.dt_segments LIMIT 0, 1024")
		data, err := os.ReadFile("testdata/tiflash_v630_dt_segments.json")
		require.NoError(t, err)
		return &kvrpcpb.TiFlashSystemTableResponse{
			Data: data,
		}, nil
	})

	store := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from information_schema.TIFLASH_SEGMENTS;").Check(testkit.Rows(
		"db_1 t_10 mysql tables_priv 10 0 1 [-9223372036854775808,9223372036854775807) 0 0 0 <nil> 0 0 0 0 2 0 0 0 0 0 2032 3 0 0 1 1 0 0 0 0 127.0.0.1:3933",
		"db_2 t_70 test segment 70 436272981189328904 1 [01,FA) 5 102000 169874232 0 0 0 0 0 2 0 0 0 0 0 2032 3 102000 169874232 1 68 102000 169874232 43951837 20 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 1 [01,013130303030393535FF61653666642D6136FF61382D343032382DFF616436312D663736FF3062323736643461FF3600000000000000F8) 2 0 0 <nil> 0 0 1 1 110 0 0 4 4 0 2032 111 0 0 1 70 0 0 0 0 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 113 [013130303030393535FF61653666642D6136FF61382D343032382DFF616436312D663736FF3062323736643461FF3600000000000000F8,013139393938363264FF33346535382D3735FF31382D343661612DFF626235392D636264FF3139333434623736FF3100000000000000F9) 2 10167 16932617 0.4887380741615029 0 0 0 0 114 4969 8275782 2 0 0 63992 112 5198 8656835 1 71 5198 8656835 2254100 1 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 116 [013139393938363264FF33346535382D3735FF31382D343661612DFF626235392D636264FF3139333434623736FF3100000000000000F9,013330303131383034FF61323537662D6638FF63302D346466622DFF383235632D353361FF3236306338616662FF3400000000000000F8) 3 8 13322 0.5 3 4986 1 0 117 1 1668 4 3 4986 2032 115 4 6668 1 78 4 6668 6799 1 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 125 [013330303131383034FF61323537662D6638FF63302D346466622DFF383235632D353361FF3236306338616662FF3400000000000000F8,013339393939613861FF30663062332D6537FF32372D346234642DFF396535632D363865FF3336323066383431FF6300000000000000F9) 2 8677 14451079 0.4024432407514118 3492 5816059 3 0 126 0 0 0 0 5816059 2032 124 5185 8635020 1 79 5185 8635020 2247938 1 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 128 [013339393939613861FF30663062332D6537FF32372D346234642DFF396535632D363865FF3336323066383431FF6300000000000000F9,013730303031636230FF32663330652D3539FF62352D346134302DFF613539312D383930FF6132316364633466FF3200000000000000F8) 0 1 1668 1 0 0 0 0 129 1 1668 5 4 0 2032 127 0 0 1 78 4 6668 6799 1 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 119 [013730303031636230FF32663330652D3539FF62352D346134302DFF613539312D383930FF6132316364633466FF3200000000000000F8,013739393939386561FF36393566612D3534FF64302D346437642DFF383136612D646335FF6432613130353533FF3200000000000000F9) 2 10303 17158730 0.489372027564787 0 0 0 0 120 5042 8397126 2 0 0 63992 118 5261 8761604 1 77 5261 8761604 2280506 1 127.0.0.1:3933",
		"db_2 t_75 test segment 75 0 122 [013739393939386561FF36393566612D3534FF64302D346437642DFF383136612D646335FF6432613130353533FF3200000000000000F9,FA) 0 1 1663 1 0 0 0 0 123 1 1663 4 3 0 2032 121 0 0 1 78 4 6668 6799 1 127.0.0.1:3933",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestTiFlashSystemTableWithTiFlashV640(t *testing.T) {
	instances := []string{
		"tiflash,127.0.0.1:3933,127.0.0.1:7777,,",
		"tikv,127.0.0.1:11080,127.0.0.1:10080,,",
	}
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockStoreServerInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	mocker := newGetTiFlashSystemTableRequestMocker(t)
	mocker.MockQuery(`SELECT * FROM system.dt_tables LIMIT 0, 1024`, func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error) {
		require.EqualValues(t, req.Sql, "SELECT * FROM system.dt_tables LIMIT 0, 1024")
		data, err := os.ReadFile("testdata/tiflash_v640_dt_tables.json")
		require.NoError(t, err)
		return &kvrpcpb.TiFlashSystemTableResponse{
			Data: data,
		}, nil
	})

	store := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from information_schema.TIFLASH_TABLES;").Check(testkit.Rows(
		"db_70 t_135 tpcc customer 135 0 4 3528714 2464079200 0 0.002329177144988231 1 0 929227 0.16169850346757514 0 8128 882178.5 616019800 4 8219 5747810 2054.75 1436952.5 0 4 3520495 2458331390 1601563417 880123.75 614582847.5 24 8 6 342.4583333333333 239492.08333333334 482 120.5 7303.9315352697095 5100272.593360996 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_137 tpcc district 137 0 1 7993 1346259 0 0.8748905292130614 1 0.8055198055198055 252168 0.21407121407121407 0 147272 7993 1346259 1 6993 1178050 6993 1178050 0 1 1000 168209 91344 1000 168209 6 6 6 1165.5 196341.66666666666 10 10 100 16820.9 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_139 tpcc history 139 0 19 19379697 1629276978 0 0.0006053758219233252 0.5789473684210527 0.4626662120695534 253640 0.25434708489601093 0 293544 1019984.052631579 85751419.89473684 11 11732 997220 1066.5454545454545 90656.36363636363 0 19 19367965 1628279758 625147717 1019366.5789473684 85698934.63157895 15 4 1.3636363636363635 782.1333333333333 66481.33333333333 2378 125.15789473684211 8144.644659377628 684726.559293524 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_141 tpcc item 141 0 1 100000 10799081 0 0 0 <nil> 0 <nil> <nil> 0 100000 10799081 0 0 0 <nil> <nil> <nil> 1 100000 10799081 7357726 100000 10799081 0 0 <nil> <nil> <nil> 13 13 7692.307692307692 830698.5384615385 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_143 tpcc new_order 143 0 4 2717707 78813503 0 0.02266763856442214 1 0.9678592299201351 52809 0.029559768846178818 0 1434208 679426.75 19703375.75 4 61604 1786516 15401 446629 0 3 2656103 77026987 40906492 885367.6666666666 25675662.333333332 37 24 9.25 1664.972972972973 48284.21621621621 380 126.66666666666667 6989.744736842105 202702.59736842106 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_145 tpcc order_line 145 0 203 210607202 20007684190 0 0.0054566462546708164 0.5862068965517241 0.7810067620424135 620065 0.005679558722564825 0 22607144 1037473.9014778325 98560020.64039409 119 1149209 109174855 9657.218487394957 917435.756302521 0 203 209457993 19898509335 8724002804 1031812.7733990147 98022213.47290641 893 39 7.504201680672269 1286.9081746920492 122256.27659574468 31507 155.20689655172413 6647.982765734599 631558.3627447869 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_147 tpcc orders 147 0 22 21903301 1270391458 0 0.02021357420052804 0.7272727272727273 0.9239944527763222 260536 0.010145817899282655 0 10025264 995604.5909090909 57745066.27272727 16 442744 25679152 27671.5 1604947 0 22 21460557 1244712306 452173775 975479.8636363636 56577832.09090909 242 34 15.125 1829.5206611570247 106112.19834710743 2973 135.13636363636363 7218.485368314833 418672.15136226034 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_149 tpcc stock 149 0 42 11112720 4811805131 0 0.028085203262567582 0.9761904761904762 0.8463391893060944 10227093 0.07567373591410528 0 6719064 264588.5714285714 114566788.83333333 41 312103 135131097 7612.268292682927 3295880.4146341463 0 42 10800617 4676674034 3231872509 257157.54761904763 111349381.76190476 238 26 5.804878048780488 1311.357142857143 567777.718487395 1644 39.142857142857146 6569.718369829684 2844692.234793187 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
		"db_70 t_151 tpcc warehouse 151 0 1 5842 923615 0 0.9828825744608011 1 0.9669104841518634 70220 0.07732497387669801 0 133048 5842 923615 1 5742 907807 5742 907807 0 1 100 15808 11642 100 15808 5 5 5 1148.4 181561.4 5 5 20 3161.6 0 0 0  0 0 0  0 0 0  0 127.0.0.1:3933",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

// https://github.com/pingcap/tidb/issues/32459.
func TestJoinSystemTableContainsView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a timestamp, b int);")
	tk.MustExec("insert into t values (null, 100);")
	tk.MustExec("create view v as select * from t;")
	// This is used by grafana when TiDB is specified as the data source.
	// See https://github.com/grafana/grafana/blob/e86b6662a187c77656f72bef3b0022bf5ced8b98/public/app/plugins/datasource/mysql/meta_query.ts#L31
	for i := 0; i < 10; i++ {
		tk.MustQuery(`
SELECT
    table_name as table_name,
    ( SELECT
        column_name as column_name
      FROM information_schema.columns c
      WHERE
        c.table_schema = t.table_schema AND
        c.table_name = t.table_name AND
        c.data_type IN ('timestamp', 'datetime')
      ORDER BY ordinal_position LIMIT 1
    ) AS time_column,
    ( SELECT
        column_name AS column_name
      FROM information_schema.columns c
      WHERE
        c.table_schema = t.table_schema AND
        c.table_name = t.table_name AND
        c.data_type IN('float', 'int', 'bigint')
      ORDER BY ordinal_position LIMIT 1
    ) AS value_column
  FROM information_schema.tables t
  WHERE
    t.table_schema = database() AND
    EXISTS
    ( SELECT 1
      FROM information_schema.columns c
      WHERE
        c.table_schema = t.table_schema AND
        c.table_name = t.table_name AND
        c.data_type IN ('timestamp', 'datetime')
    ) AND
    EXISTS
    ( SELECT 1
      FROM information_schema.columns c
      WHERE
        c.table_schema = t.table_schema AND
        c.table_name = t.table_name AND
        c.data_type IN('float', 'int', 'bigint')
    )
  LIMIT 1
;
`)
	}
}

// https://github.com/pingcap/tidb/issues/36426.
func TestShowColumnsWithSubQueryView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	if tk.MustQuery("select @@tidb_schema_cache_size > 0").Equal(testkit.Rows("1")) {
		// infoschema v2 requires network, so it cannot be tested this way.
		t.Skip()
	}

	tk.MustExec("CREATE TABLE added (`id` int(11), `name` text, `some_date` timestamp);")
	tk.MustExec("CREATE TABLE incremental (`id` int(11), `name`text, `some_date` timestamp);")
	tk.MustExec("create view temp_view as (select * from `added` where id > (select max(id) from `incremental`));")
	// Show columns should not send coprocessor request to the storage.
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")`))
	tk.MustQuery("show columns from temp_view;").Check(testkit.Rows(
		"id int(11) YES  <nil> ",
		"name text YES  <nil> ",
		"some_date timestamp YES  <nil> "))
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))
}

// Code below are helper utilities for the test cases.

type getTiFlashSystemTableRequestMocker struct {
	tikv.Client
	t        *testing.T
	handlers map[string]func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error)
}

func (client *getTiFlashSystemTableRequestMocker) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdGetTiFlashSystemTable {
		if handler, ok := client.handlers[req.Req.(*kvrpcpb.TiFlashSystemTableRequest).Sql]; ok {
			resp, err := handler(req.GetTiFlashSystemTable())
			if err != nil {
				return nil, err
			}
			return &tikvrpc.Response{Resp: resp}, nil
		}
		// If we enter here, it means no handler is matching. We should fail!
		require.Fail(client.t, fmt.Sprintf("Received request %s but no matching handler, maybe caused by unexpected query", req.Req.(*kvrpcpb.TiFlashSystemTableRequest).Sql))
	}
	return client.Client.SendRequest(ctx, addr, req, timeout)
}

func (client *getTiFlashSystemTableRequestMocker) MockQuery(query string, fn func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error)) *getTiFlashSystemTableRequestMocker {
	client.handlers[query] = fn
	return client
}

func (client *getTiFlashSystemTableRequestMocker) AsOpt() mockstore.MockTiKVStoreOption {
	return mockstore.WithClientHijacker(func(kvClient tikv.Client) tikv.Client {
		client.Client = kvClient
		return client
	})
}

func newGetTiFlashSystemTableRequestMocker(t *testing.T) *getTiFlashSystemTableRequestMocker {
	return &getTiFlashSystemTableRequestMocker{
		handlers: make(map[string]func(req *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error), 0),
		t:        t,
	}
}

// https://github.com/pingcap/tidb/issues/52350
func TestReferencedTableSchemaWithForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test;")
	tk.MustExec("create database if not exists test2;")
	tk.MustExec("drop table if exists test.t1;")
	tk.MustExec("drop table if exists test2.t2;")
	tk.MustExec("create table test.t1(id int primary key);")
	tk.MustExec("create table test2.t2(i int, id int, foreign key (id) references test.t1(id));")

	tk.MustQuery(`SELECT column_name, referenced_column_name, referenced_table_name, table_schema, referenced_table_schema 
	FROM information_schema.key_column_usage
	WHERE table_name = 't2' AND table_schema = 'test2';`).Check(testkit.Rows(
		"id id t1 test2 test"))
}
