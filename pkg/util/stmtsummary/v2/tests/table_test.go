// Copyright 2023 PingCAP, Inc.
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

package tests

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestStmtSummaryTable(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

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

	// Create a new session to test.
	tk = newTestKitWithRoot(t, store)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // affect est-rows in this UT

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t values(2, 'b')")
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
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t")

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

	tk1 := newTestKit(t, store)
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
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b varchar(10), key k(a))")
	defer tk.MustExec("drop table if exists t1")

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

	tk1 := newTestKit(t, store)
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

func TestStmtSummaryErrorCount(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
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
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
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
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
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

func TestStmtSummaryTableOther(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)

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
			"show databases 0e247706bf6e791fbf4af8c8e7658af5ffc45c63179871202d8f91551ee03161",
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
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)

	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")

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
			"show databases 0e247706bf6e791fbf4af8c8e7658af5ffc45c63179871202d8f91551ee03161",
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

func TestPerformanceSchemaforNonPrepPlanCache(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, key(a))`)
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustQuery("select exec_count, digest_text, prepared, plan_in_cache, plan_cache_hits, query_sample_text " +
		"from information_schema.statements_summary where digest_text='select * from `t` where `a` = ?'").Check(testkit.Rows(
		"2 select * from `t` where `a` = ? 0 1 1 select * from t where a=1"))

	tk.MustExec(`select * from t where a=2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`select * from t where a=3`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// exec_count 2->4, plan_cache_hits 1->3
	tk.MustQuery("select exec_count, digest_text, prepared, plan_in_cache, plan_cache_hits, query_sample_text " +
		"from information_schema.statements_summary where digest_text='select * from `t` where `a` = ?'").Check(testkit.Rows(
		"4 select * from `t` where `a` = ? 0 1 3 select * from t where a=1"))

	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
	tk.MustExec(`select * from t where a=2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=3`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	// exec_count 4->6, plan_cache_hits 3->3
	tk.MustQuery("select exec_count, digest_text, prepared, plan_in_cache, plan_cache_hits, query_sample_text " +
		"from information_schema.statements_summary where digest_text='select * from `t` where `a` = ?'").Check(testkit.Rows(
		"6 select * from `t` where `a` = ? 0 0 3 select * from t where a=1"))
}

func TestPlanCacheUnqualified2(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	// queries accessing temporary tables or generated columns
	tk.MustExec(`create table t1 (a int, b int)`)
	tk.MustExec(`create temporary table t_temp_unqualified_test (a int)`)
	tk.MustExec(`create table t_gen_unqualified_test (id int, addr json, city VARCHAR(64) AS (JSON_UNQUOTE(JSON_EXTRACT(addr, '$.city'))))`)
	tk.MustExec(`prepare st from 'select * from t1, t_temp_unqualified_test where t1.a > 10'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_temp_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t1` , `t_temp_unqualified_test` where `t1` . `a` > ? 1 1 query accesses temporary tables is un-cacheable"))

	tk.MustExec(`set @@tidb_opt_fix_control = "45798:off"`)
	tk.MustExec(`prepare st from 'select * from t1, t_gen_unqualified_test where t1.a > 10'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_gen_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t1` , `t_gen_unqualified_test` where `t1` . `a` > ? 1 1 query accesses generated columns is un-cacheable"))

	// queries containing non-correlated sub-queries
	tk.MustExec(`create table t_subquery_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select * from t1 where t1.a > (select max(a) from t_subquery_unqualified_test)'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_subquery_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t1` where `t1` . `a` > ( select max ( `a` ) from `t_subquery_unqualified_test` ) 1 1 query has uncorrelated sub-queries is un-cacheable"))

	// query plans that contain PhysicalApply
	tk.MustExec(`create table t_apply_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select * from t1 where t1.a > (select a from t_apply_unqualified_test where t1.b > t_apply_unqualified_test.b)'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_apply_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t1` where `t1` . `a` > ( select `a` from `t_apply_unqualified_test` where `t1` . `b` > `t_apply_unqualified_test` . `b` ) 1 1 PhysicalApply plan is un-cacheable"))

	// queries containing ignore_plan_cache or set_var hints
	tk.MustExec(`create table t_ignore_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select /*+ ignore_plan_cache() */ * from t_ignore_unqualified_test'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_ignore_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t_ignore_unqualified_test` 1 1 ignore plan cache by hint"))

	tk.MustExec(`create table t_setvar_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select /*+ set_var(max_execution_time=10000) */ * from t_setvar_unqualified_test'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_setvar_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t_setvar_unqualified_test` 1 1 SET_VAR is used in the SQL"))

	// queries containing non-deterministic functions
	tk.MustExec(`create table t_non_deterministic_1_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select user() from t_non_deterministic_1_unqualified_test'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_non_deterministic_1_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select user ( ) from `t_non_deterministic_1_unqualified_test` 1 1 query has 'user' is un-cacheable"))

	tk.MustExec(`create table t_non_deterministic_2_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select version() from t_non_deterministic_2_unqualified_test'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_non_deterministic_2_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select `version` ( ) from `t_non_deterministic_2_unqualified_test` 1 1 query has 'version' is un-cacheable"))

	// queries with a larger LIMIT
	tk.MustExec(`create table t_limit_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select * from t_limit_unqualified_test limit ?'`)
	tk.MustExec(`set @x=1000000`)
	tk.MustExec(`execute st using @x`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_limit_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select * from `t_limit_unqualified_test` limit ? 1 1 limit count is too large"))

	// queries accessing any system tables
	tk.MustExec(`create table t_system_unqualified_test (a int, b int)`)
	tk.MustExec(`prepare st from 'select 1 from t_system_unqualified_test, information_schema.tables'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%select%from%t_system_unqualified_test%'`).Sort().Check(testkit.Rows(
		"select ? from `t_system_unqualified_test` , `information_schema` . `tables` 1 1 PhysicalMemTable plan is un-cacheable"))
}

func TestPlanCacheUnqualified(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int)`)
	tk.MustExec(`create table t2 (a int, b int)`)

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec(`prepare st1 from 'select * from t1 where a<=?'`)
	tk.MustExec(`set @x1 = '123'`)
	tk.MustExec(`prepare st2 from 'select * from t1 where t1.a > (select 1 from t2 where t2.b<1)'`)
	tk.MustExec(`prepare st3 from 'select /*+ ignore_plan_cache() */ * from t1'`)
	tk.MustExec(`prepare st4 from 'select database() from t1';`)

	tk.MustExec(`execute st1 using @x1`)
	tk.MustExec(`execute st1 using @x1`)
	tk.MustExec(`execute st1 using @x1`)
	tk.MustExec(`execute st1 using @x1`)

	tk.MustExec(`execute st2`)
	tk.MustExec(`execute st2`)
	tk.MustExec(`execute st2`)

	tk.MustExec(`execute st3`)
	tk.MustExec(`execute st3`)

	tk.MustExec(`execute st4`)
	tk.MustExec(`execute st4`)

	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where plan_cache_unqualified > 0`).Sort().Check(testkit.Rows(
		"select * from `t1` 2 2 ignore plan cache by hint",
		"select * from `t1` where `a` <= ? 4 4 '123' may be converted to INT",
		"select * from `t1` where `t1` . `a` > ( select ? from `t2` where `t2` . `b` < ? ) 3 3 query has uncorrelated sub-queries is un-cacheable",
		"select database ( ) from `t1` 2 2 query has 'database' is un-cacheable"))

	for i := 0; i < 100; i++ {
		tk.MustExec(`execute st3`)
		tk.MustExec(`execute st4`)
	}
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where plan_cache_unqualified > 0`).Sort().Check(testkit.Rows(
		"select * from `t1` 102 102 ignore plan cache by hint",
		"select * from `t1` where `a` <= ? 4 4 '123' may be converted to INT",
		"select * from `t1` where `t1` . `a` > ( select ? from `t2` where `t2` . `b` < ? ) 3 3 query has uncorrelated sub-queries is un-cacheable",
		"select database ( ) from `t1` 102 102 query has 'database' is un-cacheable"))

	tk.MustExec(`set @x2=123`)
	for i := 0; i < 20; i++ {
		tk.MustExec(`execute st1 using @x1`)
		tk.MustExec(`execute st1 using @x2`)
	}
	tk.MustQuery(`select digest_text, exec_count, plan_cache_unqualified, last_plan_cache_unqualified_reason
    from information_schema.statements_summary where digest_text like '%<= ?%'`).Check(
		testkit.Rows("select * from `t1` where `a` <= ? 44 24 '123' may be converted to INT"))
}

func TestPerformanceSchemaforPlanCache(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)
	tmp := testkit.NewTestKit(t, store)
	tmp.MustExec("set tidb_enable_prepared_plan_cache=ON")
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

func newTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk
}

func newTestKitWithRoot(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := newTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func newTestKitWithPlanCache(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: plannercore.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.RefreshConnectionID()
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func setupStmtSummary() {
	stmtsummaryv2.Setup(&stmtsummaryv2.Config{
		Filename: "tidb-statements.log",
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = true
	})
}

func closeStmtSummary() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = false
	})
	stmtsummaryv2.GlobalStmtSummary.Close()
	stmtsummaryv2.GlobalStmtSummary = nil
	_ = os.Remove(config.GetGlobalConfig().Instance.StmtSummaryFilename)
}
