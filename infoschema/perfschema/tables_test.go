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

package perfschema_test

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testTableSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	s.store.Close()
}

func (s *testTableSuite) TestPredefinedTables(c *C) {
	c.Assert(perfschema.IsPredefinedTable("EVENTS_statements_summary_by_digest"), IsTrue)
	c.Assert(perfschema.IsPredefinedTable("statements"), IsFalse)
}

func (s *testTableSuite) TestPerfSchemaTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use performance_schema")
	tk.MustQuery("select * from global_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from session_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from setup_actors").Check(testkit.Rows())
	tk.MustQuery("select * from events_stages_history_long").Check(testkit.Rows())
}

// Test events_statements_summary_by_digest.
func (s *testTableSuite) TestStmtSummaryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

<<<<<<< HEAD
	// Statement summary is disabled by default.
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustQuery("select * from performance_schema.events_statements_summary_by_digest").Check(testkit.Rows())
=======
	// Clear all statements.
	tk.MustExec("set session tidb_enable_stmt_summary = 0")
	tk.MustExec("set session tidb_enable_stmt_summary = ''")
>>>>>>> 6905549... *: support more system variables in statement summary (#15508)

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	defer tk.MustExec("set global tidb_enable_stmt_summary = ''")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Invalidate the cache manually so that tidb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = testkit.NewTestKitWithInit(c, s.store)

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t    values(2, 'b')")
	tk.MustExec("insert into t VALUES(3, 'c')")
	tk.MustExec("/**/insert into t values(4, 'd')")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a') "))

	// Test point get.
	tk.MustExec("drop table if exists p")
	tk.MustExec("create table p(a int primary key, b int)")
	for i := 1; i < 3; i++ {
		tk.MustQuery("select b from p where a=1")
		expectedResult := fmt.Sprintf("%d \tPoint_Get_1\troot\t1\ttable:p, handle:1 %s", i, "test.p")
		// Also make sure that the plan digest is not empty
		tk.MustQuery(`select exec_count, plan, table_names
			from performance_schema.events_statements_summary_by_digest
			where digest_text like 'select b from p%' and plan_digest != ''`,
		).Check(testkit.Rows(expectedResult))
	}

	// Point get another database.
	tk.MustQuery("select variable_value from mysql.tidb where variable_name = 'system_tz'")
	tk.MustQuery(`select table_names
			from performance_schema.events_statements_summary_by_digest
			where digest_text like 'select variable_value%' and schema_name='test'`,
	).Check(testkit.Rows("mysql.tidb"))

	// Test `create database`.
	tk.MustExec("create database if not exists test")
	tk.MustQuery(`select table_names
			from performance_schema.events_statements_summary_by_digest
			where digest_text like 'create database%' and schema_name='test'`,
	).Check(testkit.Rows("<nil>"))

	// Test SELECT.
	const failpointName = "github.com/pingcap/tidb/planner/core/mockPlanRowCount"
	c.Assert(failpoint.Enable(failpointName, "return(100)"), IsNil)
	defer func() { c.Assert(failpoint.Disable(failpointName), IsNil) }()
	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tIndexLookUp_10\troot\t100\t\n" +
		"\t├─IndexScan_8 \tcop \t100\ttable:t, index:a, range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t100\ttable:t, keep order:false, stats:pseudo"))

	// select ... order by
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		order by exec_count desc limit 1`,
	).Check(testkit.Rows("insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a') "))

	// Test different plans with same digest.
	c.Assert(failpoint.Enable(failpointName, "return(1000)"), IsNil)
	tk.MustQuery("select * from t where a=3")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tIndexLookUp_10\troot\t100\t\n" +
		"\t├─IndexScan_8 \tcop \t100\ttable:t, index:a, range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t100\ttable:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustExec("set global tidb_enable_stmt_summary = false")
<<<<<<< HEAD
=======
	tk.MustExec("set session tidb_enable_stmt_summary = false")
	defer tk.MustExec("set global tidb_enable_stmt_summary = ''")
	defer tk.MustExec("set session tidb_enable_stmt_summary = ''")
>>>>>>> 6905549... *: support more system variables in statement summary (#15508)
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new session to test
	tk = testkit.NewTestKitWithInit(c, s.store)

	// This statement shouldn't be summarized.
	tk.MustQuery("select * from t where a=2")

	// The table should be cleared.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest`,
	).Check(testkit.Rows())

	// Enable it in session scope.
	tk.MustExec("set session tidb_enable_stmt_summary = on")
	// It should work immediately.
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("commit")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("insert test test.t <nil> 1 0 0 0 0 0 0 0 0 0 1 insert into t values(1, 'a')  "))
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text='commit'`,
	).Check(testkit.Rows("commit test <nil> <nil> 1 0 0 0 0 0 2 2 1 1 0 commit insert into t values(1, 'a') "))

	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tIndexLookUp_10\troot\t1000\t\n" +
		"\t├─IndexScan_8 \tcop \t1000\ttable:t, index:a, range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t1000\ttable:t, keep order:false, stats:pseudo"))

	// Disable it in global scope.
	tk.MustExec("set global tidb_enable_stmt_summary = off")

	// Create a new session to test.
	tk = testkit.NewTestKitWithInit(c, s.store)

	tk.MustQuery("select * from t where a=2")

	// Statement summary is still enabled.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tIndexLookUp_10\troot\t1000\t\n" +
		"\t├─IndexScan_8 \tcop \t1000\ttable:t, index:a, range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t1000\ttable:t, keep order:false, stats:pseudo"))

	// Unset session variable.
	tk.MustExec("set session tidb_enable_stmt_summary = ''")
	tk.MustQuery("select * from t where a=2")

	// Statement summary is disabled.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest`,
	).Check(testkit.Rows())
}

// Test events_statements_summary_by_digest_history.
func (s *testTableSuite) TestStmtSummaryHistoryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// Statement summary is disabled by default.
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustQuery("select * from performance_schema.events_statements_summary_by_digest_history").Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	defer tk.MustExec("set global tidb_enable_stmt_summary = ''")

	// Invalidate the cache manually so that tidb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = testkit.NewTestKitWithInit(c, s.store)

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t    values(2, 'b')")
	tk.MustExec("insert into t VALUES(3, 'c')")
	tk.MustExec("/**/insert into t values(4, 'd')")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest_history
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a') "))

	tk.MustExec("set global tidb_stmt_summary_history_size = 0")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, cop_task_num, avg_total_keys, 
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, 
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan 
		from performance_schema.events_statements_summary_by_digest_history`,
	).Check(testkit.Rows())
}
