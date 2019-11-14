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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
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

func (s *testTableSuite) TestPerfSchemaTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use performance_schema")
	tk.MustQuery("select * from global_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from session_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from setup_actors").Check(testkit.Rows())
	tk.MustQuery("select * from events_stages_history_long").Check(testkit.Rows())
}

// Test events_statements_summary_by_digest
func (s *testTableSuite) TestStmtSummaryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")

	// Statement summary is disabled by default
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustQuery("select * from performance_schema.events_statements_summary_by_digest").Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Invalidate the cache manually so that tidb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()

	// Create a new session to test
	tk = testkit.NewTestKitWithInit(c, s.store)

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t    values(2, 'b')")
	tk.MustExec("insert into t VALUES(3, 'c')")
	tk.MustExec("/**/insert into t values(4, 'd')")
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("test 4 0 0 0 0 0 1 1 1 1 1 insert into t values(1, 'a')"))

	// Test SELECT
	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("test 1 1 0 0 0 0 0 0 0 0 0 select * from t where a=2"))

	// select ... order by
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		order by exec_count desc limit 1`,
	).Check(testkit.Rows("test 4 0 0 0 0 0 1 1 1 1 1 insert into t values(1, 'a')"))

	// Disable it again
	tk.MustExec("set global tidb_enable_stmt_summary = false")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new session to test
	tk = testkit.NewTestKitWithInit(c, s.store)

	// This statement shouldn't be summarized
	tk.MustQuery("select * from t where a=2")

	// The table should be cleared
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest`,
	).Check(testkit.Rows())

	// Enable it in session scope
	tk.MustExec("set session tidb_enable_stmt_summary = on")
	// It should work immediately
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("commit")
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("test 1 0 0 0 0 0 0 0 0 0 1 insert into t values(1, 'a')"))
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text='commit'`,
	).Check(testkit.Rows("test 1 0 0 0 0 0 1 1 1 1 0 commit"))

	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("test 1 1 0 0 0 0 0 0 0 0 0 select * from t where a=2"))

	// Disable it in global scope
	tk.MustExec("set global tidb_enable_stmt_summary = off")

	// Create a new session to test
	tk = testkit.NewTestKitWithInit(c, s.store)

	tk.MustQuery("select * from t where a=2")

	// Statement summary is still enabled
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("test 2 2 0 0 0 0 0 0 0 0 0 select * from t where a=2"))

	// Unset session variable
	tk.MustExec("set session tidb_enable_stmt_summary = ''")
	tk.MustQuery("select * from t where a=2")

	// Statement summary is disabled
	tk.MustQuery(`select schema_name, exec_count, cop_task_num, avg_total_keys, max_total_keys, avg_processed_keys, 
		max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, max_prewrite_regions, avg_affected_rows, 
		query_sample_text from performance_schema.events_statements_summary_by_digest`,
	).Check(testkit.Rows())
}
