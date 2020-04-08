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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/terror"
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

	tk.MustQuery("select column_comment from information_schema.columns " +
		"where table_name='events_statements_summary_by_digest' and column_name='STMT_TYPE'",
	).Check(testkit.Rows("Statement type"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// Clear all statements.
	tk.MustExec("set session tidb_enable_stmt_summary = 0")
	tk.MustExec("set session tidb_enable_stmt_summary = ''")

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
		"\t├─IndexScan_8 \tcop \t100\ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
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
		"\t├─IndexScan_8 \tcop \t100\ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t100\ttable:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustExec("set global tidb_enable_stmt_summary = false")
	tk.MustExec("set session tidb_enable_stmt_summary = false")
	defer tk.MustExec("set global tidb_enable_stmt_summary = ''")
	defer tk.MustExec("set session tidb_enable_stmt_summary = ''")
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
		"\t├─IndexScan_8 \tcop \t1000\ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableScan_9 \tcop \t1000\ttable:t, keep order:false, stats:pseudo"))

	// Disable it in global scope.
	tk.MustExec("set global tidb_enable_stmt_summary = false")

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
		"\t├─IndexScan_8 \tcop \t1000\ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
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

	// Create a new session to test
	tk = testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set global tidb_enable_stmt_summary = on")
	tk.MustExec("set global tidb_stmt_summary_history_size = 24")

	// Create a new user to test statements summary table privilege
	tk.MustExec("create user 'test_user'@'localhost'")
	tk.MustExec("grant select on *.* to 'test_user'@'localhost'")
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
	tk.MustExec("select * from t where a=1")
	result := tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	)
	// Super user can query all reocrds
	c.Assert(len(result.Rows()), Equals, 1)
	result = tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "test_user",
		Hostname:     "localhost",
		AuthUsername: "test_user",
		AuthHostname: "localhost",
	}, nil, nil)
	result = tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	)
	// Ordinary users can not see others' records
	c.Assert(len(result.Rows()), Equals, 0)
	result = tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("select * from t where a=1")
	result = tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	tk.MustExec("select * from t where a=1")
	result = tk.MustQuery(`select *
		from performance_schema.events_statements_summary_by_digest_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	// use root user to set variables back
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
	tk.MustExec("set global tidb_enable_stmt_summary = off")
}

// Test events_statements_summary_by_digest_history.
func (s *testTableSuite) TestStmtSummaryHistoryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

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

// Test events_statements_summary_by_digest_history.
func (s *testTableSuite) TestStmtSummaryInternalQuery(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// We use the sql binding evolve to check the internal query summary.
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines = 1")
	tk.MustExec("create global binding for select * from t where t.a = 1 using select * from t ignore index(k) where t.a = 1")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	defer tk.MustExec("set global tidb_enable_stmt_summary = ''")
	// Invalidate the cache manually so that tidb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Test Internal

	// Create a new session to test.
	tk = testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("select * from t where t.a = 1")
	tk.MustQuery(`select exec_count, digest_text
		from performance_schema.events_statements_summary_by_digest
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows())

	// Enable internal query and evolve baseline.
	tk.MustExec("set global tidb_stmt_summary_internal_query = 1")
	defer tk.MustExec("set global tidb_stmt_summary_internal_query = false")

	// Create a new session to test.
	tk = testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("admin flush bindings")
	tk.MustExec("admin evolve bindings")

	tk.MustQuery(`select exec_count, digest_text
		from performance_schema.events_statements_summary_by_digest
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows(
		"1 select original_sql , bind_sql , default_db , status , create_time , update_time , charset , collation from mysql . bind_info" +
			" where update_time > ? order by update_time"))
}

func currentSourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func (s *testTableSuite) TestTiKVProfileCPU(c *C) {
	router := http.NewServeMux()
	mockServer := httptest.NewServer(router)
	mockAddr := strings.TrimPrefix(mockServer.URL, "http://")
	defer mockServer.Close()

	copyHandler := func(filename string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			file, err := os.Open(filepath.Join(currentSourceDir(), filename))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer func() { terror.Log(file.Close()) }()
			_, err = io.Copy(w, file)
			terror.Log(err)
		}
	}
	// mock tikv profile
	router.HandleFunc("/debug/pprof/profile", copyHandler("testdata/tikv.cpu.profile"))

	// failpoint setting
	servers := []string{
		strings.Join([]string{"tikv", mockAddr, mockAddr}, ","),
		strings.Join([]string{"pd", mockAddr, mockAddr}, ","),
	}
	fpExpr := strings.Join(servers, ";")
	fpName := "github.com/pingcap/tidb/infoschema/perfschema/mockRemoteNodeStatusAddress"
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use performance_schema")
	result := tk.MustQuery("select function, percent_abs, percent_rel from tikv_profile_cpu where depth < 3")

	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	result.Check(testkit.Rows(
		"root 100% 100%",
		"├─tikv::server::load_statistics::linux::ThreadLoadStatistics::record::h59facb8d680e7794 75.00% 75.00%",
		"│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 75.00% 100%",
		"├─nom::nom::digit::h905aaaeff7d8ec8e 16.07% 16.07%",
		"│ ├─<core::iter::adapters::Enumerate<I> as core::iter::traits::iterator::Iterator>::next::h16936f9061bb75e4 6.25% 38.89%",
		"│ ├─Unknown 3.57% 22.22%",
		"│ ├─<&u8 as nom::traits::AsChar>::is_dec_digit::he9eacc3fad26ab81 2.68% 16.67%",
		"│ ├─<&[u8] as nom::traits::InputIter>::iter_indices::h6192338433683bff 1.79% 11.11%",
		"│ └─<&[T] as nom::traits::Slice<core::ops::range::RangeFrom<usize>>>::slice::h38d31f11f84aa302 1.79% 11.11%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::realloc::h5199c50710ab6f9d 1.79% 1.79%",
		"│ └─rallocx 1.79% 100%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::dealloc::hea83459aa98dd2dc 1.79% 1.79%",
		"│ └─sdallocx 1.79% 100%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::alloc::hc7962e02169a5c56 0.89% 0.89%",
		"│ └─mallocx 0.89% 100%",
		"├─engine::rocks::util::engine_metrics::flush_engine_iostall_properties::h64a7661c95aa1db7 0.89% 0.89%",
		"│ └─rocksdb::rocksdb::DB::get_map_property_cf::h9722f9040411af44 0.89% 100%",
		"├─core::ptr::real_drop_in_place::h8def0d99e7136f33 0.89% 0.89%",
		"│ └─<alloc::raw_vec::RawVec<T,A> as core::ops::drop::Drop>::drop::h9b59b303bffde02c 0.89% 100%",
		"├─tikv_util::metrics::threads_linux::ThreadInfoStatistics::record::ha8cc290b3f46af88 0.89% 0.89%",
		"│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 0.89% 100%",
		"├─crossbeam_utils::backoff::Backoff::snooze::h5c121ef4ce616a3c 0.89% 0.89%",
		"│ └─core::iter::range::<impl core::iter::traits::iterator::Iterator for core::ops::range::Range<A>>::next::hdb23ceb766e7a91f 0.89% 100%",
		"└─<hashbrown::raw::bitmask::BitMaskIter as core::iter::traits::iterator::Iterator>::next::he129c78b3deb639d 0.89% 0.89%",
		"  └─Unknown 0.89% 100%"))

	// We can use current processe profile to mock profile of PD because the PD has the
	// same way of retrieving profile with TiDB. And the purpose of this test case is used
	// to make sure all profile HTTP API have been accessed.
	accessed := map[string]struct{}{}
	handlerFactory := func(name string, debug ...int) func(w http.ResponseWriter, _ *http.Request) {
		debugLevel := 0
		if len(debug) > 0 {
			debugLevel = debug[0]
		}
		return func(w http.ResponseWriter, _ *http.Request) {
			profile := pprof.Lookup(name)
			if profile == nil {
				http.Error(w, fmt.Sprintf("profile %s not found", name), http.StatusBadRequest)
				return
			}
			if err := profile.WriteTo(w, debugLevel); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			accessed[name] = struct{}{}
		}
	}

	// mock PD profile
	router.HandleFunc("/pd/api/v1/debug/pprof/profile", copyHandler("../../util/profile/testdata/test.pprof"))
	router.HandleFunc("/pd/api/v1/debug/pprof/heap", handlerFactory("heap"))
	router.HandleFunc("/pd/api/v1/debug/pprof/mutex", handlerFactory("mutex"))
	router.HandleFunc("/pd/api/v1/debug/pprof/allocs", handlerFactory("allocs"))
	router.HandleFunc("/pd/api/v1/debug/pprof/block", handlerFactory("block"))
	router.HandleFunc("/pd/api/v1/debug/pprof/goroutine", handlerFactory("goroutine", 2))

	tk.MustQuery("select * from pd_profile_cpu where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_memory where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_mutex where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_allocs where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_block where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_goroutines")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	c.Assert(len(accessed), Equals, 5, Commentf("expect all HTTP API had been accessed, but found: %v", accessed))
}
