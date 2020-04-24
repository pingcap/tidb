// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite2) TestSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testSQL := "SET @a = 1;"
	tk.MustExec(testSQL)

	testSQL = `SET @a = "1";`
	tk.MustExec(testSQL)

	testSQL = "SET @a = null;"
	tk.MustExec(testSQL)

	testSQL = "SET @@global.autocommit = 1;"
	tk.MustExec(testSQL)

	// TODO: this test case should returns error.
	// testSQL = "SET @@global.autocommit = null;"
	// _, err := tk.Exec(testSQL)
	// c.Assert(err, NotNil)

	testSQL = "SET @@autocommit = 1;"
	tk.MustExec(testSQL)

	testSQL = "SET @@autocommit = null;"
	_, err := tk.Exec(testSQL)
	c.Assert(err, NotNil)

	errTestSQL := "SET @@date_format = 1;"
	_, err = tk.Exec(errTestSQL)
	c.Assert(err, NotNil)

	errTestSQL = "SET @@rewriter_enabled = 1;"
	_, err = tk.Exec(errTestSQL)
	c.Assert(err, NotNil)

	errTestSQL = "SET xxx = abcd;"
	_, err = tk.Exec(errTestSQL)
	c.Assert(err, NotNil)

	errTestSQL = "SET @@global.a = 1;"
	_, err = tk.Exec(errTestSQL)
	c.Assert(err, NotNil)

	errTestSQL = "SET @@global.timestamp = 1;"
	_, err = tk.Exec(errTestSQL)
	c.Assert(err, NotNil)

	// For issue 998
	testSQL = "SET @issue998a=1, @issue998b=5;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998a, @issue998b;`).Check(testkit.Rows("1 5"))
	testSQL = "SET @@autocommit=0, @issue998a=2;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998a, @@autocommit;`).Check(testkit.Rows("2 0"))
	testSQL = "SET @@global.autocommit=1, @issue998b=6;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998b, @@global.autocommit;`).Check(testkit.Rows("6 1"))

	// For issue 4302
	testSQL = "use test;drop table if exists x;create table x(a int);insert into x value(1);"
	tk.MustExec(testSQL)
	testSQL = "SET @issue4302=(select a from x limit 1);"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue4302;`).Check(testkit.Rows("1"))

	// Set default
	// {ScopeGlobal | ScopeSession, "low_priority_updates", "OFF"},
	// For global var
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("1"))
	tk.MustExec(`set @@global.low_priority_updates=DEFAULT;`) // It will be set to compiled-in default value.
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("0"))
	// For session
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustExec(`set @@session.low_priority_updates=DEFAULT;`) // It will be set to global var value.
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("1"))

	// For mysql jdbc driver issue.
	tk.MustQuery(`select @@session.tx_read_only;`).Check(testkit.Rows("0"))

	// Test session variable states.
	vars := tk.Se.(sessionctx.Context).GetSessionVars()
	tk.Se.CommitTxn(context.TODO())
	tk.MustExec("set @@autocommit = 1")
	c.Assert(vars.InTxn(), IsFalse)
	c.Assert(vars.IsAutocommit(), IsTrue)
	tk.MustExec("set @@autocommit = 0")
	c.Assert(vars.IsAutocommit(), IsFalse)

	tk.MustExec("set @@sql_mode = 'strict_trans_tables'")
	c.Assert(vars.StrictSQLMode, IsTrue)
	tk.MustExec("set @@sql_mode = ''")
	c.Assert(vars.StrictSQLMode, IsFalse)

	tk.MustExec("set names utf8")
	charset, collation := vars.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_bin")

	tk.MustExec("set names latin1 collate latin1_swedish_ci")
	charset, collation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "latin1")
	c.Assert(collation, Equals, "latin1_swedish_ci")

	tk.MustExec("set names utf8 collate default")
	charset, collation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_bin")

	tk.MustExec("set character_set_results = NULL")
	tk.MustQuery("select @@character_set_results").Check(testkit.Rows(""))

	tk.MustExec("set @@session.ddl_slow_threshold=12345")
	tk.MustQuery("select @@session.ddl_slow_threshold").Check(testkit.Rows("12345"))
	c.Assert(variable.DDLSlowOprThreshold, Equals, uint32(12345))
	tk.MustExec("set session ddl_slow_threshold=\"54321\"")
	tk.MustQuery("show variables like 'ddl_slow_threshold'").Check(testkit.Rows("ddl_slow_threshold 54321"))
	c.Assert(variable.DDLSlowOprThreshold, Equals, uint32(54321))

	// Test set transaction isolation level, which is equivalent to setting variable "tx_isolation".
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	// error
	_, err = tk.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	// Fails
	_, err = tk.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// test synonyms variables
	tk.MustExec("SET SESSION tx_isolation = 'READ-COMMITTED'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	_, err = tk.Exec("SET SESSION tx_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET SESSION transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL tx_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	tk.MustExec("SET SESSION tx_read_only = 1")
	tk.MustExec("SET SESSION tx_read_only = 0")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("0"))

	tk.MustExec("SET GLOBAL tx_read_only = 1")
	tk.MustExec("SET GLOBAL tx_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("0"))

	tk.MustExec("SET SESSION transaction_read_only = 1")
	tk.MustExec("SET SESSION transaction_read_only = 0")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("0"))

	tk.MustExec("SET SESSION transaction_read_only = 1")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("1"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("1"))

	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	tk.MustExec("SET GLOBAL transaction_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("0"))

	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("1"))

	// Even the transaction fail, set session variable would success.
	tk.MustExec("BEGIN")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	_, err = tk.Exec(`INSERT INTO t VALUES ("sdfsdf")`)
	c.Assert(err, NotNil)
	tk.MustExec("COMMIT")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set global avoid_temporal_upgrade = on")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Rows("1"))
	tk.MustExec("set @@global.avoid_temporal_upgrade = off")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Rows("0"))
	tk.MustExec("set session sql_log_bin = on")
	tk.MustQuery(`select @@session.sql_log_bin;`).Check(testkit.Rows("1"))
	tk.MustExec("set sql_log_bin = off")
	tk.MustQuery(`select @@session.sql_log_bin;`).Check(testkit.Rows("0"))
	tk.MustExec("set @@sql_log_bin = on")
	tk.MustQuery(`select @@session.sql_log_bin;`).Check(testkit.Rows("1"))

	tk.MustQuery(`select @@global.log_bin;`).Check(testkit.Rows(variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)))
	tk.MustQuery(`select @@log_bin;`).Check(testkit.Rows(variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)))

	tk.MustExec("set @@tidb_general_log = 1")
	tk.MustExec("set @@tidb_general_log = 0")

	tk.MustExec("set @@tidb_pprof_sql_cpu = 1")
	tk.MustExec("set @@tidb_pprof_sql_cpu = 0")

	tk.MustExec(`set tidb_force_priority = "no_priority"`)
	tk.MustQuery(`select @@tidb_force_priority;`).Check(testkit.Rows("NO_PRIORITY"))
	tk.MustExec(`set tidb_force_priority = "low_priority"`)
	tk.MustQuery(`select @@tidb_force_priority;`).Check(testkit.Rows("LOW_PRIORITY"))
	tk.MustExec(`set tidb_force_priority = "high_priority"`)
	tk.MustQuery(`select @@tidb_force_priority;`).Check(testkit.Rows("HIGH_PRIORITY"))
	tk.MustExec(`set tidb_force_priority = "delayed"`)
	tk.MustQuery(`select @@tidb_force_priority;`).Check(testkit.Rows("DELAYED"))
	tk.MustExec(`set tidb_force_priority = "abc"`)
	tk.MustQuery(`select @@tidb_force_priority;`).Check(testkit.Rows("NO_PRIORITY"))
	_, err = tk.Exec(`set global tidb_force_priority = ""`)
	c.Assert(err, NotNil)

	tk.MustExec("set tidb_constraint_check_in_place = 1")
	tk.MustQuery(`select @@session.tidb_constraint_check_in_place;`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_constraint_check_in_place = 0")
	tk.MustQuery(`select @@global.tidb_constraint_check_in_place;`).Check(testkit.Rows("0"))

	tk.MustExec("set tidb_slow_log_threshold = 0")
	tk.MustQuery("select @@session.tidb_slow_log_threshold;").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_slow_log_threshold = 1")
	tk.MustQuery("select @@session.tidb_slow_log_threshold;").Check(testkit.Rows("1"))
	_, err = tk.Exec("set global tidb_slow_log_threshold = 0")
	c.Assert(err, NotNil)

	tk.MustExec("set tidb_query_log_max_len = 0")
	tk.MustQuery("select @@session.tidb_query_log_max_len;").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_query_log_max_len = 20")
	tk.MustQuery("select @@session.tidb_query_log_max_len;").Check(testkit.Rows("20"))
	_, err = tk.Exec("set global tidb_query_log_max_len = 20")
	c.Assert(err, NotNil)

	tk.MustExec("set tidb_batch_commit = 0")
	tk.MustQuery("select @@session.tidb_batch_commit;").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_batch_commit = 1")
	tk.MustQuery("select @@session.tidb_batch_commit;").Check(testkit.Rows("1"))
	_, err = tk.Exec("set global tidb_batch_commit = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set global tidb_batch_commit = 2")
	c.Assert(err, NotNil)

	// test skip isolation level check: init
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// test skip isolation level check: error
	_, err = tk.Exec("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	_, err = tk.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The isolation level 'READ-UNCOMMITTED' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))

	// test skip isolation level check: reset
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")

	tk.MustExec("set global read_only = 0")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("0"))
	tk.MustExec("set global read_only = off")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("0"))
	tk.MustExec("set global read_only = 1")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("1"))
	tk.MustExec("set global read_only = on")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("1"))
	_, err = tk.Exec("set global read_only = abc")
	c.Assert(err, NotNil)

	// test for tidb_wait_split_region_finish
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_wait_split_region_finish = 1")
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_wait_split_region_finish = 0")
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("0"))

	// test for tidb_scatter_region
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_scatter_region = 1")
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_scatter_region = 0")
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows("0"))
	_, err = tk.Exec("set session tidb_scatter_region = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec(`select @@session.tidb_scatter_region;`)
	c.Assert(err, NotNil)

	// test for tidb_wait_split_region_timeout
	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows(strconv.Itoa(variable.DefWaitSplitRegionTimeout)))
	tk.MustExec("set tidb_wait_split_region_timeout = 1")
	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows("1"))
	_, err = tk.Exec("set tidb_wait_split_region_timeout = 0")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "tidb_wait_split_region_timeout(0) cannot be smaller than 1")
	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows("1"))

	tk.MustExec("set session tidb_backoff_weight = 3")
	tk.MustQuery("select @@session.tidb_backoff_weight;").Check(testkit.Rows("3"))
	tk.MustExec("set session tidb_backoff_weight = 20")
	tk.MustQuery("select @@session.tidb_backoff_weight;").Check(testkit.Rows("20"))
	_, err = tk.Exec("set session tidb_backoff_weight = -1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set global tidb_backoff_weight = 0")
	c.Assert(err, NotNil)
	tk.MustExec("set global tidb_backoff_weight = 10")
	tk.MustQuery("select @@global.tidb_backoff_weight;").Check(testkit.Rows("10"))

	tk.MustExec("set @@tidb_expensive_query_time_threshold=70")
	tk.MustQuery("select @@tidb_expensive_query_time_threshold;").Check(testkit.Rows("70"))

	tk.MustExec("set @@tidb_record_plan_in_slow_log = 1")
	tk.MustQuery("select @@tidb_record_plan_in_slow_log;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_record_plan_in_slow_log = 0")
	tk.MustQuery("select @@tidb_record_plan_in_slow_log;").Check(testkit.Rows("0"))

	tk.MustQuery("select @@tidb_store_limit;").Check(testkit.Rows("0"))
	tk.MustExec("set @@tidb_store_limit = 100")
	tk.MustQuery("select @@tidb_store_limit;").Check(testkit.Rows("100"))
	tk.MustQuery("select @@session.tidb_store_limit;").Check(testkit.Rows("100"))
	tk.MustQuery("select @@global.tidb_store_limit;").Check(testkit.Rows("0"))
	tk.MustExec("set @@tidb_store_limit = 0")

	tk.MustExec("set global tidb_store_limit = 100")
	tk.MustQuery("select @@tidb_store_limit;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.tidb_store_limit;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.tidb_store_limit;").Check(testkit.Rows("100"))
}

func (s *testSuite2) TestSetCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`SET NAMES latin1`)

	ctx := tk.Se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	for _, v := range variable.SetNamesVariables {
		sVar, err := variable.GetSessionSystemVar(sessionVars, v)
		c.Assert(err, IsNil)
		c.Assert(sVar != "utf8", IsTrue)
	}
	tk.MustExec(`SET NAMES utf8`)
	for _, v := range variable.SetNamesVariables {
		sVar, err := variable.GetSessionSystemVar(sessionVars, v)
		c.Assert(err, IsNil)
		c.Assert(sVar, Equals, "utf8")
	}
	sVar, err := variable.GetSessionSystemVar(sessionVars, variable.CollationConnection)
	c.Assert(err, IsNil)
	c.Assert(sVar, Equals, "utf8_bin")

	// Issue 1523
	tk.MustExec(`SET NAMES binary`)
}

func (s *testSuite2) TestValidateSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	_, err := tk.Exec("set global tidb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tidb_distsql_scan_concurrency=-1;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_distsql_scan_concurrency=-1;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_batch_delete='ok';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@tidb_batch_delete='On';")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_batch_delete='oFf';")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("0"))
	tk.MustExec("set @@tidb_batch_delete=1;")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_batch_delete=0;")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_opt_agg_push_down=off;")
	tk.MustQuery("select @@tidb_opt_agg_push_down;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_constraint_check_in_place=on;")
	tk.MustQuery("select @@tidb_constraint_check_in_place;").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_general_log=0;")
	tk.MustQuery("select @@tidb_general_log;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_enable_streaming=1;")
	tk.MustQuery("select @@tidb_enable_streaming;").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_pprof_sql_cpu=1;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_pprof_sql_cpu=0;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("0"))

	_, err = tk.Exec("set @@tidb_batch_delete=3;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_mem_quota_mergejoin='tidb';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result := tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	_, err = tk.Exec("set @@group_concat_max_len = 18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	// Test illegal type
	_, err = tk.Exec("set @@group_concat_max_len='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@default_week_format=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '-1'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@default_week_format=9")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '9'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Rows("7"))

	_, err = tk.Exec("set @@error_count = 0")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@warning_count = 0")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))

	tk.MustExec("set time_zone='SySTeM'")
	result = tk.MustQuery("select @@time_zone;")
	result.Check(testkit.Rows("SYSTEM"))

	// The following cases test value out of range and illegal type when setting system variables.
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html for more details.
	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("100000"))

	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@global.max_connections='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.thread_pool_size=65")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '65'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Rows("64"))

	tk.MustExec("set @@global.thread_pool_size=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '-1'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@global.thread_pool_size='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_allowed_packet=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '-1'"))
	result = tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Rows("1024"))

	_, err = tk.Exec("set @@global.max_allowed_packet='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_connect_errors=18446744073709551615")

	tk.MustExec("set @@global.max_connect_errors=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connect_errors value: '-1'"))
	result = tk.MustQuery("select @@global.max_connect_errors;")
	result.Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@global.max_connect_errors=18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("100000"))

	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@global.max_connections='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@max_sort_length=1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '1'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("4"))

	tk.MustExec("set @@max_sort_length=-100")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '-100'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("4"))

	tk.MustExec("set @@max_sort_length=8388609")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '8388609'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("8388608"))

	_, err = tk.Exec("set @@max_sort_length='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.table_definition_cache=399")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '399'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("400"))

	tk.MustExec("set @@global.table_definition_cache=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '-1'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("400"))

	tk.MustExec("set @@global.table_definition_cache=524289")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '524289'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("524288"))

	_, err = tk.Exec("set @@global.table_definition_cache='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@old_passwords=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '-1'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@old_passwords=3")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '3'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Rows("2"))

	_, err = tk.Exec("set @@old_passwords='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@tmp_table_size=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tmp_table_size value: '-1'"))
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("1024"))

	tk.MustExec("set @@tmp_table_size=1020")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tmp_table_size value: '1020'"))
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("1024"))

	tk.MustExec("set @@tmp_table_size=167772161")
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("167772161"))

	tk.MustExec("set @@tmp_table_size=18446744073709551615")
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("18446744073709551615"))

	_, err = tk.Exec("set @@tmp_table_size=18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	_, err = tk.Exec("set @@tmp_table_size='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.connect_timeout=1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '1'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("2"))

	tk.MustExec("set @@global.connect_timeout=31536000")
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("31536000"))

	tk.MustExec("set @@global.connect_timeout=31536001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '31536001'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("31536000"))

	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Rows("18446744073709551615"))
	tk.MustExec("set @@sql_select_limit=default")
	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Rows("18446744073709551615"))

	tk.MustExec("set @@sql_auto_is_null=00")
	result = tk.MustQuery("select @@sql_auto_is_null;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@sql_warnings=001")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@sql_warnings=000")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@global.super_read_only=-0")
	result = tk.MustQuery("select @@global.super_read_only;")
	result.Check(testkit.Rows("0"))

	_, err = tk.Exec("set @@global.super_read_only=-1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.innodb_status_output_locks=-1")
	result = tk.MustQuery("select @@global.innodb_status_output_locks;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@global.innodb_ft_enable_stopword=0000000")
	result = tk.MustQuery("select @@global.innodb_ft_enable_stopword;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@global.innodb_stats_on_metadata=1")
	result = tk.MustQuery("select @@global.innodb_stats_on_metadata;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@global.innodb_file_per_table=-50")
	result = tk.MustQuery("select @@global.innodb_file_per_table;")
	result.Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@global.innodb_ft_enable_stopword=2")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@query_cache_type=0")
	result = tk.MustQuery("select @@query_cache_type;")
	result.Check(testkit.Rows("OFF"))

	tk.MustExec("set @@query_cache_type=2")
	result = tk.MustQuery("select @@query_cache_type;")
	result.Check(testkit.Rows("DEMAND"))

	tk.MustExec("set @@global.sync_binlog=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '-1'"))

	tk.MustExec("set @@global.sync_binlog=4294967299")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '4294967299'"))

	tk.MustExec("set @@global.flush_time=31536001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect flush_time value: '31536001'"))

	tk.MustExec("set @@global.interactive_timeout=31536001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect interactive_timeout value: '31536001'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = -1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '-1'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = 1001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '1001'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = -1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '-1'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = 3")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '3'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@global.validate_password_number_count=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect validate_password_number_count value: '-1'"))

	tk.MustExec("set @@global.validate_password_length=-1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect validate_password_length value: '-1'"))

	tk.MustExec("set @@global.validate_password_length=8")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	_, err = tk.Exec("set @@tx_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tx_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@transaction_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global transaction_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tx_isolation='REPEATABLE-READ1'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@tx_isolation='READ-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='read-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='REPEATABLE-READ'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("REPEATABLE-READ"))

	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")
	_, err = tk.Exec("set @@tx_isolation='SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedValueForVar), IsTrue, Commentf("err %v", err))
}

func (s *testSuite2) TestSelectGlobalVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Rows("151"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Rows("151"))

	tk.MustExec("set @@global.max_connections=100;")

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Rows("100"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Rows("100"))

	tk.MustExec("set @@global.max_connections=151;")

	// test for unknown variable.
	err := tk.ExecToErr("select @@invalid")
	c.Assert(terror.ErrorEqual(err, variable.UnknownSystemVar), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select @@global.invalid")
	c.Assert(terror.ErrorEqual(err, variable.UnknownSystemVar), IsTrue, Commentf("err %v", err))
}
