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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	"golang.org/x/net/context"
)

func (s *testSuite) TestSetVar(c *C) {
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

	errTestSql := "SET @@date_format = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@rewriter_enabled = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET xxx = abcd;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.a = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.timestamp = 1;"
	_, err = tk.Exec(errTestSql)
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

	// Test set transaction isolation level, which is equivalent to setting variable "tx_isolation".
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	// test synonyms variables
	tk.MustExec("SET SESSION tx_isolation = 'READ-COMMITTED'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("SET SESSION tx_isolation = 'READ-UNCOMMITTED'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))

	tk.MustExec("SET SESSION transaction_isolation = 'SERIALIZABLE'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	tk.MustExec("SET GLOBAL transaction_isolation = 'SERIALIZABLE'")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	tk.MustExec("SET GLOBAL transaction_isolation = 'READ-UNCOMMITTED'")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))

	tk.MustExec("SET GLOBAL tx_isolation = 'SERIALIZABLE'")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

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

	tk.MustExec("set @@tidb_general_log = 1")
	tk.MustExec("set @@tidb_general_log = 0")
}

func (s *testSuite) TestSetCharset(c *C) {
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

func (s *testSuite) TestValidateSetVar(c *C) {
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
	tk.MustExec("set @@tidb_batch_delete='oFf';")
	tk.MustExec("set @@tidb_batch_delete=1;")
	tk.MustExec("set @@tidb_batch_delete=0;")

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

	tk.MustExec("set @@global.flush_time=31536001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect flush_time value: '31536001'"))

	tk.MustExec("set @@global.interactive_timeout=31536001")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect interactive_timeout value: '31536001'"))
}
