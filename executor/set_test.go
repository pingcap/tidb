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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite5) TestSetVar(c *C) {
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

	coll := "latin1_swedish_ci"
	if collate.NewCollationEnabled() {
		coll = "latin1_bin"
	}
	tk.MustExec("set names latin1 collate " + coll)
	charset, collation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "latin1")
	c.Assert(collation, Equals, coll)

	tk.MustExec("set names utf8 collate default")
	charset, collation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_bin")

	expectErrMsg := "[ddl:1273]Unknown collation: 'non_exist_collation'"
	tk.MustGetErrMsg("set names utf8 collate non_exist_collation", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_server='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_database='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_connection='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_server='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_database='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_connection='non_exist_collation'", expectErrMsg)

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
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	// Fails
	_, err = tk.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// test synonyms variables
	tk.MustExec("SET SESSION tx_isolation = 'READ-COMMITTED'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	_, err = tk.Exec("SET SESSION tx_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET SESSION transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL tx_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
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
		"Warning 8048 The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 8048 The isolation level 'READ-UNCOMMITTED' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))

	// test skip isolation level check: reset
	tk.MustExec("SET GLOBAL transaction_isolation='REPEATABLE-READ'") // should reset tx_isolation back to rr before reset tidb_skip_isolation_level_check
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

	tk.MustQuery("select @@tidb_enable_change_column_type;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_change_column_type = 1")
	tk.MustQuery("select @@tidb_enable_change_column_type;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_change_column_type = off")
	tk.MustQuery("select @@tidb_enable_change_column_type;").Check(testkit.Rows("0"))

	tk.MustQuery("select @@session.tidb_metric_query_step;").Check(testkit.Rows("60"))
	tk.MustExec("set @@session.tidb_metric_query_step = 120")
	_, err = tk.Exec("set @@session.tidb_metric_query_step = 9")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "tidb_metric_query_step(9) cannot be smaller than 10 or larger than 216000")
	tk.MustQuery("select @@session.tidb_metric_query_step;").Check(testkit.Rows("120"))

	tk.MustQuery("select @@session.tidb_metric_query_range_duration;").Check(testkit.Rows("60"))
	tk.MustExec("set @@session.tidb_metric_query_range_duration = 120")
	_, err = tk.Exec("set @@session.tidb_metric_query_range_duration = 9")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "tidb_metric_query_range_duration(9) cannot be smaller than 10 or larger than 216000")
	tk.MustQuery("select @@session.tidb_metric_query_range_duration;").Check(testkit.Rows("120"))

	// test for tidb_slow_log_masking
	tk.MustQuery(`select @@global.tidb_slow_log_masking;`).Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_slow_log_masking = 1")
	tk.MustQuery(`select @@global.tidb_slow_log_masking;`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_slow_log_masking = 0")
	tk.MustQuery(`select @@global.tidb_slow_log_masking;`).Check(testkit.Rows("0"))
	_, err = tk.Exec("set session tidb_slow_log_masking = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec(`select @@session.tidb_slow_log_masking;`)
	c.Assert(err, NotNil)

	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("0"))
	tk.MustExec("set @@session.tidb_dml_batch_size = 120")
	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("120"))
	c.Assert(tk.ExecToErr("set @@session.tidb_dml_batch_size = -120"), NotNil)
	c.Assert(tk.ExecToErr("set @@global.tidb_dml_batch_size = 120"), NotNil)
	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("120"))

	_, err = tk.Exec("set tidb_enable_parallel_apply=-1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue)
}

func (s *testSuite5) TestTruncateIncorrectIntSessionVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	testCases := []struct {
		sessionVarName string
		minValue       int
		maxValue       int
	}{
		{"auto_increment_increment", 1, 65535},
		{"auto_increment_offset", 1, 65535},
	}

	for _, tc := range testCases {
		name := tc.sessionVarName
		selectSQL := fmt.Sprintf("select @@%s;", name)
		validValue := tc.minValue + (tc.maxValue-tc.minValue)/2
		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, validValue))
		tk.MustQuery(selectSQL).Check(testkit.Rows(fmt.Sprintf("%d", validValue)))

		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, tc.minValue-1))
		warnMsg := fmt.Sprintf("Warning 1292 Truncated incorrect %s value: '%d'", name, tc.minValue-1)
		tk.MustQuery("show warnings").Check(testkit.Rows(warnMsg))
		tk.MustQuery(selectSQL).Check(testkit.Rows(fmt.Sprintf("%d", tc.minValue)))

		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, tc.maxValue+1))
		warnMsg = fmt.Sprintf("Warning 1292 Truncated incorrect %s value: '%d'", name, tc.maxValue+1)
		tk.MustQuery("show warnings").Check(testkit.Rows(warnMsg))
		tk.MustQuery(selectSQL).Check(testkit.Rows(fmt.Sprintf("%d", tc.maxValue)))
	}
}

func (s *testSuite5) TestSetCharset(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	ctx := tk.Se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()

	var characterSetVariables = []string{
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"character_set_server",
		"character_set_database",
		"character_set_system",
		"character_set_filesystem",
	}

	check := func(args ...string) {
		for i, v := range characterSetVariables {
			sVar, err := variable.GetSessionSystemVar(sessionVars, v)
			c.Assert(err, IsNil)
			c.Assert(sVar, Equals, args[i], Commentf("%d: %s", i, characterSetVariables[i]))
		}
	}

	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES latin1`)
	check(
		"latin1",
		"latin1",
		"latin1",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES default`)
	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	// Issue #1523
	tk.MustExec(`SET NAMES binary`)
	check(
		"binary",
		"binary",
		"binary",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES utf8`)
	check(
		"utf8",
		"utf8",
		"utf8",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET CHARACTER SET latin1`)
	check(
		"latin1",
		"utf8mb4",
		"latin1",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET CHARACTER SET default`)
	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)
}

func (s *testSuite5) TestValidateSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	_, err := tk.Exec("set global tidb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tidb_distsql_scan_concurrency=-2;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@tidb_distsql_scan_concurrency=-2;")
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

	tk.MustExec("set @@tidb_pprof_sql_cpu=1;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_pprof_sql_cpu=0;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_enable_streaming=1;")
	tk.MustQuery("select @@tidb_enable_streaming;").Check(testkit.Rows("1"))

	_, err = tk.Exec("set @@tidb_batch_delete=3;")
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
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))

	tk.MustExec("set global allow_auto_random_explicit_insert=on;")
	tk.MustQuery("select @@global.allow_auto_random_explicit_insert;").Check(testkit.Rows("1"))
}

func (s *testSuite5) TestSelectGlobalVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Rows("151"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Rows("151"))

	tk.MustExec("set @@global.max_connections=100;")

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Rows("100"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Rows("100"))

	tk.MustExec("set @@global.max_connections=151;")

	// test for unknown variable.
	err := tk.ExecToErr("select @@invalid")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownSystemVar), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select @@global.invalid")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownSystemVar), IsTrue, Commentf("err %v", err))
}

func (s *testSuite5) TestSetConcurrency(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test default value
	tk.MustQuery("select @@tidb_executor_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.DefExecutorConcurrency)))

	tk.MustQuery("select @@tidb_index_lookup_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_index_lookup_join_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hash_join_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hashagg_partial_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hashagg_final_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_window_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_projection_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_distsql_scan_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.DefDistSQLScanConcurrency)))

	tk.MustQuery("select @@tidb_index_serial_scan_concurrency;").Check(testkit.Rows(strconv.Itoa(variable.DefIndexSerialScanConcurrency)))

	vars := tk.Se.(sessionctx.Context).GetSessionVars()
	c.Assert(vars.ExecutorConcurrency, Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.DistSQLScanConcurrency(), Equals, variable.DefDistSQLScanConcurrency)

	c.Assert(vars.IndexSerialScanConcurrency(), Equals, variable.DefIndexSerialScanConcurrency)

	// test setting deprecated variables
	warnTpl := "Warning 1287 '%s' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead"

	checkSet := func(v string) {
		tk.MustExec(fmt.Sprintf("set @@%s=1;", v))
		tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", fmt.Sprintf(warnTpl, v)))
		tk.MustQuery(fmt.Sprintf("select @@%s;", v)).Check(testkit.Rows("1"))
	}

	checkSet(variable.TiDBIndexLookupConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, 1)

	checkSet(variable.TiDBIndexLookupJoinConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, 1)

	checkSet(variable.TiDBHashJoinConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, 1)

	checkSet(variable.TiDBHashAggPartialConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, 1)

	checkSet(variable.TiDBHashAggFinalConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, 1)

	checkSet(variable.TiDBProjectionConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, 1)

	checkSet(variable.TiDBWindowConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, 1)

	tk.MustExec(fmt.Sprintf("set @@%s=1;", variable.TiDBDistSQLScanConcurrency))
	tk.MustQuery(fmt.Sprintf("select @@%s;", variable.TiDBDistSQLScanConcurrency)).Check(testkit.Rows("1"))
	c.Assert(vars.DistSQLScanConcurrency(), Equals, 1)

	tk.MustExec("set @@tidb_index_serial_scan_concurrency=4")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select @@tidb_index_serial_scan_concurrency;").Check(testkit.Rows("4"))
	c.Assert(vars.IndexSerialScanConcurrency(), Equals, 4)

	// test setting deprecated value unset
	tk.MustExec("set @@tidb_index_lookup_concurrency=-1;")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=-1;")
	tk.MustExec("set @@tidb_hash_join_concurrency=-1;")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=-1;")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=-1;")
	tk.MustExec("set @@tidb_window_concurrency=-1;")
	tk.MustExec("set @@tidb_projection_concurrency=-1;")

	c.Assert(vars.IndexLookupConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, variable.DefExecutorConcurrency)

	_, err := tk.Exec("set @@tidb_executor_concurrency=-1;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))
}

func (s *testSuite5) TestEnableNoopFunctionsVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test for tidb_enable_noop_functions
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("0"))

	_, err := tk.Exec(`select get_lock('lock1', 2);`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`select release_lock('lock1');`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))

	// change session var to 1
	tk.MustExec(`set tidb_enable_noop_functions=1;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("0"))
	tk.MustQuery(`select get_lock("lock", 10)`).Check(testkit.Rows("1"))
	tk.MustQuery(`select release_lock("lock")`).Check(testkit.Rows("1"))

	// restore to 0
	tk.MustExec(`set tidb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("0"))

	_, err = tk.Exec(`select get_lock('lock2', 10);`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`select release_lock('lock2');`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))

	// set test
	_, err = tk.Exec(`set tidb_enable_noop_functions='abc'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`set tidb_enable_noop_functions=11`)
	c.Assert(err, NotNil)
	tk.MustExec(`set tidb_enable_noop_functions="off";`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("0"))
	tk.MustExec(`set tidb_enable_noop_functions="on";`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("1"))
	tk.MustExec(`set tidb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("0"))
}

func (s *testSuite5) TestSetClusterConfig(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	serversInfo := []infoschema.ServerInfo{
		{ServerType: "tidb", Address: "127.0.0.1:1111", StatusAddr: "127.0.0.1:1111"},
		{ServerType: "tidb", Address: "127.0.0.1:2222", StatusAddr: "127.0.0.1:2222"},
		{ServerType: "pd", Address: "127.0.0.1:3333", StatusAddr: "127.0.0.1:3333"},
		{ServerType: "pd", Address: "127.0.0.1:4444", StatusAddr: "127.0.0.1:4444"},
		{ServerType: "tikv", Address: "127.0.0.1:5555", StatusAddr: "127.0.0.1:5555"},
		{ServerType: "tikv", Address: "127.0.0.1:6666", StatusAddr: "127.0.0.1:6666"},
	}
	var serverInfoErr error
	serverInfoFunc := func(sessionctx.Context) ([]infoschema.ServerInfo, error) {
		return serversInfo, serverInfoErr
	}
	tk.Se.SetValue(executor.TestSetConfigServerInfoKey, serverInfoFunc)

	c.Assert(tk.ExecToErr("set config xxx log.level='info'"), ErrorMatches, "unknown type xxx")
	c.Assert(tk.ExecToErr("set config tidb log.level='info'"), ErrorMatches, "TiDB doesn't support to change configs online, please use SQL variables")
	c.Assert(tk.ExecToErr("set config '127.0.0.1:1111' log.level='info'"), ErrorMatches, "TiDB doesn't support to change configs online, please use SQL variables")
	c.Assert(tk.ExecToErr("set config '127.a.b.c:1234' log.level='info'"), ErrorMatches, "invalid instance 127.a.b.c:1234")
	c.Assert(tk.ExecToErr("set config tikv log.level=null"), ErrorMatches, "can't set config to null")
	c.Assert(tk.ExecToErr("set config '1.1.1.1:1111' log.level='info'"), ErrorMatches, "instance 1.1.1.1:1111 is not found in this cluster")

	httpCnt := 0
	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		httpCnt++
		return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(nil)}, nil
	})
	tk.MustExec("set config tikv log.level='info'")
	c.Assert(httpCnt, Equals, 2)

	httpCnt = 0
	tk.MustExec("set config '127.0.0.1:5555' log.level='info'")
	c.Assert(httpCnt, Equals, 1)

	httpCnt = 0
	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("something wrong")
	})
	tk.MustExec("set config tikv log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 something wrong", "Warning 1105 something wrong"))

	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Body: ioutil.NopCloser(bytes.NewBufferString("WRONG"))}, nil
	})
	tk.MustExec("set config tikv log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 bad request to http://127.0.0.1:5555/config: WRONG", "Warning 1105 bad request to http://127.0.0.1:6666/config: WRONG"))
}

func (s *testSuite5) TestSetClusterConfigJSONData(c *C) {
	var d types.MyDecimal
	c.Assert(d.FromFloat64(123.456), IsNil)
	tyBool := types.NewFieldType(mysql.TypeTiny)
	tyBool.Flag |= mysql.IsBooleanFlag
	cases := []struct {
		val    expression.Expression
		result string
		succ   bool
	}{
		{&expression.Constant{Value: types.NewIntDatum(1), RetType: tyBool}, `{"k":true}`, true},
		{&expression.Constant{Value: types.NewIntDatum(0), RetType: tyBool}, `{"k":false}`, true},
		{&expression.Constant{Value: types.NewIntDatum(2333), RetType: types.NewFieldType(mysql.TypeLong)}, `{"k":2333}`, true},
		{&expression.Constant{Value: types.NewFloat64Datum(23.33), RetType: types.NewFieldType(mysql.TypeDouble)}, `{"k":23.33}`, true},
		{&expression.Constant{Value: types.NewStringDatum("abcd"), RetType: types.NewFieldType(mysql.TypeString)}, `{"k":"abcd"}`, true},
		{&expression.Constant{Value: types.NewDecimalDatum(&d), RetType: types.NewFieldType(mysql.TypeNewDecimal)}, `{"k":123.456}`, true},
		{&expression.Constant{Value: types.NewDatum(nil), RetType: types.NewFieldType(mysql.TypeLonglong)}, "", false},
		{&expression.Constant{RetType: types.NewFieldType(mysql.TypeJSON)}, "", false}, // unsupported type
		{nil, "", false},
	}

	ctx := mock.NewContext()
	for _, t := range cases {
		result, err := executor.ConvertConfigItem2JSON(ctx, "k", t.val)
		if t.succ {
			c.Assert(t.result, Equals, result)
		} else {
			c.Assert(err, NotNil)
		}
	}
}
