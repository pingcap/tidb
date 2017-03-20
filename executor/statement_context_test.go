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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

const (
	strictModeSQL    = "set sql_mode = 'STRICT_TRANS_TABLES'"
	nonStrictModeSQL = "set sql_mode = ''"
)

func (s *testSuite) TestStatementContext(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table sc (a int)")
	tk.MustExec("insert sc values (1), (2)")

	tk.MustExec(strictModeSQL)
	tk.MustQuery("select * from sc where a > cast(1.1 as decimal)").Check(testkit.Rows("2"))
	_, err := tk.Exec(`select * from sc where a > cast(1.1 as decimal);
		        update sc set a = 4 where a > cast(1.1 as decimal)`)
	c.Check(terror.ErrorEqual(err, types.ErrTruncated), IsTrue)

	tk.MustExec(nonStrictModeSQL)
	tk.MustExec("update sc set a = 3 where a > cast(1.1 as decimal)")
	tk.MustQuery("select * from sc").Check(testkit.Rows("1", "3"))

	tk.MustExec(strictModeSQL)
	tk.MustExec("delete from sc")
	tk.MustExec("insert sc values ('1.8'+1)")
	tk.MustQuery("select * from sc").Check(testkit.Rows("3"))

	// Handle coprocessor flags, '1x' is an invalid int.
	// UPDATE and DELETE do select request first which is handled by coprocessor.
	// In strict mode we expect error.
	_, err = tk.Exec("update sc set a = 4 where a > '1x'")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from sc where a < '1x'")
	c.Assert(err, NotNil)
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Rows("3"))

	// Non-strict mode never returns error.
	tk.MustExec(nonStrictModeSQL)
	tk.MustExec("update sc set a = 4 where a > '1x'")
	tk.MustExec("delete from sc where a < '1x'")
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Rows("4"))

	// TODO: enable UTF8 check
	// Test invalid UTF8
	//tk.MustExec("create table sc2 (a varchar(255))")
	//// Insert an invalid UTF8
	//tk.MustExec("insert sc2 values (unhex('4040ffff'))")
	//c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Greater, uint16(0))
	//tk.MustQuery("select * from sc2").Check(testkit.Rows(fmt.Sprintf("%v", []byte("@@"))))
	//tk.MustExec(strictModeSQL)
	//_, err = tk.Exec("insert sc2 values (unhex('4040ffff'))")
	//c.Assert(err, NotNil)
	//c.Assert(terror.ErrorEqual(err, table.ErrTruncateWrongValue), IsTrue)
}
