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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

const (
	strictModeSQL    = "set sql_mode = 'STRICT_TRANS_TABLES'"
	nonStrictModeSQL = "set sql_mode = ''"
)

func TestStatementContext(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table sc (a int)")
	tk.MustExec("insert sc values (1), (2)")

	tk.MustExec(strictModeSQL)
	tk.MustQuery("select * from sc where a > cast(1.1 as decimal)").Check(testkit.Rows("2"))
	tk.MustExec("update sc set a = 4 where a > cast(1.1 as decimal)")

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
	_, err := tk.Exec("update sc set a = 4 where a > '1x'")
	require.Error(t, err)
	_, err = tk.Exec("delete from sc where a < '1x'")
	require.Error(t, err)
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Rows("3"))

	// Non-strict mode never returns error.
	tk.MustExec(nonStrictModeSQL)
	tk.MustExec("update sc set a = 4 where a > '1x'")
	tk.MustExec("delete from sc where a < '1x'")
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Rows("4"))

	// Test invalid UTF8
	tk.MustExec("create table sc2 (a varchar(255))")
	// Insert an invalid UTF8
	tk.MustExec("insert sc2 values (unhex('4040ffff'))")
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(0))
	tk.MustQuery("select * from sc2").Check(testkit.Rows("@@"))
	tk.MustExec(strictModeSQL)
	_, err = tk.Exec("insert sc2 values (unhex('4040ffff'))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), "err %v", err)

	tk.MustExec("set @@tidb_skip_utf8_check = '1'")
	_, err = tk.Exec("insert sc2 values (unhex('4040ffff'))")
	require.NoError(t, err)
	tk.MustQuery("select length(a) from sc2").Check(testkit.Rows("2", "4"))

	tk.MustExec("set @@tidb_skip_utf8_check = '0'")
	runeErrStr := string(utf8.RuneError)
	tk.MustExec(fmt.Sprintf("insert sc2 values ('%s')", runeErrStr))

	// Test invalid ASCII
	tk.MustExec("create table sc3 (a varchar(255)) charset ascii")

	tk.MustExec(nonStrictModeSQL)
	tk.MustExec("insert sc3 values (unhex('4040ffff'))")
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(0))
	tk.MustQuery("select * from sc3").Check(testkit.Rows("@@"))

	tk.MustExec(strictModeSQL)
	_, err = tk.Exec("insert sc3 values (unhex('4040ffff'))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), "err %v", err)

	tk.MustExec("set @@tidb_skip_ascii_check = '1'")
	_, err = tk.Exec("insert sc3 values (unhex('4040ffff'))")
	require.NoError(t, err)
	tk.MustQuery("select length(a) from sc3").Check(testkit.Rows("2", "4"))

	// no placeholder in ASCII, so just insert '@@'...
	tk.MustExec("set @@tidb_skip_ascii_check = '0'")
	tk.MustExec("insert sc3 values (unhex('4040'))")

	// Test non-BMP characters.
	tk.MustExec(nonStrictModeSQL)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a varchar(100) charset utf8);")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("insert t1 values (unhex('f09f8c80'))")
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Rows(""))
	tk.MustExec("insert t1 values (unhex('4040f09f8c80'))")
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Rows("", "@@"))
	tk.MustQuery("select length(a) from t1").Check(testkit.Rows("0", "2"))
	tk.MustExec(strictModeSQL)
	_, err = tk.Exec("insert t1 values (unhex('f09f8c80'))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), "err %v", err)
	_, err = tk.Exec("insert t1 values (unhex('F0A48BAE'))")
	require.Error(t, err)
	require.Truef(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), "err %v", err)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.CheckMb4ValueInUTF8.Store(false)
	})
	tk.MustExec("insert t1 values (unhex('f09f8c80'))")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.CheckMb4ValueInUTF8.Store(true)
	})
	_, err = tk.Exec("insert t1 values (unhex('F0A48BAE'))")
	require.Error(t, err)
}
