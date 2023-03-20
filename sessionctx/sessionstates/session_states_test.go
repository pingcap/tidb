// Copyright 2022 PingCAP, Inc.
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

package sessionstates_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sem"
	"github.com/stretchr/testify/require"
)

func TestGrammar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show session_states").Rows()
	require.Len(t, rows, 1)
	tk.MustExec("set session_states '{}'")
	tk.MustGetErrCode("set session_states 1", errno.ErrParse)
}

func TestUserVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t1(" +
		"j json, b blob, s varchar(255), st set('red', 'green', 'blue'), en enum('red', 'green', 'blue'))")
	tk.MustExec("insert into test.t1 values('{\"color:\": \"red\"}', 'red', 'red', 'red,green', 'red')")

	tests := []string{
		"",
		"set @%s=null",
		"set @%s=1",
		"set @%s=1.0e10",
		"set @%s=1.0-1",
		"set @%s=now()",
		"set @%s=1, @%s=1.0-1",
		"select @%s:=1+1",
		// TiDB doesn't support following features.
		//"select j into @%s from test.t1",
		//"select j,b,s,st,en into @%s,@%s,@%s,@%s,@%s from test.t1",
	}

	for _, tt := range tests {
		tk1 := testkit.NewTestKit(t, store)
		tk2 := testkit.NewTestKit(t, store)
		namesNum := strings.Count(tt, "%s")
		names := make([]any, 0, namesNum)
		for i := 0; i < namesNum; i++ {
			names = append(names, fmt.Sprintf("a%d", i))
		}
		var sql string
		if len(tt) > 0 {
			sql = fmt.Sprintf(tt, names...)
			tk1.MustExec(sql)
		}
		showSessionStatesAndSet(t, tk1, tk2)
		for _, name := range names {
			sql := fmt.Sprintf("select @%s", name)
			msg := fmt.Sprintf("sql: %s, var name: %s", sql, name)
			value1 := tk1.MustQuery(sql).Rows()[0][0]
			value2 := tk2.MustQuery(sql).Rows()[0][0]
			require.Equal(t, value1, value2, msg)
		}
	}
}

func TestSystemVars(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tests := []struct {
		stmts           []string
		varName         string
		inSessionStates bool
		checkStmt       string
		expectedValue   string
	}{
		{
			// normal variable
			inSessionStates: true,
			varName:         variable.TiDBMaxTiFlashThreads,
			expectedValue:   strconv.Itoa(variable.DefTiFlashMaxThreads),
		},
		{
			// hidden variable
			inSessionStates: true,
			varName:         variable.TiDBTxnReadTS,
			expectedValue:   "",
		},
		{
			// none-scoped variable
			inSessionStates: false,
			varName:         variable.DataDir,
			expectedValue:   "/usr/local/mysql/data/",
		},
		{
			// instance-scoped variable
			inSessionStates: false,
			varName:         variable.TiDBGeneralLog,
			expectedValue:   "0",
		},
		{
			// global-scoped variable
			inSessionStates: false,
			varName:         variable.TiDBAutoAnalyzeStartTime,
			expectedValue:   variable.DefAutoAnalyzeStartTime,
		},
		{
			// sem invisible variable
			inSessionStates: false,
			varName:         variable.TiDBConfig,
		},
		{
			// noop variables
			stmts:           []string{"set sql_buffer_result=true"},
			inSessionStates: true,
			varName:         "sql_buffer_result",
			expectedValue:   "1",
		},
		{
			stmts:           []string{"set transaction isolation level repeatable read"},
			inSessionStates: true,
			varName:         "tx_isolation_one_shot",
			expectedValue:   "REPEATABLE-READ",
		},
		{
			inSessionStates: false,
			varName:         variable.Timestamp,
		},
		{
			stmts:           []string{"set timestamp=100"},
			inSessionStates: true,
			varName:         variable.Timestamp,
			expectedValue:   "100",
		},
		{
			stmts:           []string{"set rand_seed1=10000000, rand_seed2=1000000"},
			inSessionStates: true,
			varName:         variable.RandSeed1,
			checkStmt:       "select rand()",
			expectedValue:   "0.028870999839968048",
		},
		{
			stmts:           []string{"set rand_seed1=10000000, rand_seed2=1000000", "select rand()"},
			inSessionStates: true,
			varName:         variable.RandSeed1,
			checkStmt:       "select rand()",
			expectedValue:   "0.11641535266900002",
		},
	}

	if !sem.IsEnabled() {
		sem.Enable()
		defer sem.Disable()
	}
	for _, tt := range tests {
		tk1 := testkit.NewTestKit(t, store)
		for _, stmt := range tt.stmts {
			if strings.HasPrefix(stmt, "select") {
				tk1.MustQuery(stmt)
			} else {
				tk1.MustExec(stmt)
			}
		}
		tk2 := testkit.NewTestKit(t, store)
		rows := tk1.MustQuery("show session_states").Rows()
		state := rows[0][0].(string)
		msg := fmt.Sprintf("var name: '%s', expected value: '%s'", tt.varName, tt.expectedValue)
		require.Equal(t, tt.inSessionStates, strings.Contains(state, tt.varName), msg)
		state = strconv.Quote(state)
		setSQL := fmt.Sprintf("set session_states %s", state)
		tk2.MustExec(setSQL)
		if len(tt.expectedValue) > 0 {
			checkStmt := tt.checkStmt
			if len(checkStmt) == 0 {
				checkStmt = fmt.Sprintf("select @@%s", tt.varName)
			}
			tk2.MustQuery(checkStmt).Check(testkit.Rows(tt.expectedValue))
		}
	}

	{
		// The session value should not change even if the global value changes.
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustQuery("select @@autocommit").Check(testkit.Rows("1"))
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("set global autocommit=0")
		tk3 := testkit.NewTestKit(t, store)
		showSessionStatesAndSet(t, tk1, tk3)
		tk3.MustQuery("select @@autocommit").Check(testkit.Rows("1"))
	}
}

func TestSessionCtx(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t1(id int)")

	tests := []struct {
		setFunc   func(tk *testkit.TestKit) any
		checkFunc func(tk *testkit.TestKit, param any)
	}{
		{
			// check PreparedStmtID
			checkFunc: func(tk *testkit.TestKit, param any) {
				require.Equal(t, uint32(1), tk.Session().GetSessionVars().GetNextPreparedStmtID())
			},
		},
		{
			// check PreparedStmtID
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("prepare stmt from 'select ?'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				require.Equal(t, uint32(2), tk.Session().GetSessionVars().GetNextPreparedStmtID())
			},
		},
		{
			// check Status
			checkFunc: func(tk *testkit.TestKit, param any) {
				require.Equal(t, mysql.ServerStatusAutocommit, tk.Session().GetSessionVars().Status&mysql.ServerStatusAutocommit)
			},
		},
		{
			// check Status
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("set autocommit=0")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				require.Equal(t, uint16(0), tk.Session().GetSessionVars().Status&mysql.ServerStatusAutocommit)
			},
		},
		{
			// check CurrentDB
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select database()").Check(testkit.Rows("<nil>"))
			},
		},
		{
			// check CurrentDB
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("use test")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select database()").Check(testkit.Rows("test"))
			},
		},
		{
			// check CurrentDB
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create database test1")
				tk.MustExec("use test1")
				tk.MustExec("drop database test1")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select database()").Check(testkit.Rows("<nil>"))
			},
		},
		{
			// check LastTxnInfo
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@tidb_last_txn_info").Check(testkit.Rows(""))
			},
		},
		{
			// check LastTxnInfo
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("begin")
				tk.MustExec("insert test.t1 value(1)")
				tk.MustExec("commit")
				rows := tk.MustQuery("select @@tidb_last_txn_info").Rows()
				require.NotEqual(t, "", rows[0][0].(string))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@tidb_last_txn_info").Check(param.([][]interface{}))
			},
		},
		{
			// check LastQueryInfo
			setFunc: func(tk *testkit.TestKit) any {
				rows := tk.MustQuery("select @@tidb_last_query_info").Rows()
				require.NotEqual(t, "", rows[0][0].(string))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@tidb_last_query_info").Check(param.([][]interface{}))
			},
		},
		{
			// check LastQueryInfo
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustQuery("select * from test.t1")
				startTS := tk.Session().GetSessionVars().LastQueryInfo.StartTS
				require.NotEqual(t, uint64(0), startTS)
				return startTS
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				startTS := tk.Session().GetSessionVars().LastQueryInfo.StartTS
				require.Equal(t, param.(uint64), startTS)
			},
		},
		{
			// check LastDDLInfo
			setFunc: func(tk *testkit.TestKit) any {
				rows := tk.MustQuery("select @@tidb_last_ddl_info").Rows()
				require.NotEqual(t, "", rows[0][0].(string))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@tidb_last_ddl_info").Check(param.([][]interface{}))
			},
		},
		{
			// check LastDDLInfo
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("truncate table test.t1")
				rows := tk.MustQuery("select @@tidb_last_ddl_info").Rows()
				require.NotEqual(t, "", rows[0][0].(string))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@tidb_last_ddl_info").Check(param.([][]interface{}))
			},
		},
		{
			// check LastFoundRows
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("insert test.t1 value(1), (2), (3), (4), (5)")
				// SQL_CALC_FOUND_ROWS is not supported now, so we just test normal select.
				rows := tk.MustQuery("select * from test.t1 limit 3").Rows()
				require.Equal(t, 3, len(rows))
				return "3"
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select found_rows()").Check(testkit.Rows(param.(string)))
			},
		},
		{
			// check SequenceState
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create sequence test.s")
				tk.MustQuery("select nextval(test.s)").Check(testkit.Rows("1"))
				tk.MustQuery("select lastval(test.s)").Check(testkit.Rows("1"))
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select lastval(test.s)").Check(testkit.Rows("1"))
				tk.MustQuery("select nextval(test.s)").Check(testkit.Rows("2"))
			},
		},
		{
			// check FoundInPlanCache
			setFunc: func(tk *testkit.TestKit) any {
				require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
			},
		},
		{
			// check FoundInPlanCache
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("prepare stmt from 'select * from test.t1'")
				tk.MustQuery("execute stmt")
				tk.MustQuery("execute stmt")
				require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
			},
		},
		{
			// check FoundInBinding
			setFunc: func(tk *testkit.TestKit) any {
				require.False(t, tk.Session().GetSessionVars().FoundInBinding)
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
			},
		},
		{
			// check FoundInBinding
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1")
				tk.MustQuery("select * from test.t1")
				require.True(t, tk.Session().GetSessionVars().FoundInBinding)
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			},
		},
	}

	for _, tt := range tests {
		tk1 := testkit.NewTestKit(t, store)
		var param any
		if tt.setFunc != nil {
			param = tt.setFunc(tk1)
		}
		tk2 := testkit.NewTestKit(t, store)
		showSessionStatesAndSet(t, tk1, tk2)
		tt.checkFunc(tk2, param)
	}
}

func TestStatementCtx(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t1(id int auto_increment primary key, str char(1))")

	tests := []struct {
		setFunc   func(tk *testkit.TestKit) any
		checkFunc func(tk *testkit.TestKit, param any)
	}{
		{
			// check LastAffectedRows
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustQuery("show warnings")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select row_count()").Check(testkit.Rows("0"))
				tk.MustQuery("select row_count()").Check(testkit.Rows("-1"))
			},
		},
		{
			// check LastAffectedRows
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustQuery("select 1")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select row_count()").Check(testkit.Rows("-1"))
			},
		},
		{
			// check LastAffectedRows
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("insert into test.t1(str) value('a'), ('b'), ('c')")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select row_count()").Check(testkit.Rows("3"))
				tk.MustQuery("select row_count()").Check(testkit.Rows("-1"))
			},
		},
		{
			// check LastInsertID
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_insert_id").Check(testkit.Rows("0"))
			},
		},
		{
			// check LastInsertID
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("insert into test.t1(str) value('d')")
				rows := tk.MustQuery("select @@last_insert_id").Rows()
				require.NotEqual(t, "0", rows[0][0].(string))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("select @@last_insert_id").Check(param.([][]any))
			},
		},
		{
			// check Warning
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustQuery("select 1")
				tk.MustQuery("show warnings").Check(testkit.Rows())
				tk.MustQuery("show errors").Check(testkit.Rows())
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show warnings").Check(testkit.Rows())
				tk.MustQuery("show errors").Check(testkit.Rows())
				tk.MustQuery("select @@warning_count, @@error_count").Check(testkit.Rows("0 0"))
			},
		},
		{
			// check Warning
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustGetErrCode("insert into test.t1(str) value('ef')", errno.ErrDataTooLong)
				rows := tk.MustQuery("show warnings").Rows()
				require.Equal(t, 1, len(rows))
				tk.MustQuery("show errors").Check(rows)
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show warnings").Check(param.([][]any))
				tk.MustQuery("show errors").Check(param.([][]any))
				tk.MustQuery("select @@warning_count, @@error_count").Check(testkit.Rows("1 1"))
			},
		},
		{
			// check Warning
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("set sql_mode=''")
				tk.MustExec("insert into test.t1(str) value('ef'), ('ef')")
				rows := tk.MustQuery("show warnings").Rows()
				require.Equal(t, 2, len(rows))
				tk.MustQuery("show errors").Check(testkit.Rows())
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show warnings").Check(param.([][]any))
				tk.MustQuery("show errors").Check(testkit.Rows())
				tk.MustQuery("select @@warning_count, @@error_count").Check(testkit.Rows("2 0"))
			},
		},
	}

	for _, tt := range tests {
		tk1 := testkit.NewTestKit(t, store)
		var param any
		if tt.setFunc != nil {
			param = tt.setFunc(tk1)
		}
		tk2 := testkit.NewTestKit(t, store)
		showSessionStatesAndSet(t, tk1, tk2)
		tt.checkFunc(tk2, param)
	}
}

func TestPreparedStatements(t *testing.T) {
	store := testkit.CreateMockStore(t)
	sv := server.CreateMockServer(t, store)
	defer sv.Close()

	tests := []struct {
		setFunc    func(tk *testkit.TestKit, conn server.MockConn) any
		checkFunc  func(tk *testkit.TestKit, conn server.MockConn, param any)
		restoreErr int
		cleanFunc  func(tk *testkit.TestKit)
	}{
		{
			// no such statement
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustGetErrCode("execute stmt", errno.ErrPreparedStmtNotFound)
			},
		},
		{
			// deallocate it after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("prepare stmt from 'select 1'")
				tk.MustExec("deallocate prepare stmt")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustGetErrCode("execute stmt", errno.ErrPreparedStmtNotFound)
			},
		},
		{
			// statement with no parameters
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert into test.t1 value(1), (2), (3)")
				tk.MustExec("prepare stmt from 'select * from test.t1 order by id'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2", "3"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// statement with user-defined parameters
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert into test.t1 value(1), (2), (3)")
				tk.MustExec("prepare stmt from 'select * from test.t1 where id>? order by id limit ?'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustExec("set @a=1, @b=1")
				tk.MustQuery("execute stmt using @a, @b").Check(testkit.Rows("2"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// execute the statement multiple times
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("prepare stmt1 from 'insert into test.t1 value(?), (?), (?)'")
				tk.MustExec("prepare stmt2 from 'select * from test.t1 order by id'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt2").Check(testkit.Rows())
				tk.MustExec("set @a=1, @b=2, @c=3")
				tk.MustExec("execute stmt1 using @a, @b, @c")
				tk.MustQuery("execute stmt2").Check(testkit.Rows("1", "2", "3"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// update session variables after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci")
				tk.MustExec("prepare stmt from 'select @@character_set_client, @@collation_connection'")
				tk.MustQuery("execute stmt").Check(testkit.Rows("utf8mb4 utf8mb4_general_ci"))
				tk.MustExec("set names gbk collate gbk_chinese_ci")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("gbk gbk_chinese_ci"))
			},
		},
		{
			// session-scoped ANSI_QUOTES
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("set sql_mode='ANSI_QUOTES'")
				tk.MustExec("prepare stmt from 'select \\'a\\''")
				tk.MustQuery("execute stmt").Check(testkit.Rows("a"))
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("a"))
			},
		},
		{
			// global-scoped ANSI_QUOTES
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("set global sql_mode='ANSI_QUOTES'")
				tk.MustExec("prepare stmt from \"select \\\"a\\\"\"")
				tk.MustQuery("execute stmt").Check(testkit.Rows("a"))
				return nil
			},
			restoreErr: errno.ErrBadField,
		},
		{
			// statement name
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("prepare `stmt 1` from 'select 1'")
				tk.MustQuery("execute `stmt 1`").Check(testkit.Rows("1"))
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute `stmt 1`").Check(testkit.Rows("1"))
			},
		},
		{
			// multiple prepared statements
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert into test.t1 value(1), (2), (3)")
				tk.MustExec("prepare stmt1 from 'select * from test.t1 order by id'")
				tk.MustExec("prepare stmt2 from 'select * from test.t1 where id=?'")
				tk.MustExec("set @a=1")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt1").Check(testkit.Rows("1", "2", "3"))
				tk.MustQuery("execute stmt2 using @a").Check(testkit.Rows("1"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// change current db after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("use test")
				tk.MustExec("create table t1(id int)")
				tk.MustExec("insert into t1 value(1), (2), (3)")
				tk.MustExec("prepare stmt from 'select * from t1 order by id'")
				tk.MustExec("use mysql")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("select database()").Check(testkit.Rows("mysql"))
				tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2", "3"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// update user variable after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert into test.t1 value(1), (2), (3)")
				tk.MustExec("set @a=1")
				tk.MustExec("prepare stmt from 'select * from test.t1 where id=?'")
				tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
				tk.MustExec("set @a=2")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt using @a").Check(testkit.Rows("2"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// alter table after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert into test.t1 value(1)")
				tk.MustExec("prepare stmt from 'select * from test.t1'")
				tk.MustExec("alter table test.t1 add column c char(1) default 'a'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("1 a"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// drop and create table after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("prepare stmt from 'select * from test.t1'")
				tk.MustExec("drop table test.t1")
				tk.MustExec("create table test.t1(id int, c char(1))")
				tk.MustExec("insert into test.t1 value(1, 'a')")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("1 a"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// drop table after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("prepare stmt from 'select * from test.t1'")
				tk.MustExec("drop table test.t1")
				return nil
			},
			restoreErr: errno.ErrNoSuchTable,
		},
		{
			// drop db after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create database test1")
				tk.MustExec("use test1")
				tk.MustExec("create table t1(id int)")
				tk.MustExec("prepare stmt from 'select * from t1'")
				tk.MustExec("drop database test1")
				return nil
			},
			restoreErr: errno.ErrNoSuchTable,
		},
		{
			// update sql_mode after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("set sql_mode=''")
				tk.MustExec("create table test.t1(id int, name char(10))")
				tk.MustExec("insert into test.t1 value(1, 'a')")
				tk.MustExec("prepare stmt from 'select id, name from test.t1 group by id'")
				tk.MustExec("set sql_mode='ONLY_FULL_GROUP_BY'")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				// The prepare statement is decoded after decoding session variables,
				// so `SET SESSION_STATES` won't report errors.
				tk.MustGetErrCode("execute stmt", errno.ErrFieldNotInGroupBy)
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// update global sql_mode after prepare
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("set sql_mode=''")
				tk.MustExec("create table test.t1(id int, name char(10))")
				tk.MustExec("prepare stmt from 'select id, name from test.t1 group by id'")
				tk.MustExec("set global sql_mode='ONLY_FULL_GROUP_BY'")
				return nil
			},
			restoreErr: errno.ErrFieldNotInGroupBy,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
				tk.MustExec("set global sql_mode=default")
			},
		},
		{
			// warnings won't be affected
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				// Decoding this prepared statement should report a warning.
				tk.MustExec("prepare stmt from 'select 0/0'")
				// Override the warning.
				tk.MustQuery("select 1")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("show warnings").Check(testkit.Rows())
			},
		},
		{
			// test binary-protocol prepared statement
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				stmtID, _, _, err := tk.Session().PrepareStmt("select ?")
				require.NoError(t, err)
				return stmtID
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				rs, err := tk.Session().ExecutePreparedStmt(context.Background(), param.(uint32), expression.Args2Expressions4Test(1))
				require.NoError(t, err)
				tk.ResultSetToResult(rs, "").Check(testkit.Rows("1"))
			},
		},
		{
			// no such prepared statement
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				_, err := tk.Session().ExecutePreparedStmt(context.Background(), 1, nil)
				errEqualsCode(t, err, errno.ErrPreparedStmtNotFound)
			},
		},
		{
			// both text and binary protocols
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("prepare stmt from 'select 10'")
				stmtID, _, _, err := tk.Session().PrepareStmt("select ?")
				require.NoError(t, err)
				return stmtID
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				tk.MustQuery("execute stmt").Check(testkit.Rows("10"))
				rs, err := tk.Session().ExecutePreparedStmt(context.Background(), param.(uint32), expression.Args2Expressions4Test(1))
				require.NoError(t, err)
				tk.ResultSetToResult(rs, "").Check(testkit.Rows("1"))
			},
		},
		{
			// drop binary protocol statements
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				stmtID, _, _, err := tk.Session().PrepareStmt("select ?")
				require.NoError(t, err)
				return stmtID
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				err := tk.Session().DropPreparedStmt(param.(uint32))
				require.NoError(t, err)
			},
		},
		{
			// execute the statement multiple times
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				tk.MustExec("create table test.t1(id int)")
				stmtID1, _, _, err := tk.Session().PrepareStmt("insert into test.t1 value(?), (?), (?)")
				require.NoError(t, err)
				stmtID2, _, _, err := tk.Session().PrepareStmt("select * from test.t1 order by id")
				require.NoError(t, err)
				return []uint32{stmtID1, stmtID2}
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				stmtIDs := param.([]uint32)
				rs, err := tk.Session().ExecutePreparedStmt(context.Background(), stmtIDs[1], nil)
				require.NoError(t, err)
				tk.ResultSetToResult(rs, "").Check(testkit.Rows())
				_, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtIDs[0], expression.Args2Expressions4Test(1, 2, 3))
				require.NoError(t, err)
				rs, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtIDs[1], nil)
				require.NoError(t, err)
				tk.ResultSetToResult(rs, "").Check(testkit.Rows("1", "2", "3"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// the latter stmt ID should be bigger
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				stmtID, _, _, err := tk.Session().PrepareStmt("select ?")
				require.NoError(t, err)
				return stmtID
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				stmtID, _, _, err := tk.Session().PrepareStmt("select ?")
				require.NoError(t, err)
				require.True(t, stmtID > param.(uint32))
			},
		},
		{
			// execute the statement with cursor
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select ?")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, true, true, paramInfo{value: 1, isNull: false})
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getFetchBytes(1, 10)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				// This COM_STMT_FETCH returns EOF.
				cmd = getFetchBytes(1, 10)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				return uint32(1)
			},
			checkFunc: func(tk *testkit.TestKit, conn server.MockConn, param any) {
				cmd := getExecuteBytes(param.(uint32), false, false, paramInfo{value: 1, isNull: false})
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
		},
		// Skip this case. Refer to https://github.com/pingcap/tidb/issues/35784.
		//{
		//	// update privilege after prepare
		//	setFunc: func(tk *testkit.TestKit, conn server.MockConn) any {
		//		rootTk := testkit.NewTestKit(t, store)
		//		rootTk.MustExec(`CREATE USER 'u1'@'localhost'`)
		//		rootTk.MustExec("create table test.t1(id int)")
		//		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
		//		rootTk.MustExec(`GRANT SELECT ON test.t1 TO 'u1'@'localhost'`)
		//		tk.MustExec("prepare stmt from 'select * from test.t1'")
		//		rootTk.MustExec(`REVOKE SELECT ON test.t1 FROM 'u1'@'localhost'`)
		//		return nil
		//	},
		//	prepareFunc: func(tk *testkit.TestKit, conn server.MockConn) {
		//		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
		//	},
		//	restoreErr: errno.ErrNoSuchTable,
		//	cleanFunc: func(tk *testkit.TestKit) {
		//		rootTk := testkit.NewTestKit(t, store)
		//		rootTk.MustExec("drop user 'u1'@'localhost'")
		//		rootTk.MustExec("drop table test.t1")
		//	},
		//},
	}

	for _, tt := range tests {
		conn1 := server.CreateMockConn(t, sv)
		tk1 := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
		conn1.Context().Session.GetSessionVars().User = nil
		var param any
		if tt.setFunc != nil {
			param = tt.setFunc(tk1, conn1)
		}
		conn2 := server.CreateMockConn(t, sv)
		tk2 := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
		rows := tk1.MustQuery("show session_states").Rows()
		require.Len(t, rows, 1)
		state := rows[0][0].(string)
		state = strings.ReplaceAll(state, "\\", "\\\\")
		state = strings.ReplaceAll(state, "'", "\\'")
		setSQL := fmt.Sprintf("set session_states '%s'", state)
		if tt.restoreErr != 0 {
			tk2.MustGetErrCode(setSQL, tt.restoreErr)
		} else {
			tk2.MustExec(setSQL)
			tt.checkFunc(tk2, conn2, param)
		}
		if tt.cleanFunc != nil {
			tt.cleanFunc(tk1)
		}
		conn1.Close()
		conn2.Close()
	}
}

func TestSQLBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t1(id int primary key, name varchar(10), key(name))")

	tests := []struct {
		setFunc    func(tk *testkit.TestKit) any
		checkFunc  func(tk *testkit.TestKit, param any)
		restoreErr int
		cleanFunc  func(tk *testkit.TestKit)
	}{
		{
			// no bindings
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show session bindings").Check(testkit.Rows())
			},
		},
		{
			// use binding and drop it
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				rows := tk.MustQuery("show session bindings").Rows()
				require.Equal(t, 1, len(rows))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show session bindings").Check(param.([][]any))
				require.True(t, tk.HasPlan("select * from test.t1", "IndexFullScan"))
				tk.MustExec("drop session binding for select * from test.t1")
				tk.MustQuery("show session bindings").Check(testkit.Rows())
			},
		},
		{
			// use hint
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select /*+ use_index(test.t1, name) */ * from test.t1")
				rows := tk.MustQuery("show session bindings").Rows()
				require.Equal(t, 1, len(rows))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show session bindings").Check(param.([][]any))
				require.True(t, tk.HasPlan("select * from test.t1", "IndexFullScan"))
				tk.MustExec("drop session binding for select * from test.t1")
				tk.MustQuery("show session bindings").Check(testkit.Rows())
			},
		},
		{
			// drop binding
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				tk.MustExec("drop session binding for select * from test.t1")
				return nil
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show session bindings").Check(testkit.Rows())
			},
		},
		{
			// default db
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("use test")
				tk.MustExec("create session binding for select * from t1 using select * from t1 use index(name)")
				tk.MustExec("use mysql")
				rows := tk.MustQuery("show session bindings").Rows()
				require.Equal(t, 1, len(rows))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show session bindings").Check(param.([][]any))
				require.True(t, tk.HasPlan("select * from test.t1", "IndexFullScan"))
			},
		},
		{
			// drop table
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				tk.MustExec("drop table test.t1")
				return nil
			},
			restoreErr: errno.ErrNoSuchTable,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("create table test.t1(id int primary key, name varchar(10), key(name))")
			},
		},
		{
			// drop db
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create database test1")
				tk.MustExec("use test1")
				tk.MustExec("create table t1(id int primary key, name varchar(10), key(name))")
				tk.MustExec("create session binding for select * from t1 using select /*+ use_index(t1, name) */ * from t1")
				tk.MustExec("drop database test1")
				return nil
			},
			restoreErr: errno.ErrNoSuchTable,
		},
		{
			// alter the table
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				tk.MustExec("alter table test.t1 drop index name")
				return nil
			},
			restoreErr: errno.ErrKeyDoesNotExist,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("alter table test.t1 add index name(name)")
			},
		},
		{
			// both global and session bindings
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create global binding for select * from test.t1 using select * from test.t1 use index(primary)")
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				sessionRows := tk.MustQuery("show bindings").Rows()
				require.Equal(t, 1, len(sessionRows))
				globalRows := tk.MustQuery("show global bindings").Rows()
				require.Equal(t, 1, len(globalRows))
				return [][][]any{sessionRows, globalRows}
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				rows := param.([][][]any)
				tk.MustQuery("show bindings").Check(rows[0])
				tk.MustQuery("show global bindings").Check(rows[1])
				require.True(t, tk.HasPlan("select * from test.t1", "IndexFullScan"))
				tk.MustExec("drop session binding for select * from test.t1")
				require.True(t, tk.HasPlan("select * from test.t1", "TableFullScan"))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop global binding for select * from test.t1")
			},
		},
		{
			// multiple bindings
			setFunc: func(tk *testkit.TestKit) any {
				tk.MustExec("create session binding for select * from test.t1 using select * from test.t1 use index(name)")
				tk.MustExec("create session binding for select count(*) from test.t1 using select count(*) from test.t1 use index(primary)")
				tk.MustExec("create session binding for select name from test.t1 using select name from test.t1 use index(primary)")
				rows := tk.MustQuery("show bindings").Rows()
				require.Equal(t, 3, len(rows))
				return rows
			},
			checkFunc: func(tk *testkit.TestKit, param any) {
				tk.MustQuery("show bindings").Check(param.([][]any))
				require.True(t, tk.HasPlan("select * from test.t1", "IndexFullScan"))
			},
		},
	}

	for _, tt := range tests {
		tk1 := testkit.NewTestKit(t, store)
		var param any
		if tt.setFunc != nil {
			param = tt.setFunc(tk1)
		}
		rows := tk1.MustQuery("show session_states").Rows()
		require.Len(t, rows, 1)
		state := rows[0][0].(string)
		state = strconv.Quote(state)
		setSQL := fmt.Sprintf("set session_states %s", state)
		tk2 := testkit.NewTestKit(t, store)
		if tt.restoreErr != 0 {
			tk2.MustGetErrCode(setSQL, tt.restoreErr)
		} else {
			tk2.MustExec(setSQL)
			tt.checkFunc(tk2, param)
		}
		if tt.cleanFunc != nil {
			tt.cleanFunc(tk1)
		}
	}
}

func TestShowStateFail(t *testing.T) {
	store := testkit.CreateMockStore(t)
	sv := server.CreateMockServer(t, store)
	defer sv.Close()

	tests := []struct {
		setFunc   func(tk *testkit.TestKit, conn server.MockConn)
		showErr   int
		cleanFunc func(tk *testkit.TestKit)
	}{
		{
			// in an active transaction
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("begin")
			},
			showErr: errno.ErrCannotMigrateSession,
		},
		{
			// out of transaction
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("begin")
				tk.MustExec("commit")
			},
		},
		{
			// created a global temporary table
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create global temporary table test.t1(id int) on commit delete rows")
				tk.MustExec("insert into test.t1 value(1)")
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// created a local temporary table
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create temporary table test.t1(id int)")
			},
			showErr: errno.ErrCannotMigrateSession,
		},
		{
			// drop the local temporary table
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create temporary table test.t1(id int)")
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// hold and advisory lock
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustQuery("SELECT get_lock('testlock1', 0)").Check(testkit.Rows("1"))
			},
			showErr: errno.ErrCannotMigrateSession,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustQuery("SELECT release_lock('testlock1')").Check(testkit.Rows("1"))
			},
		},
		{
			// release the advisory lock
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustQuery("SELECT get_lock('testlock1', 0)").Check(testkit.Rows("1"))
				tk.MustQuery("SELECT release_lock('testlock1')").Check(testkit.Rows("1"))
			},
		},
		{
			// hold table locks
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("lock tables test.t1 write")
				tk.MustQuery("show warnings").Check(testkit.Rows())
			},
			showErr: errno.ErrCannotMigrateSession,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// unlock the tables
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("lock tables test.t1 write")
				tk.MustExec("unlock tables")
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// enable sandbox mode
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.Session().EnableSandBoxMode()
			},
			showErr: errno.ErrCannotMigrateSession,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.Session().DisableSandBoxMode()
			},
		},
		{
			// after COM_STMT_SEND_LONG_DATA
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select ?")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getLongDataBytes(1, 0, []byte("abc"))
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
			showErr: errno.ErrCannotMigrateSession,
		},
		{
			// after COM_STMT_SEND_LONG_DATA and COM_STMT_EXECUTE
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select ?")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getLongDataBytes(1, 0, []byte("abc"))
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, false, true, paramInfo{value: 1, isNull: false})
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
		},
		{
			// query with cursor, and data is not fetched
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert test.t1 value(1), (2), (3)")
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select * from test.t1")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, true, false)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
			showErr: errno.ErrCannotMigrateSession,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// fetched all the data but the EOF packet is not sent
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert test.t1 value(1), (2), (3)")
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select * from test.t1")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, true, false)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getFetchBytes(1, 10)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
			showErr: errno.ErrCannotMigrateSession,
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// EOF is sent
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert test.t1 value(1), (2), (3)")
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select * from test.t1")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, true, false)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getFetchBytes(1, 10)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				// This COM_STMT_FETCH returns EOF.
				cmd = getFetchBytes(1, 10)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
		{
			// statement is reset
			setFunc: func(tk *testkit.TestKit, conn server.MockConn) {
				tk.MustExec("create table test.t1(id int)")
				tk.MustExec("insert test.t1 value(1), (2), (3)")
				cmd := append([]byte{mysql.ComStmtPrepare}, []byte("select * from test.t1")...)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getExecuteBytes(1, true, false)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
				cmd = getResetBytes(1)
				require.NoError(t, conn.Dispatch(context.Background(), cmd))
			},
			cleanFunc: func(tk *testkit.TestKit) {
				tk.MustExec("drop table test.t1")
			},
		},
	}

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	for _, tt := range tests {
		conn1 := server.CreateMockConn(t, sv)
		conn1.Context().Session.GetSessionVars().User = nil
		tk1 := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
		tt.setFunc(tk1, conn1)
		if tt.showErr == 0 {
			tk2 := testkit.NewTestKit(t, store)
			showSessionStatesAndSet(t, tk1, tk2)
		} else {
			err := tk1.QueryToErr("show session_states")
			errEqualsCode(t, err, tt.showErr)
		}
		if tt.cleanFunc != nil {
			tt.cleanFunc(tk1)
		}
		conn1.Close()
	}
}

func showSessionStatesAndSet(t *testing.T, tk1, tk2 *testkit.TestKit) {
	rows := tk1.MustQuery("show session_states").Rows()
	require.Len(t, rows, 1)
	state := rows[0][0].(string)
	state = strconv.Quote(state)
	setSQL := fmt.Sprintf("set session_states %s", state)
	tk2.MustExec(setSQL)
}

func errEqualsCode(t *testing.T, err error, code int) {
	require.NotNil(t, err)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, code, int(sqlErr.Code))
}

// create bytes for COM_STMT_SEND_LONG_DATA
func getLongDataBytes(stmtID uint32, paramID uint16, param []byte) []byte {
	buf := make([]byte, 7+len(param))
	pos := 0
	buf[pos] = mysql.ComStmtSendLongData
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], stmtID)
	pos += 4
	binary.LittleEndian.PutUint16(buf[pos:], paramID)
	pos += 2
	buf = append(buf[:pos], param...)
	return buf
}

type paramInfo struct {
	value  uint32
	isNull bool
}

// create bytes for COM_STMT_EXECUTE. It only supports int type for convenience.
func getExecuteBytes(stmtID uint32, useCursor bool, newParam bool, params ...paramInfo) []byte {
	nullBitmapLen := (len(params) + 7) >> 3
	buf := make([]byte, 11+nullBitmapLen+len(params)*6)
	pos := 0
	buf[pos] = mysql.ComStmtExecute
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], stmtID)
	pos += 4
	if useCursor {
		buf[pos] = 1
	}
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], 1)
	pos += 4
	for i, param := range params {
		if param.isNull {
			buf[pos+(i>>3)] |= 1 << (i % 8)
		}
	}
	pos += nullBitmapLen
	if newParam {
		buf[pos] = 1
		pos++
		for i := 0; i < len(params); i++ {
			buf[pos] = mysql.TypeLong
			pos++
			buf[pos] = 0
			pos++
		}
	} else {
		buf[pos] = 0
		pos++
	}
	for _, param := range params {
		if !param.isNull {
			binary.LittleEndian.PutUint32(buf[pos:], param.value)
			pos += 4
		}
	}
	return buf[:pos]
}

// create bytes for COM_STMT_FETCH.
func getFetchBytes(stmtID, fetchSize uint32) []byte {
	buf := make([]byte, 9)
	pos := 0
	buf[pos] = mysql.ComStmtFetch
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], stmtID)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:], fetchSize)
	return buf
}

// create bytes for COM_STMT_FETCH.
func getResetBytes(stmtID uint32) []byte {
	buf := make([]byte, 5)
	pos := 0
	buf[pos] = mysql.ComStmtReset
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], stmtID)
	return buf
}
