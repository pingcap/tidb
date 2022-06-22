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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sem"
	"github.com/stretchr/testify/require"
)

func TestGrammar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show session_states").Rows()
	require.Len(t, rows, 1)
	tk.MustExec("set session_states '{}'")
	tk.MustGetErrCode("set session_states 1", errno.ErrParse)
}

func TestUserVars(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
			inSessionStates: false,
			varName:         variable.TiDBTxnReadTS,
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
			varName:         variable.TiDBAllowRemoveAutoInc,
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

	sem.Enable()
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

func showSessionStatesAndSet(t *testing.T, tk1, tk2 *testkit.TestKit) {
	rows := tk1.MustQuery("show session_states").Rows()
	require.Len(t, rows, 1)
	state := rows[0][0].(string)
	state = strconv.Quote(state)
	setSQL := fmt.Sprintf("set session_states %s", state)
	tk2.MustExec(setSQL)
}
