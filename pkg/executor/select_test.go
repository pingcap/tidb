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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func BenchmarkResetContextOfStmt(b *testing.B) {
	stmt := &ast.SelectStmt{}
	ctx := mock.NewContext()
	ctx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	for i := 0; i < b.N; i++ {
		executor.ResetContextOfStmt(ctx, stmt)
	}
}

func TestResetContextOfStmtTiKVShortCircuitExpression(t *testing.T) {
	ctx := mock.NewContext()
	ctx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	vars := ctx.GetSessionVars()

	vars.EnableTiKVShortCircuitExpression = false
	vars.StmtCtx.EnableTiKVShortCircuitExpression = true
	require.NoError(t, executor.ResetContextOfStmt(ctx, &ast.SelectStmt{}))
	require.False(t, vars.StmtCtx.EnableTiKVShortCircuitExpression)

	vars.EnableTiKVShortCircuitExpression = true
	vars.StmtCtx.EnableTiKVShortCircuitExpression = false
	require.NoError(t, executor.ResetContextOfStmt(ctx, &ast.SelectStmt{}))
	require.True(t, vars.StmtCtx.EnableTiKVShortCircuitExpression)
}

func TestImportIntoShouldHaveSameFlagsAsInsert(t *testing.T) {
	insertStmt := &ast.InsertStmt{}
	importStmt := &ast.ImportIntoStmt{}
	insertCtx := mock.NewContext()
	importCtx := mock.NewContext()
	insertCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	importCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	for _, modeStr := range []string{
		"",
		"IGNORE_SPACE",
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
		"ALLOW_INVALID_DATES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"NO_ZERO_IN_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_IN_DATE,NO_ZERO_DATE,STRICT_ALL_TABLES",
	} {
		t.Run(fmt.Sprintf("mode %s", modeStr), func(t *testing.T) {
			mode, err := mysql.GetSQLMode(modeStr)
			require.NoError(t, err)
			insertCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, executor.ResetContextOfStmt(insertCtx, insertStmt))
			importCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, executor.ResetContextOfStmt(importCtx, importStmt))

			insertTypeCtx := insertCtx.GetSessionVars().StmtCtx.TypeCtx()
			importTypeCtx := importCtx.GetSessionVars().StmtCtx.TypeCtx()
			require.EqualValues(t, insertTypeCtx.Flags(), importTypeCtx.Flags())
		})
	}
}

func TestFoundRowsAfterShowAndExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1), (2), (2)")

	t.Run("found rows after explain", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		target := testkit.NewTestKit(t, store)
		target.MustExec("use test")
		target.MustQuery("select * from t")
		process := target.Session().ShowProcess()
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*sessmgr.ProcessInfo{process}})
		for _, sql := range []string{
			"explain select * from t limit 1",
			"describe select * from t limit 1",
			fmt.Sprintf("explain for connection %d", process.ID),
		} {
			tk.MustQuery("select * from t")
			tk.MustQuery(sql)
			tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
		}
		tk.MustQuery("select * from t")
		tk.MustQuery("describe t")
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("prepare explain_found_rows from 'explain select * from t limit 1'")
		for range 2 {
			tk.MustQuery("select * from t")
			tk.MustQuery("execute explain_found_rows")
			tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
		}
		tk.MustExec("deallocate prepare explain_found_rows")
	})

	t.Run("found rows after show", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create view found_rows_view as select a from t")
		tk.MustExec("create sequence found_rows_seq")
		tk.MustExec("create placement policy found_rows_policy followers=1")
		tk.MustExec("create user 'found_rows_show'@'localhost'")
		defer tk.MustExec("drop user 'found_rows_show'@'localhost'")
		cases := []struct {
			query    string
			preserve bool
		}{
			{"show warnings", true},
			{"show errors", true},
			{"show create table t", true},
			{"show create view found_rows_view", true},
			{"show create sequence found_rows_seq", true},
			{"show create placement policy found_rows_policy", true},
			{"show create resource group default", true},
			{"show create database test", true},
			{"show create user 'found_rows_show'@'localhost'", true},
			{"show grants for 'found_rows_show'@'localhost'", true},
			{"show privileges", true},
			{"show profiles", true},
			{"show master status", true},
			{"show binary log status", true},
			{"show processlist", true},
			{"show session_states", true},
			{"show count(*) warnings", false},
			{"show count(*) errors", false},
			{"show tables like 't'", false},
			{"show columns from t", false},
			{"show variables like 'autocommit'", false},
			{"show status like 'Threads_connected'", false},
			{"show engines", false},
			{"show plugins", false},
		}
		for _, tc := range cases {
			t.Run(tc.query, func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
				tk.MustExec("use test")
				tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1", "2", "2"))
				rows := tk.MustQuery(tc.query).Rows()
				expected := len(rows)
				if tc.preserve {
					expected = 3
				}
				tk.MustQuery("select found_rows()").Check(testkit.Rows(fmt.Sprint(expected)))
				// Reading FOUND_ROWS() itself is a one-row SELECT, even after a preserving SHOW.
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
			})
		}
		// Nonempty diagnostics preserve both the previous count and the warning area.
		tk.MustQuery("select if(a=1, cast('bad' as signed), a) from t")
		require.Len(t, tk.MustQuery("show warnings").Rows(), 1)
		tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
		tk.MustQuery("select if(a=1, cast('bad' as signed), a) from t")
		require.Len(t, tk.MustQuery("show warnings").Rows(), 1)
		tk.MustQuery("show count(*) warnings").Check(testkit.Rows("1"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))

		// EXECUTE must classify the prepared SHOW, rather than the ExecuteStmt wrapper.
		tk.MustExec("prepare show_found_rows from 'show create table t'")
		for range 2 {
			tk.MustQuery("select * from t")
			tk.MustQuery("execute show_found_rows")
			tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
		}
		tk.MustExec("deallocate prepare show_found_rows")
	})

	t.Run("mysql tester warning polling", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		// mysql-tester polls SHOW WARNINGS after each query. That diagnostic
		// result must not replace the preceding SELECT's row count.
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("3"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("select count(*) from t limit 2,3").Check(testkit.Rows())
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("0"))
		tk.MustExec("prepare stmt from 'select found_rows()'")
		for _, expected := range []string{"0", "1"} {
			tk.MustQuery("select a from t where a = " + expected)
			tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("execute stmt").Check(testkit.Rows(expected))
			tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("execute stmt").Check(testkit.Rows("1"))
		}
		tk.MustExec("deallocate prepare stmt")
	})
}
