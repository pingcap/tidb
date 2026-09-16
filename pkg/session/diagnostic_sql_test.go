// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticSQLAllowlist(t *testing.T) {
	p := parser.New()
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "select", sql: "explain select 1", want: true},
		{name: "union", sql: "explain select 1 union all select 2", want: true},
		{name: "cte", sql: "explain with cte as (select 1 as a) select a from cte", want: true},
		{name: "scalar subquery", sql: "explain select (select 1)", want: false},
		{name: "where scalar subquery", sql: "explain select * from rider_reputation where lifetime_score < (select min(country_code) from rider_reputation)", want: false},
		{name: "exists subquery", sql: "explain select 1 where exists (select 1)", want: false},
		{name: "use", sql: "use test", want: true},
		{name: "show databases", sql: "show databases", want: true},
		{name: "show tables", sql: "show tables", want: true},
		{name: "show create table", sql: "show create table mysql.user", want: true},
		{name: "show create database", sql: "show create database test", want: true},
		{name: "show where assignment", sql: "show tables where @x := 1", want: false},
		{name: "show regions", sql: "show table t regions", want: false},
		{name: "show backups", sql: "show backups", want: false},
		{name: "analyze", sql: "explain analyze select 1", want: false},
		{name: "insert", sql: "explain insert into t values (1)", want: false},
		{name: "update", sql: "explain update t set a = 1", want: false},
		{name: "delete", sql: "explain delete from t", want: false},
		{name: "show unsupported", sql: "show profile", want: false},
		{name: "transaction", sql: "begin", want: false},
		{name: "ddl", sql: "create table t (a int)", want: false},
		{name: "prepared", sql: "prepare s from 'select 1'", want: false},
		{name: "lock", sql: "explain select * from t for update", want: false},
		{name: "outfile", sql: "explain select 1 into outfile '/tmp/diagnostic.out'", want: false},
		{name: "nextval", sql: "explain select nextval(seq)", want: false},
		{name: "setval", sql: "explain select setval(seq, 1)", want: false},
		{name: "advisory lock", sql: "explain select get_lock('diagnostic', 0)", want: false},
		{name: "last insert id assignment", sql: "explain select last_insert_id(1)", want: false},
		{name: "set var hint", sql: "explain select /*+ set_var(max_execution_time=1) */ 1", want: false},
		{name: "variable assignment", sql: "explain select @x := 1", want: false},
		{name: "nested assignment", sql: "explain select * from (select @x := 1 as a) t", want: false},
		{name: "union values", sql: "explain select 1 union all values row(2)", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(test.sql, "", "")
			require.NoError(t, err)
			require.Equal(t, test.want, isDiagnosticSQLAllowed(stmt))
		})
	}

	stmt, err := p.ParseOneStmt("explain select 1", "", "")
	require.NoError(t, err)
	require.IsType(t, &ast.ExplainStmt{}, stmt)
}

func TestDiagnosticSQLExecutionGuard(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(se.Close)

	t.Run("diagnostic mode", func(t *testing.T) {
		restore := diagnosticmode.SetForTest(true)
		t.Cleanup(restore)

		resultSets, err := se.Execute(context.Background(), "explain select 1")
		require.NoError(t, err)
		require.Len(t, resultSets, 1)
		require.NoError(t, resultSets[0].Close())

		for _, sql := range []string{
			"use test",
			"show databases",
			"show tables",
			"show create table mysql.user",
		} {
			resultSets, err = se.Execute(context.Background(), sql)
			require.NoError(t, err, sql)
			for _, resultSet := range resultSets {
				require.NoError(t, resultSet.Close(), sql)
			}
		}

		for _, sql := range []string{
			"select 1",
			"insert into t values (1)",
			"begin",
			"create table t (a int)",
			"explain analyze select 1",
			"explain select 1 for update",
			"set global tidb_distsql_scan_concurrency=5;",
			"explain select * from t where t.a < (select max(b) from t);",
		} {
			_, err = se.Execute(context.Background(), sql)
			require.Error(t, err, sql)
			require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode), sql)
		}

		// Binary protocol prepare is rejected before it can prepare a transaction;
		// it cannot be used to bypass the SQL allowlist.
		_, _, _, err = se.PrepareStmt("select 1")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode))

		// Internal restricted SQL is used by diagnostic startup and must remain
		// available even while the process-wide diagnostic mode is enabled.
		rs, err := se.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers), "select 1")
		require.NoError(t, err)
		require.NoError(t, rs.Close())
	})

	t.Run("normal mode", func(t *testing.T) {
		restore := diagnosticmode.SetForTest(false)
		t.Cleanup(restore)
		resultSets, err := se.Execute(context.Background(), "select 1")
		require.NoError(t, err)
		require.Len(t, resultSets, 1)
		require.NoError(t, resultSets[0].Close())
	})
}
