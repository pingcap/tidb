// Copyright 2015 PingCAP, Inc.
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

//go:build fusion

package commontest

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPreparedPointGetFastPathBasic(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		if unistore.CheckResourceTagForTopSQLInGoTest {
			// fast path won't set topsql tag, disable the check temporarily.
			unistore.CheckResourceTagForTopSQLInGoTest = false
			defer func() {
				unistore.CheckResourceTagForTopSQLInGoTest = true
			}()
		}

		dbt.MustExec("use test")
		dbt.MustExec("set global tidbx_fast_path = 1")

		schemas := []string{
			"create table t1 (id int key, b varchar(64))",
			"create table t1 (id int, b varchar(64), unique index idx (id))",
		}
		stmtQuery := func(stmt *sql.Stmt, args []any, results ...string) {
			rows, err := stmt.Query(args...)
			require.NoError(t, err)
			ts.CheckRows(t, rows, results...)
		}
		for _, schema := range schemas {
			dbt.MustExec("drop table if exists t1;")
			dbt.MustExec(schema)
			dbt.MustExec("insert into t1 values (1, 'a'), (2, 'b');")

			ctx := context.Background()
			conn, err := dbt.GetDB().Conn(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, conn.Close())
			}()

			stmt, err := conn.PrepareContext(ctx, "select * from t1 where id = ?")
			require.NoError(t, err)
			stmt2, err := conn.PrepareContext(ctx, "select * from t1 where id in (?, ?)")
			require.NoError(t, err)
			_ = stmt2

			stmtQuery(stmt, []any{1}, "1 a")
			stmtQuery(stmt, []any{2}, "2 b")
			stmtQuery(stmt2, []any{1, 2}, "1 a", "2 b")
			for i := 0; i < 10; i++ {
				if i == 5 {
					ts.RunTests(t, nil, func(dbt2 *testkit.DBTestKit) {
						dbt2.MustExec("use test")
						dbt2.MustExec("update t1 set b = 'bb' where id = 2")
					})
				}
				if i < 5 {
					stmtQuery(stmt, []any{2}, "2 b")
					stmtQuery(stmt2, []any{1, 2}, "1 a", "2 b")
				} else {
					stmtQuery(stmt, []any{2}, "2 bb")
					stmtQuery(stmt2, []any{1, 2}, "1 a", "2 bb")
				}
			}
		}
	})
}
