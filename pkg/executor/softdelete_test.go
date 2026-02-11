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

package executor_test

import (
	"io"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSoftDeleteAffectedRowsAndStmtMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_normal")
	tk.MustExec("drop table if exists t_soft")
	tk.MustExec("create table t_normal (id int primary key, v int)")
	tk.MustExec("create table t_soft (id int primary key, v int) softdelete retention 7 day")

	prepare := func(insert, deleteIn string, rewriteMode bool) {
		tk.MustExec("set @@tidb_translate_softdelete_sql = 'OFF'")
		tk.MustExec("delete from t_normal")
		tk.MustExec("delete from t_soft")
		tk.MustExec("insert into t_normal values" + insert)
		tk.MustExec("insert into t_soft values" + insert)

		tk.MustExec("set @@tidb_translate_softdelete_sql = 'ON'")
		tk.MustExec("delete from t_soft where id in " + deleteIn)
		if rewriteMode {
			// In rewrite mode, also delete from the normal table. When not in rewrite mode,
			// we keep tidb_translate_softdelete_sql set to 'OFF' to preserve the row count behavior.
			tk.MustExec("delete from t_normal where id in " + deleteIn)
		} else {
			tk.MustExec("set @@tidb_translate_softdelete_sql = 'OFF'")
		}
	}

	type stmtResult struct {
		affectedRows uint64
		message      string
	}

	setupLoadDataReader := func(data string) func() {
		sctx := tk.Session().(sessionctx.Context)
		var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (io.ReadCloser, error) {
			return mydump.NewStringReader(data), nil
		}
		sctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
		return func() {
			sctx.SetValue(executor.LoadDataReaderBuilderKey, nil)
		}
	}

	mustExecDML := func(t *testing.T, sql string, expect stmtResult) {
		require.Contains(t, sql, "{{t}}", "the sql must contain {{t}} placeholder")
		normalSQL := strings.ReplaceAll(sql, "{{t}}", "t_normal")
		tk.MustExec(normalSQL)
		normalResult := stmtResult{
			affectedRows: tk.Session().AffectedRows(),
			message:      tk.Session().GetSessionVars().StmtCtx.GetMessage(),
		}
		require.Equal(t, expect, normalResult)

		softSQL := strings.ReplaceAll(sql, "{{t}}", "t_soft")
		tk.MustExec(softSQL)
		softResult := stmtResult{
			affectedRows: tk.Session().AffectedRows(),
			message:      tk.Session().GetSessionVars().StmtCtx.GetMessage(),
		}
		require.Equal(t, normalResult, softResult)
	}

	t.Run("insert", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		mustExecDML(t, "INSERT INTO {{t}} VALUES (2, 200), (3, 300)", stmtResult{
			affectedRows: 2,
			message:      "Records: 2  Duplicates: 0  Warnings: 0",
		})
	})

	t.Run("insert ignore", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		mustExecDML(t, "INSERT IGNORE INTO {{t}} VALUES (1, 100), (2, 200), (3, 300)", stmtResult{
			affectedRows: 2,
			message:      "Records: 3  Duplicates: 1  Warnings: 1",
		})
	})

	t.Run("insert on duplicate key update", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30)", "(1, 3)", true)
		// 2 rows softdeleted, 1 row normal insert, 1 row update
		mustExecDML(t, "INSERT INTO {{t}} VALUES (1, 100), (2, 200), (3, 300), (4, 400) ON DUPLICATE KEY UPDATE v = v+1", stmtResult{
			affectedRows: 5,
			message:      "Records: 4  Duplicates: 1  Warnings: 0",
		})

		prepare("(1, 10), (2, 20), (3, 20)", "(1)", true)
		// 1 row softdeleted, 2 row update with no change
		mustExecDML(t, "INSERT INTO {{t}} VALUES (1, 100), (2, 200), (3, 300) ON DUPLICATE KEY UPDATE v = 20", stmtResult{
			affectedRows: 1,
			message:      "Records: 3  Duplicates: 0  Warnings: 0",
		})
	})

	t.Run("insert ignore on duplicate key update", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30)", "(1, 3)", true)
		// 2 rows softdeleted, 1 row normal insert, 1 row update
		mustExecDML(t, "INSERT IGNORE INTO {{t}} VALUES (1, 100), (2, 200), (3, 300), (4, 400) ON DUPLICATE KEY UPDATE v = v+1", stmtResult{
			affectedRows: 5,
			message:      "Records: 4  Duplicates: 0  Warnings: 0",
		})

		prepare("(1, 10), (2, 20), (3, 20)", "(1)", true)
		// 1 row softdeleted, 2 row update with no change
		mustExecDML(t, "INSERT IGNORE INTO {{t}} VALUES (1, 100), (2, 200), (3, 300) ON DUPLICATE KEY UPDATE v = 20", stmtResult{
			affectedRows: 1,
			message:      "Records: 3  Duplicates: 2  Warnings: 0",
		})
	})

	t.Run("insert select", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		// insert softdeleted row
		mustExecDML(t, "INSERT INTO {{t}} SELECT * FROM (SELECT 2 AS id, 200 AS v) AS s", stmtResult{
			affectedRows: 1,
			message:      "Records: 1  Duplicates: 0  Warnings: 0",
		})

		// insert empty result
		mustExecDML(t, "INSERT INTO {{t}} SELECT * FROM t_normal WHERE 0", stmtResult{
			affectedRows: 0,
			message:      "Records: 0  Duplicates: 0  Warnings: 0",
		})
	})

	t.Run("update", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30), (4, 40)", "(2)", true)
		// 1 unchanged row, 1 softdeleted row, 1 not matched row, 2 real updated rows
		mustExecDML(t, "UPDATE {{t}} SET v = 10 WHERE id in (1, 2, 3, 4, 5)", stmtResult{
			affectedRows: 2,
			message:      "Rows matched: 3  Changed: 2  Warnings: 0",
		})
		// no matched row
		mustExecDML(t, "UPDATE {{t}} SET v = 99 WHERE id = 2", stmtResult{
			affectedRows: 0,
			message:      "Rows matched: 0  Changed: 0  Warnings: 0",
		})
	})

	t.Run("replace", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30), (4, 40)", "(2)", true)
		// 1 softdeleted row and inserted, 1 unchanged row, 2 updated rows
		mustExecDML(t, "REPLACE INTO {{t}} VALUES (1, 10), (2, 200), (3, 31), (4, 41)", stmtResult{
			affectedRows: 6,
			message:      "Records: 4  Duplicates: 2  Warnings: 0",
		})

		prepare("(1, 10)", "(1)", true)
		// 1 softdeleted row and inserted, 2 inserted rows
		mustExecDML(t, "REPLACE INTO {{t}} VALUES (1, 10), (10, 20), (30, 40)", stmtResult{
			affectedRows: 3,
			message:      "Records: 3  Duplicates: 0  Warnings: 0",
		})
	})

	t.Run("replace select", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		// 1 softdeleted row and inserted, 1 updated row
		mustExecDML(t, "REPLACE INTO {{t}} SELECT * FROM (SELECT 1 AS id, 101 AS v UNION ALL SELECT 2, 202) AS s", stmtResult{
			affectedRows: 3,
			message:      "Records: 2  Duplicates: 1  Warnings: 0",
		})

		// empty rows
		mustExecDML(t, "REPLACE INTO {{t}} SELECT * FROM t_normal WHERE 0", stmtResult{
			affectedRows: 0,
			message:      "Records: 0  Duplicates: 0  Warnings: 0",
		})
	})

	t.Run("delete", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30)", "(2)", true)
		mustExecDML(t, "DELETE FROM {{t}}", stmtResult{
			affectedRows: 2,
			message:      "",
		})
	})

	t.Run("load data", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		defer setupLoadDataReader("2 200\n3 300")()
		mustExecDML(
			t,
			"LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' INTO TABLE {{t}} FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' (id, v)",
			stmtResult{
				affectedRows: 2,
				message:      "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0",
			},
		)
	})

	t.Run("load data ignore", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", true)
		// 1 duplicated row, 1 softdeleted row and inserted, 1 normal inserted row
		defer setupLoadDataReader("1 100\n2 200\n3 300")()
		mustExecDML(
			t,
			"LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' IGNORE INTO TABLE {{t}} FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' (id, v)",
			stmtResult{
				affectedRows: 2,
				message:      "Records: 3  Deleted: 0  Skipped: 1  Warnings: 1",
			},
		)
	})

	t.Run("load data replace", func(t *testing.T) {
		prepare("(1, 10), (2, 20), (3, 30)", "(2)", true)
		// 1 softdeleted row and inserted, 2 updated row, 4 inserted row
		defer setupLoadDataReader("1 100\n2 200\n3 300\n5 500\n6 600\n7 700\n8 800")()
		mustExecDML(
			t,
			"LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' REPLACE INTO TABLE {{t}} FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' (id, v)",
			stmtResult{
				affectedRows: 7,
				message:      "Records: 7  Deleted: 2  Skipped: 0  Warnings: 0",
			},
		)

		// 1 softdeleted row and inserted, 2 same rows
		prepare("(1, 10), (2, 20), (3, 30)", "(1)", true)
		defer setupLoadDataReader("1 10\n2 20\n3 30")()
		mustExecDML(
			t,
			"LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' REPLACE INTO TABLE {{t}} FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' (id, v)",
			stmtResult{
				affectedRows: 1,
				message:      "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0",
			},
		)
	})

	t.Run("rewrite off insert", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", false)
		mustExecDML(t, "INSERT INTO {{t}} VALUES (3, 300)", stmtResult{
			affectedRows: 1,
			message:      "",
		})
	})

	t.Run("rewrite off insert ignore", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", false)
		mustExecDML(t, "INSERT IGNORE INTO {{t}} VALUES (2, 200), (3, 300)", stmtResult{
			affectedRows: 1,
			message:      "Records: 2  Duplicates: 1  Warnings: 1",
		})
	})

	t.Run("rewrite off insert on duplicate key update", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", false)
		mustExecDML(t, "INSERT INTO {{t}} VALUES (2, 200), (3, 300) ON DUPLICATE KEY UPDATE v = 1000", stmtResult{
			affectedRows: 3,
			message:      "Records: 2  Duplicates: 1  Warnings: 0",
		})
	})

	t.Run("rewrite off replace", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", false)
		mustExecDML(t, "REPLACE INTO {{t}} VALUES (2, 200), (3, 300)", stmtResult{
			affectedRows: 3,
			message:      "Records: 2  Duplicates: 1  Warnings: 0",
		})
	})

	t.Run("rewrite off update delete", func(t *testing.T) {
		prepare("(1, 10), (2, 20)", "(2)", false)
		mustExecDML(t, "UPDATE {{t}} SET v = v + 1 WHERE id in (1, 2)", stmtResult{
			affectedRows: 2,
			message:      "Rows matched: 2  Changed: 2  Warnings: 0",
		})
		mustExecDML(t, "DELETE FROM {{t}} WHERE id in (2, 5)", stmtResult{
			affectedRows: 1,
			message:      "",
		})
	})
}
