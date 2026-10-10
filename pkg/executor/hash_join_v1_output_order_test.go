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
	"testing"

	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestNaturalRightJoinHashJoinV1OutputOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setupTK := testkit.NewTestKit(t, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("CREATE TABLE t1(c0 SMALLINT)")
	setupTK.MustExec("CREATE TABLE t4(c0 BIGINT DEFAULT 1383675574, c2 BLOB(55))")
	setupTK.MustExec("INSERT IGNORE INTO t4 VALUES(NULL, '') ON DUPLICATE KEY UPDATE c0=t4.c2")
	setupTK.MustExec("INSERT IGNORE INTO t4(c2) VALUES('MZ')")
	setupTK.MustExec("DELETE FROM mysql.opt_rule_blacklist")
	setupTK.MustExec("INSERT INTO mysql.opt_rule_blacklist VALUES('predicate_push_down'),('column_prune'),('projection_eliminate')")
	setupTK.MustExec("ADMIN RELOAD opt_rule_blacklist")
	t.Cleanup(func() {
		setupTK.MustExec("DELETE FROM mysql.opt_rule_blacklist")
		setupTK.MustExec("ADMIN RELOAD opt_rule_blacklist")
	})

	originalQuery := "SELECT t4.c2, t4.c0 FROM t1 NATURAL RIGHT JOIN t4 WHERE t4.c0"
	explicitRightJoin := "SELECT t4.c2, t4.c0 FROM t1 RIGHT JOIN t4 ON t1.c0=t4.c0 WHERE t4.c0"
	swappedLeftJoin := "SELECT t4.c2, t4.c0 FROM t4 LEFT JOIN t1 ON t4.c0=t1.c0 WHERE t4.c0"
	expected := testkit.Rows("MZ 1383675574")
	hashJoinVersions := []struct {
		name string
		sql  string
	}{
		{name: "legacy", sql: join.DisableHashJoinV2},
		{name: "optimized", sql: join.EnableHashJoinV2},
	}

	for _, vectorized := range []string{"OFF", "ON"} {
		for _, hashJoinVersion := range hashJoinVersions {
			t.Run(vectorized+"/"+hashJoinVersion.name, func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("SET @@tidb_enable_vectorized_expression=" + vectorized)
				tk.MustExec(hashJoinVersion.sql)

				tk.MustQuery("EXPLAIN FORMAT='brief' " + originalQuery).
					CheckContain("CARTESIAN right outer join")
				tk.MustQuery(originalQuery).Check(expected)
				tk.MustQuery(explicitRightJoin).Check(expected)
				tk.MustQuery(swappedLeftJoin).Check(expected)
			})
		}
	}
}
