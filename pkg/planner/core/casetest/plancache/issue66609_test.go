// Copyright 2025 PingCAP, Inc.
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

package plancache

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestParamMarkerConstantPredicateElimination tests that always-true/false
// ParamMarker constant predicates (e.g. WHERE ?) are eliminated, and that the
// plan is correctly marked as uncacheable.
// See https://github.com/pingcap/tidb/issues/66609
func TestParamMarkerConstantPredicateElimination(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")

	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	// Case 1: WHERE ? with ?=true should eliminate the Selection and NOT cache.
	tk.MustExec(`prepare stmt1 from 'select * from t where ?'`)
	tk.MustExec(`set @v=true`)
	tk.MustQuery(`execute stmt1 using @v`).Sort().Check(testkit.Rows("1 10", "2 20", "3 30"))
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	for _, row := range rows {
		op := row[0].(string)
		require.False(t, strings.Contains(op, "Selection"),
			"WHERE true should not produce a Selection node, got: %s", op)
	}
	// Execute again; plan should NOT come from cache.
	tk.MustQuery(`execute stmt1 using @v`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// Case 2: WHERE ? with ?=false should return no rows and NOT cache.
	tk.MustExec(`set @v=false`)
	tk.MustQuery(`execute stmt1 using @v`).Check(testkit.Rows()) // no rows
	tk.MustQuery(`execute stmt1 using @v`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`deallocate prepare stmt1`)

	// Case 3: Normal WHERE col = ? should still be cacheable.
	tk.MustExec(`prepare stmt2 from 'select * from t where a = ?'`)
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute stmt2 using @a`).Check(testkit.Rows("1 10"))
	tk.MustExec(`set @a=2`)
	tk.MustQuery(`execute stmt2 using @a`).Check(testkit.Rows("2 20"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec(`deallocate prepare stmt2`)

	// Case 4: HAVING ? with ?=true should eliminate the HAVING Selection and NOT cache.
	tk.MustExec(`prepare stmt3 from 'select a, sum(b) from t group by a having ?'`)
	tk.MustExec(`set @v=true`)
	tk.MustQuery(`execute stmt3 using @v`).Sort().Check(testkit.Rows("1 10", "2 20", "3 30"))
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	for _, row := range rows {
		op := row[0].(string)
		if strings.Contains(op, "Selection") {
			t.Fatalf("HAVING true should not produce a Selection node, got: %s", op)
		}
	}
	tk.MustQuery(`execute stmt3 using @v`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`deallocate prepare stmt3`)
}
