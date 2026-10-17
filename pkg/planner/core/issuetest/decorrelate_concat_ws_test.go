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

package issuetest

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDecorrelateConcatWs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE mr_ast_53597 (id BIGINT PRIMARY KEY, t_cd VARCHAR(60), t_nm VARCHAR(20), KEY idx_ast_id(id))")
	tk.MustExec("INSERT INTO mr_ast_53597 VALUES (4611686018427387945, 'TIKV_REGION_PEERS', 'xxx')")

	concatWS := `SELECT
		(SELECT CONCAT_WS('', t.t_cd, ':')
		 FROM mr_ast_53597 AS t
		 WHERE t.id = a.id AND t.t_nm IS NULL) AS val,
		a.t_cd
	FROM mr_ast_53597 AS a
	WHERE a.id = 4611686018427387945`
	tk.MustQuery(concatWS).Check(testkit.Rows("<nil> TIKV_REGION_PEERS"))
	hintedConcatWS := strings.Replace(concatWS, "SELECT CONCAT_WS", "SELECT /*+ NO_DECORRELATE() */ CONCAT_WS", 1)
	tk.MustQuery(hintedConcatWS).
		Check(testkit.Rows("<nil> TIKV_REGION_PEERS"))
	requirePlanContains(t, tk, concatWS, "Apply")

	coalesce := `SELECT
		(SELECT COALESCE(t.t_cd, 'fallback')
		 FROM mr_ast_53597 AS t
		 WHERE t.id = a.id AND t.t_nm IS NULL) AS val,
		a.t_cd
	FROM mr_ast_53597 AS a
	WHERE a.id = 4611686018427387945`
	tk.MustQuery(coalesce).Check(testkit.Rows("<nil> TIKV_REGION_PEERS"))
	requirePlanContains(t, tk, coalesce, "Apply")

	concat := `SELECT
		(SELECT CONCAT(t.t_cd, ':')
		 FROM mr_ast_53597 AS t
		 WHERE t.id = a.id AND t.t_nm IS NULL) AS val,
		a.t_cd
	FROM mr_ast_53597 AS a
	WHERE a.id = 4611686018427387945`
	tk.MustQuery(concat).Check(testkit.Rows("<nil> TIKV_REGION_PEERS"))
	requirePlanNotContains(t, tk, concat, "Apply")
	hintedConcat := strings.Replace(concat, "SELECT CONCAT", "SELECT /*+ NO_DECORRELATE() */ CONCAT", 1)
	tk.MustQuery(hintedConcat).Check(testkit.Rows("<nil> TIKV_REGION_PEERS"))
	requirePlanContains(t, tk, hintedConcat, "Apply")

	tk.MustExec("INSERT INTO mr_ast_53597 VALUES (2, 'MATCH', NULL)")
	matched := strings.Replace(concatWS, "4611686018427387945", "2", 1)
	tk.MustQuery(matched).Check(testkit.Rows("MATCH: MATCH"))
}

func requirePlanContains(t *testing.T, tk *testkit.TestKit, sql, expected string) {
	t.Helper()
	plan := tk.MustQuery("EXPLAIN FORMAT='brief' " + sql).String()
	if !strings.Contains(plan, expected) {
		t.Fatalf("expected plan to contain %q, got:\n%s", expected, plan)
	}
}

func requirePlanNotContains(t *testing.T, tk *testkit.TestKit, sql, unexpected string) {
	t.Helper()
	plan := tk.MustQuery("EXPLAIN FORMAT='brief' " + sql).String()
	if strings.Contains(plan, unexpected) {
		t.Fatalf("expected plan not to contain %q, got:\n%s", unexpected, plan)
	}
}
