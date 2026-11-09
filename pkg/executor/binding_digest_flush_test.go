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

package executor

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The normalizer's restore-failure hook: injected inside the
// shared normalizer at the exact value a real restore failure produces (the
// restore helper's "" error return), so the flush-time fill fails through
// the real chain instead of a post-normalization digest clear.
const fpFlushRestoreFail = "github.com/pingcap/tidb/pkg/bindinfo/norm/bindingDigestRestoreFail"

func parseForLazyFillTest(t *testing.T, sql string) ast.StmtNode {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	return stmt
}

// Gate at unit level: the flush-time lazy fill computes the binding
// digest for DML-shaped statements whose memo is empty, hands the ORIGINAL
// statement to the shared normalizer (which strips EXPLAIN itself with the
// correct Text() gate — the manual unwrap that lost the inner Text() is
// gone), never overwrites an existing value, and counts every empty outcome
// once with its classified reason.
func TestLazyFillBindingDigest(t *testing.T) {
	// A fresh SessionVars per sub-case: the memo is per-statement in
	// production (the StatementContext resets between statements), so sharing
	// vars across cases would let an earlier case's memo satisfy a later
	// case's fill.
	newVars := func() *variable.SessionVars {
		vars := variable.NewSessionVars(nil)
		vars.CurrentDB = "test"
		return vars
	}

	digestOf := func(sql string) string {
		_, digest := norm.NormalizeStmtForBinding(parseForLazyFillTest(t, sql), norm.WithSpecifiedDB("test"))
		return digest
	}
	counterOf := func(reason string) float64 {
		return testutil.ToFloat64(metrics.BindingDigestEmptyCounter.WithLabelValues(reason))
	}

	// DML fill: memo empty → the value comes from the normalizer, nothing is
	// counted.
	items := &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "select * from t where a in (1)"), newVars(), items)
	require.Equal(t, digestOf("select * from t where a in (1)"), items.BindingDigest)

	// An existing value is never recomputed or counted over (first producer
	// wins; the flush fill only serves statements both entries skipped).
	beforeRF := counterOf(metrics.LblBindingDigestRestoreFail)
	items = &variable.SlowQueryLogItems{BindingDigest: "already"}
	lazyFillBindingDigest(parseForLazyFillTest(t, "select * from t where a in (1)"), newVars(), items)
	require.Equal(t, "already", items.BindingDigest)
	require.Equal(t, float64(0), counterOf(metrics.LblBindingDigestRestoreFail)-beforeRF)

	// EXPLAIN semantics via the flush fill: the outer ExplainStmt goes
	// to the normalizer, which strips explain itself — the row carries the
	// inner statement's digest, identical to the bare statement's.
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "explain select * from t where a in (1)"), newVars(), items)
	require.Equal(t, digestOf("select * from t where a in (1)"), items.BindingDigest,
		"an explain row filled at flush time must carry the inner statement's digest")

	// EXPLAIN with an empty Text() is the "no bind record wanted" shape: the
	// normalizer returns "" for it and the outcome lands on restore-fail (no
	// operational difference from a real restore failure; documented on
	// bindingDigestEmptyReason).
	emptyTextExplain := parseForLazyFillTest(t, "explain select * from t where a in (1)").(*ast.ExplainStmt)
	emptyTextExplain.SetText(nil, "")
	beforeRF = counterOf(metrics.LblBindingDigestRestoreFail)
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(emptyTextExplain, newVars(), items)
	require.Empty(t, items.BindingDigest)
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestRestoreFail)-beforeRF)

	// restore-fail on the flush computation itself: the failpoint fails
	// the normalizer's restore step — for a point query the flush fill is the
	// row's only consumer, so this is the deterministic trigger of its empty
	// path.
	require.NoError(t, failpoint.Enable(fpFlushRestoreFail, "return(true)"))
	beforeRF = counterOf(metrics.LblBindingDigestRestoreFail)
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "select * from t where a in (1)"), newVars(), items)
	require.Empty(t, items.BindingDigest)
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestRestoreFail)-beforeRF)
	require.NoError(t, failpoint.Disable(fpFlushRestoreFail))

	// execute resolved on demand: EXECUTE resolves its prepared
	// statement at the consume site — a DML inner statement yields the
	// inner binding digest and nothing is counted.
	vars := newVars()
	vars.PreparedStmtNameToID["s_dml"] = 1
	vars.PreparedStmts[1] = &plannercore.PlanCacheStmt{
		PreparedAst: &ast.Prepared{Stmt: parseForLazyFillTest(t, "select * from t where a in (1)")},
	}
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "execute s_dml"), vars, items)
	require.Equal(t, digestOf("select * from t where a in (1)"), items.BindingDigest,
		"a resolved EXECUTE must carry the inner statement's binding digest")

	// execute-unresolved, name not found: the on-demand resolution itself
	// fails.
	beforeEU := counterOf(metrics.LblBindingDigestExecuteUnresolved)
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "execute s"), newVars(), items)
	require.Empty(t, items.BindingDigest)
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestExecuteUnresolved)-beforeEU)

	// execute-unresolved, inner without binding semantics: a RESOLVED
	// EXECUTE whose inner statement is non-DML still counts
	// execute-unresolved, not non-dml.
	vars = newVars()
	vars.PreparedStmtNameToID["s_set"] = 2
	vars.PreparedStmts[2] = &plannercore.PlanCacheStmt{
		PreparedAst: &ast.Prepared{Stmt: parseForLazyFillTest(t, "set @x = 1")},
	}
	beforeEU = counterOf(metrics.LblBindingDigestExecuteUnresolved)
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "execute s_set"), vars, items)
	require.Empty(t, items.BindingDigest)
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestExecuteUnresolved)-beforeEU)

	// non-dml: statements without binding semantics count with their reason.
	beforeND := counterOf(metrics.LblBindingDigestNonDML)
	items = &variable.SlowQueryLogItems{}
	lazyFillBindingDigest(parseForLazyFillTest(t, "set @x = 1"), newVars(), items)
	require.Empty(t, items.BindingDigest)
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestNonDML)-beforeND)
}
