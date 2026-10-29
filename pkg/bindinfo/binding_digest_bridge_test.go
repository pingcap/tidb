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

// Gates for the binding-digest bridge and its vocabulary matrix: the binding
// digest is computed ON DEMAND by its only consumers — the slow-log rule
// evaluation and the slow-log flush — from the statement AST with the
// execution-time current database (the binding match path no longer computes
// it: statements that are never slow-logged pay nothing). These gates
// therefore force the consumer with tidb_slow_log_threshold=0 and observe
// the stmtctx memo. The digest must live in the
// mysql.bind_info.sql_digest vocabulary for unqualified SQL, stay stable
// across the cache-miss first execution and the re-execution (the plan-cache
// key is built before this round of preprocess, so the AST's
// name-resolution state differs between them), and honor the execution-time
// current database per the vocabulary matrix.
package bindinfo_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Fully-qualified failpoint path of the binding-digest restore-failure hook:
// it is injected inside the shared normalizer at the exact
// value a real restore failure produces (the restore helper's "" error
// return), so every on-demand consumer fails through the real chain.
const fpNormRestoreFail = "github.com/pingcap/tidb/pkg/bindinfo/norm/bindingDigestRestoreFail"

// qaMemoDigest returns the binding digest memoized on the last statement's
// context. Under the on-demand model the memo is written only when a
// consumer needed the value, so gates force the consumer first (threshold=0
// slow log flush, or a Binding_digest trigger rule).
func qaMemoDigest(t *testing.T, tk *testkit.TestKit) string {
	digest, ok := tk.Session().GetSessionVars().StmtCtx.BindingDigest()
	require.True(t, ok, "a slow-logged DML statement must have its binding digest memoized")
	return digest
}

func qaWaitBindingCacheLoaded(t *testing.T, tk *testkit.TestKit) {
	require.Eventually(t, func() bool {
		return fmt.Sprint(tk.MustQuery("show binding_cache status").Rows()[0][0]) != "0"
	}, 10*time.Second, 200*time.Millisecond, "binding must be loaded into the cache")
}

// Gate (vocabulary): for unqualified SQL the runtime digest must equal
// the persisted mysql.bind_info.sql_digest of the binding it reconciles with
// — anchored against the DBMS's own persisted vocabulary, not our normalizer.
// The single-element in-list is the key case: the exec vocabulary collapses
// `in (1)` and `in (1, 2, 3)` into different `in (?)` digests while the
// binding vocabulary folds them together, so this pins that we can never land
// on the exec side. Also pins cross-execution stability (first execution vs
// re-execution — the on-demand fill normalizes the same shared plan-cache AST
// the match path sees) and that an explicitly db-qualified spelling of the
// same statement produces the same digest.
func TestBindingDigestBridgeVocabulary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// A plain (no-PK) table: the statement takes the regular optimize path.
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create index index_t on t(b)")
	tk.MustExec("create global binding for select * from t where a in (1) using select * from t use index(index_t) where a in (1)")
	qaWaitBindingCacheLoaded(t, tk)

	// Force the consumer: threshold=0 slow-logs every statement, so the
	// flush-time on-demand fill computes and memoizes the digest.
	tk.MustExec("set tidb_slow_log_threshold=0")
	defer tk.MustExec("set tidb_slow_log_threshold=300")

	// First execution (plan-cache miss): the fill normalizes a
	// name-unresolved AST; the digest must still come out db-qualified via
	// WithSpecifiedDB.
	tk.MustQuery("select * from t where a in (1)").Rows()
	digest1 := qaMemoDigest(t, tk)
	require.NotEmpty(t, digest1)

	// The vocabulary anchor: the runtime digest must be exactly the persisted
	// sql_digest of the fixture binding's bind_info row.
	rows := tk.MustQuery(fmt.Sprintf(
		"select original_sql from mysql.bind_info where sql_digest = '%s' and status = 'enabled'", digest1)).Rows()
	require.Len(t, rows, 1,
		"the runtime digest must equal a persisted bind_info.sql_digest (same vocabulary)")
	// The persisted original_sql is the binding-vocabulary normalization: the
	// completed db name and the folded in-list (never the exec `in (?)` form).
	require.Contains(t, rows[0][0], "`test` . `t`",
		"the persisted original_sql must carry the completed db name")
	require.Contains(t, rows[0][0], "in ( ... )",
		"the persisted original_sql must keep the binding-vocabulary in-list")

	// The exec-vocabulary contrast: the bridge digest must differ from the
	// exec digest of the same statement text.
	_, execDigest := parser.NormalizeDigest("select * from t where a in (1)")
	require.NotEqual(t, execDigest.String(), digest1,
		"the bridge digest must not collapse into the exec vocabulary (in (1) is the key case)")

	// Re-execution (plan cache hit; the shared AST is now name-resolved):
	// same digest as the first execution — the resolution-state drift the
	// baseline defect caused must stay fixed.
	tk.MustQuery("select * from t where a in (1)").Rows()
	digest2 := qaMemoDigest(t, tk)
	require.Equal(t, digest1, digest2,
		"the digest must be stable across cache-miss and cache-hit executions")

	// An explicitly db-qualified spelling is the same binding-vocabulary
	// statement, so it yields the same digest.
	tk.MustQuery("select * from test.t where a in (1)").Rows()
	digest3 := qaMemoDigest(t, tk)
	require.Equal(t, digest1, digest3,
		"qualified and unqualified spellings must land on one binding digest")
}

// Gate vocabulary matrix (deterministic values): the digest
// follows the execution-time current database for every fresh parse, while a
// prepared statement whose AST has been baked by an earlier EXECUTE keeps the
// baked db — matching what the binding match would actually hit. Expectations
// are anchored to persisted bind_info rows created under each database.
func TestBindingDigestExecutionTimeDBMatrix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	for _, db := range []string{"d1", "d2"} {
		tk.MustExec("create database " + db)
		tk.MustExec("create table " + db + ".t(a int, b int)")
		tk.MustExec("create index index_t on " + db + ".t(b)")
		tk.MustExec("use " + db)
		tk.MustExec("create global binding for select * from t where a in (1) using select * from t use index(index_t) where a in (1)")
	}
	qaWaitBindingCacheLoaded(t, tk)

	// The persisted vocabulary anchor for each database. bind_info uses
	// utf8mb4_bin and stores original_sql in the lowercase spaced-dot
	// binding-vocabulary form (e.g. "select * from `d1` . `t` where ..."),
	// so match the db name anywhere in the text rather than a fixed prefix.
	digestOf := func(db string) string {
		rows := tk.MustQuery(fmt.Sprintf(
			"select sql_digest from mysql.bind_info where status = 'enabled' and sql_digest != '' and original_sql like '%%`%s`%%'", db)).Rows()
		require.Len(t, rows, 1, "exactly one enabled fixture binding per database")
		return rows[0][0].(string)
	}
	d1, d2 := digestOf("d1"), digestOf("d2")
	require.NotEqual(t, d1, d2, "db completion must separate the vocabularies")

	// Force the consumer for every row (non-prepared and EXECUTE alike): the
	// digest is computed on demand by the flush fill — an unlogged statement
	// would leave the memo absent.
	tk.MustExec("set tidb_slow_log_threshold=0")
	defer tk.MustExec("set tidb_slow_log_threshold=300")

	// Matrix row 3 (control, non-prepared): every execution parses fresh, so
	// the digest follows the live current database.
	tk.MustExec("use d1")
	tk.MustQuery("select * from t where a in (1)").Rows()
	require.Equal(t, d1, qaMemoDigest(t, tk), "non-prepared in d1 must be d1-vocabulary")
	tk.MustExec("use d2")
	tk.MustQuery("select * from t where a in (1)").Rows()
	require.Equal(t, d2, qaMemoDigest(t, tk), "non-prepared after use d2 must be d2-vocabulary")

	// Matrix row 1 (baked): PREPARE in d1, EXECUTE bakes d1 into the shared
	// AST, `use d2`, EXECUTE again — the baked schema wins over the live db,
	// which is also the side the binding match would actually hit.
	tk.MustExec("use d1")
	tk.MustExec("prepare s_baked from 'select * from t where a in (1)'")
	tk.MustQuery("execute s_baked").Rows()
	require.Equal(t, d1, qaMemoDigest(t, tk), "first EXECUTE in d1 must be d1-vocabulary")
	tk.MustExec("use d2")
	tk.MustQuery("execute s_baked").Rows()
	require.Equal(t, d1, qaMemoDigest(t, tk),
		"the baked d1 schema must win over the live d2 db (vocabulary matrix row 1)")

	// Matrix row 2 (baked at PREPARE, corrected by measurement): PREPARE
	// itself runs Preprocess (session.go, InPrepare) and handleTableName
	// completes the schema with the PREPARE-time db (preprocess.go — not
	// gated by the inPrepare flag). The prepared AST is therefore already
	// baked at PREPARE time, so the first EXECUTE under `use d2` still lands
	// on the d1 vocabulary — the same value the fuzzy binding match would
	// hit (its table names are d1-qualified too).
	tk.MustExec("use d1")
	tk.MustExec("prepare s_unbaked from 'select * from t where a in (1)'")
	tk.MustExec("use d2")
	tk.MustQuery("execute s_unbaked").Rows()
	require.Equal(t, d1, qaMemoDigest(t, tk),
		"PREPARE bakes the db at PREPARE time: the first EXECUTE under d2 is still d1-vocabulary")
}

// Gate (EXECUTE on demand): a fast-plan prepared statement must
// still carry a binding digest when it is logged, computed by the consumer —
// the slow-log fill resolves the prepared statement itself (there is no
// execution-time producer anymore). Prepared plan cache off removes the
// plan-cache-key entry; a PK in-list point query takes the TryFastPlan early
// return (optimize.go, before the unconditional MatchSQLBinding entry). And
// the inverse gate: with no consumer at all the memo stays absent — an
// EXECUTE nobody logs pays no binding normalization.
func TestBindingDigestExecuteOnDemand(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Primary key on the in-list column: deliberately the TryFastPlan
	// point-query shape — this gate tests the path that skips matching.
	tk.MustExec("create table pt(a int primary key, b int)")
	tk.MustExec("create global binding for select * from pt where a in (1) using select * from pt where a in (1)")
	qaWaitBindingCacheLoaded(t, tk)

	tk.MustExec("set session tidb_enable_prepared_plan_cache=0")
	tk.MustExec("prepare s_pt from 'select * from pt where a in (1)'")

	// Consumer-driven: below the slow threshold and with no
	// binding_digest rule, nobody computes the digest — the memo stays
	// absent (the reset-time EXECUTE producer is gone).
	tk.MustQuery("execute s_pt").Rows()
	_, ok := tk.Session().GetSessionVars().StmtCtx.BindingDigest()
	require.False(t, ok, "an EXECUTE nobody logs must not pay a binding normalization")

	// Force the flush consumer: the fill resolves the prepared statement on
	// demand and memoizes.
	tk.MustExec("set tidb_slow_log_threshold=0")
	defer tk.MustExec("set tidb_slow_log_threshold=300")
	tk.MustQuery("execute s_pt").Rows()

	// The gate: both fast paths skipped, yet the memo carries the binding
	// digest — computed on demand by the flush fill.
	memoDigest, ok := tk.Session().GetSessionVars().StmtCtx.BindingDigest()
	require.True(t, ok, "the flush fill must resolve the prepared statement when the match path is skipped")
	require.NotEmpty(t, memoDigest)

	// Vocabulary anchor: the memo digest equals the persisted sql_digest of
	// the fixture binding (db-completed binding vocabulary) — and not the
	// exec digest of the same text.
	rows := tk.MustQuery(fmt.Sprintf(
		"select 1 from mysql.bind_info where sql_digest = '%s' and status = 'enabled'", memoDigest)).Rows()
	require.Len(t, rows, 1,
		"the on-demand EXECUTE digest must equal a persisted bind_info.sql_digest (same vocabulary)")
	_, execDigest := parser.NormalizeDigest("select * from pt where a in (1)")
	require.NotEqual(t, execDigest.String(), memoDigest,
		"the memo digest must be binding-vocabulary, not the exec digest of the same text")

	// Re-EXECUTE: the memo is per-execution (statement context reset), so the
	// second EXECUTE recomputes and lands on the same anchored value.
	tk.MustQuery("execute s_pt").Rows()
	memoDigest2, ok := tk.Session().GetSessionVars().StmtCtx.BindingDigest()
	require.True(t, ok)
	require.Equal(t, memoDigest, memoDigest2, "re-EXECUTE must land on the same binding digest")
}

// Gate (slow log exposure + flush on-demand fill): the slow log row
// must carry `Binding_digest` in the bind_info vocabulary. The point-query
// fixture (PK in-list) is deliberate — TryFastPlan returns before the binding
// match, so the only possible producer of that row's value is the flush-time
// on-demand fill; a baseline-off row must still carry a value (the fill is
// baseline-independent — no match entry is involved, the fill is always
// on); and a non-DML row must omit the field entirely (omitted, never
// written empty).
func TestBindingDigestSlowLogField(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f1, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	newCfg.Log.SlowQueryFile = f1.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(f1.Name()))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer tk.MustExec("set tidb_slow_log_threshold=300;")

	// DML fixture with a persisted binding as the vocabulary anchor.
	tk.MustExec("create table pt(a int primary key, b int)")
	tk.MustExec("create global binding for select * from pt where a in (1) using select * from pt where a in (1)")
	qaWaitBindingCacheLoaded(t, tk)
	rows := tk.MustQuery("select sql_digest from mysql.bind_info where status='enabled' and original_sql like '%`pt`%'").Rows()
	require.Len(t, rows, 1, "exactly one enabled fixture binding on pt")
	ptDigest := rows[0][0].(string)

	tk.MustExec("set tidb_slow_log_threshold=0;")
	// Point query: the binding-match entries are skipped (TryFastPlan), so
	// the row's value can only come from the flush-time on-demand fill.
	tk.MustQuery("select * from pt where a in (1)").Rows()
	// Baselines off: the flush fill is baseline-independent (no match entry
	// is involved), so the row keeps its value even with bindings globally
	// disabled — the fill is always on.
	tk.MustExec("set tidb_use_plan_baselines=0")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustQuery("select * from t2 where a in (1)").Rows()
	tk.MustExec("set tidb_use_plan_baselines=1")
	tk.MustExec("set tidb_slow_log_threshold=300;")

	t2Stmt, err := parser.New().ParseOneStmt("select * from t2 where a in (1)", "", "")
	require.NoError(t, err)
	_, t2Digest := norm.NormalizeStmtForBinding(t2Stmt, norm.WithSpecifiedDB("test"))
	require.NotEmpty(t, t2Digest)

	content, err := os.ReadFile(f1.Name())
	require.NoError(t, err)
	require.Contains(t, string(content), "# Binding_digest: "+ptDigest,
		"the point-query slow log row must carry the bind_info-vocabulary digest (flush on-demand fill)")
	require.Contains(t, string(content), "# Binding_digest: "+t2Digest,
		"the baseline-off row must still carry a binding digest (always-on)")

	// Non-DML: the SET statement's own record proves the field is omitted
	// (not written empty) for statements without binding semantics. Slow log
	// records start with the "# Time: " line, so scope the assertions to the
	// SET record — the file also holds the DML rows above.
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec("set @x = 1")
	tk.MustExec("set tidb_slow_log_threshold=300;")
	content, err = os.ReadFile(f1.Name())
	require.NoError(t, err)
	var setBlock string
	for _, block := range strings.Split(string(content), "# Time: ") {
		// The slow log prints the restored statement text, hence the
		// backticked `SET @`x`=1;` spelling of `set @x = 1`.
		if strings.Contains(block, "SET @`x`=1") {
			setBlock = block
			break
		}
	}
	require.NotEmpty(t, setBlock, "the SET statement must have been slow-logged")
	require.Contains(t, setBlock, "# Digest: ", "the SET row must carry its exec digest")
	require.NotContains(t, setBlock, "Binding_digest",
		"non-DML rows must omit the field entirely")
}

// Gate (the rule-evaluation consumer, regression for the review
// finding): a `Binding_digest` trigger rule must fire for fast-path
// statements too. The query below is far below the slow threshold; before the
// on-demand fill at rule time, the rule's memo-only read saw an empty digest
// for this point query (TryFastPlan returns before the binding match), the
// rule never matched, and the row was never written.
func TestBindingDigestRuleFiresForPointQuery(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f1, err := os.CreateTemp("", "tidb-slow-rule-*.log")
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	newCfg.Log.SlowQueryFile = f1.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(f1.Name()))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer tk.MustExec("set session tidb_slow_log_rules=''")
	tk.MustExec("set tidb_slow_log_threshold=300;") // rules only, no threshold trigger

	tk.MustExec("create table pr(a int primary key, b int)") // PK in-list: TryFastPlan point query
	tk.MustExec("insert into pr values (1, 1)")
	prStmt, err := parser.New().ParseOneStmt("select * from pr where a in (1)", "", "")
	require.NoError(t, err)
	_, prDigest := norm.NormalizeStmtForBinding(prStmt, norm.WithSpecifiedDB("test"))
	require.NotEmpty(t, prDigest)
	// NOTE: the negative control's digest must differ from the target's —
	// the binding vocabulary folds in-lists, so `a in (2)` would land on the
	// same digest as `a in (1)`.
	_, otherDigest := norm.NormalizeStmtForBinding(
		mustParse(t, "select * from pr where b in (7)"), norm.WithSpecifiedDB("test"))
	require.NotEqual(t, prDigest, otherDigest)

	// A rule on a DIFFERENT digest must not log the statement (negative
	// control: the row below proves the rule actually gated the write).
	tk.MustExec("set session tidb_slow_log_rules='Binding_digest:" + otherDigest + "'")
	tk.MustQuery("select * from pr where a in (1)").Rows()

	// The rule on the statement's own digest must log it despite being far
	// below the threshold — this is the fixed fast-path regression.
	tk.MustExec("set session tidb_slow_log_rules='Binding_digest:" + prDigest + "'")
	tk.MustQuery("select * from pr where a in (1)").Rows()
	tk.MustExec("set session tidb_slow_log_rules=''")

	content, err := os.ReadFile(f1.Name())
	require.NoError(t, err)
	counts := strings.Count(string(content), "select * from pr where a in (1)")
	require.Equal(t, 1, counts,
		"only the execution matching the rule digest may be logged (the negative control must not be)")
	require.Contains(t, string(content), "# Binding_digest: "+prDigest,
		"the rule-triggered row must carry the binding digest it matched on")
}

func mustParse(t *testing.T, sql string) ast.StmtNode {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	return stmt
}

// Gate (empty-value counter): every slow-logged statement whose
// Binding_digest stays empty is counted once at flush time with its reason —
// non-dml by design, restore-fail for DML whose normalization fails (the
// failpoint is injected inside the shared normalizer at the exact value a
// real restore failure produces; for a point query the flush-time fill is the
// row's only consumer, so its empty path triggers deterministically),
// execute-unresolved for EXECUTE whose memo never resolved (inner statement
// without binding semantics).
func TestBindingDigestEmptyCounter(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f1, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	newCfg.Log.SlowQueryFile = f1.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(f1.Name()))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table pt(a int primary key, b int)") // PK in-list: TryFastPlan skips the match entries
	defer tk.MustExec("set tidb_slow_log_threshold=300;")

	counterOf := func(reason string) float64 {
		return testutil.ToFloat64(metrics.BindingDigestEmptyCounter.WithLabelValues(reason))
	}
	tk.MustExec("set tidb_slow_log_threshold=0;")

	// non-dml: the SET statement counts, its row omits the field.
	before := counterOf(metrics.LblBindingDigestNonDML)
	tk.MustExec("set @x = 1")
	require.GreaterOrEqual(t, counterOf(metrics.LblBindingDigestNonDML)-before, float64(1),
		"a non-DML slow-logged statement must count as non-dml")

	// restore-fail: a PK in-list point query skips both binding-match
	// entries (TryFastPlan), so the flush-time fill is the row's ONLY
	// consumer — the normalizer hook fails it through the real chain and the
	// count is provably single-sourced from the flush.
	//
	// The assertion is a file reconciliation rather than a bare counter
	// delta: under threshold=0 the DDL owner's periodic internal poll
	// (`select ... from mysql.tidb_ddl_job ...`) is slow-logged too, and with
	// the hook on it also lands on restore-fail — a precise delta flakes
	// whenever the poll falls inside the window (observed ~50% single-run
	// rate).
	// Reconciling counter against the file proves the per-row counting
	// directly (every new empty-field row is counted exactly once, poll or
	// not) and stays stable either way.
	snapshotRows := func() []string {
		content, err := os.ReadFile(f1.Name())
		require.NoError(t, err)
		blocks := strings.Split(string(content), "# Time: ")
		return blocks[1:] // blocks[0] is the preamble before the first record
	}
	beforeRF := counterOf(metrics.LblBindingDigestRestoreFail)
	beforeND := counterOf(metrics.LblBindingDigestNonDML)
	beforeEU := counterOf(metrics.LblBindingDigestExecuteUnresolved)
	beforeRows := snapshotRows()
	require.NoError(t, failpoint.Enable(fpNormRestoreFail, "return(true)"))
	tk.MustQuery("select * from pt where a in (1)").Rows()
	require.NoError(t, failpoint.Disable(fpNormRestoreFail))

	// The slow log only appends, so the new records are the tail blocks.
	// The equality re-reads both sides until they agree: a poll record's
	// counter increment and its file append are two steps, so a single read
	// can catch them mid-flight on one side only (review hardening). Both
	// settle immediately; the target statement's own events are already
	// complete when MustQuery returns, so the retry cannot mask a real
	// deviation of the statement under test.
	require.Eventually(t, func() bool {
		rows := snapshotRows()
		if len(rows) < len(beforeRows) {
			return false
		}
		empty := 0
		for _, block := range rows[len(beforeRows):] {
			if !strings.Contains(block, "Binding_digest") {
				empty++
			}
		}
		return counterOf(metrics.LblBindingDigestRestoreFail)-beforeRF == float64(empty)
	}, 5*time.Second, 100*time.Millisecond,
		"restore-fail must count every new empty-field slow log row exactly once (poll rows included)")

	// Deterministic tail: exactly one new row for the target point query,
	// and it omits the field.
	afterRows := snapshotRows()
	require.GreaterOrEqual(t, len(afterRows), len(beforeRows))
	targetRows := 0
	for _, block := range afterRows[len(beforeRows):] {
		if strings.Contains(block, "a in (1)") {
			targetRows++
		}
	}
	require.Equal(t, 1, targetRows, "exactly one new row for the target point query")
	// Reconciliation premise: nothing else in this window counts under the
	// other reasons (no internal non-DML or EXECUTE statements observed;
	// if this ever trips, the reconciliation above needs the same treatment).
	require.Equal(t, float64(0), counterOf(metrics.LblBindingDigestNonDML)-beforeND)
	require.Equal(t, float64(0), counterOf(metrics.LblBindingDigestExecuteUnresolved)-beforeEU)

	// execute-unresolved: EXECUTE whose inner statement has no binding
	// semantics — the EXECUTE branch's normalize returns "" for the inner
	// SET so the memo never resolves, the statement itself succeeds, and
	// its flush classifies the outer ExecuteStmt as execute-unresolved.
	// (An EXECUTE of an unknown prepare name errors inside
	// ResetContextOfStmt before FinishExecuteStmt, so that row never
	// flushes and is outside the counter's slow-logged scope by design.)
	tk.MustExec("prepare s_set from 'set @x = 2'")
	beforeEU = counterOf(metrics.LblBindingDigestExecuteUnresolved)
	tk.MustExec("execute s_set")
	require.Equal(t, float64(1), counterOf(metrics.LblBindingDigestExecuteUnresolved)-beforeEU,
		"an EXECUTE of a non-DML prepared statement must count as execute-unresolved")

	// The restore-fail row in the file carries no Binding_digest. Slow log
	// records start with the "# Time: " line, so scope the assertion to the
	// point query's record (the file also holds the SET row above); the
	// matcher avoids assuming whether the SQL line carries the original or
	// the restored spelling.
	content, err := os.ReadFile(f1.Name())
	require.NoError(t, err)
	var dmlBlock string
	for _, block := range strings.Split(string(content), "# Time: ") {
		if strings.Contains(block, "a in (1)") {
			dmlBlock = block
			break
		}
	}
	require.NotEmpty(t, dmlBlock)
	require.Contains(t, dmlBlock, "# Digest: ")
	require.NotContains(t, dmlBlock, "Binding_digest",
		"the restore-fail row must omit the field despite being DML")
}

// Write-side guard (review follow-up): the persisted binding paths — CREATE
// BINDING, CREATE BINDING FROM HISTORY and baseline capture — all route
// through the guarded normalizer now, so a restore failure can no longer be
// hashed into the digest-of-empty-string trap constant and persisted into
// mysql.bind_info.sql_digest. This gate pins the CREATE shape end to end:
// the DDL fails cleanly and no row carries the trap digest.
func TestCreateBindingRestoreFailRejected(t *testing.T) {
	const trapDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table crf(a int)")

	require.NoError(t, failpoint.Enable(fpNormRestoreFail, "return(true)"))
	err := tk.ExecToErr("create global binding for select * from crf where a > 1 using select * from crf where a > 1")
	require.NoError(t, failpoint.Disable(fpNormRestoreFail))
	require.Error(t, err, "CREATE BINDING must fail when the origin SQL cannot be normalized")
	require.Contains(t, err.Error(), "cannot be normalized")

	tk.MustQuery("select count(*) from mysql.bind_info where sql_digest = '" + trapDigest + "'").
		Check(testkit.Rows("0"))
}
