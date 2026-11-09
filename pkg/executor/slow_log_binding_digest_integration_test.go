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

// Integration tests for the slow-log Binding_digest field.
// Complementary to pkg/bindinfo/binding_digest_bridge_test.go:
// that file anchors the PK point-query / baseline-off / SET-empty shapes against
// the persisted mysql.bind_info value; this file walks the full scenario
// matrix with expectations computed from the vocabulary functions themselves
// (independent anchoring) plus the cross-db EXECUTE scenarios, redact
// stability and the trap-constant scan.
//
// Fixture rule: match-path cases use no-PK tables — TryFastPlan
// point lookups (PK/unique in-list) exit before the binding-match entry. The
// flush-lazy-fill-only shape is covered by the bridge file's PK gate.

package executor_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const qaTrapDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

var qaBindingDigestRe = regexp.MustCompile(`Binding_digest: ([0-9a-f]{64})`)
var qaDigestRe = regexp.MustCompile(`Digest: ([0-9a-f]{64})`)

// The restore-failure hook: injected inside the shared
// normalizer at the exact value a real restore failure produces (the restore
// helper's "" error return), so every on-demand computation — the flush fill,
// including its EXECUTE resolution — fails through the real chain, exactly as
// a real failure would.
const qaFpNormRestoreFail = "github.com/pingcap/tidb/pkg/bindinfo/norm/bindingDigestRestoreFail"

// qaEmptyCounter reads the process-global empty-reason counter:
// every slow-logged statement whose Binding_digest stays empty counts exactly
// once, with its reason label.
func qaEmptyCounter(reason string) float64 {
	return testutil.ToFloat64(metrics.BindingDigestEmptyCounter.WithLabelValues(reason))
}

// qaEmptyFieldSQLLines returns the SQL lines of the slow log records that do
// NOT carry a Binding_digest — exactly the rows the counter counts. Diffing a
// before/after snapshot of this list attributes counter deltas to statements:
// async internal statements (the DDL owner's periodic tidb_ddl_job poll is
// the known one) land in BOTH the counter and the file, so agreement is
// stable while a bare exact-delta window is not (~50% flake, measured).
func qaEmptyFieldSQLLines(t *testing.T, path string) []string {
	var lines []string
	for _, r := range qaSlowLogRecords(t, path) {
		if !strings.Contains(r, "Binding_digest:") {
			lines = append(lines, qaSQLLineOf(r))
		}
	}
	return lines
}

// qaDMLShapedLine reports whether a logged SQL line looks DML-shaped (the
// statements whose empty flush outcome classifies restore-fail).
func qaDMLShapedLine(line string) bool {
	lower := strings.ToLower(line)
	for _, p := range []string{"select", "insert", "update", "delete", "replace", "with"} {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

// qaCounterAgreesWithFile asserts the once-per-row invariant for a finished
// window: the reason counter's delta equals the number of newly
// logged rows whose shape classifies to that reason, and exactly wantTarget of
// the new rows are the target statement. The equality is retried — an async
// row's counter Inc and file append can straddle the two reads (a microsecond
// window; review hardening) — while the target's own events completed
// synchronously before the window closed, so a real deviation never converges.
func qaCounterAgreesWithFile(t *testing.T, path, reason, targetSubstr string, wantTarget int, beforeCount float64, beforeFile []string) {
	dmlReason := reason == metrics.LblBindingDigestRestoreFail
	require.Eventually(t, func() bool {
		delta := qaEmptyCounter(reason) - beforeCount
		added := qaEmptyFieldSQLLines(t, path)
		if len(added) < len(beforeFile) {
			return false
		}
		shaped, targets := 0, 0
		for _, l := range added[len(beforeFile):] {
			if qaDMLShapedLine(l) == dmlReason {
				shaped++
			}
			if strings.Contains(l, targetSubstr) {
				targets++
			}
		}
		return delta == float64(shaped) && targets == wantTarget
	}, 10*time.Second, 100*time.Millisecond,
		"counter/file once-per-row agreement for reason="+reason+" target="+targetSubstr)
}

// qaBindingDigestOf computes the expected Binding_digest for sql under
// defaultDB — the vocabulary the field must expose.
func qaBindingDigestOf(t *testing.T, sql, defaultDB string) string {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	_, d := norm.NormalizeStmtForBinding(stmt, norm.WithSpecifiedDB(defaultDB))
	require.NotEmpty(t, d, "expected DML with non-empty binding vocabulary: "+sql)
	return d
}

// qaSlowLogFile redirects the slow log WRITE side (global config logger) to a
// temp file and makes every statement slow. Env fact:
// tidb_slow_query_file only redirects the READ side; writes follow the global
// config logger, so the switch must happen at config level + InitLogger.
func qaSlowLogFile(t *testing.T) (*testkit.TestKit, string) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-binding-digest-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_slow_log_threshold=0;")
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, logutil.InitLogger(originCfg.Log.ToLogConfig()))
		require.NoError(t, os.Remove(f.Name()))
	})
	return tk, f.Name()
}

// qaSlowLogRecords returns slow log records, each chunked at its "# Time:"
// header and joined — the unit a row's fields belong to.
func qaSlowLogRecords(t *testing.T, path string) []string {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var records []string
	for _, chunk := range strings.Split(string(raw), "# Time:") {
		chunk = strings.TrimSpace(chunk)
		if chunk != "" {
			records = append(records, chunk)
		}
	}
	return records
}

// qaBindingDigestIn extracts the Binding_digest of a record, "" if absent.
func qaBindingDigestIn(record string) string {
	if m := qaBindingDigestRe.FindStringSubmatch(record); m != nil {
		return m[1]
	}
	return ""
}

// qaSQLLineOf returns the record's SQL statement line — the last non-empty
// line that is not a "# "-prefixed item line. Anchoring matches here is what
// keeps internal DDL/DML records (which legitimately mention table names in
// job text and may carry Binding_digest for explicit-db no-in-list DML) out
// of the assertions. EXECUTE records log the INNER statement's text here;
// PREPARE records log "prepare … from '…'".
func qaSQLLineOf(record string) string {
	sqlLine := ""
	for _, l := range strings.Split(record, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			sqlLine = l
		}
	}
	return sqlLine
}

// qaLastRecordFor returns the LAST record whose SQL line contains sqlSubstr.
func qaLastRecordFor(t *testing.T, path, sqlSubstr string) string {
	var found string
	for _, r := range qaSlowLogRecords(t, path) {
		if strings.Contains(qaSQLLineOf(r), sqlSubstr) {
			found = r
		}
	}
	require.NotEmpty(t, found, "no slow log record for: "+sqlSubstr)
	require.NotContains(t, found, qaTrapDigest, "digest-of-empty-string leaked (trap constant)")
	return found
}

// DML statements carry Binding_digest equal to the with-db vocabulary;
// the exec Digest stays on exec vocabulary (frozen).
func TestQASlowLogBindingDigestDML(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai1 (a int, b int)") // no PK: takes the match path
	tk.MustExec("insert into qai1 values (1, 2)")
	for _, sql := range []string{
		"select * from qai1 where a in (1)",
		"select * from qai1 where a = 1",
		"update qai1 set b = 3 where a in (1)",
		"delete from qai1 where a = 9",
	} {
		tk.MustExec(sql)
		rec := qaLastRecordFor(t, path, sql)
		got := qaBindingDigestIn(rec)
		require.NotEmpty(t, got, "Binding_digest missing for: "+sql)
		require.Equal(t, qaBindingDigestOf(t, sql, "test"), got, sql)
		dm := qaDigestRe.FindStringSubmatch(rec)
		require.NotNil(t, dm, "exec Digest must stay present: "+sql)
		require.NotEqual(t, dm[1], got, "exec Digest and Binding_digest are different vocabularies: "+sql)
	}
}

// DDL / SET / `use` / BEGIN / COMMIT produce NO Binding_digest key —
// empty means no binding semantics; the field is only printed when non-empty.
func TestQASlowLogBindingDigestEmptyForNonDML(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai2 (a int)")
	// The non-DML row counts once with its reason,
	// asserted by file reconciliation rather than a bare exact delta.
	beforeND := qaEmptyCounter(metrics.LblBindingDigestNonDML)
	beforeFile := qaEmptyFieldSQLLines(t, path)
	tk.MustExec("set @x = 1")
	// Direct statements log the RESTORED text ("SET @`x`=1;"), EXECUTE rows
	// log the raw inner text — the two target spellings differ accordingly.
	qaCounterAgreesWithFile(t, path, metrics.LblBindingDigestNonDML, "@`x`", 1, beforeND, beforeFile)
	tk.MustExec("begin")
	tk.MustExec("commit")
	for _, marker := range []string{"create table qai2", "set @x", "begin", "commit", "use test"} {
		for _, r := range qaSlowLogRecords(t, path) {
			// Anchor on the SQL line: internal DML noise (e.g. DDL cleanup on
			// mysql.tidb_mdl_info) contains words like "commit" in its item
			// lines and is itself a legitimate Binding_digest carrier.
			if strings.HasPrefix(qaSQLLineOf(r), marker) {
				require.NotContains(t, r, "Binding_digest:", marker+" row must not carry the field")
			}
		}
	}
}

// Prepared-statement vocabulary (final matrix, measured and reviewer-
// confirmed): PREPARE itself preprocesses and bakes the AST
// (incl. schema backfill, preprocess.go not gated by inPrepare), so ALL
// prepared scenarios land on the PREPARE-time db; non-prepared uses live db.
func TestQASlowLogBindingDigestExecuteSnapshot(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("create database qa_db2")
	tk.MustExec("use test")
	tk.MustExec("create table qai3 (a int)")
	tk.MustExec("insert into qai3 values (1)")
	tk.MustExec("create table qa_db2.qai3 (a int)")
	tk.MustExec("insert into qa_db2.qai3 values (1)")

	tk.MustExec("prepare s1 from 'select * from qai3 where a in (1)'")
	tk.MustExec("execute s1") // db=test
	tk.MustExec("use qa_db2")
	tk.MustExec("execute s1") // same AST, still test vocabulary

	testDBDigest := qaBindingDigestOf(t, "select * from qai3 where a in (1)", "test")
	db2Digest := qaBindingDigestOf(t, "select * from qai3 where a in (1)", "qa_db2")
	require.NotEqual(t, testDBDigest, db2Digest)

	// EXECUTE records log the inner statement's text as their SQL line; the
	// PREPARE record's SQL line starts with "prepare" and is excluded.
	var execRecs []string
	for _, r := range qaSlowLogRecords(t, path) {
		if strings.HasPrefix(qaSQLLineOf(r), "select * from qai3") {
			execRecs = append(execRecs, r)
		}
	}
	require.Len(t, execRecs, 2, "expected exactly the two EXECUTE rows")
	d1 := qaBindingDigestIn(execRecs[0])
	d2 := qaBindingDigestIn(execRecs[1])
	require.NotEmpty(t, d1, execRecs[0])
	require.NotEmpty(t, d2, "EXECUTE row must carry Binding_digest: "+execRecs[1])
	require.Equal(t, d1, d2, "same prepared stmt must yield one stable digest across executes")
	require.Equal(t, testDBDigest, d1, "prepared statements stay on PREPARE-time db vocabulary (both executes)")
	require.Equal(t, testDBDigest, d2)

	// Contrast: non-prepared, fresh AST under qa_db2 → live-db vocabulary.
	tk.MustExec("select * from qai3 where a in (1)")
	rec := qaLastRecordFor(t, path, "select * from qai3 where a in (1)")
	require.Equal(t, db2Digest, qaBindingDigestIn(rec), "non-prepared statement under qa_db2 must use live-db vocabulary")
}

// PREPARE(A) → use B → FIRST EXECUTE — also PREPARE-time db (the AST was
// baked at PREPARE).
func TestQASlowLogBindingDigestExecuteFirstAfterUse(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("create database qa_db3")
	tk.MustExec("use test")
	tk.MustExec("create table qai3b (a int)")
	tk.MustExec("insert into qai3b values (1)")
	tk.MustExec("create table qa_db3.qai3b (a int)")
	tk.MustExec("insert into qa_db3.qai3b values (1)")

	tk.MustExec("prepare s2 from 'select * from qai3b where a in (1)'")
	tk.MustExec("use qa_db3")
	tk.MustExec("execute s2") // first execute under qa_db3, AST baked with test

	testDBDigest := qaBindingDigestOf(t, "select * from qai3b where a in (1)", "test")
	db3Digest := qaBindingDigestOf(t, "select * from qai3b where a in (1)", "qa_db3")
	require.NotEqual(t, testDBDigest, db3Digest)
	var execRec string
	for _, r := range qaSlowLogRecords(t, path) {
		if strings.HasPrefix(qaSQLLineOf(r), "select * from qai3b") {
			execRec = r
		}
	}
	require.NotEmpty(t, execRec)
	require.Equal(t, testDBDigest, qaBindingDigestIn(execRec),
		"first EXECUTE after use B carries PREPARE-time db vocabulary (AST baked at PREPARE)")
}

// EXECUTE of an unknown stmt name → early failure → empty field.
// Execute-unresolved — the reachable shapes (as-landed semantics):
//   - EXECUTE of a non-DML prepared statement: the flush fill resolves the
//     prepared SET and maps its "no binding semantics" outcome onto the
//     EXECUTE wrapper — counted execute-unresolved, field omitted.
//   - EXECUTE of an unknown prepare name: ResetContextOfStmt errors before
//     FinishExecuteStmt — no slow log row exists at all, so it is
//     outside the counter's scope by design.
func TestQASlowLogBindingDigestExecuteUnresolved(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("prepare qa_i4 from 'set @x = 1'")
	beforeEU := qaEmptyCounter(metrics.LblBindingDigestExecuteUnresolved)
	beforeFile := qaEmptyFieldSQLLines(t, path)
	tk.MustExec("execute qa_i4")
	// execute-unresolved has no internal pollution source (only EXECUTE rows
	// produce it), so the exact delta is stable without reconciliation.
	require.Equal(t, float64(1), qaEmptyCounter(metrics.LblBindingDigestExecuteUnresolved)-beforeEU,
		"one EXECUTE-of-non-DML row counts exactly once as execute-unresolved")
	targets := 0
	for _, l := range qaEmptyFieldSQLLines(t, path)[len(beforeFile):] {
		if strings.Contains(l, "set @x = 1") {
			targets++
		}
	}
	require.Equal(t, 1, targets, "exactly one new empty-field row is the EXECUTE (SQL line = raw inner text)")
	rec := qaLastRecordFor(t, path, "set @x = 1")
	require.NotContains(t, rec, "Binding_digest:", "unresolved EXECUTE row must omit the field")

	_, err := tk.Exec("execute no_such_stmt")
	require.Error(t, err)
	for _, r := range qaSlowLogRecords(t, path) {
		require.NotContains(t, qaSQLLineOf(r), "no_such_stmt",
			"unknown-name EXECUTE errors before FinishExecuteStmt — no slow log row exists")
	}
}

// EXPLAIN strips the prefix — the explain row's Binding_digest equals the
// plain select's (same normalized binding shape).
func TestQASlowLogBindingDigestExplainStrip(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai5 (a int)")
	tk.MustExec("insert into qai5 values (1)")
	tk.MustExec("explain select * from qai5 where a in (1)")
	plain := qaBindingDigestOf(t, "select * from qai5 where a in (1)", "test")
	explain := qaBindingDigestOf(t, "explain select * from qai5 where a in (1)", "test")
	require.Equal(t, plain, explain, "vocabulary level: EXPLAIN prefix is stripped")
	rec := qaLastRecordFor(t, path, "explain select")
	got := qaBindingDigestIn(rec)
	require.NotEmpty(t, got, "explain row must carry the field")
	require.Equal(t, plain, got)
}

// Redact off/marker/on — the digest hashes the normalized text, no
// literals; the three modes must not change Binding_digest.
func TestQASlowLogBindingDigestRedactStable(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai6 (a int)")
	tk.MustExec("insert into qai6 values (1)")
	expected := qaBindingDigestOf(t, "select * from qai6 where a in (1)", "test")
	digests := make([]string, 0, 3)
	for _, mode := range []string{"off", "marker", "on"} {
		tk.MustExec("set @@tidb_redact_log=" + mode)
		tk.MustExec("select * from qai6 where a in (1)")
		rec := qaLastRecordFor(t, path, "select * from qai6 where a in (1)")
		got := qaBindingDigestIn(rec)
		require.NotEmpty(t, got, "redact="+mode)
		require.Equal(t, expected, got, "redact="+mode)
		digests = append(digests, got)
	}
	require.Equal(t, digests[0], digests[1])
	require.Equal(t, digests[0], digests[2])
}

// Multi-statement COM_QUERY — each statement gets its own record.
func TestQASlowLogBindingDigestMultiStatement(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai7 (a int)")
	tk.MustExec("insert into qai7 values (1)")
	_, err := tk.Exec("select * from qai7 where a in (1); select * from qai7 where a = 2;")
	if err != nil {
		t.Skipf("multi-statement not accepted by this testkit path (%v) — relocate to pkg/server/tests/commontest", err)
	}
	d1 := qaBindingDigestOf(t, "select * from qai7 where a in (1)", "test")
	d2 := qaBindingDigestOf(t, "select * from qai7 where a = 2", "test")
	saw1, saw2 := false, false
	for _, r := range qaSlowLogRecords(t, path) {
		switch qaBindingDigestIn(r) {
		case d1:
			saw1 = true
		case d2:
			saw2 = true
		}
	}
	require.True(t, saw1, "first statement of the multi-statement batch must carry its digest")
	require.True(t, saw2, "second statement of the multi-statement batch must carry its digest")
}

// Prepared + plan cache — both executions carry the same value (the EXECUTE
// branch fills on every round, not only the cache-miss first one).
func TestQASlowLogBindingDigestPlanCacheSecondExec(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai8 (a int)")
	tk.MustExec("insert into qai8 values (1)")
	tk.MustExec("prepare s from 'select * from qai8 where a in (1)'")
	tk.MustExec("execute s")
	tk.MustExec("execute s")
	expected := qaBindingDigestOf(t, "select * from qai8 where a in (1)", "test")
	count := 0
	for _, r := range qaSlowLogRecords(t, path) {
		if strings.HasPrefix(qaSQLLineOf(r), "select * from qai8") {
			got := qaBindingDigestIn(r)
			require.NotEmpty(t, got, "cache-hit execution must still fill: "+r)
			require.Equal(t, expected, got)
			count++
		}
	}
	require.GreaterOrEqual(t, count, 2)
}

// Plan baselines off — the on-demand fill is baseline-independent (no
// binding-match entry is involved in computing the digest; the fill is
// always on); the row keeps its value. (Double-anchored with the bridge
// file's gate.)
func TestQASlowLogBindingDigestBaselineOff(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("set tidb_use_plan_baselines=off")
	tk.MustExec("create table qai9 (a int)") // no PK: match path
	tk.MustExec("insert into qai9 values (1)")
	tk.MustExec("select * from qai9 where a in (1)")
	expected := qaBindingDigestOf(t, "select * from qai9 where a in (1)", "test")
	rec := qaLastRecordFor(t, path, "select * from qai9 where a in (1)")
	require.Equal(t, expected, qaBindingDigestIn(rec), "baselines off must still fill (always-on on-demand fill)")
}

// True empty path (restore failure): the restore-failure hook is injected
// inside the shared normalizer at the exact value a real restore failure
// produces, so every on-demand consumer fails through the real chain:
//
//   - (a) PK in-list point query: TryFastPlan skips the binding-match
//     entries, the flush fill is the row's only consumer — the row's field
//     stays empty and it counts restore-fail once.
//   - (b) no-PK select: the flush fill is likewise the only consumer —
//     still one count per row.
//   - (c) EXECUTE of a DML prepared statement: the flush fill resolves the
//     prepared statement and normalizes through the same hooked chain, so
//     the row classifies restore-fail like any DML row — execute-unresolved
//     is only for EXECUTE whose resolution fails or whose inner statement
//     has no binding semantics.
//   - (d) non-DML under the hook: the type gate classifies before any
//     normalize attempt — counted non-dml, the hook is irrelevant.
//
// Counter assertions use file reconciliation (qaCounterAgreesWithFile): the
// DDL owner's periodic internal poll is slow-logged under threshold=0 and
// counts restore-fail under the hook, so bare exact-delta windows flake.
func TestQASlowLogBindingDigestRestoreFailTrueEmpty(t *testing.T) {
	testfailpoint.Enable(t, qaFpNormRestoreFail, "return(true)")
	defer testfailpoint.Disable(t, qaFpNormRestoreFail)

	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai13a (id int primary key, b int)") // PK in-list: point path
	tk.MustExec("insert into qai13a values (1, 1)")
	tk.MustExec("create table qai13b (a int)") // no PK
	tk.MustExec("insert into qai13b values (1)")
	tk.MustExec("prepare qa_sel13 from 'select * from qai13b where a in (1)'")

	// (a) point query — the row's only consumer fails, once per row.
	beforeRF := qaEmptyCounter(metrics.LblBindingDigestRestoreFail)
	beforeFile := qaEmptyFieldSQLLines(t, path)
	tk.MustExec("select * from qai13a where id in (1)")
	qaCounterAgreesWithFile(t, path, metrics.LblBindingDigestRestoreFail, "qai13a where id in (1)", 1, beforeRF, beforeFile)
	rec := qaLastRecordFor(t, path, "select * from qai13a where id in (1)")
	require.NotContains(t, rec, "Binding_digest:", "true empty path: field omitted, never a fallback")
	require.NotNil(t, qaDigestRe.FindStringSubmatch(rec), "exec Digest unaffected")
	require.NotContains(t, rec, qaTrapDigest)

	// (b) no-PK statement — the flush fill fails the same way, one count per row.
	beforeRF = qaEmptyCounter(metrics.LblBindingDigestRestoreFail)
	beforeFile = qaEmptyFieldSQLLines(t, path)
	tk.MustExec("select * from qai13b where a in (1)")
	qaCounterAgreesWithFile(t, path, metrics.LblBindingDigestRestoreFail, "qai13b where a in (1)", 1, beforeRF, beforeFile)
	rec = qaLastRecordFor(t, path, "qai13b where a in (1)")
	require.NotContains(t, rec, "Binding_digest:")

	// (c) EXECUTE of DML under the hook — the flush fill resolves the
	// prepared statement and normalizes through the same hooked chain, so
	// the row classifies restore-fail like any DML row.
	beforeRF = qaEmptyCounter(metrics.LblBindingDigestRestoreFail)
	beforeEU := qaEmptyCounter(metrics.LblBindingDigestExecuteUnresolved)
	beforeFile = qaEmptyFieldSQLLines(t, path)
	tk.MustExec("execute qa_sel13")
	require.Equal(t, float64(1), qaEmptyCounter(metrics.LblBindingDigestRestoreFail)-beforeRF,
		"a restore-failed EXECUTE counts restore-fail like any DML row")
	require.Equal(t, float64(0), qaEmptyCounter(metrics.LblBindingDigestExecuteUnresolved)-beforeEU,
		"a resolved EXECUTE never counts execute-unresolved")
	rec = qaLastRecordFor(t, path, "select * from qai13b where a in (1)")
	require.NotContains(t, rec, "Binding_digest:", "the restore-failed EXECUTE row omits the field")

	// (d) non-DML — type gate first, hook irrelevant.
	beforeND := qaEmptyCounter(metrics.LblBindingDigestNonDML)
	beforeFile = qaEmptyFieldSQLLines(t, path)
	tk.MustExec("set @qa_i13 = 1")
	qaCounterAgreesWithFile(t, path, metrics.LblBindingDigestNonDML, "@`qa_i13`", 1, beforeND, beforeFile)
}

// No scenario may ever leak the digest-of-empty-string trap constant.
func TestQASlowLogBindingDigestNoTrapConstant(t *testing.T) {
	tk, path := qaSlowLogFile(t)
	tk.MustExec("use test")
	tk.MustExec("create table qai13 (a int)")
	tk.MustExec("insert into qai13 values (1)")
	tk.MustExec("select * from qai13 where a in (1)")
	tk.MustExec("set @x = 1")
	_, err := tk.Exec("execute no_such_stmt_x") // unresolved EXECUTE, error expected
	require.Error(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), qaTrapDigest,
		"digest-of-empty-string leaked into the slow log")
}
