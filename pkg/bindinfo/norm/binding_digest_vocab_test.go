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

// Vocabulary tests for the slow-log Binding_digest field: they pin the
// binding vocabulary (bind_info.sql_digest / Binding_digest) against the
// exec vocabulary (slow-log Digest) for the same statements. Hardcoded
// digests are intentional drift guards.

package norm

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

// qaVocabCase pins both vocabularies for one statement: exec (slow-log Digest)
// and binding-with-default-DB (bind_info.sql_digest / Binding_digest).
type qaVocabCase struct {
	sql           string
	defaultDB     string // "" means no WithSpecifiedDB option (baseline-defect vocabulary)
	execDigest    string // parser.NormalizeDigest, verified on d31a8f1908
	bindingDigest string // NormalizeStmtForBinding with WithSpecifiedDB, verified on d31a8f1908
	bindingNorm   string
	execNorm      string
	emptyBinding  bool // binding vocabulary is empty by design (non-DML)
}

var qaVocabMatrix = []qaVocabCase{
	{
		// Matrix row 1: identical except db completion.
		sql:           "select * from t where a = 1",
		defaultDB:     "test",
		execNorm:      "select * from `t` where `a` = ?",
		execDigest:    "077a87a576e4", // verified prefix on d31a8f1908; full hex at landing
		bindingNorm:   "select * from `test` . `t` where `a` = ?",
		bindingDigest: "6909a1bbce5f",
	},
	{
		// Matrix row 2: single-element in-list — THE reconciliation case (exec can never equal).
		sql:           "select * from t where a in (1)",
		defaultDB:     "test",
		execNorm:      "select * from `t` where `a` in ( ? )",
		execDigest:    "0809bc0859b7",
		bindingNorm:   "select * from `test` . `t` where `a` in ( ... )",
		bindingDigest: "da38bf216db4",
	},
	{
		// Matrix row 3: multi-element in-list — exec folds too, only db differs.
		sql:           "select * from t where a in (1,2,3)",
		defaultDB:     "test",
		execNorm:      "select * from `t` where `a` in ( ... )",
		execDigest:    "27b5708e7f94",
		bindingNorm:   "select * from `test` . `t` where `a` in ( ... )",
		bindingDigest: "da38bf216db4",
	},
	{
		// Matrix row 4: row-constructor in-list.
		sql:           "select * from t where (a,b) in (row(1,2),row(3,4))",
		defaultDB:     "test",
		execNorm:      "select * from `t` where ( `a` , `b` ) in ( row ( ... ) , row ( ... ) )",
		execDigest:    "9fd0930e7530",
		bindingNorm:   "select * from `test` . `t` where row ( `a` , `b` ) in ( ... )",
		bindingDigest: "91b944f4b8e8",
	},
	{
		// Matrix row 5: explicit db — difference is the in-list rule alone.
		sql:           "select * from test.t where a in (1)",
		defaultDB:     "test",
		execNorm:      "select * from `test` . `t` where `a` in ( ? )",
		execDigest:    "8b9c4e6ab8fa",
		bindingNorm:   "select * from `test` . `t` where `a` in ( ... )",
		bindingDigest: "da38bf216db4",
	},
	{
		// EXPLAIN prefix strip: binding vocabulary merges explain into select.
		sql:           "explain select * from t where a in (1)",
		defaultDB:     "test",
		execNorm:      "explain select * from `t` where `a` in ( ? )",
		execDigest:    "0b30bfdba6e4",
		bindingNorm:   "select * from `test` . `t` where `a` in ( ... )",
		bindingDigest: "da38bf216db4",
	},
	{
		// DDL: binding vocabulary empty by design.
		sql:          "create table t1 (a int)",
		defaultDB:    "test",
		execNorm:     "create table `t1` ( `a` int )",
		execDigest:   "3f311095b484",
		emptyBinding: true,
	},
}

// TestQABindingVocabularyMatrix pins both digests for every vocabulary-matrix row.
// Full 64-hex digests are filled at adaptation time (first 12 kept from the
// verification run); short prefixes still catch accidental vocabulary swaps.
func TestQABindingVocabularyMatrix(t *testing.T) {
	p := parser.New()
	for _, c := range qaVocabMatrix {
		stmts, _, err := p.ParseSQL(c.sql)
		require.NoError(t, err, c.sql)
		execNorm, execDigest := parser.NormalizeDigest(c.sql)
		require.Equal(t, c.execNorm, execNorm, c.sql)
		require.True(t, len(execDigest.String()) == 64, c.sql)
		require.Equal(t, c.execDigest, execDigest.String()[:len(c.execDigest)], "exec digest drift: "+c.sql)

		if c.emptyBinding {
			// Non-DML must yield the EMPTY binding vocabulary — never exec fallback.
			bn, bd := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB(c.defaultDB))
			require.Empty(t, bn, c.sql)
			require.Empty(t, bd, c.sql)
			continue
		}
		bn, bd := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB(c.defaultDB))
		require.Equal(t, c.bindingNorm, bn, c.sql)
		require.Equal(t, c.bindingDigest, bd[:len(c.bindingDigest)], "binding digest drift: "+c.sql)
		require.NotEqual(t, execDigest.String(), bd, "vocabularies must differ for: "+c.sql)
	}
}

// TestQAInListFoldingEquality is the bridge's core property: single- and
// multi-element in-lists share ONE binding digest while exec digests differ.
// This is the equality the reconciliation query relies on and the case
// that is impossible under exec vocabulary.
func TestQAInListFoldingEquality(t *testing.T) {
	p := parser.New()
	bindingDigestOf := func(sql string) string {
		stmts, _, err := p.ParseSQL(sql)
		require.NoError(t, err)
		_, d := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB("test"))
		require.NotEmpty(t, d)
		return d
	}
	execDigestOf := func(sql string) string {
		_, d := parser.NormalizeDigest(sql)
		return d.String()
	}
	d1 := bindingDigestOf("select * from t where a in (1)")
	d2 := bindingDigestOf("select * from t where a in (1,2)")
	d3 := bindingDigestOf("select * from t where a in (1,2,3)")
	require.Equal(t, d1, d2)
	require.Equal(t, d1, d3)
	require.NotEqual(t, execDigestOf("select * from t where a in (1)"), execDigestOf("select * from t where a in (1,2)"))
}

// TestQADBCompletionIsolatesVocabulary: same statement under different default
// DBs yields different binding digests (locating-key semantics, never an
// aggregation key).
func TestQADBCompletionIsolatesVocabulary(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.ParseSQL("select * from t where a = 1")
	require.NoError(t, err)
	_, dTest := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB("test"))
	_, dOther := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB("other"))
	require.NotEqual(t, dTest, dOther)

	// Explicit db is immune to the default-db option.
	stmts2, _, err := p.ParseSQL("select * from test.t where a = 1")
	require.NoError(t, err)
	_, dExplicitA := NormalizeStmtForBinding(stmts2[0], WithSpecifiedDB("other"))
	_, dExplicitB := NormalizeStmtForBinding(stmts2[0], WithSpecifiedDB("test"))
	require.Equal(t, dExplicitA, dExplicitB)
}

// TestQAExplainPrefixStrip: binding vocabulary of `explain <DML>` equals that
// of the inner DML (binding match semantics); exec vocabulary keeps them apart.
func TestQAExplainPrefixStrip(t *testing.T) {
	p := parser.New()
	es, _, err := p.ParseSQL("explain select * from t where a in (1)")
	require.NoError(t, err)
	ss, _, err := p.ParseSQL("select * from t where a in (1)")
	require.NoError(t, err)
	_, ed := NormalizeStmtForBinding(es[0], WithSpecifiedDB("test"))
	_, sd := NormalizeStmtForBinding(ss[0], WithSpecifiedDB("test"))
	require.NotEmpty(t, ed)
	require.Equal(t, sd, ed)

	_, eExec := parser.NormalizeDigest("explain select * from t where a in (1)")
	_, sExec := parser.NormalizeDigest("select * from t where a in (1)")
	require.NotEqual(t, eExec.String(), sExec.String())
}

// TestQABaselineDefectNoDBVocabulary guards the reviewer-confirmed baseline
// defect (binding_match.go bindingSQLDigest missing WithSpecifiedDB): the
// no-db vocabulary is a THIRD digest set, off bind_info vocabulary. Every
// runtime path must use the WITH-db vocabulary; this test documents
// what the wrong value looks like so regressions to it are caught.
func TestQABaselineDefectNoDBVocabulary(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.ParseSQL("select * from t where a in (1)")
	require.NoError(t, err)
	_, noDB := NormalizeStmtForBinding(stmts[0])
	require.Equal(t, "27b5708e7f94", noDB[:12])
	_, withDB := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB("test"))
	require.Equal(t, "da38bf216db4", withDB[:12])
	require.NotEqual(t, noDB, withDB)
	// The no-db value coincides with exec vocabulary of `in (1,2,3)` — the exact
	// silent-wrong-value trap the fix eliminates.
	_, exec123 := parser.NormalizeDigest("select * from t where a in (1,2,3)")
	require.Equal(t, exec123.String()[:12], noDB[:12])
}

// TestQARestoreFailureYieldsEmptyDigest guards that a REAL restore failure
// (not a failpoint) must propagate as an empty digest.
// The shared restore helpers swallow the error and return "", and hashing
// that "" used to fabricate the digest-of-empty-string trap constant — a
// non-empty digest shared by every failing statement, mistaken for success
// by every `digest != ""` caller. The fixture is a genuine restore error: a
// SELECT ... INTO whose target list mixes user variables and procedure
// local variables, which SelectIntoOption.Restore rejects.
func TestQARestoreFailureYieldsEmptyDigest(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.ParseSQL("select * from t into @a")
	require.NoError(t, err)
	sel := stmts[0].(*ast.SelectStmt)
	require.NotNil(t, sel.SelectIntoOpt)
	// Force the deterministic restore error: both variable lists non-empty.
	sel.SelectIntoOpt.ProcedureVarList = []ast.ExprNode{&ast.VariableExpr{Name: "proc_var"}}

	normalized, digest := NormalizeStmtForBinding(stmts[0], WithSpecifiedDB("test"))
	require.Empty(t, normalized, "a restore failure must yield an empty normalized SQL")
	require.Empty(t, digest, "a restore failure must yield an empty digest, never the trap constant")
}

// TestQAExplainSetOprRestoreFailureYieldsEmptyDigest: the EXPLAIN-over-set-
// operation branch discards the normalizer's digest and re-hashes a substring
// of the normalized SQL, so a restore failure used to reach DigestNormalized("")
// through the idx == -1 path — the one branch that bypassed the empty-digest
// guard and returned the digest-of-empty-string trap constant. The fixture
// reuses the deterministic restore error from
// TestQARestoreFailureYieldsEmptyDigest, attached inside the set operation.
func TestQAExplainSetOprRestoreFailureYieldsEmptyDigest(t *testing.T) {
	p := parser.New()
	es, _, err := p.ParseSQL("explain select * from t union select * from t2")
	require.NoError(t, err)
	into, _, err := parser.New().ParseSQL("select * from x into @a")
	require.NoError(t, err)
	intoOpt := into[0].(*ast.SelectStmt).SelectIntoOpt
	require.NotNil(t, intoOpt)
	intoOpt.ProcedureVarList = []ast.ExprNode{&ast.VariableExpr{Name: "proc_var"}}
	setOpr := es[0].(*ast.ExplainStmt).Stmt.(*ast.SetOprStmt)
	firstSelect := setOpr.SelectList.Selects[0].(*ast.SelectStmt)
	firstSelect.SelectIntoOpt = intoOpt

	normalized, digest := NormalizeStmtForBinding(es[0], WithSpecifiedDB("test"))
	require.Empty(t, normalized, "a restore failure must yield an empty normalized SQL")
	require.Empty(t, digest, "a restore failure must yield an empty digest, never the trap constant")
}
