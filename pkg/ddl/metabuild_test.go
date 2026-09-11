// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNewMetaBuildContextWithSctx(t *testing.T) {
	sqlMode := mysql.ModeStrictAllTables | mysql.ModeNoZeroDate
	sctx := mock.NewContext()
	sctx.GetSessionVars().SQLMode = sqlMode
	sessVars := sctx.GetSessionVars()
	cases := []struct {
		field    string
		setSctx  func(val any)
		testVals []any
		getter   func(*metabuild.Context) any
		check    func(*metabuild.Context)
		extra    func()
	}{
		{
			field: "exprCtx",
			check: func(ctx *metabuild.Context) {
				require.Same(t, sctx.GetExprCtx(), ctx.GetExprCtx())
				require.Equal(t, sqlMode, ctx.GetSQLMode())
				require.Equal(t, sctx.GetSessionVars().DefaultCollationForUTF8MB4, ctx.GetDefaultCollationForUTF8MB4())
				require.Equal(t, "utf8mb4_bin", ctx.GetDefaultCollationForUTF8MB4())
				warn := errors.New("warn1")
				note := errors.New("note1")
				ctx.AppendWarning(warn)
				ctx.AppendNote(note)
				require.Equal(t, []contextutil.SQLWarn{
					{Level: contextutil.WarnLevelWarning, Err: warn},
					{Level: contextutil.WarnLevelNote, Err: note},
				}, ctx.GetExprCtx().GetEvalCtx().CopyWarnings(nil))
			},
		},
		{
			field: "enableAutoIncrementInGenerated",
			setSctx: func(val any) {
				sessVars.EnableAutoIncrementInGenerated = val.(bool)
			},
			testVals: []any{true, false},
			getter: func(ctx *metabuild.Context) any {
				return ctx.EnableAutoIncrementInGenerated()
			},
		},
		{
			field: "primaryKeyRequired",
			setSctx: func(val any) {
				sessVars.PrimaryKeyRequired = val.(bool)
			},
			testVals: []any{true, false},
			getter: func(ctx *metabuild.Context) any {
				return ctx.PrimaryKeyRequired()
			},
			extra: func() {
				// `PrimaryKeyRequired` should always return false if `InRestrictedSQL` is true.
				sessVars.PrimaryKeyRequired = true
				sessVars.InRestrictedSQL = true
				require.False(t, NewMetaBuildContextWithSctx(sctx).PrimaryKeyRequired())
			},
		},
		{
			field: "clusteredIndexDefMode",
			setSctx: func(val any) {
				sessVars.EnableClusteredIndex = val.(variable.ClusteredIndexDefMode)
			},
			testVals: []any{
				variable.ClusteredIndexDefModeIntOnly,
				variable.ClusteredIndexDefModeOff,
				variable.ClusteredIndexDefModeOn,
			},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetClusteredIndexDefMode()
			},
		},
		{
			field: "shardRowIDBits",
			setSctx: func(val any) {
				sessVars.ShardRowIDBits = val.(uint64)
			},
			testVals: []any{uint64(variable.DefShardRowIDBits), uint64(6)},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetShardRowIDBits()
			},
		},
		{
			field: "preSplitRegions",
			setSctx: func(val any) {
				sessVars.PreSplitRegions = val.(uint64)
			},
			testVals: []any{uint64(variable.DefPreSplitRegions), uint64(123)},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetPreSplitRegions()
			},
		},
		{
			field: "suppressTooLongIndexErr",
			extra: func() {
				require.True(t,
					NewMetaBuildContextWithSctx(sctx, metabuild.WithSuppressTooLongIndexErr(true)).
						SuppressTooLongIndexErr(),
				)
				require.False(t,
					NewMetaBuildContextWithSctx(sctx, metabuild.WithSuppressTooLongIndexErr(false)).
						SuppressTooLongIndexErr(),
				)
			},
		},
		{
			field: "fullTextAnalyzer",
			check: func(ctx *metabuild.Context) {
				// A FULLTEXT index freezes the analyzer into the schema, so the
				// settings must be read from the session issuing the statement
				// rather than during meta building.
				config, err := fulltext.AnalyzerConfigFromSessionVars(
					sctx.GetSessionVars(), model.FullTextParserTypeStandardV1)
				require.NoError(t, err)
				ctxConfig, err := ctx.GetFullTextAnalyzer()
				require.NoError(t, err)
				require.Equal(t, config, ctxConfig)
			},
		},
		{
			field: "fullTextAnalyzerErr",
			check: func(ctx *metabuild.Context) {
				// Reading the settings from a healthy session succeeds, so no
				// failure is carried; the error path is exercised in
				// pkg/meta/metabuild.
				_, err := ctx.GetFullTextAnalyzer()
				require.NoError(t, err)
			},
		},
		{
			field: "is",
			check: func(ctx *metabuild.Context) {
				sctxInfoSchema := sctx.GetDomainInfoSchema()
				require.NotNil(t, sctxInfoSchema)
				is, ok := ctx.GetInfoSchema()
				require.True(t, ok)
				require.Same(t, sctxInfoSchema, is)
			},
		},
	}

	allFields := make([]string, 0, len(cases))
	for _, f := range cases {
		t.Run(f.field, func(t *testing.T) {
			require.NotEmpty(t, f.field)
			allFields = append(allFields, "$."+f.field)
			if f.check != nil {
				ctx := NewMetaBuildContextWithSctx(sctx)
				f.check(ctx)
			}
			for _, testVal := range f.testVals {
				f.setSctx(testVal)
				ctx := NewMetaBuildContextWithSctx(sctx)
				require.Equal(t, testVal, f.getter(ctx), "field: %s, v: %v", f.field, testVal)
				if f.check != nil {
					f.check(ctx)
				}
			}
			if f.extra != nil {
				f.extra()
			}
		})
	}

	// make sure all fields are tested (WithIgnorePath contains all fields that the below asserting will pass).
	deeptest.AssertRecursivelyNotEqual(t, &metabuild.Context{}, &metabuild.Context{}, deeptest.WithIgnorePath(allFields))
}

// TestBuildFullTextIndexOfflineUsesDefaultAnalyzer covers the callers that build
// table metadata with no session to read: Lightning and the importer parse user
// DDL, and a dump produced by SHOW CREATE TABLE contains a FULLTEXT
// declaration, so refusing to build one would make such a dump unimportable.
func TestBuildFullTextIndexOfflineUsesDefaultAnalyzer(t *testing.T) {
	p := parser.New()
	// BuildTableInfoFromAST rewrites the statement it is given - the FULLTEXT
	// constraint becomes an expression index, and the expression is then
	// replaced by a reference to the generated column it created - so each call
	// needs its own parse. Reusing one makes the second call fail with
	// "column does not exist: _V$_idx_0", which looks like a context problem
	// and is not.
	fresh := func() *ast.CreateTableStmt {
		stmt, err := p.ParseOneStmt(
			"create table t(a text, fulltext index idx(a))", mysql.UTF8MB4Charset, mysql.UTF8MB4DefaultCollation)
		require.NoError(t, err)
		return stmt.(*ast.CreateTableStmt)
	}

	// NewNonStrictContext is what lightning/pkg/importer and cmd/importer build
	// table metadata with; NewContext is used by bootstrap and test helpers.
	for name, ctx := range map[string]*metabuild.Context{
		"NewContext":          metabuild.NewContext(),
		"NewNonStrictContext": metabuild.NewNonStrictContext(),
	} {
		tblInfo, err := BuildTableInfoFromAST(ctx, fresh())
		require.NoError(t, err, name)
		idx := tblInfo.FindIndexByName("idx")
		require.NotNil(t, idx, name)
		// The index must carry the settings a default-configured server would
		// use, not a zero-valued configuration - whose 0..0 token-size bounds
		// would build an index holding nothing at all.
		hidden := tblInfo.Columns[idx.Columns[0].Offset]
		require.True(t, hidden.Hidden, name)
		require.Contains(t, hidden.GeneratedExprString, "'STANDARD', 3, 84, 1", name)
	}
}

// TestBuildFullTextIndexReportsAnalyzerFailure covers a session whose analyzer
// settings could not be read. That is a genuine fault and is reported, rather
// than quietly substituting defaults the session never asked for.
func TestBuildFullTextIndexReportsAnalyzerFailure(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(
		"create table t(a text, fulltext index idx(a))", mysql.UTF8MB4Charset, mysql.UTF8MB4DefaultCollation)
	require.NoError(t, err)

	// A failure to read the settings from a session is reported rather than
	// replaced by defaults: an index built from settings the session did not
	// ask for would tokenize differently than that session expects.
	cause := errors.New("read innodb_ft_min_token_size")
	_, err = BuildTableInfoFromAST(
		metabuild.NewContext(metabuild.WithFullTextAnalyzerError(cause)), stmt.(*ast.CreateTableStmt))
	require.ErrorIs(t, err, cause)

	// A multi-column FULLTEXT index is metadata-only and needs no analyzer at
	// build time, so it must not be caught by this.
	multi, err := p.ParseOneStmt(
		"create table t(a text, b text, fulltext index idx(a, b))", mysql.UTF8MB4Charset, mysql.UTF8MB4DefaultCollation)
	require.NoError(t, err)
	_, err = BuildTableInfoFromAST(metabuild.NewContext(), multi.(*ast.CreateTableStmt))
	require.NoError(t, err)
}
