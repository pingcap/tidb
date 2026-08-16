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

package ddl_test

import (
	"testing"

	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestFullTextIndexBuildsMVIndex checks that FULLTEXT INDEX on the classic
// kernel materialises as a real multi-valued KV index rather than a columnar
// index, which would write no index data at all.
func TestFullTextIndexBuildsMVIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles add fulltext index idx_body (body)")

	tbl, err := dom.InfoSchema().TableByName(t.Context(), ast.NewCIStr("test"), ast.NewCIStr("articles"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	idx := tblInfo.FindIndexByName("idx_body")
	require.NotNil(t, idx)
	require.True(t, idx.MVIndex, "FULLTEXT index must be a multi-valued index on the classic kernel")
	require.Nil(t, idx.FullTextInfo,
		"FullTextInfo would make IsColumnarIndex true and suppress the KV index")
	require.False(t, idx.IsColumnarIndex())

	// The tokenize expression lives on an auto-created hidden generated column,
	// so it never appears in SELECT * or in the user's column list.
	require.Len(t, idx.Columns, 1)
	hidden := tblInfo.Columns[idx.Columns[0].Offset]
	require.True(t, hidden.Hidden)
	require.Contains(t, hidden.GeneratedExprString, "fts_tokenize")
	require.Contains(t, hidden.GeneratedExprString, "`body`")
	tk.MustQuery("select * from articles").Check(testkit.Rows())
}

// TestFullTextIndexPinsAnalyzerConfig is the property that keeps the index
// reproducible: the analyzer settings are frozen into the schema at DDL time,
// so a later SET cannot make new rows tokenize differently from indexed ones.
func TestFullTextIndexPinsAnalyzerConfig(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global innodb_ft_min_token_size = 3")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles add fulltext index idx_body (body)")

	getExpr := func() string {
		tbl, err := dom.InfoSchema().TableByName(t.Context(), ast.NewCIStr("test"), ast.NewCIStr("articles"))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		idx := tblInfo.FindIndexByName("idx_body")
		return tblInfo.Columns[idx.Columns[0].Offset].GeneratedExprString
	}
	before := getExpr()
	require.Contains(t, before, "3")

	tk.MustExec("set global innodb_ft_min_token_size = 5")
	require.Equal(t, before, getExpr(), "existing index must keep its analyzer snapshot")

	// A newly created index picks up the new setting and records that instead.
	tk.MustExec("create table articles2 (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles2 add fulltext index idx_body (body)")
	tbl2, err := dom.InfoSchema().TableByName(t.Context(), ast.NewCIStr("test"), ast.NewCIStr("articles2"))
	require.NoError(t, err)
	idx2 := tbl2.Meta().FindIndexByName("idx_body")
	require.Contains(t, tbl2.Meta().Columns[idx2.Columns[0].Offset].GeneratedExprString, "5")
}

// TestFullTextIndexIsPopulated checks the index actually carries token entries,
// by driving a member-of lookup that can only be answered from it.
func TestFullTextIndexIsPopulated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("insert into articles values (1, 'distributed sql database'), (2, 'relational storage')")
	tk.MustExec("alter table articles add fulltext index idx_body (body)")
	tk.MustExec("insert into articles values (3, 'distributed storage layer')")

	// Rows indexed by backfill and rows indexed on write must both be present.
	tk.MustExec("admin check table articles")
	tk.MustQuery("select id from articles use index (idx_body) order by id").
		Check(testkit.Rows("1", "2", "3"))
}

// TestFullTextIndexShowCreateTableRoundTrip checks that the index is displayed
// as the FULLTEXT index it was declared as, rather than as the multi-valued
// index it is stored as, and that the displayed form recreates it.
func TestFullTextIndexShowCreateTableRoundTrip(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles add fulltext index idx_body (body)")

	// The index is shown as the expression index it is stored as. Rendering it
	// back as FULLTEXT KEY was withdrawn: the expression shape alone cannot
	// prove the index came from FULLTEXT DDL, so a hand-written expression
	// index over FTS_TOKENIZE was shown the same way and lost its literals.
	createSQL := tk.MustQuery("show create table articles").Rows()[0][1].(string)
	require.Contains(t, createSQL, "fts_tokenize(`body`")
	require.Contains(t, createSQL, "array")
	// The hidden column's generated name must not leak.
	require.NotContains(t, strings.ToLower(createSQL), "_v$_")

	// The definition recreates the same index, literals included.
	tk.MustExec("drop table articles")
	tk.MustExec(createSQL)
	require.Equal(t, createSQL, tk.MustQuery("show create table articles").Rows()[0][1].(string))

	// A non-default parser is recorded in the expression.
	tk.MustExec("create table ngram_articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table ngram_articles add fulltext index idx_body (body) with parser ngram")
	require.Contains(t,
		strings.ToUpper(tk.MustQuery("show create table ngram_articles").Rows()[0][1].(string)),
		"NGRAM")
}

// TestFullTextIndexInCreateTable covers the copy-paste contract: the output of
// SHOW CREATE TABLE must recreate the table, which requires an inline FULLTEXT
// KEY to be rewritten just as ALTER TABLE / CREATE INDEX is.
func TestFullTextIndexInCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255), fulltext key idx_body (body))")

	createSQL := tk.MustQuery("show create table articles").Rows()[0][1].(string)
	require.Contains(t, createSQL, "fts_tokenize(`body`")
	tk.MustExec("drop table articles")
	tk.MustExec(createSQL)
	require.Equal(t, createSQL, tk.MustQuery("show create table articles").Rows()[0][1].(string))
}

// TestOrdinaryMVIndexShowCreateTableUnchanged guards the detection from
// claiming multi-valued indexes that have nothing to do with full-text search.
func TestOrdinaryMVIndexShowCreateTableUnchanged(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, j json, key idx((cast(j->'$.tags' as char(20) array))))")

	createSQL := tk.MustQuery("show create table t").Rows()[0][1].(string)
	require.NotContains(t, createSQL, "FULLTEXT")
	require.Contains(t, createSQL, "cast(json_extract(`j`, _utf8mb4'$.tags') as char(20) array)")
}

func TestFullTextIndexRejectsUnsupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255), n int)")

	tk.MustContainErrMsg("alter table articles add fulltext index idx_n (n)",
		"FULLTEXT index requires a string column")
	// Multi-column FULLTEXT is already rejected by the preprocessor, before the
	// DDL rewrite sees it; the check in buildFullTextMVIndexSpec is a backstop.
	tk.MustContainErrMsg("alter table articles add fulltext index idx_multi (body, n)",
		"FULLTEXT index must specify one column name")
	tk.MustContainErrMsg("alter table articles add fulltext index idx_p (body) with parser multilingual",
		"MULTILINGUAL")
	// INVISIBLE is rejected on a FULLTEXT index before the rewrite runs, which
	// is why fullTextMVIndexOption's visibility handling is unreachable today.
	tk.MustContainErrMsg("alter table articles add fulltext index idx_v (body) invisible",
		"INVISIBLE can not be used in FULLTEXT INDEX")
}

// TestFullTextIndexWithoutExpressionIndexConfig checks that FULLTEXT INDEX
// works on a default server. The index is built as an expression index over
// FTS_TOKENIZE, and expression indexes over functions outside
// GAFunction4ExpressionIndex require allow-expression-index in config - which
// this test package enables globally, hiding the problem from every other test
// here. Users never write FTS_TOKENIZE themselves; DDL generates the call.
func TestFullTextIndexWithoutExpressionIndexConfig(t *testing.T) {
	original := config.GetGlobalConfig().Experimental.AllowsExpressionIndex
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = false
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = original
	})

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles add fulltext index idx_body (body)")
	tk.MustExec("create table inline_articles (id int primary key, body varchar(255), fulltext key idx_body (body))")

	tk.MustExec("insert into articles values (1, 'distributed sql')")
	tk.MustExec("admin check table articles")
}

// TestFullTextIndexPreservesVisibilityAndComment checks that attributes of the
// declared index survive the rewrite into a multi-valued index. They describe
// the index the user asked for rather than how it is stored, and SHOW CREATE
// TABLE reports them, so losing them would break the round trip.
func TestFullTextIndexPreservesVisibilityAndComment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// ALTER TABLE path.
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table articles add fulltext index idx_body (body) comment 'fts on body'")
	altered := tk.MustQuery("show create table articles").Rows()[0][1].(string)
	require.Contains(t, altered, "fts_tokenize(`body`")
	require.Contains(t, altered, "COMMENT 'fts on body'")

	// The definition must recreate the same thing.
	tk.MustExec("drop table articles")
	tk.MustExec(altered)
	require.Equal(t, altered, tk.MustQuery("show create table articles").Rows()[0][1].(string))

	// Inline CREATE TABLE path.
	tk.MustExec("create table inline_articles (id int primary key, body varchar(255), " +
		"fulltext key idx_body (body) comment 'inline fts')")
	inline := tk.MustQuery("show create table inline_articles").Rows()[0][1].(string)
	require.Contains(t, inline, "fts_tokenize(`body`")
	require.Contains(t, inline, "COMMENT 'inline fts'")
	tk.MustExec("drop table inline_articles")
	tk.MustExec(inline)
	require.Equal(t, inline, tk.MustQuery("show create table inline_articles").Rows()[0][1].(string))

	// An invisible index must not be picked by the optimizer, which is the
	// behaviour the flag exists for.
	tk.MustExec("insert into articles values (1, 'distributed sql')")
	tk.MustExec("admin check table articles")
}

// TestFullTextIndexRejectsBinaryColumn covers a case where the index would
// build successfully and be useless. A binary column holds bytes rather than
// text, so the analyzer would tokenize whatever byte sequences resembled words
// and the index would quietly contain nonsense.
func TestFullTextIndexRejectsBinaryColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table b (id int primary key, blob_col blob, bin_col varbinary(255), " +
		"txt varchar(255), bin_coll varchar(255) collate utf8mb4_bin)")

	tk.MustContainErrMsg("alter table b add fulltext index idx_bin (bin_col)",
		"FULLTEXT index requires a non-binary string column")
	tk.MustContainErrMsg("alter table b add fulltext index idx_blob (blob_col)",
		"FULLTEXT index requires a non-binary string column")
	tk.MustContainErrMsg("create table b2 (id int primary key, bin_col varbinary(255), "+
		"fulltext key idx_bin (bin_col))",
		"FULLTEXT index requires a non-binary string column")

	// A binary *collation* on a character column is still text, so it stays
	// allowed - only the binary charset is rejected.
	tk.MustExec("alter table b add fulltext index idx_txt (txt)")
	tk.MustExec("alter table b add fulltext index idx_bin_coll (bin_coll)")
}
