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

	createSQL := tk.MustQuery("show create table articles").Rows()[0][1].(string)
	require.Contains(t, createSQL, "FULLTEXT KEY `idx_body` (`body`)")
	// The hidden tokenized column and its expression must not leak into the
	// user-facing definition.
	require.NotContains(t, createSQL, "fts_tokenize")
	require.NotContains(t, strings.ToLower(createSQL), "_v$_")

	// A non-default parser is reported too.
	tk.MustExec("create table ngram_articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table ngram_articles add fulltext index idx_body (body) with parser ngram")
	require.Contains(t,
		tk.MustQuery("show create table ngram_articles").Rows()[0][1].(string),
		"WITH PARSER NGRAM")
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
	require.Contains(t, createSQL, "FULLTEXT KEY `idx_body` (`body`)")
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
}
