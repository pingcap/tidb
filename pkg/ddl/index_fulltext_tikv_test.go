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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func tikvFullTextIndex(t *testing.T, dom *domain.Domain, tbl, idx string) (*model.TableInfo, *model.IndexInfo) {
	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tbl))
	require.NoError(t, err)
	idxInfo := tblInfo.Meta().FindIndexByName(idx)
	require.NotNil(t, idxInfo, "index %s on %s", idx, tbl)
	return tblInfo.Meta(), idxInfo
}

// TestFullTextIndexBuiltInTiKV covers the metadata a FULLTEXT index takes on
// the classic kernel: an ordinary KV index over the column carrying the
// TiKVFullText marker and the analyzer snapshot, never a columnar index.
func TestFullTextIndexBuiltInTiKV(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	check := func(tbl, idx string, parser model.FullTextParserType) *model.IndexInfo {
		tblInfo, idxInfo := tikvFullTextIndex(t, dom, tbl, idx)
		require.Equal(t, model.StatePublic, idxInfo.State)
		require.Equal(t, ast.IndexTypeFulltext, idxInfo.Tp)
		require.NotNil(t, idxInfo.TiKVFullText)
		require.Equal(t, parser, idxInfo.TiKVFullText.ParserType)
		// Never the columnar shape: that would tell the storage layer the
		// index holds no KV data.
		require.Nil(t, idxInfo.FullTextInfo)
		require.False(t, idxInfo.IsColumnarIndex())
		require.False(t, idxInfo.MVIndex)
		require.False(t, idxInfo.Unique)
		require.Len(t, idxInfo.Columns, 1)
		require.Equal(t, "body", idxInfo.Columns[0].Name.L)
		require.False(t, tblInfo.Columns[idxInfo.Columns[0].Offset].Hidden)
		return idxInfo
	}

	// The three ways of declaring the index agree with each other.
	tk.MustExec("create table t_inline (id int primary key, body text, fulltext index idx (body))")
	inline := check("t_inline", "idx", model.FullTextParserTypeStandardV1)
	tk.MustExec("create table t_create (id int primary key, body text)")
	tk.MustExec("create fulltext index idx on t_create (body)")
	check("t_create", "idx", model.FullTextParserTypeStandardV1)
	tk.MustExec("create table t_alter (id int primary key, body text)")
	tk.MustExec("alter table t_alter add fulltext index idx (body) with parser ngram comment 'notes'")
	ngram := check("t_alter", "idx", model.FullTextParserTypeNgramV1)
	require.Equal(t, 2, ngram.TiKVFullText.NgramTokenSize)
	require.Equal(t, "notes", ngram.Comment)

	// The analyzer snapshot is the session's settings at creation time.
	require.Equal(t, 3, inline.TiKVFullText.MinTokenSize)
	require.Equal(t, 84, inline.TiKVFullText.MaxTokenSize)
	require.True(t, inline.TiKVFullText.EnableStopword)
	tk.MustExec("set global innodb_ft_min_token_size = 8")
	tk.MustExec("set global ngram_token_size = 3")
	defer func() {
		tk.MustExec("set global innodb_ft_min_token_size = 3")
		tk.MustExec("set global ngram_token_size = 2")
	}()
	tk.MustExec("create table t_frozen (id int primary key, body varchar(255), fulltext index idx (body))")
	frozen := check("t_frozen", "idx", model.FullTextParserTypeStandardV1)
	require.Equal(t, 8, frozen.TiKVFullText.MinTokenSize)
	// Changing the variable does not reinterpret an existing index.
	_, inline = tikvFullTextIndex(t, dom, "t_inline", "idx")
	require.Equal(t, 3, inline.TiKVFullText.MinTokenSize)
	tk.MustExec("create table t_frozen_ngram (id int primary key, body varchar(255), fulltext index idx (body) with parser ngram)")
	frozenNgram := check("t_frozen_ngram", "idx", model.FullTextParserTypeNgramV1)
	require.Equal(t, 3, frozenNgram.TiKVFullText.NgramTokenSize)

	// The declared form round-trips through SHOW CREATE TABLE, and the copy
	// keeps the marker.
	tk.MustQuery("show create table t_alter").CheckContain("FULLTEXT INDEX `idx`(`body`) WITH PARSER NGRAM COMMENT 'notes'")
	tk.MustQuery("show index from t_alter where Key_name = 'idx'").CheckContain("FULLTEXT")
	tk.MustExec("create table t_like like t_alter")
	like := check("t_like", "idx", model.FullTextParserTypeNgramV1)
	require.Equal(t, ngram.TiKVFullText, like.TiKVFullText)

	// Lifecycle follows an ordinary index.
	tk.MustExec("insert into t_inline values (1, 'distributed sql database')")
	tk.MustExec("update t_inline set body = 'relational storage' where id = 1")
	tk.MustExec("delete from t_inline where id = 1")
	tk.MustExec("admin check table t_inline")
	tk.MustExec("alter table t_inline rename index idx to idx_renamed")
	check("t_inline", "idx_renamed", model.FullTextParserTypeStandardV1)
	tk.MustExec("alter table t_inline alter index idx_renamed invisible")
	tk.MustExec("alter table t_inline drop index idx_renamed")
	tblInfo, _ := tikvFullTextIndex(t, dom, "t_create", "idx")
	require.Len(t, tblInfo.Indices, 1)
	tk.MustExec("alter table t_create drop column body")
	dropped, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_create"))
	require.NoError(t, err)
	require.Empty(t, dropped.Meta().Indices)

	// The key holds analyzed terms, not the column value, so the index can
	// neither serve a column-value predicate nor be analyzed as a column.
	tk.MustQuery("explain format = 'brief' select * from t_alter where body = 'x'").CheckNotContain("idx")
	tk.MustExec("analyze table t_alter index idx")
	tk.MustQuery("show warnings").CheckContain("analyzing fulltext index is not supported, skip idx")
	tk.MustExec("analyze table t_alter")
	tk.MustContainErrMsg("admin check index t_alter idx", "admin check index is not supported for fulltext index idx")

	// Partitioned tables build one local index per partition.
	tk.MustExec("create table pt (id int, body text, fulltext index idx (body)) partition by hash(id) partitions 2")
	check("pt", "idx", model.FullTextParserTypeStandardV1)
	tk.MustExec("create table pt_add (id int, body text) partition by hash(id) partitions 2")
	tk.MustExec("create fulltext index idx on pt_add (body)")
	check("pt_add", "idx", model.FullTextParserTypeStandardV1)
	tk.MustExec("admin check table pt")
	tk.MustExec("truncate table pt")
	tk.MustExec("alter table pt drop index idx")
}

// TestFullTextIndexBuiltInTiKVRefusals covers definitions that cannot be built
// as a positional inverted index over one text column.
func TestFullTextIndexBuiltInTiKVRefusals(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, n int, b blob, body text, title varchar(100))")
	tk.MustExec("create table pt (id int, body text) partition by hash(id) partitions 2")

	for _, tc := range []struct{ sql, err string }{
		{"create fulltext index idx on t (n)", "FULLTEXT index requires a string column"},
		{"create fulltext index idx on t (b)", "FULLTEXT index requires a non-binary string column"},
		{"create fulltext index idx on t (body) with parser unknown", "Unsupported parser 'unknown'"},
		{"create fulltext index idx on t (body) with parser multilingual", "has no analyzer in TiDB"},
		{"create fulltext index idx on t (body(10))", "FULLTEXT index does not support prefix length"},
		{"create fulltext index idx on t (body desc)", "FULLTEXT index does not support DESC order"},
		{"create fulltext index idx on t (body, title)", "FULLTEXT index must specify one column name"},
		{"create fulltext index idx on t (body) using hash", "'USING HASH' is not supported for FULLTEXT INDEX"},
		{"create fulltext index idx on t (body) where id > 0", "FULLTEXT index does not support a partial condition"},
		{"create fulltext index idx on pt (body) global", "FULLTEXT index does not support GLOBAL"},
		{"create table bad (n int, fulltext index idx (n))", "FULLTEXT index requires a string column"},
		{"create table bad (body text, title text, fulltext index idx (body, title))", "FULLTEXT index must specify one column name"},
		{"alter table t add fulltext index idx (body) with parser unknown", "Unsupported parser 'unknown'"},
	} {
		tk.MustContainErrMsg(tc.sql, tc.err)
	}

	// An existing name under IF NOT EXISTS is a note, as for an ordinary
	// index, and wins over any complaint about the definition.
	tk.MustExec("create fulltext index idx_taken on t (body)")
	tk.MustExec("create fulltext index if not exists idx_taken on t (n)")
	tk.MustQuery("show warnings").CheckContain("Duplicate key name")
	tk.MustContainErrMsg("create fulltext index idx_taken on t (body)", "Duplicate key name")

	// Crossed token bounds would build an index that admits no token.
	tk.MustExec("set global innodb_ft_min_token_size = 16")
	tk.MustExec("set global innodb_ft_max_token_size = 10")
	defer func() {
		tk.MustExec("set global innodb_ft_min_token_size = 3")
		tk.MustExec("set global innodb_ft_max_token_size = 84")
	}()
	tk.MustContainErrMsg("create fulltext index idx_bounds on t (title)", "minimum token size 16 above maximum 10, which admits no token")
}
