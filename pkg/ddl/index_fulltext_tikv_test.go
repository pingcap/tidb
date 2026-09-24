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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
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

	// Key columns ahead of the tokenized column: ordinary columns of any
	// indexable type, rendered and copied like the rest of the definition.
	tk.MustExec("create table t_tenant (id int primary key, tenant_id bigint, region varchar(16), body text, fulltext index idx (tenant_id, body), fulltext index idx_region (tenant_id, region, body))")
	_, tenant := tikvFullTextIndex(t, dom, "t_tenant", "idx")
	require.Equal(t, []string{"tenant_id", "body"}, []string{tenant.Columns[0].Name.L, tenant.Columns[1].Name.L})
	require.Equal(t, "body", tenant.TiKVFullTextColumn().Name.L)
	require.NotNil(t, tenant.TiKVFullText)
	_, region := tikvFullTextIndex(t, dom, "t_tenant", "idx_region")
	require.Len(t, region.Columns, 3)
	require.Equal(t, "body", region.TiKVFullTextColumn().Name.L)
	tk.MustQuery("show create table t_tenant").CheckContain("FULLTEXT INDEX `idx`(`tenant_id`,`body`)")
	tk.MustQuery("show create table t_tenant").CheckContain("FULLTEXT INDEX `idx_region`(`tenant_id`,`region`,`body`)")
	tk.MustExec("create table t_tenant_like like t_tenant")
	_, tenantLike := tikvFullTextIndex(t, dom, "t_tenant_like", "idx_region")
	require.Len(t, tenantLike.Columns, 3)
	tk.MustExec("alter table t_tenant add fulltext index idx_added (region, body)")
	_, added := tikvFullTextIndex(t, dom, "t_tenant", "idx_added")
	require.Equal(t, "body", added.TiKVFullTextColumn().Name.L)
	tk.MustExec("insert into t_tenant values (1, 10, 'eu', 'distributed sql database')")
	tk.MustExec("update t_tenant set tenant_id = 11 where id = 1")
	tk.MustExec("alter table t_tenant modify column tenant_id int")
	tk.MustExec("admin check table t_tenant")
	// A key column is covered by the index like any composite index column.
	tk.MustContainErrMsg("alter table t_tenant drop column region", "can't drop column region with composite index covered")
	tk.MustExec("alter table t_tenant drop index idx_region, drop index idx_added")
	tk.MustExec("alter table t_tenant drop column region")
	tenantTbl, _ := tikvFullTextIndex(t, dom, "t_tenant", "idx")
	require.Len(t, tenantTbl.Indices, 1)

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
	tk.MustExec("admin check index t_alter idx")

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
	tk.MustExec("create table t (id int primary key, n int, b blob, body text, title varchar(100), j json)")
	tk.MustExec("create table pt (id int, body text) partition by hash(id) partitions 2")

	for _, tc := range []struct{ sql, err string }{
		{"create fulltext index idx on t (n)", "FULLTEXT index requires a string column"},
		{"create fulltext index idx on t (b)", "FULLTEXT index requires a non-binary string column"},
		{"create fulltext index idx on t (body) with parser unknown", "Unsupported parser 'unknown'"},
		{"create fulltext index idx on t (body) with parser multilingual", "has no analyzer in TiDB"},
		{"create fulltext index idx on t (body(10))", "FULLTEXT index does not support prefix length"},
		{"create fulltext index idx on t (body desc)", "FULLTEXT index does not support DESC order"},
		// Only the last column is tokenized; the ones before it are key
		// columns under the ordinary rules.
		{"create fulltext index idx on t (body, title)", "BLOB/TEXT column 'body' used in key specification without a key length"},
		{"create fulltext index idx on t (title, n)", "FULLTEXT index requires a string column"},
		{"create fulltext index idx on t (n, b)", "FULLTEXT index requires a non-binary string column"},
		{"create fulltext index idx on t (title(3), body)", "FULLTEXT index does not support prefix length"},
		{"create fulltext index idx on t (n desc, body)", "FULLTEXT index does not support DESC order"},
		{"create fulltext index idx on t (j, body)", "JSON column 'j' cannot be used in key specification"},
		{"create fulltext index idx on t ((cast(j as signed array)), body)", "FULLTEXT index does not support an expression key part"},
		{"create fulltext index idx on t ((lower(title)), body)", "FULLTEXT index does not support an expression key part"},
		{"create table bad (j json, body text, fulltext index idx ((cast(j as signed array)), body))", "FULLTEXT index does not support an expression key part"},
		{"create fulltext index idx on t (body) using hash", "'USING HASH' is not supported for FULLTEXT INDEX"},
		{"create fulltext index idx on t (body) where id > 0", "FULLTEXT index does not support a partial condition"},
		{"create fulltext index idx on pt (body) global", "FULLTEXT index does not support GLOBAL"},
		{"create table bad (n int, fulltext index idx (n))", "FULLTEXT index requires a string column"},
		{"create table bad (body text, title text, fulltext index idx (body, title))", "BLOB/TEXT column 'body' used in key specification without a key length"},
		{"alter table t add fulltext index idx (body) with parser unknown", "Unsupported parser 'unknown'"},
	} {
		tk.MustContainErrMsg(tc.sql, tc.err)
	}

	// Every FULLTEXT index over a column is built with one analyzer, so a
	// MATCH compiles its search string the same way whichever index serves
	// it; a second index with another parser or other settings is refused.
	tk.MustExec("create fulltext index idx_body on t (body)")
	tk.MustContainErrMsg("create fulltext index idx_tenant_body on t (n, body) with parser ngram", "FULLTEXT index over body must use the parser and analyzer settings of the existing FULLTEXT index idx_body")
	tk.MustExec("set global innodb_ft_min_token_size = 4")
	tk.MustContainErrMsg("create fulltext index idx_tenant_body on t (n, body)", "must use the parser and analyzer settings of the existing FULLTEXT index idx_body")
	tk.MustExec("set global innodb_ft_min_token_size = 3")
	tk.MustExec("create fulltext index idx_tenant_body on t (n, body)")
	tk.MustExec("create fulltext index idx_title on t (title) with parser ngram")
	tk.MustContainErrMsg("create table bad (n int, body text, fulltext index a (body), fulltext index b (n, body) with parser ngram)", "must use the parser and analyzer settings of the existing FULLTEXT index a")
	tk.MustExec("alter table t drop index idx_body, drop index idx_tenant_body, drop index idx_title")

	// An existing name under IF NOT EXISTS is a note, as for an ordinary
	// index, and wins over any complaint about the definition.
	tk.MustExec("create fulltext index idx_taken on t (body)")
	tk.MustExec("create fulltext index if not exists idx_taken on t (n)")
	tk.MustQuery("show warnings").CheckContain("Duplicate key name")
	tk.MustContainErrMsg("create fulltext index idx_taken on t (body)", "Duplicate key name")

	// A cluster with a node that would not maintain the index cannot create
	// one; the DDL version detection loop decides that.
	model.SetTiKVFullTextSupported(false)
	tk.MustContainErrMsg("create fulltext index idx_mixed on t (title)", "requires every TiDB node in the cluster to support it")
	tk.MustContainErrMsg("create table mixed (body text, fulltext index idx (body))", "requires every TiDB node in the cluster to support it")
	tk.MustContainErrMsg("alter table t add fulltext index idx_mixed (title)", "requires every TiDB node in the cluster to support it")
	model.SetTiKVFullTextSupported(true)
	tk.MustExec("create table mixed (body text, fulltext index idx (body))")

	// Crossed token bounds would build an index that admits no token.
	tk.MustExec("set global innodb_ft_min_token_size = 16")
	tk.MustExec("set global innodb_ft_max_token_size = 10")
	defer func() {
		tk.MustExec("set global innodb_ft_min_token_size = 3")
		tk.MustExec("set global innodb_ft_max_token_size = 84")
	}()
	tk.MustContainErrMsg("create fulltext index idx_bounds on t (title)", "minimum token size 16 above maximum 10, which admits no token")
}

// fullTextEntries reads every entry of a FULLTEXT index built in TiKV as
// handle -> term -> positions, and the decoded key-column values each
// handle's entries were written under, which must agree across its terms.
func fullTextEntries(t *testing.T, tk *testkit.TestKit, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (map[int64]map[string][]int, map[int64][]types.Datum) {
	entries := make(map[int64]map[string][]int)
	keys := make(map[int64][]types.Datum)
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	defer func() { require.NoError(t, txn.Rollback()) }()
	physicalIDs := []int64{tblInfo.ID}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		physicalIDs = physicalIDs[:0]
		for _, def := range pi.Definitions {
			physicalIDs = append(physicalIDs, def.ID)
		}
	}
	for _, pid := range physicalIDs {
		prefix := tablecodec.EncodeTableIndexPrefix(pid, idxInfo.ID)
		iter, err := txn.Iter(prefix, prefix.PrefixNext())
		require.NoError(t, err)
		for iter.Valid() {
			keyValues, term, err := tables.DecodeTiKVFullTextIndexKey(idxInfo, iter.Key())
			require.NoError(t, err)
			handle, err := tablecodec.DecodeIndexHandle(iter.Key(), iter.Value(), len(idxInfo.Columns))
			require.NoError(t, err)
			positions, err := tablecodec.DecodeTiKVFullTextIndexValue(iter.Value())
			require.NoError(t, err)
			if entries[handle.IntValue()] == nil {
				entries[handle.IntValue()] = make(map[string][]int)
			}
			entries[handle.IntValue()][string(term)] = positions
			decoded := make([]types.Datum, 0, len(keyValues))
			for _, encoded := range keyValues {
				_, value, err := codec.DecodeOne(encoded)
				require.NoError(t, err)
				decoded = append(decoded, value)
			}
			if seen, ok := keys[handle.IntValue()]; ok {
				require.Equal(t, seen, decoded, "handle %d", handle.IntValue())
			}
			keys[handle.IntValue()] = decoded
			require.NoError(t, iter.Next())
		}
		iter.Close()
	}
	return entries, keys
}

// TestFullTextIndexBuiltInTiKVEntries covers the entries a FULLTEXT index
// holds: one per distinct term per row, keyed by the term and carrying the
// term's positions, kept in step with inserts, updates, deletes and backfill.
func TestFullTextIndexBuiltInTiKVEntries(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Every write below is checked against the row by the mutation checker
	// and against the store by assertions.
	tk.MustExec("set tidb_enable_mutation_checker = 1")
	tk.MustExec("set tidb_txn_assertion_level = strict")

	tk.MustExec("create table t (id int primary key, n int, body text, fulltext index idx (body))")
	tk.MustExec("insert into t values (1, 0, 'hello world of distributed sql'), (2, 0, 'sql sql sql'), (3, 0, null), (4, 0, ''), (5, 0, 'of')")
	tblInfo, idxInfo := tikvFullTextIndex(t, dom, "t", "idx")
	// "of" is shorter than innodb_ft_min_token_size and drops out, but the
	// positions of the tokens around it are their ordinals in the stream.
	entries, _ := fullTextEntries(t, tk, tblInfo, idxInfo)
	require.Equal(t, map[int64]map[string][]int{
		1: {"hello": {0}, "world": {1}, "distributed": {3}, "sql": {4}},
		2: {"sql": {0, 1, 2}},
	}, entries)
	tk.MustExec("admin check table t")
	tk.MustExec("admin check index t idx")

	// The check reads the index: removing one term's entry behind its back
	// is reported, and putting it back clears the report.
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	prefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idxInfo.ID)
	iter, err := txn.Iter(prefix, prefix.PrefixNext())
	require.NoError(t, err)
	require.True(t, iter.Valid())
	removedKey, removedValue := iter.Key().Clone(), append([]byte(nil), iter.Value()...)
	iter.Close()
	require.NoError(t, txn.Delete(removedKey))
	require.NoError(t, txn.Commit(context.Background()))
	tk.MustContainErrMsg("admin check table t", "data inconsistency in table: t, index: idx, handle: 1")
	tk.MustContainErrMsg("admin check index t idx", "data inconsistency in table: t, index: idx, handle: 1")
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Set(removedKey, removedValue))
	require.NoError(t, txn.Commit(context.Background()))
	tk.MustExec("admin check table t")

	// ADMIN RECOVER INDEX puts a missing entry back; cleanup cannot read
	// the index and says so.
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(removedKey))
	require.NoError(t, txn.Commit(context.Background()))
	tk.MustQuery("admin recover index t idx").Check(testkit.Rows("1 5"))
	tk.MustExec("admin check table t")
	tk.MustContainErrMsg("admin cleanup index t idx", "fulltext index `idx` is not supported for cleanup index")

	// The other direction: an entry whose row is gone, and an entry for a
	// term the row does not contain, are both reported.
	mutate := func(fn func(txn kv.Transaction)) {
		require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		txn, err := tk.Session().Txn(true)
		require.NoError(t, err)
		fn(txn)
		require.NoError(t, txn.Commit(context.Background()))
	}
	rowKey := tablecodec.EncodeRowKeyWithHandle(tblInfo.ID, kv.IntHandle(2))
	var rowValue []byte
	mutate(func(txn kv.Transaction) {
		rowValue, err = kv.GetValue(context.Background(), txn, rowKey)
		require.NoError(t, err)
		require.NoError(t, txn.Delete(rowKey))
	})
	tk.MustContainErrMsg("admin check table t", "data inconsistency in table: t, index: idx, handle: 2")
	mutate(func(txn kv.Transaction) { require.NoError(t, txn.Set(rowKey, rowValue)) })
	tk.MustExec("admin check table t")
	bogusKey := tablecodec.EncodeIndexSeekKey(tblInfo.ID, idxInfo.ID, nil)
	encodedTerm, err := codec.EncodeKey(time.UTC, nil, types.NewBytesDatum([]byte("bogus")), types.NewIntDatum(1))
	require.NoError(t, err)
	bogusKey = append(bogusKey, encodedTerm...)
	mutate(func(txn kv.Transaction) {
		require.NoError(t, txn.Set(bogusKey, tablecodec.EncodeTiKVFullTextIndexValue(nil, tablecodec.EncodeTiKVFullTextPositions(nil, []int{0}), false)))
	})
	tk.MustContainErrMsg("admin check table t", "data inconsistency in table: t, index: idx, handle: 1")
	mutate(func(txn kv.Transaction) { require.NoError(t, txn.Delete(bogusKey)) })
	tk.MustExec("admin check table t")

	// Updating the document replaces its entries; updating another column
	// leaves them alone; deleting the row removes them.
	tk.MustExec("update t set body = 'relational storage' where id = 1")
	tk.MustExec("update t set n = 1 where id = 2")
	tk.MustExec("update t set body = 'now indexed' where id = 3")
	tk.MustExec("update t set body = null where id = 2")
	tk.MustExec("delete from t where id = 5")
	entries, _ = fullTextEntries(t, tk, tblInfo, idxInfo)
	require.Equal(t, map[int64]map[string][]int{
		1: {"relational": {0}, "storage": {1}},
		3: {"now": {0}, "indexed": {1}},
	}, entries)
	tk.MustExec("admin check table t")

	// The same within one transaction, including a row that is inserted,
	// rewritten and removed before commit.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (6, 0, 'transient text'), (7, 0, 'kept text')")
	tk.MustExec("update t set body = 'kept words' where id = 7")
	tk.MustExec("update t set n = 2 where id = 7")
	tk.MustExec("delete from t where id = 6")
	tk.MustExec("update t set body = 'relational engine' where id = 1")
	tk.MustExec("commit")
	entries, _ = fullTextEntries(t, tk, tblInfo, idxInfo)
	require.Equal(t, map[int64]map[string][]int{
		1: {"relational": {0}, "engine": {1}},
		3: {"now": {0}, "indexed": {1}},
		7: {"kept": {0}, "words": {1}},
	}, entries)
	tk.MustExec("admin check table t")
	tk.MustExec("rollback")

	// NGRAM entries are the grams, positioned by character.
	tk.MustExec("create table ng (id int primary key, body varchar(100), fulltext index idx (body) with parser ngram)")
	tk.MustExec("insert into ng values (1, 'abcab'), (2, '数据库')")
	ngInfo, ngIdx := tikvFullTextIndex(t, dom, "ng", "idx")
	entries, _ = fullTextEntries(t, tk, ngInfo, ngIdx)
	require.Equal(t, map[int64]map[string][]int{
		1: {"ab": {0, 3}, "bc": {1}, "ca": {2}},
		2: {"数据": {0}, "据库": {1}},
	}, entries)
	tk.MustExec("admin check table ng")

	// Adding the index to a populated table backfills it, and a long NGRAM
	// document fans out into one entry per distinct gram with all of its
	// positions.
	tk.MustExec("create table backfilled (id int primary key, body text)")
	long := strings.Repeat("the quick brown fox jumps over the lazy dog ", 500)
	tk.MustExec("insert into backfilled values (1, 'distributed sql database'), (2, null), (3, ?)", long)
	tk.MustExec("alter table backfilled add fulltext index idx (body) with parser ngram")
	bfInfo, bfIdx := tikvFullTextIndex(t, dom, "backfilled", "idx")
	entries, _ = fullTextEntries(t, tk, bfInfo, bfIdx)
	require.Len(t, entries, 2)
	require.Equal(t, []int{0}, entries[1]["di"])
	// Grams are positioned densely across the token stream: "the" is the
	// first word and, 19 grams later, the seventh.
	require.Len(t, entries[3]["th"], 1000)
	require.Equal(t, []int{0, 19}, entries[3]["th"][:2])
	tk.MustExec("admin check table backfilled")
	tk.MustExec("update backfilled set body = concat(body, ' end') where id = 3")
	tk.MustExec("admin check table backfilled")

	// Each partition holds the entries of its own rows.
	tk.MustExec("create table pt (id int primary key, body text, fulltext index idx (body)) partition by hash(id) partitions 2")
	tk.MustExec("insert into pt values (1, 'first partition'), (2, 'second partition')")
	ptInfo, ptIdx := tikvFullTextIndex(t, dom, "pt", "idx")
	entries, _ = fullTextEntries(t, tk, ptInfo, ptIdx)
	require.Equal(t, map[int64]map[string][]int{
		1: {"first": {0}, "partition": {1}},
		2: {"second": {0}, "partition": {1}},
	}, entries)
	tk.MustExec("admin check table pt")
	tk.MustExec("delete from pt where id = 1")
	tk.MustExec("admin check table pt")
	tk.MustExec("alter table pt drop index idx")

	// The column must stay text the analyzer can read.
	tk.MustContainErrMsg("alter table t modify column body int", "FULLTEXT index requires a string column")
	// A collation change under an index is refused before the index rules
	// are consulted, which covers the binary and other-charset cases.
	tk.MustContainErrMsg("alter table t modify column body blob", "Unsupported modifying collation")
	tk.MustContainErrMsg("alter table t modify column body text charset gbk", "Unsupported modifying collation")
	tk.MustExec("alter table t modify column body varchar(500)")
	tk.MustExec("admin check table t")
	tk.MustContainErrMsg("create table bad_charset (body text charset gbk, fulltext index idx (body))", "FULLTEXT index requires a utf8mb4, utf8, ascii or latin1 column")

	// Key columns ahead of the tokenized column: each entry of a row carries
	// the row's key values before the term, follows the row when they
	// change, and an entry under another tenant is reported by the check.
	tk.MustExec("create table kt (id int primary key, tenant int, body text, fulltext index idx (tenant, body))")
	tk.MustExec("insert into kt values (1, 10, 'hello world'), (2, 20, 'hello sql'), (3, null, 'hello null')")
	ktInfo, ktIdx := tikvFullTextIndex(t, dom, "kt", "idx")
	entries, keys := fullTextEntries(t, tk, ktInfo, ktIdx)
	require.Equal(t, map[int64]map[string][]int{
		1: {"hello": {0}, "world": {1}},
		2: {"hello": {0}, "sql": {1}},
		3: {"hello": {0}, "null": {1}},
	}, entries)
	require.Equal(t, map[int64][]types.Datum{
		1: {types.NewIntDatum(10)}, 2: {types.NewIntDatum(20)}, 3: {types.NewDatum(nil)},
	}, keys)
	tk.MustExec("update kt set tenant = 30 where id = 1")
	tk.MustExec("update kt set body = 'moved along' where id = 2")
	_, keys = fullTextEntries(t, tk, ktInfo, ktIdx)
	require.Equal(t, []types.Datum{types.NewIntDatum(30)}, keys[1])
	require.Equal(t, []types.Datum{types.NewIntDatum(20)}, keys[2])
	tk.MustExec("admin check table kt")
	tk.MustExec("admin check index kt idx")
	// An entry with the right term under the wrong tenant does not belong
	// to the row.
	strayKey := tablecodec.EncodeIndexSeekKey(ktInfo.ID, ktIdx.ID, nil)
	encodedStray, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(99), types.NewBytesDatum([]byte("hello")), types.NewIntDatum(1))
	require.NoError(t, err)
	strayKey = append(strayKey, encodedStray...)
	mutate(func(txn kv.Transaction) {
		require.NoError(t, txn.Set(strayKey, tablecodec.EncodeTiKVFullTextIndexValue(nil, tablecodec.EncodeTiKVFullTextPositions(nil, []int{0}), false)))
	})
	tk.MustContainErrMsg("admin check table kt", "data inconsistency in table: kt, index: idx, handle: 1")
	mutate(func(txn kv.Transaction) { require.NoError(t, txn.Delete(strayKey)) })
	tk.MustExec("admin check table kt")
	tk.MustExec("delete from kt")
	entries, _ = fullTextEntries(t, tk, ktInfo, ktIdx)
	require.Empty(t, entries)
}
