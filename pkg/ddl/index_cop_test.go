// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestAddIndexFetchRowsFromCoprocessor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testFetchRowsForIndexes := func(db, tb string, idxNames []string, targetIdx string) ([]kv.Handle, [][]types.Datum) {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(tb))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		idxInfos := make([]*model.IndexInfo, 0, len(idxNames))
		for _, idx := range idxNames {
			idxInfo := tblInfo.FindIndexByName(idx)
			require.NotNil(t, idxInfo)
			idxInfos = append(idxInfos, idxInfo)
		}
		targetIdxInfo := tblInfo.FindIndexByName(targetIdx)
		require.NotNil(t, targetIdxInfo)

		sctx := tk.Session()
		copCtx, err := ddl.NewReorgCopContext(ddl.NewDDLReorgMeta(sctx), tblInfo, idxInfos, "")
		require.NoError(t, err)
		startKey := tbl.RecordPrefix()
		endKey := startKey.PrefixNext()
		txn, err := store.Begin()
		require.NoError(t, err)
		copChunk, err := FetchChunk4Test(copCtx, tbl.(table.PhysicalTable), startKey, endKey, store, 10)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback())

		iter := chunk.NewIterator4Chunk(copChunk)
		handles := make([]kv.Handle, 0, copChunk.NumRows())
		values := make([][]types.Datum, 0, copChunk.NumRows())
		handleDataBuf := make([]types.Datum, len(copCtx.GetBase().HandleOutputOffsets))
		idxDataBuf := make([]types.Datum, len(targetIdxInfo.Columns))

		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, idxDatum, err := ConvertRowToHandleAndIndexDatum(
				tk.Session().GetExprCtx().GetEvalCtx(), handleDataBuf, idxDataBuf, row, copCtx, targetIdxInfo.ID)
			require.NoError(t, err)
			handles = append(handles, handle)
			copiedIdxDatum := make([]types.Datum, len(idxDatum))
			copy(copiedIdxDatum, idxDatum)
			values = append(values, copiedIdxDatum)
		}
		return handles, values
	}
	testFetchRows := func(db, tb, idx string) ([]kv.Handle, [][]types.Datum) {
		return testFetchRowsForIndexes(db, tb, []string{idx}, idx)
	}

	// Test nonclustered primary key table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := range 8 {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	hds, vals := testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := range 8 {
		require.Equal(t, hds[i].IntValue(), int64(i+1))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}

	// Test clustered primary key table(pk_is_handle).
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint primary key, b int, index idx (b));")
	for i := range 8 {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := range 8 {
		require.Equal(t, hds[i].IntValue(), int64(i))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}

	// Test clustered primary key table(common_handle).
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(10), b int, c char(10), primary key (a, c) clustered, index idx (b));")
	for i := range 8 {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, strconv.Itoa(i))
	}
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := range 8 {
		require.Equal(t, hds[i].String(), fmt.Sprintf("{%d, %d}", i, i))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}

	// Test prefix index values are truncated in coprocessor for index-only columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20) character set utf8mb4 collate utf8mb4_bin, index idx (a(2)));")
	tk.MustExec("insert into t values ('abcdef'), ('你好世界');")
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 2)
	require.Len(t, vals[0], 1)
	require.Equal(t, "ab", vals[0][0].GetString())
	require.Len(t, vals[1], 1)
	require.Equal(t, "你好", vals[1][0].GetString())

	// Test binary prefix index values are truncated by bytes.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varbinary(20), index idx (a(2)));")
	tk.MustExec("insert into t values (x'e4bda0e5a5bd');")
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 1)
	require.Len(t, vals[0], 1)
	require.Equal(t, []byte{0xe4, 0xbd}, vals[0][0].GetBytes())

	// Test common-handle prefix columns fetch the longest prefix needed by all
	// handle and secondary-index consumers.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20), primary key (a(4)) clustered, index idx (a(2)));")
	tk.MustExec("insert into t values ('abcdef');")
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 1)
	require.Equal(t, "{abcd}", hds[0].String())
	require.Len(t, vals[0], 1)
	require.Equal(t, "abcd", vals[0][0].GetString())

	// Test full common-handle columns still fetch the full value.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20), primary key (a) clustered, index idx (a(2)));")
	tk.MustExec("insert into t values ('abcdef');")
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 1)
	require.Equal(t, "{abcdef}", hds[0].String())
	require.Len(t, vals[0], 1)
	require.Equal(t, "abcdef", vals[0][0].GetString())

	// Test merged add-index scan fetches the longest prefix needed by all indexes
	// over the same source column.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20), index idx_short(a(2)), index idx_long(a(4)));")
	tk.MustExec("insert into t values ('abcdef');")
	hds, vals = testFetchRowsForIndexes("test", "t", []string{"idx_short", "idx_long"}, "idx_short")
	require.Len(t, hds, 1)
	require.Len(t, vals[0], 1)
	require.Equal(t, "abcd", vals[0][0].GetString())

	// Test any full-value consumer disables prefix projection for that source column.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20), index idx_prefix(a(2)), index idx_full(a));")
	tk.MustExec("insert into t values ('abcdef');")
	hds, vals = testFetchRowsForIndexes("test", "t", []string{"idx_prefix", "idx_full"}, "idx_prefix")
	require.Len(t, hds, 1)
	require.Len(t, vals[0], 1)
	require.Equal(t, "abcdef", vals[0][0].GetString())

	// Test partial-index conditions keep full values for condition/checker safety.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(20), b int, index idx (a(2)) where b > 0);")
	tk.MustExec("insert into t values ('abcdef', 1);")
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 1)
	require.Len(t, vals[0], 1)
	require.Equal(t, "abcdef", vals[0][0].GetString())
}
