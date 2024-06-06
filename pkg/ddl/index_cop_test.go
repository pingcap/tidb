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
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
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

	testFetchRows := func(db, tb, idx string) ([]kv.Handle, [][]types.Datum) {
		tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(tb))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		idxInfo := tblInfo.FindIndexByName(idx)

		sctx := tk.Session()
		copCtx, err := ddl.NewReorgCopContext(store, ddl.NewDDLReorgMeta(sctx), tblInfo, []*model.IndexInfo{idxInfo}, "")
		require.NoError(t, err)
		require.IsType(t, copCtx, &copr.CopContextSingleIndex{})
		startKey := tbl.RecordPrefix()
		endKey := startKey.PrefixNext()
		txn, err := store.Begin()
		require.NoError(t, err)
		copChunk := ddl.FetchChunk4Test(copCtx, tbl.(table.PhysicalTable), startKey, endKey, store, 10)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback())

		iter := chunk.NewIterator4Chunk(copChunk)
		handles := make([]kv.Handle, 0, copChunk.NumRows())
		values := make([][]types.Datum, 0, copChunk.NumRows())
		handleDataBuf := make([]types.Datum, len(copCtx.GetBase().HandleOutputOffsets))
		idxDataBuf := make([]types.Datum, len(idxInfo.Columns))

		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, idxDatum, err := ddl.ConvertRowToHandleAndIndexDatum(tk.Session().GetExprCtx().GetEvalCtx(), handleDataBuf, idxDataBuf, row, copCtx, idxInfo.ID)
			require.NoError(t, err)
			handles = append(handles, handle)
			copiedIdxDatum := make([]types.Datum, len(idxDatum))
			copy(copiedIdxDatum, idxDatum)
			values = append(values, copiedIdxDatum)
		}
		return handles, values
	}

	// Test nonclustered primary key table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	hds, vals := testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := 0; i < 8; i++ {
		require.Equal(t, hds[i].IntValue(), int64(i+1))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}

	// Test clustered primary key table(pk_is_handle).
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint primary key, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := 0; i < 8; i++ {
		require.Equal(t, hds[i].IntValue(), int64(i))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}

	// Test clustered primary key table(common_handle).
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(10), b int, c char(10), primary key (a, c) clustered, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, strconv.Itoa(i))
	}
	hds, vals = testFetchRows("test", "t", "idx")
	require.Len(t, hds, 8)
	for i := 0; i < 8; i++ {
		require.Equal(t, hds[i].String(), fmt.Sprintf("{%d, %d}", i, i))
		require.Len(t, vals[i], 1)
		require.Equal(t, vals[i][0].GetInt64(), int64(i))
	}
}
