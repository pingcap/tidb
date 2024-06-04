// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func BenchmarkExtractDatumByOffsets(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")
	sctx := tk.Session()
	copCtx, err := ddl.NewReorgCopContext(store, ddl.NewDDLReorgMeta(sctx), tblInfo, []*model.IndexInfo{idxInfo}, "")
	require.NoError(b, err)
	require.IsType(b, copCtx, &copr.CopContextSingleIndex{})
	require.NoError(b, err)
	startKey := tbl.RecordPrefix()
	endKey := startKey.PrefixNext()
	txn, err := store.Begin()
	require.NoError(b, err)
	copChunk := ddl.FetchChunk4Test(copCtx, tbl.(table.PhysicalTable), startKey, endKey, store, 10)
	require.NoError(b, err)
	require.NoError(b, txn.Rollback())

	handleDataBuf := make([]types.Datum, len(copCtx.GetBase().HandleOutputOffsets))

	iter := chunk.NewIterator4Chunk(copChunk)
	row := iter.Begin()
	c := copCtx.GetBase()
	offsets := copCtx.IndexColumnOutputOffsets(idxInfo.ID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ddl.ExtractDatumByOffsetsForTest(tk.Session().GetExprCtx().GetEvalCtx(), row, offsets, c.ExprColumnInfos, handleDataBuf)
	}
}

func BenchmarkGenerateIndexKV(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int, index idx (b));")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(b, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")

	index := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sctx := tk.Session().GetSessionVars().StmtCtx
	idxDt := []types.Datum{types.NewIntDatum(10)}
	buf := make([]byte, 0, 64)
	handle := kv.IntHandle(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		iter := index.GenIndexKVIter(sctx.ErrCtx(), sctx.TimeZone(), idxDt, handle, nil)
		_, _, _, err = iter.Next(buf, nil)
		if err != nil {
			break
		}
	}
	require.NoError(b, err)
}
