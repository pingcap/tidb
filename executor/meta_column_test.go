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

package executor_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestMetaColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t1 values (2)")
	tk.MustExec("insert into t1 values (3)")
	tk.MustExec("set @@tidb_write_by_ticdc=1")
	tk.MustExec("insert into t1 values (4)")
	tk.MustExec("insert into t1 values (5)")
	tk.MustExec("insert into t1 values (6)")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	checkMetaColumn(t, t1, store, func(m map[int64]types.Datum) {
		datum := m[t1.Cols()[0].ID]
		meta := m[model.ExtraMetaColID]
		if datum.GetInt64() < 4 {
			require.True(t, meta.IsNull())
		} else {
			compare := meta.GetBinaryLiteral4Cmp().Compare(types.NewBinaryLiteralFromUint(1, 1))
			require.Equal(t, 0, compare)
		}
	})

	tk.MustExec("update t1 set a = a - 1 where a <= 3")
	checkMetaColumn(t, t1, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		compare := meta.GetBinaryLiteral4Cmp().Compare(types.NewBinaryLiteralFromUint(1, 1))
		require.Equal(t, 0, compare)
	})

	tk.MustExec("set @@tidb_write_by_ticdc=0")
	tk.MustExec("update t1 set a = a + 1")
	checkMetaColumn(t, t1, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		require.True(t, meta.IsNull())
	})

	tk.MustExec("create table t2(b int)")
	tk.MustExec("insert into t2 values (4)")
	tk.MustExec("insert into t2 values (5)")
	tk.MustExec("insert into t2 values (6)")

	t2, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	checkMetaColumn(t, t2, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		require.True(t, meta.IsNull())
	})

	tk.MustExec("set @@tidb_write_by_ticdc=1")
	tk.MustExec("update t1, t2 set t1.a= t1.a+1, t2.b=t2.b+1")
	checkMetaColumn(t, t1, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		compare := meta.GetBinaryLiteral4Cmp().Compare(types.NewBinaryLiteralFromUint(1, 1))
		require.Equal(t, 0, compare)
	})
	checkMetaColumn(t, t2, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		compare := meta.GetBinaryLiteral4Cmp().Compare(types.NewBinaryLiteralFromUint(1, 1))
		require.Equal(t, 0, compare)
	})

	tk.MustExec("create table t3(a int)")
	tk.MustExec("set @@tidb_write_by_ticdc=0")
	tk.MustExec("insert into t3 select * from t1")
	t3, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	checkMetaColumn(t, t3, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		require.True(t, meta.IsNull())
	})

	tk.MustExec("create table t4(a int)")
	tk.MustExec("set @@tidb_write_by_ticdc=1")
	tk.MustExec("insert into t4 select * from t3")
	t4, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	checkMetaColumn(t, t4, store, func(m map[int64]types.Datum) {
		meta := m[model.ExtraMetaColID]
		compare := meta.GetBinaryLiteral4Cmp().Compare(types.NewBinaryLiteralFromUint(1, 1))
		require.Equal(t, 0, compare)
	})
}

func checkMetaColumn(t *testing.T, tbl table.Table, store kv.Storage, fn func(map[int64]types.Datum)) {
	se := testkit.NewTestKit(t, store).Session()
	err := sessiontxn.NewTxn(context.Background(), se)
	require.NoError(t, err)
	txn, err := se.Txn(true)
	require.NoError(t, err)

	prefix := tbl.RecordPrefix()
	startKey := tablecodec.EncodeRecordKey(prefix, kv.IntHandle(math.MinInt64))
	it, err := txn.Iter(startKey, prefix.PrefixNext())
	require.NoError(t, err)

	defer it.Close()

	require.True(t, it.Valid())

	colMap := make(map[int64]*types.FieldType, 3)
	m := make([]*model.ColumnInfo, 0, len(tbl.Meta().Columns)+1)
	for i := range tbl.Meta().Columns {
		m = append(m, tbl.Meta().Columns[i].Clone())
	}

	m = append(m, model.NewMetaColumn())
	for _, col := range m {
		colMap[col.ID] = &(col.FieldType)
	}

	for it.Valid() && it.Key().HasPrefix(prefix) {
		handle, err := tablecodec.DecodeRowKey(it.Key())
		require.NoError(t, err)
		rs, err := tablecodec.DecodeRowToDatumMap(it.Value(), colMap, time.UTC)
		require.NoError(t, err)
		fn(rs)
		require.NoError(t, err)
		rk := tablecodec.EncodeRecordKey(prefix, handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		require.NoError(t, err)
	}
}
