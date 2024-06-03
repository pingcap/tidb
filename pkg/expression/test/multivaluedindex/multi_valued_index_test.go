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

package multivaluedindex

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestWriteMultiValuedIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(pk int primary key, a json, index idx((cast(a as signed array))))")
	tk.MustExec("insert into t1 values (1, '[1,2,2,3]')")
	tk.MustExec("insert into t1 values (2, '[1,2,3]')")
	tk.MustExec("insert into t1 values (3, '[]')")
	tk.MustExec("insert into t1 values (4, '[2,3,4]')")
	tk.MustExec("insert into t1 values (5, null)")
	tk.MustExec("insert into t1 values (6, '1')")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 11)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewDatum(nil), types.NewIntDatum(5)},
				{types.NewIntDatum(1), types.NewIntDatum(1)},
				{types.NewIntDatum(1), types.NewIntDatum(2)},
				{types.NewIntDatum(1), types.NewIntDatum(6)},
				{types.NewIntDatum(2), types.NewIntDatum(1)},
				{types.NewIntDatum(2), types.NewIntDatum(2)},
				{types.NewIntDatum(2), types.NewIntDatum(4)},
				{types.NewIntDatum(3), types.NewIntDatum(1)},
				{types.NewIntDatum(3), types.NewIntDatum(2)},
				{types.NewIntDatum(3), types.NewIntDatum(4)},
				{types.NewIntDatum(4), types.NewIntDatum(4)},
			})
		}
	}
	tk.MustExec("delete from t1")
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 0)
		}
	}

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1(pk int primary key, a json, index idx((cast(a as char(5) array))))")
	tk.MustExec("insert into t1 values (1, '[\"abc\", \"abc \"]')")
	tk.MustExec("insert into t1 values (2, '[\"b\"]')")
	tk.MustExec("insert into t1 values (3, '[\"b   \"]')")
	tk.MustQuery("select pk from t1 where 'b   ' member of (a)").Check(testkit.Rows("3"))

	t1, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 4)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewBytesDatum([]byte("abc")), types.NewIntDatum(1)},
				{types.NewBytesDatum([]byte("abc ")), types.NewIntDatum(1)},
				{types.NewBytesDatum([]byte("b")), types.NewIntDatum(2)},
				{types.NewBytesDatum([]byte("b   ")), types.NewIntDatum(3)},
			})
		}
	}

	tk.MustExec("update t1 set a = json_array_append(a, '$', 'bcd') where pk = 1")
	tk.MustExec("update t1 set a = '[]' where pk = 2")
	tk.MustExec("update t1 set a = '[\"abc\"]' where pk = 3")

	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 4)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewBytesDatum([]byte("abc")), types.NewIntDatum(1)},
				{types.NewBytesDatum([]byte("abc")), types.NewIntDatum(3)},
				{types.NewBytesDatum([]byte("abc ")), types.NewIntDatum(1)},
				{types.NewBytesDatum([]byte("bcd")), types.NewIntDatum(1)},
			})
		}
	}

	tk.MustExec("delete from t1")
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 0)
		}
	}

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1(pk int primary key, a json, index idx((cast(a as unsigned array))))")
	tk.MustExec("insert into t1 values (1, '[1,2,2,3]')")
	tk.MustExec("insert into t1 values (2, '[1,2,3]')")
	tk.MustExec("insert into t1 values (3, '[]')")
	tk.MustExec("insert into t1 values (4, '[2,3,4]')")
	tk.MustExec("insert into t1 values (5, null)")
	tk.MustExec("insert into t1 values (6, '1')")

	t1, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 11)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewDatum(nil), types.NewIntDatum(5)},
				{types.NewUintDatum(1), types.NewIntDatum(1)},
				{types.NewUintDatum(1), types.NewIntDatum(2)},
				{types.NewUintDatum(1), types.NewIntDatum(6)},
				{types.NewUintDatum(2), types.NewIntDatum(1)},
				{types.NewUintDatum(2), types.NewIntDatum(2)},
				{types.NewUintDatum(2), types.NewIntDatum(4)},
				{types.NewUintDatum(3), types.NewIntDatum(1)},
				{types.NewUintDatum(3), types.NewIntDatum(2)},
				{types.NewUintDatum(3), types.NewIntDatum(4)},
				{types.NewUintDatum(4), types.NewIntDatum(4)},
			})
		}
	}
	tk.MustExec("delete from t1")
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 0)
		}
	}
}

func TestWriteMultiValuedIndexPartitionTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1
(
    pk int primary key,
    a  json,
    index idx ((cast(a as signed array)))
) partition by range columns (pk) (partition p0 values less than (10), partition p1 values less than (20));`)
	tk.MustExec("insert into t1 values (1, '[1,2,2,3]')")
	tk.MustExec("insert into t1 values (11, '[1,2,3]')")
	tk.MustExec("insert into t1 values (2, '[]')")
	tk.MustExec("insert into t1 values (12, '[2,3,4]')")
	tk.MustExec("insert into t1 values (3, null)")
	tk.MustExec("insert into t1 values (13, null)")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)

	expect := map[string]struct {
		count int
		vals  [][]types.Datum
	}{
		"p0": {4, [][]types.Datum{
			{types.NewDatum(nil), types.NewIntDatum(3)},
			{types.NewIntDatum(1), types.NewIntDatum(1)},
			{types.NewIntDatum(2), types.NewIntDatum(1)},
			{types.NewIntDatum(3), types.NewIntDatum(1)},
		}},
		"p1": {7, [][]types.Datum{
			{types.NewDatum(nil), types.NewIntDatum(13)},
			{types.NewIntDatum(1), types.NewIntDatum(11)},
			{types.NewIntDatum(2), types.NewIntDatum(11)},
			{types.NewIntDatum(2), types.NewIntDatum(12)},
			{types.NewIntDatum(3), types.NewIntDatum(11)},
			{types.NewIntDatum(3), types.NewIntDatum(12)},
			{types.NewIntDatum(4), types.NewIntDatum(12)},
		}},
	}

	for _, def := range t1.Meta().GetPartitionInfo().Definitions {
		partition := t1.(table.PartitionedTable).GetPartition(def.ID)
		for _, index := range partition.Indices() {
			if index.Meta().MVIndex {
				checkCount(t, partition.IndexPrefix(), index, store, expect[def.Name.L].count)
				checkKey(t, partition.IndexPrefix(), index, store, expect[def.Name.L].vals)
			}
		}
	}

	tk.MustExec("delete from t1")
	for _, def := range t1.Meta().GetPartitionInfo().Definitions {
		partition := t1.(table.PartitionedTable).GetPartition(def.ID)
		for _, index := range partition.Indices() {
			if index.Meta().MVIndex {
				checkCount(t, partition.IndexPrefix(), index, store, 0)
			}
		}
	}
}

func TestWriteMultiValuedIndexUnique(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(pk int primary key, a json, unique index idx((cast(a as signed array))))")
	tk.MustExec("insert into t1 values (1, '[1,2,2]')")
	tk.MustGetErrCode("insert into t1 values (2, '[1]')", errno.ErrDupEntry)
	tk.MustExec("insert into t1 values (3, '[3,3,4]')")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 4)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewIntDatum(1)},
				{types.NewIntDatum(2)},
				{types.NewIntDatum(3)},
				{types.NewIntDatum(4)},
			})
		}
	}
}

func TestWriteMultiValuedIndexComposite(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(pk int primary key, a json, c int, d int, index idx(c, (cast(a as signed array)), d))")
	tk.MustExec("insert into t1 values (1, '[1,2,2]', 1, 1)")
	tk.MustExec("insert into t1 values (2, '[2,2,2]', 2, 2)")
	tk.MustExec("insert into t1 values (3, '[3,3,4]', 3, 3)")
	tk.MustExec("insert into t1 values (4, null, 4, 4)")
	tk.MustExec("insert into t1 values (5, '[]', 5, 5)")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	for _, index := range t1.Indices() {
		if index.Meta().MVIndex {
			checkCount(t, t1.IndexPrefix(), index, store, 6)
			checkKey(t, t1.IndexPrefix(), index, store, [][]types.Datum{
				{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(1)},
				{types.NewIntDatum(1), types.NewIntDatum(2), types.NewIntDatum(1), types.NewIntDatum(1)},
				{types.NewIntDatum(2), types.NewIntDatum(2), types.NewIntDatum(2), types.NewIntDatum(2)},
				{types.NewIntDatum(3), types.NewIntDatum(3), types.NewIntDatum(3), types.NewIntDatum(3)},
				{types.NewIntDatum(3), types.NewIntDatum(4), types.NewIntDatum(3), types.NewIntDatum(3)},
				{types.NewIntDatum(4), types.NewDatum(nil), types.NewIntDatum(4), types.NewIntDatum(4)},
			})
		}
	}
}

func checkCount(t *testing.T, prefix kv.Key, index table.Index, store kv.Storage, except int) {
	c := 0
	checkIndex(t, prefix, index, store, func(it kv.Iterator) {
		c++
	})
	require.Equal(t, except, c)
}

func checkKey(t *testing.T, prefix kv.Key, index table.Index, store kv.Storage, except [][]types.Datum) {
	idx := 0
	checkIndex(t, prefix, index, store, func(it kv.Iterator) {
		indexKey := decodeIndexKey(t, it.Key())
		require.Equal(t, except[idx], indexKey)
		idx++
	})
}

func checkIndex(t *testing.T, prefix kv.Key, index table.Index, store kv.Storage, fn func(kv.Iterator)) {
	startKey := codec.EncodeInt(prefix, index.Meta().ID)
	prefix.Next()
	se := testkit.NewTestKit(t, store).Session()
	err := sessiontxn.NewTxn(context.Background(), se)
	require.NoError(t, err)
	txn, err := se.Txn(true)
	require.NoError(t, err)
	it, err := txn.Iter(startKey, prefix.PrefixNext())
	require.NoError(t, err)
	for it.Valid() && it.Key().HasPrefix(prefix) {
		fn(it)
		err = it.Next()
		require.NoError(t, err)
	}
	it.Close()
	se.Close()
}

func decodeIndexKey(t *testing.T, key kv.Key) []types.Datum {
	var idLen = 8
	var prefixLen = 1 + idLen /*tableID*/ + 2
	_, _, isRecord, err := tablecodec.DecodeKeyHead(key)
	require.NoError(t, err)
	require.False(t, isRecord)
	indexKey := key[prefixLen+idLen:]
	var datumValues []types.Datum
	for len(indexKey) > 0 {
		remain, d, err := codec.DecodeOne(indexKey)
		require.NoError(t, err)
		datumValues = append(datumValues, d)
		indexKey = remain
	}
	return datumValues
}
