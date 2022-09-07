// Copyright 2018 PingCAP, Inc.
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
	"testing"

	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPointGetLockIfExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// se := tk.Session()
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("set transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	// tableID := external.GetTableByName(t, tk, "test", "t1").Meta().ID
	// key1 := tablecodec.EncodeRowKeyWithHandle(tableID, tidbkv.IntHandle(1))

	// cluster index, lock wait
	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 where id1 = 1 for update")
	/* txn, err := tk.Session().Txn(false)
	require.Nil(t, err)
	memBuf := txn.GetMemBuffer()
	flags, err := memBuf.GetFlags(key1)
	require.Equal(t, flags.HasNeedLocked(), true)
	txnCtx := tk.Session().GetSessionVars().TxnCtx
	val, ok := txnCtx.GetKeyInPessimisticLockCache(key1)
	fmt.Println(val)
	require.True(t, ok) */
	_, err := tk2.Exec("update t1 set id3 = 100 where id1 = 10")
	require.Error(t, err)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), err.Error())
	tk.MustExec("commit")
}
