// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syssession_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func newSessionFromStore(t *testing.T, store kv.Storage) (tk *testkit.TestKit, _ *syssession.Session) {
	pool := syssession.NewPool(128, func() (syssession.SessionContext, error) {
		require.Nil(t, tk)
		tk = testkit.NewTestKit(t, store)
		return tk.Session(), nil
	})
	se, err := pool.Get()
	pool.Close()
	require.NoError(t, err)
	return tk, se
}

func TestIntergrationSessionResetState(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk, se := newSessionFromStore(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table t(a int)")
	tk.MustExec("create table t1(id int primary key, a int)")
	tk.MustExec("insert into t1 values(1, 2)")

	// active the transaction
	tk.MustExec("begin pessimistic")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	// write some rows to MemBuffer
	tk.MustExec("insert into t values(1)")
	tk.MustExec("insert into t1 values(3, 4)")
	tk.MustExec("update t1 set a = 10 where id=1")
	require.NoError(t, txn.Set([]byte("a"), []byte("b")))

	// ResetState and then check txn cleared.
	require.NoError(t, se.ResetState(context.Background()))
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())

	// The dirty rows should not commit
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	_, err = txn.Get(context.Background(), []byte("a"))
	require.EqualError(t, err, "[kv:8021]Error: key not exist")
	require.NoError(t, se.ResetState(context.Background()))
}
