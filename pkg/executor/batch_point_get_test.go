// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestBatchPointGetLockExistKey(t *testing.T) {
	var wg sync.WaitGroup
	errCh := make(chan error)
	store := testkit.CreateMockStore(t)

	testLock := func(rc bool, key string, tableName string) {
		doneCh := make(chan struct{}, 1)
		tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)

		errCh <- tk1.ExecToErr("use test")
		errCh <- tk2.ExecToErr("use test")
		tk1.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

		errCh <- tk1.ExecToErr(fmt.Sprintf("drop table if exists %s", tableName))
		errCh <- tk1.ExecToErr(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		errCh <- tk1.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 1), (2, 2, 2)", tableName))

		if rc {
			errCh <- tk1.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
			errCh <- tk2.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
		}

		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")

		// select for update
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2)) for update", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2), (3, 3)) for update", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			doneCh <- struct{}{}
		}()

		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))

		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"3 3 3",
			"1 1 10",
		))

		// update
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = v + 1 where (id, v) in ((2, 2), (3, 3))", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = v + 1 where (id, v) in ((2, 2), (3, 3), (4, 4))", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(4, 4, 4)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 30)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"3 4 3",
			"1 1 10",
			"4 4 4",
			"3 3 30",
		))

		// delete
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where (id, v) in ((3, 4), (4, 4))", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where (id, v) in ((3, 4), (4, 4), (5, 5))", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(5, 5, 5)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(4, 4,40)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"3 3 30",
			"5 5 5",
			"4 4 40",
		))
		wg.Done()
	}

	for i, one := range []struct {
		rc  bool
		key string
	}{
		{rc: false, key: "primary key"},
		{rc: false, key: "unique key"},
		{rc: true, key: "primary key"},
		{rc: true, key: "unique key"},
	} {
		wg.Add(1)
		tableName := fmt.Sprintf("t_%d", i)
		go testLock(one.rc, one.key, tableName)
	}

	// should works for common handle in clustered index
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id varchar(40) primary key)")
	tk.MustExec("insert into t values('1'), ('2')")
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t where id in('1', '2') for update")
	tk.MustExec("commit")

	go func() {
		wg.Wait()
		close(errCh)
	}()
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestCacheSnapShot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	ctx := context.Background()
	txn, err := se.GetStore().Begin(tikv.WithStartTS(0))
	memBuffer := txn.GetMemBuffer()
	require.NoError(t, err)
	var keys []kv.Key
	for i := 0; i < 2; i++ {
		keys = append(keys, []byte(string(rune(i))))
	}
	err = memBuffer.Set(keys[0], []byte("1111"))
	require.NoError(t, err)
	err = memBuffer.Set(keys[1], []byte("2222"))
	require.NoError(t, err)
	cacheTableSnapShot := executor.MockNewCacheTableSnapShot(nil, memBuffer)
	get, err := cacheTableSnapShot.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Equal(t, get, []byte("1111"))
	batchGet, err := cacheTableSnapShot.BatchGet(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, batchGet[string(keys[0])], []byte("1111"))
	require.Equal(t, batchGet[string(keys[1])], []byte("2222"))
}

func TestPointGetForTemporaryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create global temporary table t1 (id int primary key, val int) on commit delete rows")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustQuery("explain format = 'brief' select * from t1 where id in (1, 2, 3)").
		Check(testkit.Rows("Batch_Point_Get 3.00 root table:t1 handle:[1 2 3], keep order:false, desc:false"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy"))
	}()

	// Batch point get.
	tk.MustQuery("select * from t1 where id in (1, 2, 3)").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id in (2, 3)").Check(testkit.Rows())

	// Point get.
	tk.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id = 2").Check(testkit.Rows())
}
