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

package session_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSessionPool(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	resourcePool := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 4, 4, 0)
	pool := session.NewSessionPool(resourcePool)
	sessCtx, err := pool.Get()
	require.NoError(t, err)
	se := session.NewSession(sessCtx)
	err = se.Begin(context.Background())
	startTS := se.GetSessionVars().TxnCtx.StartTS
	require.NoError(t, err)
	rows, err := se.Execute(context.Background(), "select 2;", "test")
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(2), rows[0].GetInt64(0))
	mgr := tk.Session().GetSessionManager()
	tsList := mgr.GetInternalSessionStartTSList()
	var targetTS uint64
	for _, ts := range tsList {
		if ts == startTS {
			targetTS = ts
			break
		}
	}
	require.NotEqual(t, uint64(0), targetTS)
	err = se.Commit(context.Background())
	pool.Put(sessCtx)
	require.NoError(t, err)
	tsList = mgr.GetInternalSessionStartTSList()
	targetTS = 0
	for _, ts := range tsList {
		if ts == startTS {
			targetTS = ts
			break
		}
	}
	require.Equal(t, uint64(0), targetTS)
}

func TestPessimisticTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1)")

	resourcePool := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 4, 4, 0)
	pool := session.NewSessionPool(resourcePool)
	ctx := context.Background()

	sessCtx, err := pool.Get()
	require.NoError(t, err)
	se := session.NewSession(sessCtx)
	sessCtx2, err := pool.Get()
	require.NoError(t, err)
	se2 := session.NewSession(sessCtx2)

	err = se.BeginPessimistic(ctx)
	require.NoError(t, err)
	err = se2.BeginPessimistic(ctx)
	require.NoError(t, err)
	_, err = se.Execute(ctx, "update test.t set b = b + 1 where a = 1", "ut")
	require.NoError(t, err)
	done := make(chan struct{}, 1)
	go func() {
		_, err := se2.Execute(ctx, "update test.t set b = b + 1 where a = 1", "ut")
		require.NoError(t, err)
		done <- struct{}{}
		err = se2.Commit(ctx)
		require.NoError(t, err)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	// because this is a pessimistic transaction, the second transaction should be blocked
	require.Len(t, done, 0)
	err = se.Commit(ctx)
	require.NoError(t, err)
	<-done
	_, ok := <-done
	require.False(t, ok)
	pool.Put(sessCtx)
	pool.Put(sessCtx2)
}

func TestSessionPoolDestroyResourcePool(t *testing.T) {
	store := testkit.CreateMockStore(t)
	resourcePool := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 1, 1, 0)
	pool := session.NewSessionPool(resourcePool)

	sessCtx, err := pool.Get()
	require.NoError(t, err)

	pool.Destroy(sessCtx)

	newRes, err := resourcePool.TryGet()
	require.NoError(t, err)
	require.NotNil(t, newRes)

	// Destroy on *pools.ResourcePool should Close the session and Put(nil),
	// so the pool creates a new session next time.
	require.NotEqual(t, sessCtx.(pools.Resource), newRes)

	newRes.Close()
	resourcePool.Put(nil)
}

type mockDestroyablePool struct {
	factory func() (pools.Resource, error)

	putCnt     int64
	destroyCnt int64
}

func (p *mockDestroyablePool) Get() (pools.Resource, error) {
	return p.factory()
}

func (p *mockDestroyablePool) Put(r pools.Resource) {
	atomic.AddInt64(&p.putCnt, 1)
	r.Close()
}

func (p *mockDestroyablePool) Destroy(r pools.Resource) {
	atomic.AddInt64(&p.destroyCnt, 1)
	r.Close()
}

func (p *mockDestroyablePool) Close() {}

func TestSessionPoolDestroyDestroyableSessionPool(t *testing.T) {
	store := testkit.CreateMockStore(t)
	mp := &mockDestroyablePool{factory: func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}}
	pool := session.NewSessionPool(mp)

	sessCtx, err := pool.Get()
	require.NoError(t, err)
	pool.Destroy(sessCtx)

	require.Equal(t, int64(0), atomic.LoadInt64(&mp.putCnt))
	require.Equal(t, int64(1), atomic.LoadInt64(&mp.destroyCnt))
}
