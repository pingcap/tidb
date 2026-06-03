// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

import (
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

type runtimeHandleTestStore struct {
	kv.Storage
	ks string
}

func (s *runtimeHandleTestStore) GetKeyspace() string {
	return s.ks
}

type runtimeHandleTestSessPool struct {
	util.DestroyableSessionPool
	closeCount int
}

func (p *runtimeHandleTestSessPool) Close() {
	p.closeCount++
}

func newRuntimeHandleTestManager(targetKS string) (*Manager, *runtimeEntry, *runtimeHandleTestStore, *runtimeHandleTestSessPool) {
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	targetStore := &runtimeHandleTestStore{ks: targetKS}
	sessPool := &runtimeHandleTestSessPool{}
	entry := &runtimeEntry{
		sessMgr: &SessionManager{
			store:    targetStore,
			sessPool: sessPool,
		},
		activeHolders: make(map[string]struct{}),
	}
	mgr.runtimes[targetKS] = entry
	return mgr, entry, targetStore, sessPool
}

func unusedRuntimeHandleFactoryGetter(t *testing.T) func(string, validatorapi.Validator) pools.Factory {
	return func(string, validatorapi.Validator) pools.Factory {
		t.Fatal("test should use the pre-seeded runtime entry")
		return nil
	}
}

func TestAcquireRuntimeHandle(t *testing.T) {
	t.Run("rejects empty holderID", func(t *testing.T) {
		mgr, _, _, _ := newRuntimeHandleTestManager("ks-runtime-empty-holderID")

		handle, err := mgr.Acquire("ks-runtime-empty-holderID", "", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "holderID")
	})

	t.Run("rejects classic kernel", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("classic-kernel rejection is covered only in classic kernel")
		}
		mgr, _, _, _ := newRuntimeHandleTestManager("ks-runtime-classic")

		handle, err := mgr.Acquire("ks-runtime-classic", "test/holderID", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "cross keyspace session manager is not available in classic kernel or current keyspace")
	})

	t.Run("tracks holder IDs", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
		}
		targetKS := "ks-runtime-holderID"
		mgr, entry, targetStore, sessPool := newRuntimeHandleTestManager(targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)

		first, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Same(t, targetStore, first.Store())
		require.Same(t, sessPool, first.SessPool())
		require.Contains(t, entry.activeHolders, "holder-1")

		duplicate, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.Nil(t, duplicate)
		require.ErrorContains(t, err, "already acquired")

		second, err := mgr.Acquire(targetKS, "holder-2", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")

		first.Release()
		first.Release()
		require.NotContains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")
		require.True(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		second.Release()
		second.Release()
		require.Empty(t, entry.activeHolders)
		require.False(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		reacquired, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Zero(t, sessPool.closeCount)
		reacquired.Release()
	})
}
