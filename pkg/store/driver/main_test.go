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

package driver

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

var (
	pdAddrs  = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
	withTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("syscall.Syscall"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func createTestStore(t *testing.T) (kv.Storage, *domain.Domain) {
	if *withTiKV {
		return createTiKVStore(t)
	}
	return createUnistore(t)
}

func createTiKVStore(t *testing.T) (kv.Storage, *domain.Domain) {
	var d TiKVDriver
	store, err := d.Open(fmt.Sprintf("tikv://%s", *pdAddrs))
	require.NoError(t, err)

	// clear storage
	txn, err := store.Begin()
	require.NoError(t, err)
	iter, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	for iter.Valid() {
		require.NoError(t, txn.Delete(iter.Key()))
		require.NoError(t, iter.Next())
	}
	require.NoError(t, txn.Commit(context.Background()))

	session.ResetStoreForWithTiKVTest(store)

	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	return store, dom
}

func createUnistore(t *testing.T) (kv.Storage, *domain.Domain) {
	client, pdClient, cluster, err := unistore.New("", nil)
	require.NoError(t, err)

	unistore.BootstrapWithSingleStore(cluster)
	kvStore, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)

	coprStore, err := copr.NewStore(kvStore, nil)
	require.NoError(t, err)

	store := &tikvStore{KVStore: kvStore, coprStore: coprStore}
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	return store, dom
}

func prepareSnapshot(t *testing.T, store kv.Storage, data [][]any) kv.Snapshot {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	for _, d := range data {
		err = txn.Set(makeBytes(d[0]), makeBytes(d[1]))
		require.NoError(t, err)
	}

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	return store.GetSnapshot(kv.MaxVersion)
}

func makeBytes(s any) []byte {
	if s == nil {
		return nil
	}

	switch key := s.(type) {
	case string:
		return []byte(key)
	default:
		return key.([]byte)
	}
}

func clearStoreData(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	iter, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	for iter.Valid() {
		require.NoError(t, txn.Delete(iter.Key()))
		require.NoError(t, iter.Next())
	}

	require.NoError(t, txn.Commit(context.Background()))
}
