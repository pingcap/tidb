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

//go:build !codes

package realtikvtest

import (
	"context"
	"flag"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit/testmain"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

// WithRealTiKV is a flag identify whether tests run with real TiKV
var WithRealTiKV = flag.Bool("with-real-tikv", false, "whether tests run with real TiKV")

// RunTestMain run common setups for all real tikv tests.
func RunTestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	session.SetSchemaLease(20 * time.Millisecond)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/internal/retry.newBackoffFn.func1"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.waitRetryBackoff"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	}
	callback := func(i int) int {
		// wait for MVCCLevelDB to close, MVCCLevelDB will be closed in one second
		time.Sleep(time.Second)
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func clearTiKVStorage(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	require.NoError(t, err)
	iter, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	for iter.Valid() {
		require.NoError(t, txn.Delete(iter.Key()))
		require.NoError(t, iter.Next())
	}
	require.NoError(t, txn.Commit(context.Background()))
}

func clearEtcdStorage(t *testing.T, backend kv.EtcdBackend) {
	endpoints, err := backend.EtcdAddrs()
	require.NoError(t, err)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
		TLS: backend.TLSConfig(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	resp, err := cli.Get(context.Background(), "/tidb", clientv3.WithPrefix())
	require.NoError(t, err)
	for _, entry := range resp.Kvs {
		if entry.Lease != 0 {
			_, err := cli.Revoke(context.Background(), clientv3.LeaseID(entry.Lease))
			require.NoError(t, err)
		}
	}
	_, err = cli.Delete(context.Background(), "/tidb", clientv3.WithPrefix())
	require.NoError(t, err)
}

// CreateMockStoreAndSetup return a new kv.Storage.
func CreateMockStoreAndSetup(t *testing.T, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, func()) {
	store, _, clean := CreateMockStoreAndDomainAndSetup(t, opts...)
	return store, clean
}

// CreateMockStoreAndDomainAndSetup return a new kv.Storage and *domain.Domain.
func CreateMockStoreAndDomainAndSetup(t *testing.T, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain, func()) {
	// set it to 5 seconds for testing lock resolve.
	atomic.StoreUint64(&transaction.ManagedLockTTL, 5000)
	transaction.PrewriteMaxBackoff.Store(500)

	var store kv.Storage
	var dom *domain.Domain
	var err error

	if *WithRealTiKV {
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		store, err = d.Open("tikv://127.0.0.1:2379?disableGC=true")
		require.NoError(t, err)

		clearTiKVStorage(t, store)
		clearEtcdStorage(t, store.(kv.EtcdBackend))

		session.ResetStoreForWithTiKVTest(store)
		dom, err = session.BootstrapSession(store)
		require.NoError(t, err)

	} else {
		store, err = mockstore.NewMockStore(opts...)
		require.NoError(t, err)
		session.DisableStats4Test()
		dom, err = session.BootstrapSession(store)
		require.NoError(t, err)
	}

	return store, dom, func() {
		dom.Close()
		require.NoError(t, store.Close())
		transaction.PrewriteMaxBackoff.Store(20000)
	}
}
