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
	"flag"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

var (
	// WithRealTiKV is a flag identify whether tests run with real TiKV
	WithRealTiKV = flag.Bool("with-real-tikv", false, "whether tests run with real TiKV")

	// TiKVPath is the path of the TiKV Storage.
	TiKVPath = flag.String("tikv-path", "tikv://127.0.0.1:2379?disableGC=true", "TiKV addr")

	// PDAddr is the address of PD.
	PDAddr = "127.0.0.1:2379"

	// KeyspaceName is an option to specify the name of keyspace that the tests run on,
	// this option is only valid while the flag WithRealTiKV is set.
	KeyspaceName = flag.String("keyspace-name", "", "the name of keyspace that the tests run on")
)

// RunTestMain run common setups for all real tikv tests.
func RunTestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	session.SetSchemaLease(5 * time.Second)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/config/retry.newBackoffFn.func1"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.waitRetryBackoff"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		// top function of this routine might be "sync.runtime_notifyListWait(0xc0098f5450, 0x0)", so we use IgnoreAnyFunction.
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		// backoff function will lead to sleep, so there is a high probability of goroutine leak while it's doing backoff.
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/config/retry.(*Config).createBackoffFn.newBackoffFn.func2"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	callback := func(i int) int {
		// wait for MVCCLevelDB to close, MVCCLevelDB will be closed in one second
		time.Sleep(time.Second)
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

// CreateMockStoreAndSetup return a new kv.Storage.
func CreateMockStoreAndSetup(t *testing.T, opts ...mockstore.MockTiKVStoreOption) kv.Storage {
	store, _ := CreateMockStoreAndDomainAndSetup(t, opts...)
	return store
}

// CreateMockStoreAndDomainAndSetup return a new kv.Storage and *domain.Domain.
func CreateMockStoreAndDomainAndSetup(t *testing.T, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain) {
	// set it to 5 seconds for testing lock resolve.
	atomic.StoreUint64(&transaction.ManagedLockTTL, 5000)
	transaction.PrewriteMaxBackoff.Store(500)

	var store kv.Storage
	var dom *domain.Domain
	var err error

	session.SetSchemaLease(500 * time.Millisecond)

	if *WithRealTiKV {
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
			conf.KeyspaceName = *KeyspaceName
		})
		store, err = d.Open(*TiKVPath)
		require.NoError(t, err)

		dom, err = session.BootstrapSession(store)
		require.NoError(t, err)
		sm := testkit.MockSessionManager{}
		dom.InfoSyncer().SetSessionManager(&sm)
		tk := testkit.NewTestKit(t, store)
		// set it to default value.
		tk.MustExec(fmt.Sprintf("set global innodb_lock_wait_timeout = %d", variable.DefInnodbLockWaitTimeout))
		tk.MustExec("use test")
		rs := tk.MustQuery("show tables")
		tables := []string{}
		for _, row := range rs.Rows() {
			tables = append(tables, fmt.Sprintf("`%v`", row[0]))
		}
		for _, table := range tables {
			tk.MustExec(fmt.Sprintf("alter table %s nocache", table))
		}
		if len(tables) > 0 {
			tk.MustExec(fmt.Sprintf("drop table %s", strings.Join(tables, ",")))
		}
	} else {
		store, err = mockstore.NewMockStore(opts...)
		require.NoError(t, err)
		session.DisableStats4Test()
		dom, err = session.BootstrapSession(store)
		sm := testkit.MockSessionManager{}
		dom.InfoSyncer().SetSessionManager(&sm)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
		transaction.PrewriteMaxBackoff.Store(20000)
		view.Stop()
	})
	return store, dom
}
