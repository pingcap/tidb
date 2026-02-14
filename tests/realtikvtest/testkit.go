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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.opencensus.io/stats/view"
	uberatomic "go.uber.org/atomic"
	"go.uber.org/goleak"
)

var (
	// WithRealTiKV is a flag identify whether tests run with real TiKV
	WithRealTiKV = flag.Bool("with-real-tikv", false, "whether tests run with real TiKV")

	// TiKVPath is the path of the TiKV Storage.
	TiKVPath = flag.String("tikv-path", "tikv://127.0.0.1:2379?disableGC=true", "TiKV addr")

	// PDAddr is the address of PD.
	PDAddr = "127.0.0.1:2379"

	mockPortAlloc = uberatomic.NewInt32(4000)
)

// RunTestMain run common setups for all real tikv tests.
func RunTestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	*WithRealTiKV = true
	flag.Parse()
	vardef.SetSchemaLease(5 * time.Second)
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
		// net.cgoLookupHostIP can be in-flight when goleak runs (e.g. DNS resolve), which is noisy/flaky in tests.
		goleak.IgnoreAnyFunction("net.cgoLookupHostIP"),
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
		// the resolveFlushedLocks goroutine runs in the background to commit or rollback locks.
		goleak.IgnoreAnyFunction("github.com/tikv/client-go/v2/txnkv/transaction.(*twoPhaseCommitter).resolveFlushedLocks.func1"),
		goleak.Cleanup(testutil.CheckIngestLeakageForTest),
	}
	callback := func(i int) int {
		// wait for MVCCLevelDB to close, MVCCLevelDB will be closed in one second
		time.Sleep(time.Second)
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

type realtikvStoreOption struct {
	retainData bool
	keyspace   string
	// only used when keyspace is not SYSTEM, in that case, the SYSTEM store will
	// be closed together with its domain, if we close it before domain of SYSTEM,
	// some routine might report errors, and we don't want close twice as the storage
	// driver will cache store.
	keepSystemStore bool
	keepSelfStore   bool
	// whether to allocate port for the mock domain, else keep the default.
	// some tests depend on the default port, such as TestImportFromServer, as
	// infosync.MockGlobalServerInfoManagerEntry only have one mock server info
	// with default port 4000.
	allocPort bool
}

// RealTiKVStoreOption is the config option for creating a real TiKV store.
type RealTiKVStoreOption func(opt *realtikvStoreOption)

// WithRetainData allows the store to retain old data when creating a new store.
func WithRetainData() RealTiKVStoreOption {
	return func(opt *realtikvStoreOption) {
		opt.retainData = true
	}
}

// WithKeyspaceName allows the store to use a specific keyspace name.
func WithKeyspaceName(name string) RealTiKVStoreOption {
	return func(opt *realtikvStoreOption) {
		opt.keyspace = name
	}
}

// WithKeepSystemStore allows the store to keep the SYSTEM keyspace store
func WithKeepSystemStore(keep bool) RealTiKVStoreOption {
	return func(opt *realtikvStoreOption) {
		opt.keepSystemStore = keep
	}
}

// WithKeepSelfStore allows the store to keep the self store.
func WithKeepSelfStore(keep bool) RealTiKVStoreOption {
	return func(opt *realtikvStoreOption) {
		opt.keepSelfStore = keep
	}
}

// WithAllocPort allows the store to allocate port for the mock domain.
func WithAllocPort(alloc bool) RealTiKVStoreOption {
	return func(opt *realtikvStoreOption) {
		opt.allocPort = alloc
	}
}

// KSRuntime is a runtime environment for a keyspace.
type KSRuntime struct {
	Store kv.Storage
	Dom   *domain.Domain
}

// PrepareForCrossKSTest prepares the environment for cross keyspace tests.
func PrepareForCrossKSTest(t *testing.T, userKSs ...string) map[string]*KSRuntime {
	if !kerneltype.IsNextGen() {
		t.Fail()
	}
	res := make(map[string]*KSRuntime, len(userKSs)+1)
	// stores are cached, we want to make sure stores are closed after domain,
	// else some routine might be blocked.
	t.Cleanup(func() {
		for _, runtime := range res {
			require.NoError(t, runtime.Store.Close())
		}
	})

	ksList := append([]string{keyspace.System}, userKSs...)
	for _, ks := range ksList {
		store, dom := CreateMockStoreAndDomainAndSetup(t, WithKeyspaceName(ks),
			WithKeepSystemStore(true), WithKeepSelfStore(true), WithAllocPort(true))
		res[ks] = &KSRuntime{
			Store: store,
			Dom:   dom,
		}
	}
	return res
}

// CreateMockStoreAndSetup return a new kv.Storage.
func CreateMockStoreAndSetup(t *testing.T, opts ...RealTiKVStoreOption) kv.Storage {
	store, _ := CreateMockStoreAndDomainAndSetup(t, opts...)
	return store
}

// CreateMockStoreAndDomainAndSetup initializes a kv.Storage and a domain.Domain.
func CreateMockStoreAndDomainAndSetup(t *testing.T, opts ...RealTiKVStoreOption) (kv.Storage, *domain.Domain) {
	//nolint: errcheck
	_ = kvstore.Register(config.StoreTypeTiKV, &driver.TiKVDriver{})
	kvstore.SetSystemStorage(nil)
	// set it to 5 seconds for testing lock resolve.
	atomic.StoreUint64(&transaction.ManagedLockTTL, 5000)
	transaction.PrewriteMaxBackoff.Store(500)

	var store kv.Storage
	var dom *domain.Domain
	var err error

	option := &realtikvStoreOption{}
	for _, opt := range opts {
		opt(option)
	}
	var ks string
	if kerneltype.IsNextGen() {
		if option.keyspace == "" {
			// in nextgen kernel, SYSTEM keyspace must be bootstrapped first, if we
			// don't specify a keyspace which normally is not specified, we use SYSTEM
			// keyspace as default to make sure test cases can run correctly.
			ks = keyspace.System
		} else {
			ks = option.keyspace
		}
		t.Log("create realtikv store with keyspace:", ks)
	}
	vardef.SetSchemaLease(500 * time.Millisecond)

	path := *TiKVPath
	if len(ks) > 0 {
		path += "&keyspaceName=" + ks
	}
	var d driver.TiKVDriver
	bak := *config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(&bak)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
		conf.KeyspaceName = ks
		conf.Store = config.StoreTypeTiKV
		if option.allocPort {
			conf.Port = uint(mockPortAlloc.Add(1))
		}
	})
	if ks == keyspace.System {
		UpdateTiDBConfig()
	}
	store, err = d.Open(path)
	require.NoError(t, err)
	if kerneltype.IsNextGen() && ks != keyspace.System {
		sysPath := *TiKVPath + "&keyspaceName=" + keyspace.System
		sysStore, err := d.Open(sysPath)
		require.NoError(t, err)
		kvstore.SetSystemStorage(sysStore)
		if !option.keepSystemStore {
			t.Cleanup(func() {
				require.NoError(t, sysStore.Close())
			})
		}
	}
	require.NoError(t, ddl.StartOwnerManager(context.Background(), store))
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	sm := testkit.MockSessionManager{}
	dom.InfoSyncer().SetSessionManager(&sm)
	tk := testkit.NewTestKit(t, store)
	// set it to default value.
	tk.MustExec(fmt.Sprintf("set global innodb_lock_wait_timeout = %d", vardef.DefInnodbLockWaitTimeout))
	tk.MustExec("use test")

	if !option.retainData {
		tk.MustExec("delete from mysql.tidb_global_task;")
		tk.MustExec("delete from mysql.tidb_background_subtask;")
		tk.MustExec("delete from mysql.tidb_ddl_job;")
		rs := tk.MustQuery("show full tables where table_type = 'BASE TABLE';")
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
		rs = tk.MustQuery("show full tables where table_type = 'VIEW';")
		for _, row := range rs.Rows() {
			tk.MustExec(fmt.Sprintf("drop view `%v`", row[0]))
		}
		t.Log("cleaned up ddl and tables")
	}

	t.Cleanup(func() {
		dom.Close()
		ddl.CloseOwnerManager(store)
		if !option.keepSelfStore {
			require.NoError(t, store.Close())
		}
		transaction.PrewriteMaxBackoff.Store(20000)
		view.Stop()
	})
	return store, dom
}

// UpdateTiDBConfig updates the TiDB configuration for the real TiKV test.
func UpdateTiDBConfig() {
	// need a real PD
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
		if kerneltype.IsNextGen() {
			conf.TiKVWorkerURL = "localhost:19000"
			conf.KeyspaceName = keyspace.System
			conf.Instance.TiDBServiceScope = handle.NextGenTargetScope
			conf.MeteringStorageURI = getNextGenObjStoreURIWithArgs("metering-data", "&region=local")
		}
	})
}

// GetNextGenObjStoreURI returns a next-gen object store URI for testing.
func GetNextGenObjStoreURI(path string) string {
	return getNextGenObjStoreURIWithArgs(path, "&provider=minio")
}

func getNextGenObjStoreURIWithArgs(path string, args string) string {
	return fmt.Sprintf("s3://next-gen-test/%s?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%%3a%%2f%%2f0.0.0.0%%3a9000%s", path, args)
}
