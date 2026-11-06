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

package clusteredindextest

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcemanager"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/tikv/client-go/v2/tikv"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

var (
	clusteredIndexStore  kv.Storage
	clusteredIndexDomain *domain.Domain
)

func TestMain(m *testing.M) {
	testmain.ShortCircuitForBench(m)

	testsetup.SetupForCommonTest()

	flag.Parse()

	vardef.SetSchemaLease(20 * time.Millisecond)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	tikv.EnableFailpoints()

	if err := setupClusteredIndexStore(); err != nil {
		fmt.Fprintf(os.Stderr, "setup clustered index store: %v\n", err)
		os.Exit(1)
	}

	opts := []goleak.Option{
		// TODO: figure the reason and shorten this list
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/config/retry.newBackoffFn.func1"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.waitRetryBackoff"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}
	callback := func(i int) int {
		if clusteredIndexDomain != nil {
			clusteredIndexDomain.Close()
		}
		if clusteredIndexStore != nil {
			if err := clusteredIndexStore.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close clustered index store: %v\n", err)
				i = 1
			}
		}
		view.Stop()
		resourcemanager.InstanceResourceManager.Reset()
		gctuner.GlobalMemoryLimitTuner.Stop()
		// wait for MVCCLevelDB to close, MVCCLevelDB will be closed in one second
		time.Sleep(time.Second)
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func setupClusteredIndexStore() error {
	store, err := mockstore.NewMockStore()
	if err != nil {
		return err
	}
	vardef.SetSchemaLease(500 * time.Millisecond)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()
	dom, err := session.BootstrapSession(store)
	if err != nil {
		_ = store.Close()
		return err
	}
	dom.SetStatsUpdating(true)
	sm := testkit.MockSessionManager{}
	dom.InfoSyncer().SetSessionManager(&sm)

	clusteredIndexStore = store
	clusteredIndexDomain = dom
	return nil
}
