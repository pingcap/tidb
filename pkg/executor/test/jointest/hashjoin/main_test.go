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

package hashjoin

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/resourcemanager"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/tikv/client-go/v2/tikv"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

var issue18572Store struct {
	sync.Mutex
	store kv.Storage
	dom   *domain.Domain
	err   error
}

func TestMain(m *testing.M) {
	autoid.SetStep(5000)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()
	if issue18572RunSelected() {
		_, _ = issue18572StoreForTest()
	}

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, func(code int) int {
		closeIssue18572Store()
		return code
	}), opts...)
}

func issue18572RunSelected() bool {
	for i, arg := range os.Args {
		switch {
		case strings.HasPrefix(arg, "-test.run="):
			return strings.Contains(strings.TrimPrefix(arg, "-test.run="), "TestIssue18572_3")
		case strings.HasPrefix(arg, "-run="):
			return strings.Contains(strings.TrimPrefix(arg, "-run="), "TestIssue18572_3")
		case (arg == "-test.run" || arg == "-run") && i+1 < len(os.Args):
			return strings.Contains(os.Args[i+1], "TestIssue18572_3")
		}
	}
	return false
}

func issue18572StoreForTest() (kv.Storage, error) {
	issue18572Store.Lock()
	defer issue18572Store.Unlock()

	if issue18572Store.store != nil || issue18572Store.err != nil {
		return issue18572Store.store, issue18572Store.err
	}
	issue18572Store.store, issue18572Store.dom, issue18572Store.err = newIssue18572Store()
	return issue18572Store.store, issue18572Store.err
}

func newIssue18572Store() (kv.Storage, *domain.Domain, error) {
	gctuner.GlobalMemoryLimitTuner.Stop()
	vardef.SetSchemaLease(500 * time.Millisecond)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()

	store, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, err
	}
	dom, err := session.BootstrapSession(store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	dom.SetStatsUpdating(true)
	sm := testkit.MockSessionManager{}
	dom.InfoSyncer().SetSessionManager(&sm)
	return store, dom, nil
}

func closeIssue18572Store() {
	issue18572Store.Lock()
	store, dom := issue18572Store.store, issue18572Store.dom
	issue18572Store.store, issue18572Store.dom, issue18572Store.err = nil, nil, nil
	issue18572Store.Unlock()

	if dom != nil {
		dom.Close()
	}
	view.Stop()
	if store != nil {
		_ = store.Close()
	}
	resourcemanager.InstanceResourceManager.Reset()
}
