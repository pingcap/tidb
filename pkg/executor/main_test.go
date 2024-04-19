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
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)
var prepareMergeSuiteData testdata.TestData
var slowQuerySuiteData testdata.TestData

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	testDataMap.LoadTestSuiteData("testdata", "prepare_suite")
	testDataMap.LoadTestSuiteData("testdata", "slow_query_suite")
	prepareMergeSuiteData = testDataMap["prepare_suite"]
	slowQuerySuiteData = testDataMap["slow_query_suite"]

	autoid.SetStep(5000)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()
	variable.StatsCacheMemQuota.Store(5000)

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/pkg/ttl/ttlworker.(*ttlScanWorker).loop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/pkg/ttl/client.(*mockClient).WatchCommand.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/pkg/ttl/ttlworker.(*JobManager).jobLoop"),
	}
	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}
