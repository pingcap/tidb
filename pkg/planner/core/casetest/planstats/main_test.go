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

package planstats_test

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	flag.Parse()
	testDataMap.LoadTestSuiteData("testdata", "plan_stats_suite")

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/pkg/statistics/handle/syncload.(*statsSyncLoad).SendLoadRequests.func1"), // For TestPlanStatsLoadTimeout
	}

	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetPlanStatsData() testdata.TestData {
	return testDataMap["plan_stats_suite"]
}
