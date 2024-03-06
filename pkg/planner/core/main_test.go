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

package core

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)
var planSuiteUnexportedData testdata.TestData
var indexMergeSuiteData testdata.TestData

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	flag.Parse()
	testDataMap.LoadTestSuiteData("testdata", "plan_suite_unexported")
	testDataMap.LoadTestSuiteData("testdata", "index_merge_suite")
	testDataMap.LoadTestSuiteData("testdata", "runtime_filter_generator_suite")
	testDataMap.LoadTestSuiteData("testdata", "plan_cache_suite")

	indexMergeSuiteData = testDataMap["index_merge_suite"]
	planSuiteUnexportedData = testDataMap["plan_suite_unexported"]
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetPlanCacheSuiteData() testdata.TestData {
	return testDataMap["plan_cache_suite"]
}

func GetIndexMergeSuiteData() testdata.TestData {
	return testDataMap["index_merge_suite"]
}

func GetRuntimeFilterGeneratorData() testdata.TestData {
	return testDataMap["runtime_filter_generator_suite"]
}
