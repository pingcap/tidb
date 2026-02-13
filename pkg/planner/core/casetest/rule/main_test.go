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

package rule

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
<<<<<<< HEAD
	testDataMap.LoadTestSuiteData("testdata", "derive_topn_from_window")
	testDataMap.LoadTestSuiteData("testdata", "join_reorder_suite")
	testDataMap.LoadTestSuiteData("testdata", "predicate_pushdown_suite")
=======
	testDataMap.LoadTestSuiteData("testdata", "outer2inner", true)
	testDataMap.LoadTestSuiteData("testdata", "derive_topn_from_window", true)
	testDataMap.LoadTestSuiteData("testdata", "join_reorder_suite", true)
	testDataMap.LoadTestSuiteData("testdata", "predicate_pushdown_suite", true)
	testDataMap.LoadTestSuiteData("testdata", "predicate_simplification", true)
	testDataMap.LoadTestSuiteData("testdata", "outer_to_semi_join_suite", true)
	testDataMap.LoadTestSuiteData("testdata", "cdc_join_reorder_suite", true)

>>>>>>> f6f6d2e968e (planner: fix join reorder correctness with conflict detection algorithm (#65705))
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
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

func GetDerivedTopNSuiteData() testdata.TestData {
	return testDataMap["derive_topn_from_window"]
}

func GetJoinReorderSuiteData() testdata.TestData {
	return testDataMap["join_reorder_suite"]
}

func GetPredicatePushdownSuiteData() testdata.TestData {
	return testDataMap["predicate_pushdown_suite"]
}
<<<<<<< HEAD
=======

func GetPredicateSimplificationSuiteData() testdata.TestData {
	return testDataMap["predicate_simplification"]
}

func GetOuterToSemiJoinSuiteData() testdata.TestData {
	return testDataMap["outer_to_semi_join_suite"]
}

func GetCDCJoinReorderSuiteData() testdata.TestData {
	return testDataMap["cdc_join_reorder_suite"]
}
>>>>>>> f6f6d2e968e (planner: fix join reorder correctness with conflict detection algorithm (#65705))
