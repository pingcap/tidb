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

	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/testkit/testmain"
	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper, 3)
var indexMergeSuiteData testdata.TestData

func TestMain(m *testing.M) {
	testbridge.SetupForCommonTest()

	flag.Parse()

	testDataMap.LoadTestSuiteData("testdata", "integration_partition_suite")
	testDataMap.LoadTestSuiteData("testdata", "index_merge_suite")
	testDataMap.LoadTestSuiteData("testdata", "plan_normalized_suite")

	indexMergeSuiteData = testDataMap["index_merge_suite"]

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetIntegrationPartitionSuiteData() testdata.TestData {
	return testDataMap["integration_partition_suite"]
}

func GetPlanNormalizedSuiteData() testdata.TestData {
	return testDataMap["plan_normalized_suite"]
}
