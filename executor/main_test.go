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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/testkit/testmain"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)
var prepareMergeSuiteData testdata.TestData
var aggMergeSuiteData testdata.TestData
var executorSuiteData testdata.TestData
var pointGetSuiteData testdata.TestData

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	testDataMap.LoadTestSuiteData("testdata", "agg_suite")
	testDataMap.LoadTestSuiteData("testdata", "executor_suite")
	testDataMap.LoadTestSuiteData("testdata", "prepare_suite")
	testDataMap.LoadTestSuiteData("testdata", "point_get_suite")
	aggMergeSuiteData = testDataMap["agg_suite"]
	executorSuiteData = testDataMap["executor_suite"]
	prepareMergeSuiteData = testDataMap["prepare_suite"]
	pointGetSuiteData = testDataMap["point_get_suite"]

	autoid.SetStep(5000)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}
	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func fillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
}
