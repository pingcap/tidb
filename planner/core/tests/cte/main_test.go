// Copyright 2024 PingCAP, Inc.
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

package cte

import (
<<<<<<< HEAD:planner/core/tests/cte/main_test.go
	"flag"
	"testing"

	"github.com/pingcap/tidb/testkit/testsetup"
	"go.uber.org/goleak"
=======
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
>>>>>>> 75d080712a9 (planner: add test cases for auto analyze's tidb_skip_missing_partition_stats (#60038)):pkg/planner/core/tests/analyze/analyze_test.go
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestAutoAnalyzeForMissingPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_skip_missing_partition_stats = 1")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	originalVal3 := statistics.AutoAnalyzeMinCnt
	defer func() {
		statistics.AutoAnalyzeMinCnt = originalVal3
	}()
	statistics.AutoAnalyzeMinCnt = 0
	h := dom.StatsHandle()

	tk.MustExec("set @@tidb_skip_missing_partition_stats = 1")
	tk.MustExec("create table t (a int, b int, c int, index idx_b(b)) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than (300))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (101,101,101), (102,102,102), (201,201,201), (202,202,202)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t partition p1")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (101,101,101), (102,102,102), (201,201,201), (202,202,202)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))
	originalVal2 := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.01")
	require.True(t, h.HandleAutoAnalyze())
	tk.MustQuery("select state from mysql.analyze_jobs").Check(testkit.Rows(
		"finished",
		"finished",
		"finished",
		"finished",
		"finished",
		"finished",
		"finished"))
}
