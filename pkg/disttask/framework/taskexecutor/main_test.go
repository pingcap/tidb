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

package taskexecutor

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

func ReduceCheckInterval(t *testing.T) {
	taskCheckIntervalBak := TaskCheckInterval
	checkIntervalBak := SubtaskCheckInterval
	maxIntervalBak := MaxSubtaskCheckInterval
	t.Cleanup(func() {
		TaskCheckInterval = taskCheckIntervalBak
		SubtaskCheckInterval = checkIntervalBak
		MaxSubtaskCheckInterval = maxIntervalBak
	})
	TaskCheckInterval, MaxSubtaskCheckInterval, SubtaskCheckInterval =
		time.Millisecond, time.Millisecond, time.Millisecond
}

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("syscall.syscall"),
	}
	goleak.VerifyTestMain(m, opts...)
}
