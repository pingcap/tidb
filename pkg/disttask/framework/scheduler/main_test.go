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

package scheduler

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

// SchedulerForTest exports for testing.
type SchedulerManagerForTest interface {
	GetRunningTaskCnt() int
	DelRunningTask(id int64)
	DoCleanUpRoutine()
}

// GetRunningGTaskCnt implements Scheduler.GetRunningGTaskCnt interface.
func (sm *Manager) GetRunningTaskCnt() int {
	return sm.getSchedulerCount()
}

// DelRunningGTask implements Scheduler.DelRunningGTask interface.
func (sm *Manager) DelRunningTask(id int64) {
	sm.delScheduler(id)
}

// DoCleanUpRoutine implements Scheduler.DoCleanUpRoutine interface.
func (sm *Manager) DoCleanUpRoutine() {
	sm.doCleanupTask()
}

func (s *BaseScheduler) OnNextStage() (err error) {
	return s.onNextStage()
}

func (s *BaseScheduler) DoBalanceSubtasks(eligibleNodes []string) error {
	return s.doBalanceSubtasks(eligibleNodes)
}

func NewNodeManager() *NodeManager {
	return newNodeManager()
}

func (nm *NodeManager) RefreshManagedNodes(ctx context.Context, taskMgr TaskManager) {
	nm.refreshManagedNodes(ctx, taskMgr)
}

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	// Make test more fast.
	checkTaskRunningInterval = checkTaskRunningInterval / 10
	checkTaskFinishedInterval = checkTaskFinishedInterval / 10
	RetrySQLInterval = RetrySQLInterval / 20

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("syscall.syscall"),
	}
	goleak.VerifyTestMain(m, opts...)
}
