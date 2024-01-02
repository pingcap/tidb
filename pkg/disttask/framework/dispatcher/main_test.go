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

package dispatcher

import (
	"context"
	"testing"

<<<<<<< HEAD:pkg/disttask/framework/dispatcher/main_test.go
=======
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
>>>>>>> 3d939d4b6f1 (disttask: init capacity and check concurrency using cpu count of managed node (#49875)):pkg/disttask/framework/storage/task_table_test.go
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

// DispatcherForTest exports for testing.
type DispatcherManagerForTest interface {
	GetRunningTaskCnt() int
	DelRunningTask(globalTaskID int64)
	DoCleanUpRoutine()
}

// GetRunningGTaskCnt implements Dispatcher.GetRunningGTaskCnt interface.
func (dm *Manager) GetRunningTaskCnt() int {
	return dm.getRunningTaskCnt()
}

// DelRunningGTask implements Dispatcher.DelRunningGTask interface.
func (dm *Manager) DelRunningTask(globalTaskID int64) {
	dm.delRunningTask(globalTaskID)
}

// DoCleanUpRoutine implements Dispatcher.DoCleanUpRoutine interface.
func (dm *Manager) DoCleanUpRoutine() {
	dm.doCleanUpRoutine()
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
<<<<<<< HEAD:pkg/disttask/framework/dispatcher/main_test.go
=======

func GetCPUCountOfManagedNodes(ctx context.Context, taskMgr *TaskManager) (int, error) {
	var cnt int
	err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		cnt, err2 = taskMgr.getCPUCountOfManagedNodes(ctx, se)
		return err2
	})
	return cnt, err
}

func TestSplitSubtasks(t *testing.T) {
	tm := &TaskManager{}
	subtasks := make([]*proto.Subtask, 0, 10)
	metaBytes := make([]byte, 100)
	for i := 0; i < 10; i++ {
		subtasks = append(subtasks, &proto.Subtask{ID: int64(i), Meta: metaBytes})
	}
	bak := kv.TxnTotalSizeLimit.Load()
	t.Cleanup(func() {
		kv.TxnTotalSizeLimit.Store(bak)
	})

	kv.TxnTotalSizeLimit.Store(config.SuperLargeTxnSize)
	splitSubtasks := tm.splitSubtasks(subtasks)
	require.Len(t, splitSubtasks, 1)
	require.Equal(t, subtasks, splitSubtasks[0])

	maxSubtaskBatchSize = 300
	splitSubtasks = tm.splitSubtasks(subtasks)
	require.Len(t, splitSubtasks, 4)
	require.Equal(t, subtasks[:3], splitSubtasks[0])
	require.Equal(t, subtasks[3:6], splitSubtasks[1])
	require.Equal(t, subtasks[6:9], splitSubtasks[2])
	require.Equal(t, subtasks[9:], splitSubtasks[3])
}
>>>>>>> 3d939d4b6f1 (disttask: init capacity and check concurrency using cpu count of managed node (#49875)):pkg/disttask/framework/storage/task_table_test.go
