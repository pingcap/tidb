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

package integrationtests

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	maxConcurrentTask = flag.Int("max-concurrent-task", proto.MaxConcurrentTask, "max concurrent task")
	waitDuration      = flag.Duration("task-wait-duration", 5*time.Minute, "task wait duration")
	taskMetaSize      = flag.Int("task-meta-size", 1<<10, "task meta size")
)

// test overhead when starting multiple schedulers
//
// make failpoint-enable
// GOOS=linux GOARCH=amd64 go test -tags intest -c -o bench.test ./pkg/disttask/framework/integrationtests
// make failpoint-disable
//
// bench.test -run ^$ -test.bench=BenchmarkSchedulerOverhead -test.benchmem --with-tikv "upstream-pd:2379?disableGC=true"
func BenchmarkSchedulerOverhead(b *testing.B) {
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	statusListener, err := net.Listen("tcp", "0.0.0.0:10080")
	require.NoError(b, err)
	m := cmux.New(statusListener)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())
	statusServer := &http.Server{Handler: serverMux}
	go func() {
		if err := statusServer.Serve(httpL); err != nil {
			b.Logf("status server serve failed: %v", err)
		}
	}()
	lis4000, err := net.Listen("tcp", "0.0.0.0:4000")
	server4000 := http.Server{}
	go func() {
		if err := server4000.Serve(lis4000); err != nil {
			b.Logf("server serve failed: %v", err)
		}
	}()
	defer func() {
		_ = statusServer.Close()
		_ = server4000.Close()
	}()

	bak := proto.MaxConcurrentTask
	b.Cleanup(func() {
		proto.MaxConcurrentTask = bak
	})
	proto.MaxConcurrentTask = *maxConcurrentTask
	b.Logf("max concurrent task: %d", proto.MaxConcurrentTask)
	b.Logf("taks wait duration: %s", *waitDuration)
	b.Logf("task meta size: %d", *taskMetaSize)

	c := testutil.NewTestDXFContext(b, 1, 64)

	tk := testkit.NewTestKit(c.T, c.Store)
	tk.MustExec("delete from mysql.tidb_global_task")
	tk.MustExec("delete from mysql.tidb_global_task_history")

	stepTransition := map[proto.Step]proto.Step{
		proto.StepInit: proto.StepOne,
		proto.StepOne:  proto.StepTwo,
		proto.StepTwo:  proto.StepDone,
	}
	schedulerExt := mockDispatch.NewMockExtension(c.MockCtrl)
	schedulerExt.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	schedulerExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	schedulerExt.EXPECT().IsRetryableErr(gomock.Any()).Return(false).AnyTimes()
	schedulerExt.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			return stepTransition[task.Step]
		},
	).AnyTimes()
	schedulerExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, task *proto.Task, _ []string, nextStep proto.Step) (metas [][]byte, err error) {
			cnt := 1
			res := make([][]byte, cnt)
			for i := 0; i < cnt; i++ {
				res[i] = []byte(fmt.Sprintf("subtask-%d", i))
			}
			return res, nil
		},
	).AnyTimes()
	schedulerExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	testutil.RegisterTaskMetaWithDXFCtx(c, schedulerExt, func(ctx context.Context, subtask *proto.Subtask) error {
		select {
		case <-ctx.Done():
			taskManager, err := storage.GetTaskManager()
			if err != nil {
				return err
			}
			return taskManager.CancelTask(ctx, subtask.TaskID)
		case <-time.After(*waitDuration):
		}
		return nil
	})

	var wg util.WaitGroupWrapper
	for i := 0; i < 64; i++ {
		taskKey := fmt.Sprintf("task-%d", i)
		taskMeta := make([]byte, *taskMetaSize)
		_, err := handle.SubmitTask(c.Ctx, taskKey, proto.TaskTypeExample, 1, taskMeta)
		require.NoError(c.T, err)
		wg.RunWithLog(func() {
			testutil.WaitTaskDoneOrPaused(c.Ctx, c.T, taskKey)
		})
	}
	wg.Wait()
}
