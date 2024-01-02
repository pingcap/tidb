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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	unfinishedNormalSubtaskStates = []interface{}{
		proto.TaskStatePending, proto.TaskStateRunning,
	}
	unfinishedRevertSubtaskStates = []interface{}{
		proto.TaskStateRevertPending, proto.TaskStateReverting,
	}
)

func TestSchedulerRun(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_run"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// we don't test cancelCheck here, but in case the test is slow and trigger it.
	mockSubtaskTable.EXPECT().IsSchedulerCanceled(ctx, gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr).Times(2)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension
	err := scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())
	mockSubtaskTable.EXPECT().UpdateErrorToSubtask(runCtx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.NoError(t, err)

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, initErr.Error())

	var taskID int64 = 1
	var concurrency uint64 = 10
	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}

	// 3. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, runSubtaskErr.Error())

	// 4. run subtask success
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// 5. run subtask one by one
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(
		[]*proto.Subtask{
			{ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"},
			{ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"},
		}, nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(1)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(2)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(2), gomock.Any()).Return(nil)
	// third round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// run previous left subtask in running state again, but the subtask is not
	// idempotent, so fail it.
	subtaskID := int64(2)
	theSubtask := &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.TaskStateRunning, SchedulerID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", subtaskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.ErrorContains(t, err, "subtask in running state and is not idempotent")

	// run previous left subtask in running state again, but the subtask idempotent,
	// run it again.
	theSubtask = &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.TaskStateRunning, SchedulerID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// 6. cancel
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.TaskStateCanceled, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, ErrCancelSubtask.Error())

	// 7. RunSubtask return context.Canceled
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, context.Canceled.Error())

<<<<<<< HEAD
	// 8. grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	grpcErr := status.Error(codes.Canceled, "test cancel")
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(grpcErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, grpcErr.Error())

	// 9. annotate grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	grpcErr = status.Error(codes.Canceled, "test cancel")
	annotatedError := errors.Annotatef(
		grpcErr,
		" %s",
		"test annotate",
=======
	testutil.WaitNodeRegistered(ctx, t)

	// unknown task type
	taskID, err := mgr.CreateTask(ctx, "test", "test-type", 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "unknown task type")
	}, time.Second*10, time.Millisecond*300)

	// scheduler init error
	taskID, err = mgr.CreateTask(ctx, "test2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "mock scheduler init error")
	}, time.Second*10, time.Millisecond*300)
}

func checkDispatch(t *testing.T, taskCnt int, isSucc, isCancel, isSubtaskCancel, isPauseAndResume bool) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	// test DispatchTaskLoop
	// test parallelism control
	var originalConcurrency int
	if taskCnt == 1 {
		originalConcurrency = proto.MaxConcurrentTask
		proto.MaxConcurrentTask = 1
	}

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler")

	sch, mgr := MockSchedulerManager(t, ctrl, pool, getNumberExampleSchedulerExt(ctrl), nil)
	sch.Start()
	defer func() {
		sch.Stop()
		// make data race happy
		if taskCnt == 1 {
			proto.MaxConcurrentTask = originalConcurrency
		}
	}()

	require.NoError(t, mgr.StartManager(ctx, ":4000", "background"))

	// 3s
	cnt := 60
	checkGetRunningTaskCnt := func(expected int) {
		require.Eventually(t, func() bool {
			return sch.GetRunningTaskCnt() == expected
		}, time.Second, 50*time.Millisecond)
	}

	checkTaskRunningCnt := func() []*proto.Task {
		var tasks []*proto.Task
		require.Eventually(t, func() bool {
			var err error
			tasks, err = mgr.GetTasksInStates(ctx, proto.TaskStateRunning)
			require.NoError(t, err)
			return len(tasks) == taskCnt
		}, time.Second, 50*time.Millisecond)
		return tasks
	}

	checkSubtaskCnt := func(tasks []*proto.Task, taskIDs []int64) {
		for i, taskID := range taskIDs {
			require.Equal(t, int64(i+1), tasks[i].ID)
			require.Eventually(t, func() bool {
				cntByStates, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepOne)
				require.NoError(t, err)
				return int64(subtaskCnt) == cntByStates[proto.SubtaskStatePending]
			}, time.Second, 50*time.Millisecond)
		}
	}

	// Mock add tasks.
	taskIDs := make([]int64, 0, taskCnt)
	for i := 0; i < taskCnt; i++ {
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", i), proto.TaskTypeExample, 0, nil)
		require.NoError(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	// test OnNextSubtasksBatch.
	checkGetRunningTaskCnt(taskCnt)
	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskIDs)
	// test parallelism control
	if taskCnt == 1 {
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", taskCnt), proto.TaskTypeExample, 0, nil)
		require.NoError(t, err)
		checkGetRunningTaskCnt(taskCnt)
		// Clean the task.
		deleteTasks(t, store, taskID)
		sch.DelRunningTask(taskID)
	}

	// test DetectTaskLoop
	checkGetTaskState := func(expectedState proto.TaskState) {
		i := 0
		for ; i < cnt; i++ {
			tasks, err := mgr.GetTasksInStates(ctx, expectedState)
			require.NoError(t, err)
			if len(tasks) == taskCnt {
				break
			}
			historyTasks, err := mgr.GetTasksFromHistoryInStates(ctx, expectedState)
			require.NoError(t, err)
			if len(tasks)+len(historyTasks) == taskCnt {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
		require.Less(t, i, cnt)
	}
	// Test all subtasks are successful.
	var err error
	if isSucc {
		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateSucceed, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStateSucceed)
		require.Len(t, tasks, taskCnt)

		checkGetRunningTaskCnt(0)
		return
	}

	if isCancel {
		for i := 1; i <= taskCnt; i++ {
			err = mgr.CancelTask(ctx, int64(i))
			require.NoError(t, err)
		}
	} else if isPauseAndResume {
		for i := 0; i < taskCnt; i++ {
			found, err := mgr.PauseTask(ctx, fmt.Sprintf("%d", i))
			require.Equal(t, true, found)
			require.NoError(t, err)
		}
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStatePaused, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStatePaused)
		for i := 0; i < taskCnt; i++ {
			found, err := mgr.ResumeTask(ctx, fmt.Sprintf("%d", i))
			require.Equal(t, true, found)
			require.NoError(t, err)
		}

		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateSucceed, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStateSucceed)
		return
	} else {
		// Test each task has a subtask failed.
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr"))
		}()

		if isSubtaskCancel {
			// Mock a subtask canceled
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateCanceled, nil)
				require.NoError(t, err)
			}
		} else {
			// Mock a subtask fails.
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateFailed, nil)
				require.NoError(t, err)
			}
		}
	}

	checkGetTaskState(proto.TaskStateReverting)
	require.Len(t, tasks, taskCnt)
	// Mock all subtask reverted.
	start := subtaskCnt * taskCnt
	for i := start; i <= start+subtaskCnt*taskCnt; i++ {
		err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateReverted, nil)
		require.NoError(t, err)
	}
	checkGetTaskState(proto.TaskStateReverted)
	require.Len(t, tasks, taskCnt)
}

func TestSimple(t *testing.T) {
	checkDispatch(t, 1, true, false, false, false)
}

func TestSimpleErrStage(t *testing.T) {
	checkDispatch(t, 1, false, false, false, false)
}

func TestSimpleCancel(t *testing.T) {
	checkDispatch(t, 1, false, true, false, false)
}

func TestSimpleSubtaskCancel(t *testing.T) {
	checkDispatch(t, 1, false, false, true, false)
}

func TestParallel(t *testing.T) {
	checkDispatch(t, 3, true, false, false, false)
}

func TestParallelErrStage(t *testing.T) {
	checkDispatch(t, 3, false, false, false, false)
}

func TestParallelCancel(t *testing.T) {
	checkDispatch(t, 3, false, true, false, false)
}

func TestParallelSubtaskCancel(t *testing.T) {
	checkDispatch(t, 3, false, false, true, false)
}

func TestPause(t *testing.T) {
	checkDispatch(t, 1, false, false, false, true)
}

func TestParallelPause(t *testing.T) {
	checkDispatch(t, 3, false, false, false, true)
}

func TestVerifyTaskStateTransform(t *testing.T) {
	testCases := []struct {
		oldState proto.TaskState
		newState proto.TaskState
		expect   bool
	}{
		{proto.TaskStateRunning, proto.TaskStateRunning, true},
		{proto.TaskStatePending, proto.TaskStateRunning, true},
		{proto.TaskStatePending, proto.TaskStateReverting, false},
		{proto.TaskStateRunning, proto.TaskStateReverting, true},
		{proto.TaskStateReverting, proto.TaskStateReverted, true},
		{proto.TaskStateReverting, proto.TaskStateSucceed, false},
		{proto.TaskStateRunning, proto.TaskStatePausing, true},
		{proto.TaskStateRunning, proto.TaskStateResuming, false},
		{proto.TaskStateCancelling, proto.TaskStateRunning, false},
		{proto.TaskStateCanceled, proto.TaskStateRunning, false},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expect, scheduler.VerifyTaskStateTransform(tc.oldState, tc.newState))
	}
}

func TestIsCancelledErr(t *testing.T) {
	require.False(t, scheduler.IsCancelledErr(errors.New("some err")))
	require.False(t, scheduler.IsCancelledErr(context.Canceled))
	require.True(t, scheduler.IsCancelledErr(errors.New("cancelled by user")))
}

func TestDispatcherOnNextStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	schExt := mockDispatch.NewMockExtension(ctrl)

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")
	task := proto.Task{
		ID:    1,
		State: proto.TaskStatePending,
		Step:  proto.StepInit,
	}
	cloneTask := task
	nodeMgr := scheduler.NewNodeManager()
	sch := scheduler.NewBaseScheduler(ctx, taskMgr, nodeMgr, &cloneTask)
	sch.Extension = schExt

	// test next step is done
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepDone)
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("done err"))
	require.ErrorContains(t, sch.Switch2NextStep(), "done err")
	require.True(t, ctrl.Satisfied())
	// we update task step before OnDone
	require.Equal(t, proto.StepDone, sch.Task.Step)
	*sch.Task = task
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepDone)
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	taskMgr.EXPECT().SucceedTask(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())

	// GetEligibleInstances err
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("GetEligibleInstances err"))
	require.ErrorContains(t, sch.Switch2NextStep(), "GetEligibleInstances err")
	require.True(t, ctrl.Satisfied())
	// GetEligibleInstances no instance
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
	require.ErrorContains(t, sch.Switch2NextStep(), "no available TiDB node to dispatch subtasks")
	require.True(t, ctrl.Satisfied())

	serverNodes := []string{":4000"}
	// OnNextSubtasksBatch err
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("OnNextSubtasksBatch err"))
	schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(true)
	require.ErrorContains(t, sch.Switch2NextStep(), "OnNextSubtasksBatch err")
	require.True(t, ctrl.Satisfied())

	bak := kv.TxnTotalSizeLimit.Load()
	t.Cleanup(func() {
		kv.TxnTotalSizeLimit.Store(bak)
	})

	// dispatch in batch
	subtaskMetas := [][]byte{
		[]byte(`{"xx": "1"}`),
		[]byte(`{"xx": "2"}`),
	}
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().SwitchTaskStepInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	kv.TxnTotalSizeLimit.Store(1)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())
	// met unstable subtasks
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().SwitchTaskStepInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.Annotatef(storage.ErrUnstableSubtasks, "expected %d, got %d",
			2, 100))
	kv.TxnTotalSizeLimit.Store(1)
	startTime := time.Now()
	err := sch.Switch2NextStep()
	require.ErrorIs(t, err, storage.ErrUnstableSubtasks)
	require.ErrorContains(t, err, "expected 2, got 100")
	require.WithinDuration(t, startTime, time.Now(), 10*time.Second)
	require.True(t, ctrl.Satisfied())

	// dispatch in one txn
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	kv.TxnTotalSizeLimit.Store(config.DefTxnTotalSizeLimit)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())
}

func TestManagerDispatchLoop(t *testing.T) {
	// Mock 16 cpu node.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockScheduler := mock.NewMockScheduler(ctrl)

	_ = testkit.CreateMockStore(t)
	require.Eventually(t, func() bool {
		taskMgr, err := storage.GetTaskManager()
		return err == nil && taskMgr != nil
	}, 10*time.Second, 100*time.Millisecond)

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler")
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	require.NotNil(t, taskMgr)

	// in this test, we only test scheduler manager, so we add a subtask takes 16
	// slots to avoid reserve by slots, and make sure below test cases works.
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	require.NoError(t, err)
	for _, s := range serverInfos {
		execID := disttaskutil.GenerateExecID(s)
		testutil.InsertSubtask(t, taskMgr, 1000000, proto.StepOne, execID, []byte(""), proto.SubtaskStatePending, proto.TaskTypeExample, 16)
	}
	concurrencies := []int{4, 6, 16, 2, 4, 4}
	waitChannels := make([]chan struct{}, len(concurrencies))
	for i := range waitChannels {
		waitChannels[i] = make(chan struct{})
	}
	var counter atomic.Int32
	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr scheduler.TaskManager, nodeMgr *scheduler.NodeManager, task *proto.Task) scheduler.Scheduler {
			idx := counter.Load()
			mockScheduler = mock.NewMockScheduler(ctrl)
			mockScheduler.EXPECT().Init().Return(nil)
			mockScheduler.EXPECT().ScheduleTask().Do(func() {
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := sqlexec.ExecSQL(ctx, se, "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateRunning, proto.StepOne, task.ID)
					return err
				}))
				<-waitChannels[idx]
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := sqlexec.ExecSQL(ctx, se, "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateSucceed, proto.StepDone, task.ID)
					return err
				}))
			})
			mockScheduler.EXPECT().Close()
			counter.Add(1)
			return mockScheduler
		},
>>>>>>> 99f0349bfb6 (disttask: fix failed step is taken as success (#49971))
	)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(annotatedError)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, annotatedError.Error())

	runCancel()
}

func TestSchedulerRollback(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_rollback"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", int64(1), proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err := scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, getSubtaskErr)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// 4. rollback failed
	rollbackErr := errors.New("rollback error")
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, State: proto.TaskStateRevertPending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(rollbackErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.TaskStateRevertFailed, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, rollbackErr.Error())

	// 5. rollback success
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 1, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(1), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 2, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(2), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.TaskStateRevertPending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// rollback again for previous left subtask in TaskStateReverting state
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.TaskStateReverting, SchedulerID: "id"}, nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}

func TestSchedulerPause(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_pause"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// pause success.
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension
	mockSubtaskTable.EXPECT().PauseSubtasks(runCtx, "id", int64(1)).Return(nil)
	require.NoError(t, scheduler.Pause(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp}))

	// pause error.
	pauseErr := errors.New("pause error")
	mockSubtaskTable.EXPECT().PauseSubtasks(runCtx, "id", int64(1)).Return(pauseErr)
	err := scheduler.Pause(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
	require.EqualError(t, err, pauseErr.Error())
}

func TestScheduler(t *testing.T) {
	var tp proto.TaskType = "test_scheduler"
	var taskID int64 = 1
	var concurrency uint64 = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockSubtaskTable.EXPECT().IsSchedulerCanceled(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension

	// 1. run failed.
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// 2. rollback success.
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, Type: tp, State: proto.TaskStateRevertPending, SchedulerID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}
