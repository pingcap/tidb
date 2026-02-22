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
	goerrors "errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/backoff"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (s *BaseScheduler) onFinished() {
	task := s.GetTask()
	s.logger.Debug("schedule task, task is finished", zap.Stringer("state", task.State))
}

func (s *BaseScheduler) switch2NextStep() error {
	task := s.getTaskClone()
	nextStep := s.GetNextStep(&task.TaskBase)
	s.logger.Info("switch to next step",
		zap.String("current-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
		zap.Int("required-slots", task.RequiredSlots),
		zap.Int("runtime-slots", task.GetRuntimeSlots()),
	)

	if nextStep == proto.StepDone {
		if err := s.OnDone(s.ctx, s, task); err != nil {
			return errors.Trace(err)
		}
		if err := s.taskMgr.SucceedTask(s.ctx, task.ID); err != nil {
			return errors.Trace(err)
		}
		task.Step = nextStep
		task.State = proto.TaskStateSucceed
		s.task.Store(task)
		onTaskFinished(task.State, task.Error)
		return nil
	}

	nodes := s.nodeMgr.getNodes()
	nodeIDs := filterByScope(nodes, task.TargetScope)
	eligibleNodes, err := getEligibleNodes(s.ctx, s, nodeIDs)
	if err != nil {
		return err
	}
	if task.MaxNodeCount > 0 && len(eligibleNodes) > task.MaxNodeCount {
		// OnNextSubtasksBatch may use len(eligibleNodes) as a hint to
		// calculate the number of subtasks, so we need to do this before
		// filtering nodes by available slots in scheduleSubtask.
		eligibleNodes = eligibleNodes[:task.MaxNodeCount]
	}

	s.logger.Info("eligible instances", zap.Int("num", len(eligibleNodes)))
	if len(eligibleNodes) == 0 {
		return errors.New("no available TiDB node to dispatch subtasks")
	}

	metas, err := s.OnNextSubtasksBatch(s.ctx, s, task, eligibleNodes, nextStep)
	if err != nil {
		s.logger.Warn("generate part of subtasks failed", zap.Error(err))
		return s.handlePlanErr(err)
	}

	if err = s.scheduleSubTask(task, nextStep, metas, eligibleNodes); err != nil {
		return err
	}
	task.Step = nextStep
	task.State = proto.TaskStateRunning
	// and OnNextSubtasksBatch might change meta of task.
	s.task.Store(task)
	return nil
}

func (s *BaseScheduler) scheduleSubTask(
	task *proto.Task,
	subtaskStep proto.Step,
	metas [][]byte,
	eligibleNodes []string) error {
	s.logger.Info("schedule subtasks",
		zap.Stringer("state", task.State),
		zap.String("step", proto.Step2Str(task.Type, subtaskStep)),
		zap.Int("requiredSlots", task.RequiredSlots),
		zap.Int("subtasks", len(metas)))

	// the scheduled node of the subtask might not be optimal, as we run all
	// scheduler in parallel, and update might be called too many times when
	// multiple tasks are switching to next step.
	// balancer will assign the subtasks to the right instance according to
	// the system load of all nodes.
	if err := s.slotMgr.update(s.ctx, s.nodeMgr, s.taskMgr); err != nil {
		return err
	}
	adjustedEligibleNodes := s.slotMgr.adjustEligibleNodes(eligibleNodes, task.RequiredSlots)
	var size uint64
	subTasks := make([]*proto.Subtask, 0, len(metas))
	for i, meta := range metas {
		// our schedule target is to maximize the resource usage of all nodes while
		// fulfilling the target of schedule tasks in the task order, we will try
		// to pack the subtasks of different tasks onto as minimal number of nodes
		// as possible, to allow later tasks of higher required slots can be scheduled
		// and run, so we order nodes, see TaskManager.GetAllNodes and assign the
		// subtask to the instance in a round-robin way.
		// for example:
		//   - we have 2 node N1 and N2 of 8 cores.
		//   - we have 2 tasks T1 and T2, each is of thread 4 and have 1 subtask.
		//   - subtasks of T1 and T2 are all scheduled to N1
		//   - later we have a task T3 of thread 8, we can schedule it to N2.
		pos := i % len(adjustedEligibleNodes)
		instanceID := adjustedEligibleNodes[pos]
		s.logger.Debug("create subtasks", zap.String("instanceID", instanceID))
		subTasks = append(subTasks, proto.NewSubtask(
			subtaskStep, task.ID, task.Type, instanceID, task.RequiredSlots, meta, i+1))

		size += uint64(len(meta))
	}
	failpoint.InjectCall("cancelBeforeUpdateTask", task.ID)

	// as other fields and generated key and index KV takes space too, we limit
	// the size of subtasks to 80% of the transaction limit.
	limit := max(uint64(float64(kv.TxnTotalSizeLimit.Load())*0.8), 1)
	fn := s.taskMgr.SwitchTaskStep
	if size >= limit {
		// On default, transaction size limit is controlled by tidb_mem_quota_query
		// which is 1G on default, so it's unlikely to reach this limit, but in
		// case user set txn-total-size-limit explicitly, we insert in batch.
		s.logger.Info("subtasks size exceed limit, will insert in batch",
			zap.Uint64("size", size), zap.Uint64("limit", limit))
		fn = s.taskMgr.SwitchTaskStepInBatch
	}

	backoffer := backoff.NewExponential(RetrySQLInterval, 2, RetrySQLMaxInterval)
	return handle.RunWithRetry(s.ctx, RetrySQLTimes, backoffer, s.logger,
		func(context.Context) (bool, error) {
			err := fn(s.ctx, task, proto.TaskStateRunning, subtaskStep, subTasks)
			if goerrors.Is(err, storage.ErrUnstableSubtasks) {
				return false, err
			}
			return true, err
		},
	)
}

func (s *BaseScheduler) handlePlanErr(err error) error {
	task := s.getTaskClone()
	s.logger.Warn("generate plan failed", zap.Error(err), zap.Stringer("state", task.State))
	if s.IsRetryableErr(err) {
		dxfmetric.ScheduleEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventRetry).Inc()
		return err
	}
	return s.revertTask(err)
}

func (s *BaseScheduler) revertTask(taskErr error) error {
	task := s.getTaskClone()
	if err := s.taskMgr.RevertTask(s.ctx, task.ID, task.State, taskErr); err != nil {
		return err
	}
	task.State = proto.TaskStateReverting
	task.Error = taskErr
	s.task.Store(task)
	return nil
}

func (s *BaseScheduler) revertTaskOrManualRecover(taskErr error) error {
	task := s.getTaskClone()
	if task.ExtraParams.ManualRecovery {
		if err := s.taskMgr.AwaitingResolveTask(s.ctx, task.ID, task.State, taskErr); err != nil {
			return err
		}
		task.State = proto.TaskStateAwaitingResolution
		task.Error = taskErr
		s.task.Store(task)
		return nil
	}
	return s.revertTask(taskErr)
}

// MockServerInfo exported for scheduler_test.go
var MockServerInfo atomic.Pointer[[]string]

// GetLiveExecIDs returns all live executor node IDs.
func GetLiveExecIDs(ctx context.Context) ([]string, error) {
	failpoint.Inject("mockTaskExecutorNodes", func() {
		failpoint.Return(*MockServerInfo.Load(), nil)
	})
	serverInfos, err := generateTaskExecutorNodes(ctx)
	if err != nil {
		return nil, err
	}
	execIDs := make([]string, 0, len(serverInfos))
	for _, info := range serverInfos {
		execIDs = append(execIDs, disttaskutil.GenerateExecID(info))
	}
	return execIDs, nil
}

func generateTaskExecutorNodes(ctx context.Context) (serverNodes []*serverinfo.ServerInfo, err error) {
	var serverInfos map[string]*serverinfo.ServerInfo
	_, etcd := ctx.Value("etcd").(bool)
	if intest.InTest && !etcd {
		serverInfos = infosync.MockGlobalServerInfoManagerEntry.GetAllServerInfo()
	} else {
		serverInfos, err = infosync.GetAllServerInfo(ctx)
	}
	if err != nil {
		return nil, err
	}
	if len(serverInfos) == 0 {
		return nil, errors.New("not found instance")
	}

	serverNodes = make([]*serverinfo.ServerInfo, 0, len(serverInfos))
	for _, serverInfo := range serverInfos {
		serverNodes = append(serverNodes, serverInfo)
	}
	return serverNodes, nil
}

// GetPreviousSubtaskMetas get subtask metas from specific step.
func (s *BaseScheduler) GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error) {
	previousSubtasks, err := s.taskMgr.GetAllSubtasksByStepAndState(s.ctx, taskID, step, proto.SubtaskStateSucceed)
	if err != nil {
		s.logger.Warn("get previous succeed subtask failed",
			zap.String("step", proto.Step2Str(s.GetTask().Type, step)))
		return nil, err
	}
	previousSubtaskMetas := make([][]byte, 0, len(previousSubtasks))
	for _, subtask := range previousSubtasks {
		previousSubtaskMetas = append(previousSubtaskMetas, subtask.Meta)
	}
	return previousSubtaskMetas, nil
}

// GetPreviousSubtaskSummary gets previous subtask summaries.
func (s *BaseScheduler) GetPreviousSubtaskSummary(taskID int64, step proto.Step) ([]*execute.SubtaskSummary, error) {
	return s.taskMgr.GetAllSubtaskSummaryByStep(s.ctx, taskID, step)
}

// WithNewSession executes the function with a new session.
func (s *BaseScheduler) WithNewSession(fn func(se sessionctx.Context) error) error {
	return s.taskMgr.WithNewSession(fn)
}

// WithNewTxn executes the fn in a new transaction.
func (s *BaseScheduler) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	return s.taskMgr.WithNewTxn(ctx, fn)
}

// GetTaskMgr returns the task manager.
func (s *BaseScheduler) GetTaskMgr() TaskManager {
	return s.taskMgr
}

func (*BaseScheduler) isStepSucceed(cntByStates map[proto.SubtaskState]int64) bool {
	_, ok := cntByStates[proto.SubtaskStateSucceed]
	return len(cntByStates) == 0 || (len(cntByStates) == 1 && ok)
}

// GetLogger returns the logger.
func (s *BaseScheduler) GetLogger() *zap.Logger {
	return s.logger
}

// IsCancelledErr checks if the error is a cancelled error.
func IsCancelledErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), taskCancelMsg)
}

// getEligibleNodes returns the eligible(live) nodes for the task.
// if the task can only be scheduled to some specific nodes, return them directly,
// we don't care liveliness of them.
func getEligibleNodes(ctx context.Context, sch Scheduler, managedNodes []string) ([]string, error) {
	serverNodes, err := sch.GetEligibleInstances(ctx, sch.GetTask())
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Debug("eligible instances", zap.Int("num", len(serverNodes)))
	if len(serverNodes) == 0 {
		serverNodes = managedNodes
	}

	return serverNodes, nil
}

func onTaskFinished(state proto.TaskState, taskErr error) {
	// when task finishes, we classify the finished tasks into succeed/failed/cancelled
	var metricState string

	if state == proto.TaskStateSucceed || state == proto.TaskStateFailed {
		metricState = state.String()
	} else if state == proto.TaskStateReverted {
		metricState = proto.TaskStateFailed.String()
		if IsCancelledErr(taskErr) {
			metricState = "cancelled"
		}
	}
	if len(metricState) > 0 {
		dxfmetric.FinishedTaskCounter.WithLabelValues("all").Inc()
		dxfmetric.FinishedTaskCounter.WithLabelValues(metricState).Inc()
	}
}
