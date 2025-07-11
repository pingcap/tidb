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

package importinto

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/utils"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	registerTaskTTL        = 10 * time.Minute
	refreshTaskTTLInterval = 3 * time.Minute
	registerTimeout        = 5 * time.Second
)

// NewTaskRegisterWithTTL is the ctor for TaskRegister.
// It is exported for testing.
var NewTaskRegisterWithTTL = utils.NewTaskRegisterWithTTL

type taskInfo struct {
	taskID int64

	// operation on taskInfo is run inside detect-task goroutine, so no need to synchronize.
	lastRegisterTime time.Time

	// initialized lazily in register()
	etcdClient   *etcd.Client
	taskRegister utils.TaskRegister
}

func (t *taskInfo) register(ctx context.Context) {
	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}

	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}
	logger := logutil.BgLogger().With(zap.Int64("task-id", t.taskID))
	if t.taskRegister == nil {
		client, err := importer.GetEtcdClient()
		if err != nil {
			logger.Warn("get etcd client failed", zap.Error(err))
			return
		}
		t.etcdClient = client
		t.taskRegister = NewTaskRegisterWithTTL(client.GetClient(), registerTaskTTL,
			utils.RegisterImportInto, strconv.FormatInt(t.taskID, 10))
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	if err := t.taskRegister.RegisterTaskOnce(timeoutCtx); err != nil {
		logger.Warn("register task failed", zap.Error(err))
	} else {
		logger.Info("register task to pd or refresh lease success")
	}
	// we set it even if register failed, TTL is 10min, refresh interval is 3min,
	// we can try 2 times before the lease is expired.
	t.lastRegisterTime = time.Now()
}

func (t *taskInfo) close(ctx context.Context) {
	logger := logutil.BgLogger().With(zap.Int64("task-id", t.taskID))
	if t.taskRegister != nil {
		timeoutCtx, cancel := context.WithTimeout(ctx, registerTimeout)
		defer cancel()
		if err := t.taskRegister.Close(timeoutCtx); err != nil {
			logger.Warn("unregister task failed", zap.Error(err))
		} else {
			logger.Info("unregister task success")
		}
		t.taskRegister = nil
	}
	if t.etcdClient != nil {
		if err := t.etcdClient.Close(); err != nil {
			logger.Warn("close etcd client failed", zap.Error(err))
		}
		t.etcdClient = nil
	}
}

type importScheduler struct {
	*scheduler.BaseScheduler

	GlobalSort bool
	mu         sync.RWMutex
	// NOTE: there's no need to sync for below 2 fields actually, since we add a restriction that only one
	// task can be running at a time. but we might support task queuing in the future, leave it for now.
	// the last time we switch TiKV into IMPORT mode, this is a global operation, do it for one task makes
	// no difference to do it for all tasks. So we do not need to record the switch time for each task.
	lastSwitchTime atomic.Time
	// taskInfoMap is a map from taskID to taskInfo
	taskInfoMap sync.Map

	// currTaskID is the taskID of the current running task.
	// It may be changed when we switch to a new task or switch to a new owner.
	currTaskID            atomic.Int64
	disableTiKVImportMode atomic.Bool

	store kv.Storage
	// below fields are only used when the task keyspace doesn't equal to current
	// instance keyspace.
	taskKS         string
	taskKSSessPool util.SessionPool
}

var _ scheduler.Extension = (*importScheduler)(nil)

// NewImportScheduler creates a new import scheduler.
func NewImportScheduler(
	ctx context.Context,
	task *proto.Task,
	param scheduler.Param,
	store kv.Storage,
) scheduler.Scheduler {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	sch := &importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(subCtx, task, param),
		store:         store,
		taskKS:        task.Keyspace,
	}
	return sch
}

// NewImportSchedulerForTest creates a new import scheduler for test.
func NewImportSchedulerForTest(globalSort bool) scheduler.Scheduler {
	return &importScheduler{
		GlobalSort: globalSort,
	}
}

func (sch *importScheduler) Init() (err error) {
	task := sch.GetTask()
	defer func() {
		if err != nil {
			// if init failed, close is not called, so we need to unregister here.
			metricsManager.unregister(task.ID)
		}
	}()
	taskMeta := &TaskMeta{}
	if err = json.Unmarshal(task.Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}

	if task.Keyspace != tidb.GetGlobalKeyspaceName() {
		if err = sch.BaseScheduler.WithNewSession(func(se sessionctx.Context) error {
			var err2 error
			sch.taskKSSessPool, err2 = se.GetSQLServer().GetKSSessPool(task.Keyspace)
			return err2
		}); err != nil {
			return errors.Annotatef(err, "get session pool for keyspace %s failed", task.Keyspace)
		}
	}

	sch.GlobalSort = taskMeta.Plan.CloudStorageURI != ""
	sch.BaseScheduler.Extension = sch
	return sch.BaseScheduler.Init()
}

func (sch *importScheduler) Close() {
	metricsManager.unregister(sch.GetTask().ID)
	sch.BaseScheduler.Close()
}

// OnTick implements scheduler.Extension interface.
func (sch *importScheduler) OnTick(ctx context.Context, task *proto.Task) {
	// only switch TiKV mode or register task when task is running
	if task.State != proto.TaskStateRunning {
		return
	}
	sch.switchTiKVMode(ctx, task)
	sch.registerTask(ctx, task)
}

func (*importScheduler) isImporting2TiKV(task *proto.Task) bool {
	return task.Step == proto.ImportStepImport || task.Step == proto.ImportStepWriteAndIngest
}

func (sch *importScheduler) switchTiKVMode(ctx context.Context, task *proto.Task) {
	sch.updateCurrentTask(task)
	// only import step need to switch to IMPORT mode,
	// If TiKV is in IMPORT mode during checksum, coprocessor will time out.
	if sch.disableTiKVImportMode.Load() || !sch.isImporting2TiKV(task) {
		return
	}

	if time.Since(sch.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	sch.mu.Lock()
	defer sch.mu.Unlock()
	if time.Since(sch.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID))
	// TODO: use the TLS object from TiDB server
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	pdHTTPCli := sch.store.(kv.StorageWithPD).GetPDHTTPClient()
	switcher := importer.NewTiKVModeSwitcher(tls, pdHTTPCli, logger)

	switcher.ToImportMode(ctx)
	sch.lastSwitchTime.Store(time.Now())
}

func (sch *importScheduler) registerTask(ctx context.Context, task *proto.Task) {
	val, _ := sch.taskInfoMap.LoadOrStore(task.ID, &taskInfo{taskID: task.ID})
	info := val.(*taskInfo)
	info.register(ctx)
}

func (sch *importScheduler) unregisterTask(ctx context.Context, task *proto.Task) {
	if val, loaded := sch.taskInfoMap.LoadAndDelete(task.ID); loaded {
		info := val.(*taskInfo)
		info.close(ctx)
	}
}

// OnNextSubtasksBatch generate batch of next stage's plan.
func (sch *importScheduler) OnNextSubtasksBatch(
	ctx context.Context,
	taskHandle storage.TaskHandle,
	task *proto.Task,
	execIDs []string,
	nextStep proto.Step,
) (
	resSubtaskMeta [][]byte, err error) {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.String("curr-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
	)
	taskMeta := &TaskMeta{}
	err = json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger.Info("on next subtasks batch")

	previousSubtaskMetas := make(map[proto.Step][][]byte, 1)
	switch nextStep {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		if metrics, ok := metric.GetCommonMetric(ctx); ok {
			metrics.BytesCounter.WithLabelValues(metric.StateTotalRestore).Add(float64(taskMeta.Plan.TotalFileSize))
		}
		jobStep := importer.JobStepImporting
		if sch.GlobalSort {
			jobStep = importer.JobStepGlobalSorting
		}
		if err = sch.startJob(ctx, logger, taskMeta, jobStep); err != nil {
			return nil, err
		}
	case proto.ImportStepMergeSort:
		sortAndEncodeMeta, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepEncodeAndSort)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[proto.ImportStepEncodeAndSort] = sortAndEncodeMeta
	case proto.ImportStepWriteAndIngest:
		failpoint.Inject("failWhenDispatchWriteIngestSubtask", func() {
			failpoint.Return(nil, errors.New("injected error"))
		})
		// merge sort might be skipped for some kv groups, so we need to get all
		// subtask metas of ImportStepEncodeAndSort step too.
		encodeAndSortMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepEncodeAndSort)
		if err != nil {
			return nil, err
		}
		mergeSortMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepMergeSort)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[proto.ImportStepEncodeAndSort] = encodeAndSortMetas
		previousSubtaskMetas[proto.ImportStepMergeSort] = mergeSortMetas
		if err = sch.job2Step(ctx, logger, taskMeta, importer.JobStepImporting); err != nil {
			return nil, err
		}
	case proto.ImportStepPostProcess:
		sch.switchTiKV2NormalMode(ctx, task, logger)
		failpoint.Inject("clearLastSwitchTime", func() {
			sch.lastSwitchTime.Store(time.Time{})
		})
		if err = sch.job2Step(ctx, logger, taskMeta, importer.JobStepValidating); err != nil {
			return nil, err
		}
		failpoint.Inject("failWhenDispatchPostProcessSubtask", func() {
			failpoint.Return(nil, errors.New("injected error after ImportStepImport"))
		})
		// we need get metas where checksum is stored.
		if err := updateResult(taskHandle, task, taskMeta, sch.GlobalSort); err != nil {
			return nil, err
		}
		step := getStepOfEncode(sch.GlobalSort)
		metas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, step)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[step] = metas
		logger.Info("move to post-process step ", zap.Any("result", taskMeta.Result))
	case proto.StepDone:
		return nil, nil
	default:
		return nil, errors.Errorf("unknown step %d", task.Step)
	}

	planCtx := planner.PlanCtx{
		Ctx:                  ctx,
		TaskID:               task.ID,
		PreviousSubtaskMetas: previousSubtaskMetas,
		GlobalSort:           sch.GlobalSort,
		NextTaskStep:         nextStep,
		ExecuteNodesCnt:      len(execIDs),
		Store:                sch.store,
	}
	logicalPlan := &LogicalPlan{}
	if err := logicalPlan.FromTaskMeta(task.Meta); err != nil {
		return nil, err
	}
	physicalPlan, err := logicalPlan.ToPhysicalPlan(planCtx)
	if err != nil {
		return nil, err
	}
	metaBytes, err := physicalPlan.ToSubtaskMetas(planCtx, nextStep)
	if err != nil {
		return nil, err
	}

	logger.Info("generate subtasks", zap.Int("subtask-count", len(metaBytes)))
	return metaBytes, nil
}

// OnDone implements scheduler.Extension interface.
func (sch *importScheduler) OnDone(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
	)
	logger.Info("task done", zap.Stringer("state", task.State), zap.Error(task.Error))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	if task.State == proto.TaskStateReverting {
		errMsg := ""
		if task.Error != nil {
			if scheduler.IsCancelledErr(task.Error) {
				return sch.cancelJob(ctx, task, taskMeta, logger)
			}
			errMsg = task.Error.Error()
		}
		return sch.failJob(ctx, task, taskMeta, logger, errMsg)
	}
	return sch.finishJob(ctx, logger, task, taskMeta)
}

// GetEligibleInstances implements scheduler.Extension interface.
func (*importScheduler) GetEligibleInstances(_ context.Context, task *proto.Task) ([]string, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	res := make([]string, 0, len(taskMeta.EligibleInstances))
	for _, instance := range taskMeta.EligibleInstances {
		res = append(res, disttaskutil.GenerateExecID(instance))
	}
	return res, nil
}

// IsRetryableErr implements scheduler.Extension interface.
func (*importScheduler) IsRetryableErr(err error) bool {
	return common.IsRetryableError(err)
}

// GetNextStep implements scheduler.Extension interface.
func (sch *importScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		if sch.GlobalSort {
			return proto.ImportStepEncodeAndSort
		}
		return proto.ImportStepImport
	case proto.ImportStepEncodeAndSort:
		return proto.ImportStepMergeSort
	case proto.ImportStepMergeSort:
		return proto.ImportStepWriteAndIngest
	case proto.ImportStepImport, proto.ImportStepWriteAndIngest:
		return proto.ImportStepPostProcess
	default:
		// current step must be ImportStepPostProcess
		return proto.StepDone
	}
}

func (sch *importScheduler) switchTiKV2NormalMode(ctx context.Context, task *proto.Task, logger *zap.Logger) {
	sch.updateCurrentTask(task)
	if sch.disableTiKVImportMode.Load() {
		return
	}

	sch.mu.Lock()
	defer sch.mu.Unlock()

	// TODO: use the TLS object from TiDB server
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	pdHTTPCli := sch.store.(kv.StorageWithPD).GetPDHTTPClient()
	switcher := importer.NewTiKVModeSwitcher(tls, pdHTTPCli, logger)

	switcher.ToNormalMode(ctx)

	// clear it, so next task can switch TiKV mode again.
	sch.lastSwitchTime.Store(time.Time{})
}

func (sch *importScheduler) updateCurrentTask(task *proto.Task) {
	if sch.currTaskID.Swap(task.ID) != task.ID {
		taskMeta := &TaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err == nil {
			// for raftkv2, switch mode in local backend
			sch.disableTiKVImportMode.Store(taskMeta.Plan.DisableTiKVImportMode || taskMeta.Plan.IsRaftKV2)
		}
	}
}

// ModifyMeta implements scheduler.Extension interface.
func (*importScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

func updateMeta(task *proto.Task, taskMeta *TaskMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	task.Meta = bs

	return nil
}

func getStepOfEncode(globalSort bool) proto.Step {
	if globalSort {
		return proto.ImportStepEncodeAndSort
	}
	return proto.ImportStepImport
}

// we will update taskMeta in place and make task.Meta point to the new taskMeta.
func updateResult(handle storage.TaskHandle, task *proto.Task, taskMeta *TaskMeta, globalSort bool) error {
	stepOfEncode := getStepOfEncode(globalSort)
	summaries, err := handle.GetPreviousSubtaskSummary(task.ID, stepOfEncode)
	if err != nil {
		return err
	}

	for _, summary := range summaries {
		taskMeta.Result.LoadedRowCnt += uint64(summary.RowCnt.Load())
	}

	return updateMeta(task, taskMeta)
}

func (sch *importScheduler) startJob(ctx context.Context, logger *zap.Logger, taskMeta *TaskMeta, jobStep string) error {
	failpoint.InjectCall("syncBeforeJobStarted", taskMeta.JobID)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	// we consider all errors as retryable errors, except context done.
	// the errors include errors happened when communicate with PD and TiKV.
	// we didn't consider system corrupt cases like system table dropped/altered.
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err = handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.StartJob(ctx, exec, taskMeta.JobID, jobStep)
			})
		},
	)
	failpoint.InjectCall("syncAfterJobStarted")
	return err
}

func (sch *importScheduler) job2Step(ctx context.Context, logger *zap.Logger, taskMeta *TaskMeta, step string) error {
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// todo: use scheduler.TaskHandle
	// we might call this in taskExecutor later, there's no scheduler.Extension, so we use taskManager here.
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
			})
		},
	)
}

func (sch *importScheduler) finishJob(ctx context.Context, logger *zap.Logger,
	task *proto.Task, taskMeta *TaskMeta) error {
	// we have already switch import-mode when switch to post-process step.
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	summary := &importer.JobSummary{ImportedRows: taskMeta.Result.LoadedRowCnt}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				if err := importer.FlushTableStats(ctx, se, taskMeta.Plan.TableInfo.ID, int64(taskMeta.Result.LoadedRowCnt)); err != nil {
					logger.Warn("flush table stats failed", zap.Error(err))
				}
				exec := se.GetSQLExecutor()
				return importer.FinishJob(ctx, exec, taskMeta.JobID, summary)
			})
		},
	)
}

func (sch *importScheduler) failJob(ctx context.Context, task *proto.Task,
	taskMeta *TaskMeta, logger *zap.Logger, errorMsg string) error {
	sch.switchTiKV2NormalMode(ctx, task, logger)
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.FailJob(ctx, exec, taskMeta.JobID, errorMsg)
			})
		},
	)
}

func (sch *importScheduler) cancelJob(ctx context.Context, task *proto.Task,
	meta *TaskMeta, logger *zap.Logger) error {
	sch.switchTiKV2NormalMode(ctx, task, logger)
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.CancelJob(ctx, exec, meta.JobID)
			})
		},
	)
}

func (sch *importScheduler) getTaskMgrForAccessingImportJob() (*storage.TaskManager, error) {
	if sch.taskKS == tidb.GetGlobalKeyspaceName() {
		return storage.GetTaskManager()
	}
	return storage.NewTaskManager(sch.taskKSSessPool), nil
}

func redactSensitiveInfo(task *proto.Task, taskMeta *TaskMeta) {
	taskMeta.Stmt = ""
	taskMeta.Plan.Path = ast.RedactURL(taskMeta.Plan.Path)
	if taskMeta.Plan.CloudStorageURI != "" {
		taskMeta.Plan.CloudStorageURI = ast.RedactURL(taskMeta.Plan.CloudStorageURI)
	}
	if err := updateMeta(task, taskMeta); err != nil {
		// marshal failed, should not happen
		logutil.BgLogger().Warn("failed to update task meta", zap.Error(err))
	}
}
