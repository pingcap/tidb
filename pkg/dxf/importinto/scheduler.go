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
	goerrors "errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/utils"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// warningIndexCount is the threshold to log warning for too many indexes on
	// the target table, it's known to be slow to import in this case.
	// the value if chosen as most tables have less than 32 indexes, we can adjust
	// it later if needed.
	warningIndexCount      = 32
	registerTaskTTL        = 10 * time.Minute
	refreshTaskTTLInterval = 3 * time.Minute
	registerTimeout        = 5 * time.Second
)

var (
	// NewTaskRegisterWithTTL is the ctor for TaskRegister.
	// It is exported for testing.
	NewTaskRegisterWithTTL   = utils.NewTaskRegisterWithTTL
	errGetCrossKSSessionPool = errors.New("failed to get cross keyspace session pool")
)

type taskInfo struct {
	store  kv.Storage
	taskID int64

	// operation on taskInfo is run inside detect-task goroutine, so no need to synchronize.
	lastRegisterTime time.Time

	// initialized lazily in register()
	etcdClient   *clientv3.Client
	taskRegister utils.TaskRegister
	logger       *zap.Logger
}

func (t *taskInfo) register(ctx context.Context) {
	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}

	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}
	logger := t.logger
	if t.taskRegister == nil {
		client, err := store.NewEtcdCli(t.store)
		if err != nil {
			logger.Warn("get etcd client failed", zap.Error(err))
			return
		}
		t.etcdClient = client
		t.taskRegister = NewTaskRegisterWithTTL(client, registerTaskTTL,
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
	logger := t.logger
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
	// below fields are only used when the task keyspace doesn't equal to current
	// instance keyspace.
	taskKS string
	// the task manager for accessing import job in task keyspace.
	taskKSTaskMgr scheduler.TaskManager
}

var _ scheduler.Extension = (*importScheduler)(nil)

// NewImportScheduler creates a new import scheduler.
func NewImportScheduler(
	ctx context.Context,
	task *proto.Task,
	param scheduler.Param,
) scheduler.Scheduler {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	sch := &importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(subCtx, task, param),
		taskKS:        task.Keyspace,
	}
	return sch
}

// NewImportSchedulerForTest creates a new import scheduler for test.
func NewImportSchedulerForTest(globalSort bool, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	return &importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), task, param),
		GlobalSort:    globalSort,
		taskKS:        tidb.GetGlobalKeyspaceName(),
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

	logger := sch.GetLogger()
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
	pdHTTPCli := sch.TaskStore.(kv.StorageWithPD).GetPDHTTPClient()
	switcher := importer.NewTiKVModeSwitcher(tls, pdHTTPCli, logger)

	switcher.ToImportMode(ctx)
	sch.lastSwitchTime.Store(time.Now())
}

func (sch *importScheduler) registerTask(ctx context.Context, task *proto.Task) {
	val, _ := sch.taskInfoMap.LoadOrStore(task.ID, &taskInfo{store: sch.TaskStore, taskID: task.ID, logger: sch.GetLogger()})
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
) (resSubtaskMeta [][]byte, err error) {
	nodeCnt := len(execIDs)
	if kerneltype.IsNextGen() {
		// in nextgen, node resource are scaled out automatically, we only consider
		// the max allowed node for the task, and ignore how many node currently
		// available.
		nodeCnt = task.MaxNodeCount
	}
	taskMeta := &TaskMeta{}
	err = json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger := sch.GetLogger().With(
		zap.String("curr-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
		zap.Int("node-count", nodeCnt),
		zap.Int64("table-id", taskMeta.Plan.TableInfo.ID),
	)
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
		if importer.GetNumOfIndexGenKV(taskMeta.Plan.TableInfo) > warningIndexCount {
			dxfmetric.ScheduleEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventTooManyIdx).Inc()
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
	case proto.ImportStepCollectConflicts, proto.ImportStepConflictResolution:
		encodeAndSortMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepEncodeAndSort)
		if err != nil {
			return nil, err
		}
		mergeSortMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepMergeSort)
		if err != nil {
			return nil, err
		}
		ingestMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepWriteAndIngest)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[proto.ImportStepEncodeAndSort] = encodeAndSortMetas
		previousSubtaskMetas[proto.ImportStepMergeSort] = mergeSortMetas
		previousSubtaskMetas[proto.ImportStepWriteAndIngest] = ingestMetas
		if err = sch.job2Step(ctx, logger, taskMeta, importer.JobStepResolvingConflicts); err != nil {
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
		step := getStepOfEncode(sch.GlobalSort)
		metas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, step)
		if err != nil {
			return nil, err
		}
		conflictResMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepCollectConflicts)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[step] = metas
		previousSubtaskMetas[proto.ImportStepCollectConflicts] = conflictResMetas
		logger.Info("move to post-process step", zap.Any("result", taskMeta.Summary))
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
		ExecuteNodesCnt:      nodeCnt,
		Store:                sch.TaskStore,
		ThreadCnt:            task.GetRuntimeSlots(),
	}
	logicalPlan := &LogicalPlan{Logger: logger}
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

	if err := updateTaskSummary(taskHandle, task, taskMeta, nextStep, logicalPlan); err != nil {
		return nil, err
	}

	logger.Info("generate subtasks", zap.Int("subtask-count", len(metaBytes)))
	if nextStep == proto.ImportStepMergeSort && len(metaBytes) > 0 {
		dxfmetric.ScheduleEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventMergeSort).Inc()
	}
	return metaBytes, nil
}

// OnDone implements scheduler.Extension interface.
func (sch *importScheduler) OnDone(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	logger := sch.GetLogger().With(zap.String("step", proto.Step2Str(task.Type, task.Step)))
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
	if goerrors.Is(err, errGetCrossKSSessionPool) {
		return true
	}
	return common.IsRetryableError(err)
}

// GetNextStep implements scheduler.Extension interface.
