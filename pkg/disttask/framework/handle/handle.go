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

package handle

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	extstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/storage/recording"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/metering"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/tidbvar"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	checkTaskFinishInterval = 300 * time.Millisecond

	// TaskChangedCh used to speed up task schedule, such as when task is submitted
	// in the same node as the scheduler manager.
	// put it here to avoid cyclic import.
	TaskChangedCh = make(chan struct{}, 1)
)

const (
	// NextGenTargetScope is the target scope for new tasks in nextgen kernel.
	// on nextgen, DXF works as a service and runs only on node with scope 'dxf_service',
	// so all tasks must be submitted to that scope.
	NextGenTargetScope = "dxf_service"

	// TODO refactor and unify with lightning config, we copy them here to avoid
	// import cycle.
	defRegionSplitSize int64 = 96 * units.MiB
	defRegionSplitKeys int64 = 960_000
)

// NotifyTaskChange is used to notify the scheduler manager that the task is changed,
// either a new task is submitted or a task is finished.
func NotifyTaskChange() {
	select {
	case TaskChangedCh <- struct{}{}:
	default:
	}
}

// GetCPUCountOfNode gets the CPU count of the managed node.
func GetCPUCountOfNode(ctx context.Context) (int, error) {
	manager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return 0, err
	}
	return manager.GetCPUCountOfNode(ctx)
}

// SubmitTask submits a task.
func SubmitTask(ctx context.Context, taskKey string, taskType proto.TaskType, keyspace string, concurrency int, targetScope string, maxNodeCnt int, taskMeta []byte) (*proto.Task, error) {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return nil, err
	}
	if task != nil {
		return nil, storage.ErrTaskAlreadyExists
	}

	taskID, err := taskManager.CreateTask(ctx, taskKey, taskType, keyspace, concurrency, targetScope, maxNodeCnt, proto.ExtraParams{}, taskMeta)
	if err != nil {
		return nil, err
	}

	task, err = taskManager.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	failpoint.InjectCall("afterDXFTaskSubmitted")

	NotifyTaskChange()
	return task, nil
}

// WaitTaskDoneOrPaused waits for a task done or paused.
// this API returns error if task failed or cancelled.
func WaitTaskDoneOrPaused(ctx context.Context, id int64) error {
	logger := logutil.Logger(ctx).With(zap.Int64("task-id", id))
	_, err := WaitTask(ctx, id, func(t *proto.TaskBase) bool {
		return t.IsDone() || t.State == proto.TaskStatePaused
	})
	if err != nil {
		return err
	}
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	found, err := taskManager.GetTaskByIDWithHistory(ctx, id)
	if err != nil {
		return err
	}

	switch found.State {
	case proto.TaskStateSucceed:
		return nil
	case proto.TaskStateReverted:
		logger.Error("task reverted", zap.Error(found.Error))
		return found.Error
	case proto.TaskStatePaused:
		logger.Error("task paused")
		return nil
	case proto.TaskStateFailed:
		return errors.Errorf("task stopped with state %s, err %v", found.State, found.Error)
	}
	return nil
}

// WaitTaskDoneByKey waits for a task done by task key.
func WaitTaskDoneByKey(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil {
		return err
	}
	_, err = WaitTask(ctx, task.ID, func(t *proto.TaskBase) bool {
		return t.IsDone()
	})
	return err
}

// WaitTask waits for a task until it meets the matchFn.
func WaitTask(ctx context.Context, id int64, matchFn func(base *proto.TaskBase) bool) (*proto.TaskBase, error) {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(checkTaskFinishInterval)
	defer ticker.Stop()

	logger := logutil.Logger(ctx).With(zap.Int64("task-id", id))
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			task, err := taskManager.GetTaskBaseByIDWithHistory(ctx, id)
			if err != nil {
				logger.Error("cannot get task during waiting", zap.Error(err))
				continue
			}

			if matchFn(task) {
				return task, nil
			}
		}
	}
}

// CancelTask cancels a task.
func CancelTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		if goerrors.Is(err, storage.ErrTaskNotFound) {
			logutil.BgLogger().Info("task not exist", zap.String("taskKey", taskKey))
			return nil
		}
		return err
	}
	return taskManager.CancelTask(ctx, task.ID)
}

// PauseTask pauses a task.
func PauseTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	found, err := taskManager.PauseTask(ctx, taskKey)
	if !found {
		logutil.BgLogger().Info("task not pausable", zap.String("taskKey", taskKey))
		return nil
	}
	return err
}

// ResumeTask resumes a task.
func ResumeTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	found, err := taskManager.ResumeTask(ctx, taskKey)
	if !found {
		logutil.BgLogger().Info("task not resumable", zap.String("taskKey", taskKey))
		return nil
	}
	return err
}

// RunWithRetry runs a function with retry, when retry exceed max retry time, it
// returns the last error met.
// if the function fails with err, it should return a bool to indicate whether
// the error is retryable.
// if context done, it will stop early and return ctx.Err().
func RunWithRetry(
	ctx context.Context,
	maxRetry int,
	backoffer backoff.Backoffer,
	logger *zap.Logger,
	f func(context.Context) (bool, error),
) error {
	var lastErr error
	for i := range maxRetry {
		retryable, err := f(ctx)
		if err == nil || !retryable {
			return err
		}
		lastErr = err
		metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
		logger.Warn("met retryable error", zap.Int("retry-count", i),
			zap.Int("max-retry", maxRetry), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffer.Backoff(i)):
		}
	}
	return lastErr
}

var nodeResource atomic.Pointer[proto.NodeResource]

// GetNodeResource gets the node resource.
func GetNodeResource() *proto.NodeResource {
	return nodeResource.Load()
}

// SetNodeResource gets the node resource.
func SetNodeResource(rc *proto.NodeResource) {
	nodeResource.Store(rc)
}

// GetDefaultRegionSplitConfig gets the default region split size and keys.
func GetDefaultRegionSplitConfig() (splitSize, splitKeys int64) {
	if kerneltype.IsNextGen() {
		const nextGenRegionSplitSize = units.GiB
		// the keys:size ratio is 10 times larger than Classic, the reason seems
		// that nextgen TiKV want to control region split through size only, so
		// they set a very large keys limit.
		const nextGenRegionSplitKeys = 102_400_000
		return nextGenRegionSplitSize, nextGenRegionSplitKeys
	}
	return defRegionSplitSize, defRegionSplitKeys
}

// GetTargetScope get target scope for new tasks.
// in classical kernel, the target scope the new task is the service scope of the
// TiDB instance that user is currently connecting to.
// in nextgen kernel, it's always NextGenTargetScope.
func GetTargetScope() string {
	if kerneltype.IsNextGen() {
		return NextGenTargetScope
	}
	return vardef.ServiceScope.Load()
}

// GetCloudStorageURI returns the cloud storage URI with cluster ID appended to the path.
func GetCloudStorageURI(ctx context.Context, store kv.Storage) string {
	cloudURI := vardef.CloudStorageURI.Load()
	if s, ok := store.(kv.StorageWithPD); ok {
		// When setting the cloudURI value by SQL, we already checked the effectiveness, so we don't need to check it again here.
		u, _ := extstorage.ParseRawURL(cloudURI)
		if len(u.Path) != 0 {
			u.Path = path.Join(u.Path, strconv.FormatUint(s.GetPDClient().GetClusterID(ctx), 10))
			return u.String()
		}
	} else {
		logutil.BgLogger().Warn("Can't get cluster id from store, use default cloud storage uri")
	}
	return cloudURI
}

// UpdatePauseScaleInFlag updates the pause scale-in flag.
func UpdatePauseScaleInFlag(ctx context.Context, flag *schstatus.TTLFlag) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	manager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(flag)
	if err != nil {
		return errors.Trace(err)
	}
	return manager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		_, err2 := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`REPLACE INTO mysql.tidb(variable_name, variable_value) VALUES(%?, %?)`,
			tidbvar.DXFSchedulePauseScaleIn, string(bytes))
		return err2
	})
}

// GetScheduleTuneFactors gets the schedule tune factors for a keyspace.
// if not set or expired, it returns the default tune factors.
func GetScheduleTuneFactors(ctx context.Context, keyspace string) (*schstatus.TuneFactors, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, err
	}
	var factors *schstatus.TTLTuneFactors
	if err = mgr.WithNewSession(func(se sessionctx.Context) error {
		return kv.RunInNewTxn(ctx, se.GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
			mutator := meta.NewMutator(txn)
			var err2 error
			factors, err2 = mutator.GetDXFScheduleTuneFactors(keyspace)
			if err2 != nil {
				return err2
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	if factors == nil || factors.ExpireTime.Before(time.Now()) {
		return schstatus.GetDefaultTuneFactors(), nil
	}
	return &factors.TuneFactors, nil
}

// NewObjStoreWithRecording creates an object storage for global sort with
// request recording.
func NewObjStoreWithRecording(ctx context.Context, uri string) (*recording.AccessStats, extstorage.ExternalStorage, error) {
	rec := &recording.AccessStats{}
	store, err := newObjStore(ctx, uri, &extstorage.ExternalStorageOptions{
		AccessRecording: rec,
	})
	if err != nil {
		return nil, nil, err
	}
	return rec, store, nil
}

// NewObjStore creates an object storage for global sort.
func NewObjStore(ctx context.Context, uri string) (extstorage.ExternalStorage, error) {
	return newObjStore(ctx, uri, nil)
}

func newObjStore(ctx context.Context, uri string, opts *extstorage.ExternalStorageOptions) (extstorage.ExternalStorage, error) {
	storeBackend, err := extstorage.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	store, err := extstorage.New(ctx, storeBackend, opts)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// SendRowAndSizeMeterData sends the row count and size metering data.
func SendRowAndSizeMeterData(ctx context.Context, task *proto.Task, rows int64,
	dataKVSize, indexKVSize int64, logger *zap.Logger) (err error) {
	inLogger := log.BeginTask(logger, "send size and row metering data")
	// in case of network errors, write might success, but return error, and we
	// will retry sending metering data in next cleanup, to avoid duplicated data,
	// we always use the task's last update time as the metering time and let the
	// SDK overwrite existing file.
	ts := task.StateUpdateTime.Truncate(time.Minute).Unix()
	item := metering.GetBaseMeterItem(task.ID, task.Keyspace, task.Type.String())
	item[metering.RowCountField] = rows
	// add-index tasks don't have data kv size.
	if dataKVSize > 0 {
		item[metering.DataKVBytesField] = dataKVSize
	}
	item[metering.IndexKVBytesField] = indexKVSize
	// below 3 fields are for better analysis of the cost of the task.
	item[metering.ConcurrencyField] = task.Concurrency
	item[metering.MaxNodeCountField] = task.MaxNodeCount
	item[metering.DurationSecondsField] = int64(task.StateUpdateTime.Sub(task.CreateTime).Seconds())
	defer func() {
		inLogger.End(zap.InfoLevel, err, zap.Any("data", item))
	}()
	// same as above reason, we use the task ID as the uuid, and we also need to
	// send different file as the metering service itself to avoid overwrite.
	if err = metering.WriteMeterData(ctx, ts, fmt.Sprintf("%s_%d", task.Type, task.ID), []map[string]any{item}); err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterSendRowAndSizeMeterData", item)
	return nil
}

func init() {
	// domain will init this var at runtime, we store it here for test, as some
	// test might not start domain.
	nodeResource.Store(proto.NewNodeResource(8, 16*units.GiB, 100*units.GiB))
}
