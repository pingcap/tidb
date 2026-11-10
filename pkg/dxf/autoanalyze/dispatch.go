// Copyright 2026 PingCAP, Inc.
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

package autoanalyze

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	// cancelRetryInterval is how long to wait before trying to cancel a task again.
	cancelRetryInterval = time.Second
	// cancelWaitTimeout limits the wait of a killed statement for its task, which stops within
	// seconds. The wait must end even if this node cannot reach the storage to read the task.
	cancelWaitTimeout = 10 * time.Second
)

// Register registers the auto-analyze task type to DXF. NewDispatcher creates the dispatcher that
// submits the tasks.
func Register() {
	scheduler.RegisterSchedulerFactory(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return newScheduler(ctx, task, param)
		},
	)
	taskexecutor.RegisterTaskType(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return newTaskExecutor(ctx, task, param)
		},
	)
}

// Dispatcher runs the auto-analyze statements of a stats handle as DXF tasks.
type Dispatcher struct {
	statsCache     statstypes.StatsCacheLoader
	procIDs        statsutil.AutoAnalyzeProcIDGenerator
	sysProcTracker sysproctrack.Tracker
	shuttingDown   func() bool
}

// NewDispatcher creates a Dispatcher. It waits for each task as an auto-analyze process with an ID
// from procIDs in sysProcTracker, and after a task succeeds, it loads the new stats of the table
// into statsCache. When shuttingDown reports true, it stops waiting and leaves the task running.
func NewDispatcher(
	statsCache statstypes.StatsCacheLoader,
	procIDs statsutil.AutoAnalyzeProcIDGenerator,
	sysProcTracker sysproctrack.Tracker,
	shuttingDown func() bool,
) *Dispatcher {
	return &Dispatcher{statsCache: statsCache, procIDs: procIDs, sysProcTracker: sysProcTracker, shuttingDown: shuttingDown}
}

// Dispatch implements exec.Dispatcher.
func (d *Dispatcher) Dispatch(
	sctx sessionctx.Context,
	table exec.AnalyzedTable,
	sql string,
	params ...any,
) error {
	tblInfo, ok := sctx.GetLatestInfoSchema().TableInfoByID(table.ID)
	if !ok {
		return errors.Errorf("table %d does not exist", table.ID)
	}
	taskMeta, err := newTaskMeta(tblInfo, sql, params)
	if err != nil {
		return err
	}
	return runAsTask(sctx, d.statsCache, d.procIDs, d.sysProcTracker, d.shuttingDown, taskMeta)
}

// runAsTask runs the statement as a DXF task and waits for it as an auto-analyze process,
// so that the slot of tidb_auto_analyze_concurrency stays taken and a kill reaches the task.
func runAsTask(
	sctx sessionctx.Context,
	statsCache statstypes.StatsCacheLoader,
	procIDs statsutil.AutoAnalyzeProcIDGenerator,
	sysProcTracker sysproctrack.Tracker,
	shuttingDown func() bool,
	taskMeta *TaskMeta,
) error {
	taskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		// DXF is not ready on this node yet. The priority queue analyzes the table again later.
		return errors.Trace(err)
	}
	meta, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	sessVars := sctx.GetSessionVars()
	procID := procIDs.AutoAnalyzeProcID()
	defer procIDs.ReleaseAutoAnalyzeProcID(procID)
	// It runs before the session shows as a process, so nothing of the last
	// statement shows even for a moment.
	forgetLastStatement(sctx)
	tracker := statsutil.NewAutoAnalyzeTracker(sysProcTracker.Track, sysProcTracker.UnTrack)
	if err := tracker.Track(procID, sctx); err != nil {
		return errors.Trace(err)
	}
	// Untracking also resets the killer, so no kill signal is left to the next
	// user of the session.
	defer tracker.UnTrack(procID)
	// Tracking resets the killer, so the channel must be taken after it.
	killed := sessVars.SQLKiller.GetKillEventChan()
	defer showAsRunning(sctx, taskMeta)()

	keyspace := sctx.GetStore().GetKeyspace()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	if kv.IsUserKS(sctx.GetStore()) {
		// A DXF service node cannot ask the nodes of this keyspace to flush their
		// stats deltas, so flush them before submitting, as ANALYZE does.
		if err := flushStatsDelta(sctx, taskMeta); err != nil {
			return errors.Annotate(err, "flush stats delta before dispatching auto analyze")
		}
	}
	// The task keys of a table share a prefix, by which activeTask finds them,
	// and end in a UUID: DXF refuses a key that an earlier task has used.
	keyPrefix := fmt.Sprintf("%s/%s/%d/", keyspace, proto.AutoAnalyze, taskMeta.TableID)
	start := time.Now()
	// A stats owner that shut down or failed can have left a task that runs
	// this statement. If that task succeeds, the statement is done.
	task, err := activeTask(ctx, taskMgr, keyPrefix, taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterFindActiveTask")
	adopted := task != nil
	if adopted {
		adopted = waitTask(ctx, task, killed, shuttingDown) == nil
	}
	if !adopted {
		// A kill up to here ends the statement before it has a task of its own.
		if killErr := sessVars.SQLKiller.HandleSignal(); killErr != nil {
			return killErr
		}
		task, err = submitTask(ctx, keyPrefix+uuid.NewString(), keyspace, meta)
		if err != nil {
			return err
		}
		err = waitTask(ctx, task, killed, shuttingDown)
	}
	if err == nil {
		// Refresh the dispatching node's cache before returning success. The priority
		// queue removes the running-job entry when the auto-analyze job completes.
		// Without this refresh, the queue can schedule another ANALYZE using stale
		// stats before the background loader updates the cache.
		err = loadTableStats(ctx, sctx, statsCache, taskMeta.TableID)
	}
	if killErr := sessVars.SQLKiller.HandleSignal(); killErr != nil {
		err = killErr
	}
	statslogutil.StatsLogger().Info("auto analyze dispatched through DXF",
		zap.Int64("taskID", task.ID), zap.String("taskKey", task.Key), zap.Bool("adopted", adopted),
		zap.String("sql", taskMeta.SQL), zap.Strings("params", taskMeta.Params),
		zap.Duration("cost", time.Since(start)), zap.Error(err))
	return err
}

// flushStatsDelta makes the TiDB nodes of this keyspace flush their stats deltas of the table.
func flushStatsDelta(sctx sessionctx.Context, meta *TaskMeta) error {
	is := sctx.GetLatestInfoSchema()
	dbInfo, dbExists := is.SchemaByID(meta.DBID)
	tblInfo, tblExists := is.TableInfoByID(meta.TableID)
	if !dbExists || !tblExists {
		return errors.Errorf("table %d does not exist", meta.TableID)
	}
	return executor.FlushTableStatsDeltaForAnalyze(statsutil.StatsCtx, sctx, dbInfo.Name, tblInfo)
}

// loadTableStats loads the stats of the table and its partitions from the
// storage into statsCache.
func loadTableStats(ctx context.Context, sctx sessionctx.Context, statsCache statstypes.StatsCacheLoader, tableID int64) error {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tblInfo, ok := is.TableInfoByID(tableID)
	if !ok {
		// Nothing to load for a table that is gone.
		return nil
	}
	ids := []int64{tblInfo.ID}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
	}
	return errors.Trace(statsCache.Update(ctx, is, ids...))
}

// processInfoSetter is implemented by the sessions that show what they run.
type processInfoSetter interface {
	SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64)
	ClearProcessInfo()
}

// forgetLastStatement drops the statement context and the process info of the last statement
// of the session: the runaway checker in them would kill the process that waits for a task.
func forgetLastStatement(sctx sessionctx.Context) {
	sessVars := sctx.GetSessionVars()
	sessVars.StmtCtx = sessVars.InitStatementContext()
	if setter, ok := sctx.(processInfoSetter); ok {
		setter.ClearProcessInfo()
	}
}

// showAsRunning shows the statement as the running query of the session until done
// is called, so it is visible here and tidb_max_auto_analyze_time applies to it.
func showAsRunning(sctx sessionctx.Context, meta *TaskMeta) (done func()) {
	setter, ok := sctx.(processInfoSetter)
	if !ok {
		return func() {}
	}
	sql, err := sqlescape.EscapeSQL(meta.SQL, meta.args()...)
	if err != nil {
		sql = meta.SQL
	}
	// An internal session has no command, so its statements show as ComSleep. Another command
	// would show the start TS of the last transaction of the session, and hold back GC.
	setter.SetProcessInfo(sql, time.Now(), mysql.ComSleep, 0)
	return func() {
		setter.SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	}
}

// newTaskMeta builds the task meta of the statement. The meta holds identifiers only.
func newTaskMeta(tblInfo *model.TableInfo, sql string, params []any) (*TaskMeta, error) {
	meta := &TaskMeta{
		SQL:     sql,
		Params:  make([]string, 0, len(params)),
		DBID:    tblInfo.DBID,
		TableID: tblInfo.ID,
	}
	for _, p := range params {
		s, ok := p.(string)
		if !ok {
			return nil, errors.Errorf("unexpected parameter %v of the auto analyze statement", p)
		}
		meta.Params = append(meta.Params, s)
	}
	return meta, nil
}

// activeTask returns a pending or running task that runs the statement of meta, or nil. It
// compares the statements, because the tasks of a table can run different ones at a time.
func activeTask(ctx context.Context, taskMgr *storage.TaskManager, keyPrefix string, meta *TaskMeta) (*proto.TaskBase, error) {
	candidates, err := taskMgr.GetTaskBasesInStates(ctx, proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if !strings.HasPrefix(candidate.Key, keyPrefix) {
			continue
		}
		task, err := taskMgr.GetTaskByID(ctx, candidate.ID)
		if goerrors.Is(err, storage.ErrTaskNotFound) {
			// The task is done meanwhile, and has moved to the history.
			continue
		}
		if err != nil {
			return nil, err
		}
		if meta.sameStatement(task.Meta) {
			return candidate, nil
		}
	}
	return nil, nil
}

// submitTask submits the statement as a task that runs on one node.
func submitTask(ctx context.Context, taskKey, keyspace string, meta []byte) (*proto.TaskBase, error) {
	// The task asks for one slot whatever the build concurrency of its statement
	// is: the scan and the sampling of ANALYZE, most of its work, run in TiKV.
	task, err := handle.SubmitTask(ctx, taskKey, proto.AutoAnalyze, keyspace, 1, targetScope(), 1, meta)
	if err != nil {
		// The task may have been created, do not leave it running unobserved.
		if cancelErr := handle.CancelTask(ctx, taskKey); cancelErr != nil {
			statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(cancelErr))
		}
		return nil, errors.Trace(err)
	}
	return &task.TaskBase, nil
}

// targetScope returns the scope of the nodes that run the tasks. On the classic kernel it is empty:
// DXF then prefers the background nodes, and the scope of the stats owner does not limit the tasks.
func targetScope() string {
	if kerneltype.IsNextGen() {
		return handle.NextGenTargetScope
	}
	return ""
}

// waitTask waits until the task succeeds. A kill also cancels the task and waits for its end,
// at most cancelWaitTimeout. A shutdown of this node only ends the wait: the task runs on.
func waitTask(ctx context.Context, task *proto.TaskBase, killed <-chan struct{}, shuttingDown func() bool) error {
	waitCtx, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-killed:
		case <-waitCtx.Done():
			return
		}
		if shuttingDown() {
			// If this node runs the subtask, the shutdown also kills the
			// statement, and the task fails.
			stopWaiting()
			return
		}
		timer := time.AfterFunc(cancelWaitTimeout, stopWaiting)
		defer timer.Stop()
		cancelTask(waitCtx, task.Key)
		<-waitCtx.Done()
	}()
	found, err := handle.WaitTaskDoneOrPausedWithResult(waitCtx, task.ID)
	stopWaiting()
	wg.Wait()
	if err != nil {
		return errors.Trace(err)
	}
	if found.State != proto.TaskStateSucceed {
		return errors.Errorf("auto analyze task %d stopped with state %s", task.ID, found.State)
	}
	return nil
}

// cancelTask cancels the task, and tries again on failure until ctx is done.
func cancelTask(ctx context.Context, taskKey string) {
	for {
		err := handle.CancelTask(ctx, taskKey)
		if err == nil {
			return
		}
		statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(cancelRetryInterval):
		}
	}
}
