// Copyright 2021 PingCAP, Inc.
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

package domain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	domain_metrics "github.com/pingcap/tidb/pkg/domain/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// dumpFileGcChecker is used to gc dump file in circle
// For now it is used by `plan replayer` and `trace plan` statement
type dumpFileGcChecker struct {
	sync.Mutex
	gcLease                time.Duration
	paths                  []string
	sctx                   sessionctx.Context
	planReplayerTaskStatus *planReplayerDumpTaskStatus
}

func parseTime(s string) (time.Time, error) {
	startIdx := strings.LastIndex(s, "_")
	if startIdx == -1 {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	endIdx := strings.LastIndex(s, ".")
	if endIdx == -1 || endIdx <= startIdx+1 {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	i, err := strconv.ParseInt(s[startIdx+1:endIdx], 10, 64)
	if err != nil {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	return time.Unix(0, i), nil
}

// GCDumpFiles periodically cleans the outdated files for plan replayer and plan trace.
func (p *dumpFileGcChecker) GCDumpFiles(gcDurationDefault, gcDurationForCapture time.Duration) {
	p.Lock()
	defer p.Unlock()
	for _, path := range p.paths {
		p.gcDumpFilesByPath(path, gcDurationDefault, gcDurationForCapture)
	}
}

func (p *dumpFileGcChecker) setupSctx(sctx sessionctx.Context) {
	p.sctx = sctx
}

func (p *dumpFileGcChecker) gcDumpFilesByPath(path string, gcDurationDefault, gcDurationForCapture time.Duration) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if !os.IsNotExist(err) {
			absPath, err2 := filepath.Abs(path)
			if err2 != nil {
				logutil.BgLogger().Warn("failed to get absolute path",
					zap.Error(err2), zap.String("category", "dumpFileGcChecker"))
				absPath = path
			}
			logutil.BgLogger().Warn("open plan replayer directory failed",
				zap.Error(err), zap.String("category", "dumpFileGcChecker"),
				zap.String("path", absPath))
		}
	}

	gcTargetTimeDefault := time.Now().Add(-gcDurationDefault)
	gcTargetTimeForCapture := time.Now().Add(-gcDurationForCapture)
	for _, entry := range entries {
		f, err := entry.Info()
		if err != nil {
			logutil.BgLogger().Warn("open plan replayer directory failed", zap.String("category", "dumpFileGcChecker"), zap.Error(err))
		}
		fileName := f.Name()
		createTime, err := parseTime(fileName)
		if err != nil {
			logutil.BgLogger().Error("parseTime failed", zap.String("category", "dumpFileGcChecker"), zap.Error(err), zap.String("filename", fileName))
			continue
		}
		isPlanReplayer := strings.Contains(fileName, "replayer")
		isPlanReplayerCapture := strings.Contains(fileName, "capture")
		canGC := false
		if isPlanReplayer && isPlanReplayerCapture {
			canGC = !createTime.After(gcTargetTimeForCapture)
		} else {
			canGC = !createTime.After(gcTargetTimeDefault)
		}
		if canGC {
			err := os.Remove(filepath.Join(path, f.Name()))
			if err != nil {
				logutil.BgLogger().Warn("remove file failed", zap.String("category", "dumpFileGcChecker"), zap.Error(err), zap.String("filename", fileName))
				continue
			}
			logutil.BgLogger().Info("dumpFileGcChecker successful", zap.String("filename", fileName))
			if isPlanReplayer && p.sctx != nil {
				deletePlanReplayerStatus(context.Background(), p.sctx, fileName)
				p.planReplayerTaskStatus.clearFinishedTask()
			}
		}
	}
}

func deletePlanReplayerStatus(ctx context.Context, sctx sessionctx.Context, token string) {
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	exec := sctx.GetRestrictedSQLExecutor()
	_, _, err := exec.ExecRestrictedSQL(ctx1, nil, "delete from mysql.plan_replayer_status where token = %?", token)
	if err != nil {
		logutil.BgLogger().Warn("delete mysql.plan_replayer_status record failed", zap.String("token", token), zap.Error(err))
	}
}

// insertPlanReplayerStatus insert mysql.plan_replayer_status record
func insertPlanReplayerStatus(ctx context.Context, sctx sessionctx.Context, records []PlanReplayerStatusRecord) {
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	var instance string
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Error("failed to get server info", zap.Error(err))
		instance = "unknown"
	} else {
		instance = fmt.Sprintf("%s:%d", serverInfo.IP, serverInfo.Port)
	}
	for _, record := range records {
		if len(record.FailedReason) > 0 {
			insertPlanReplayerErrorStatusRecord(ctx1, sctx, instance, record)
		} else {
			insertPlanReplayerSuccessStatusRecord(ctx1, sctx, instance, record)
		}
	}
}

func insertPlanReplayerErrorStatusRecord(ctx context.Context, sctx sessionctx.Context, instance string, record PlanReplayerStatusRecord) {
	exec := sctx.GetRestrictedSQLExecutor()
	_, _, err := exec.ExecRestrictedSQL(
		ctx, nil,
		"insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, fail_reason, instance) values (%?,%?,%?,%?,%?)",
		record.SQLDigest, record.PlanDigest, record.OriginSQL, record.FailedReason, instance,
	)
	if err != nil {
		logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
			zap.String("sqlDigest", record.SQLDigest),
			zap.String("planDigest", record.PlanDigest),
			zap.String("sql", record.OriginSQL),
			zap.String("failReason", record.FailedReason),
			zap.String("instance", instance),
			zap.Error(err))
	}
}

func insertPlanReplayerSuccessStatusRecord(ctx context.Context, sctx sessionctx.Context, instance string, record PlanReplayerStatusRecord) {
	exec := sctx.GetRestrictedSQLExecutor()
	_, _, err := exec.ExecRestrictedSQL(
		ctx,
		nil,
		"insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, token, instance) values (%?,%?,%?,%?,%?)",
		record.SQLDigest, record.PlanDigest, record.OriginSQL, record.Token, instance,
	)
	if err != nil {
		logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
			zap.String("sqlDigest", record.SQLDigest),
			zap.String("planDigest", record.PlanDigest),
			zap.String("sql", record.OriginSQL),
			zap.String("token", record.Token),
			zap.String("instance", instance),
			zap.Error(err),
		)
		// try insert record without original sql
		_, _, err = exec.ExecRestrictedSQL(
			ctx,
			nil,
			"insert into mysql.plan_replayer_status (sql_digest, plan_digest, token, instance) values (%?,%?,%?,%?)",
			record.SQLDigest, record.PlanDigest, record.Token, instance,
		)
		if err != nil {
			logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
				zap.String("sqlDigest", record.SQLDigest),
				zap.String("planDigest", record.PlanDigest),
				zap.String("token", record.Token),
				zap.String("instance", instance),
				zap.Error(err),
			)
		}
	}
}

type planReplayerHandle struct {
	*planReplayerTaskCollectorHandle
	*planReplayerTaskDumpHandle
}

// SendTask send dumpTask in background task handler
func (h *planReplayerHandle) SendTask(task *PlanReplayerDumpTask) bool {
	select {
	case h.planReplayerTaskDumpHandle.taskCH <- task:
		// we directly remove the task key if we put task in channel successfully, if the task was failed to dump,
		// the task handle will re-add the task in next loop
		if !task.IsContinuesCapture {
			h.planReplayerTaskCollectorHandle.removeTask(task.PlanReplayerTaskKey)
		}
		domain_metrics.PlanReplayerCaptureTaskSendCounter.Inc()
		return true
	default:
		domain_metrics.PlanReplayerCaptureTaskDiscardCounter.Inc()
		// directly discard the task if the task channel is full in order not to block the query process
		logutil.BgLogger().Warn("discard one plan replayer dump task",
			zap.String("sql-digest", task.SQLDigest), zap.String("plan-digest", task.PlanDigest))
		return false
	}
}

type planReplayerTaskCollectorHandle struct {
	taskMu struct {
		sync.RWMutex
		tasks map[replayer.PlanReplayerTaskKey]struct{}
	}
	ctx  context.Context
	sctx sessionctx.Context
}

// CollectPlanReplayerTask collects all unhandled plan replayer task
func (h *planReplayerTaskCollectorHandle) CollectPlanReplayerTask() error {
	allKeys, err := h.collectAllPlanReplayerTask(h.ctx)
	if err != nil {
		return err
	}
	tasks := make([]replayer.PlanReplayerTaskKey, 0)
	for _, key := range allKeys {
		unhandled, err := checkUnHandledReplayerTask(h.ctx, h.sctx, key)
		if err != nil {
			logutil.BgLogger().Warn("collect plan replayer task failed", zap.String("category", "plan-replayer-task"), zap.Error(err))
			return err
		}
		if unhandled {
			logutil.BgLogger().Debug("collect plan replayer task success", zap.String("category", "plan-replayer-task"),
				zap.String("sql-digest", key.SQLDigest),
				zap.String("plan-digest", key.PlanDigest))
			tasks = append(tasks, key)
		}
	}
	h.setupTasks(tasks)
	domain_metrics.PlanReplayerRegisterTaskGauge.Set(float64(len(tasks)))
	return nil
}

// GetTasks get all tasks
func (h *planReplayerTaskCollectorHandle) GetTasks() []replayer.PlanReplayerTaskKey {
	tasks := make([]replayer.PlanReplayerTaskKey, 0)
	h.taskMu.RLock()
	defer h.taskMu.RUnlock()
	for taskKey := range h.taskMu.tasks {
		tasks = append(tasks, taskKey)
	}
	return tasks
}

func (h *planReplayerTaskCollectorHandle) setupTasks(tasks []replayer.PlanReplayerTaskKey) {
	r := make(map[replayer.PlanReplayerTaskKey]struct{})
	for _, task := range tasks {
		r[task] = struct{}{}
	}
	h.taskMu.Lock()
	defer h.taskMu.Unlock()
	h.taskMu.tasks = r
}

func (h *planReplayerTaskCollectorHandle) removeTask(taskKey replayer.PlanReplayerTaskKey) {
	h.taskMu.Lock()
	defer h.taskMu.Unlock()
	delete(h.taskMu.tasks, taskKey)
}

func (h *planReplayerTaskCollectorHandle) collectAllPlanReplayerTask(ctx context.Context) ([]replayer.PlanReplayerTaskKey, error) {
	exec := h.sctx.GetSQLExecutor()
	rs, err := exec.ExecuteInternal(ctx, "select sql_digest, plan_digest from mysql.plan_replayer_task")
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	allKeys := make([]replayer.PlanReplayerTaskKey, 0, len(rows))
	for _, row := range rows {
		sqlDigest, planDigest := row.GetString(0), row.GetString(1)
		allKeys = append(allKeys, replayer.PlanReplayerTaskKey{
			SQLDigest:  sqlDigest,
			PlanDigest: planDigest,
		})
	}
	return allKeys, nil
}

type planReplayerDumpTaskStatus struct {
	// running task records the task running by all workers in order to avoid multi workers running the same task key
	runningTaskMu struct {
		sync.RWMutex
		runningTasks map[replayer.PlanReplayerTaskKey]struct{}
	}

	// finished task records the finished task in order to avoid running finished task key
	finishedTaskMu struct {
		sync.RWMutex
		finishedTask map[replayer.PlanReplayerTaskKey]struct{}
	}
}

// GetRunningTaskStatusLen used for unit test
func (r *planReplayerDumpTaskStatus) GetRunningTaskStatusLen() int {
	r.runningTaskMu.RLock()
	defer r.runningTaskMu.RUnlock()
	return len(r.runningTaskMu.runningTasks)
}

// CleanFinishedTaskStatus clean then finished tasks, only used for unit test
func (r *planReplayerDumpTaskStatus) CleanFinishedTaskStatus() {
	r.finishedTaskMu.Lock()
	defer r.finishedTaskMu.Unlock()
	r.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
}

// GetFinishedTaskStatusLen used for unit test
func (r *planReplayerDumpTaskStatus) GetFinishedTaskStatusLen() int {
	r.finishedTaskMu.RLock()
	defer r.finishedTaskMu.RUnlock()
	return len(r.finishedTaskMu.finishedTask)
}

func (r *planReplayerDumpTaskStatus) occupyRunningTaskKey(task *PlanReplayerDumpTask) bool {
	r.runningTaskMu.Lock()
	defer r.runningTaskMu.Unlock()
	_, ok := r.runningTaskMu.runningTasks[task.PlanReplayerTaskKey]
	if ok {
		return false
	}
	r.runningTaskMu.runningTasks[task.PlanReplayerTaskKey] = struct{}{}
	return true
}

func (r *planReplayerDumpTaskStatus) releaseRunningTaskKey(task *PlanReplayerDumpTask) {
	r.runningTaskMu.Lock()
	defer r.runningTaskMu.Unlock()
	delete(r.runningTaskMu.runningTasks, task.PlanReplayerTaskKey)
}

func (r *planReplayerDumpTaskStatus) checkTaskKeyFinishedBefore(task *PlanReplayerDumpTask) bool {
	r.finishedTaskMu.RLock()
	defer r.finishedTaskMu.RUnlock()
	_, ok := r.finishedTaskMu.finishedTask[task.PlanReplayerTaskKey]
	return ok
}

func (r *planReplayerDumpTaskStatus) setTaskFinished(task *PlanReplayerDumpTask) {
	r.finishedTaskMu.Lock()
	defer r.finishedTaskMu.Unlock()
	r.finishedTaskMu.finishedTask[task.PlanReplayerTaskKey] = struct{}{}
}

func (r *planReplayerDumpTaskStatus) clearFinishedTask() {
	r.finishedTaskMu.Lock()
	defer r.finishedTaskMu.Unlock()
	r.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
}

type planReplayerTaskDumpWorker struct {
	ctx    context.Context
	sctx   sessionctx.Context
	taskCH <-chan *PlanReplayerDumpTask
	status *planReplayerDumpTaskStatus
}

func (w *planReplayerTaskDumpWorker) run() {
	logutil.BgLogger().Info("planReplayerTaskDumpWorker started.")
	for task := range w.taskCH {
		w.handleTask(task)
	}
	logutil.BgLogger().Info("planReplayerTaskDumpWorker exited.")
}

func (w *planReplayerTaskDumpWorker) handleTask(task *PlanReplayerDumpTask) {
	sqlDigest := task.SQLDigest
	planDigest := task.PlanDigest
	check := true
	occupy := true
	handleTask := true
	defer func() {
		logutil.BgLogger().Debug("handle task", zap.String("category", "plan-replayer-capture"),
			zap.String("sql-digest", sqlDigest),
			zap.String("plan-digest", planDigest),
			zap.Bool("check", check),
			zap.Bool("occupy", occupy),
			zap.Bool("handle", handleTask))
	}()
	defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpWorker", nil, false)

	if task.IsContinuesCapture {
		if w.status.checkTaskKeyFinishedBefore(task) {
			check = false
			return
		}
	}
	occupy = w.status.occupyRunningTaskKey(task)
	if !occupy {
		return
	}
	handleTask = w.HandleTask(task)
	w.status.releaseRunningTaskKey(task)
}

// HandleTask handled task
func (w *planReplayerTaskDumpWorker) HandleTask(task *PlanReplayerDumpTask) (success bool) {
	defer func() {
		if success && task.IsContinuesCapture {
			w.status.setTaskFinished(task)
		}
	}()
	taskKey := task.PlanReplayerTaskKey
	unhandled, err := checkUnHandledReplayerTask(w.ctx, w.sctx, taskKey)
	if err != nil {
		logutil.BgLogger().Warn("check task failed", zap.String("category", "plan-replayer-capture"),
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return false
	}
	// the task is processed, thus we directly skip it.
	if !unhandled {
		return true
	}

	file, fileName, err := replayer.GeneratePlanReplayerFile(task.IsCapture, task.IsContinuesCapture, variable.EnableHistoricalStatsForCapture.Load())
	if err != nil {
		logutil.BgLogger().Warn("generate task file failed", zap.String("category", "plan-replayer-capture"),
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return false
	}
	task.Zf = file
	task.FileName = fileName
	err = DumpPlanReplayerInfo(w.ctx, w.sctx, task)
	if err != nil {
		logutil.BgLogger().Warn("dump task result failed", zap.String("category", "plan-replayer-capture"),
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return false
	}
	return true
}

type planReplayerTaskDumpHandle struct {
	taskCH  chan *PlanReplayerDumpTask
	status  *planReplayerDumpTaskStatus
	workers []*planReplayerTaskDumpWorker
}

// GetTaskStatus used for test
func (h *planReplayerTaskDumpHandle) GetTaskStatus() *planReplayerDumpTaskStatus {
	return h.status
}

// GetWorker used for test
func (h *planReplayerTaskDumpHandle) GetWorker() *planReplayerTaskDumpWorker {
	return h.workers[0]
}

// Close make finished flag true
func (h *planReplayerTaskDumpHandle) Close() {
	close(h.taskCH)
}

// DrainTask drain a task for unit test
func (h *planReplayerTaskDumpHandle) DrainTask() *PlanReplayerDumpTask {
	return <-h.taskCH
}

func checkUnHandledReplayerTask(ctx context.Context, sctx sessionctx.Context, task replayer.PlanReplayerTaskKey) (bool, error) {
	exec := sctx.GetSQLExecutor()
	rs, err := exec.ExecuteInternal(ctx, fmt.Sprintf("select * from mysql.plan_replayer_status where sql_digest = '%v' and plan_digest = '%v' and fail_reason is null", task.SQLDigest, task.PlanDigest))
	if err != nil {
		return false, err
	}
	if rs == nil {
		return true, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) > 0 {
		return false, nil
	}
	return true, nil
}

// CheckPlanReplayerTaskExists checks whether plan replayer capture task exists already
func CheckPlanReplayerTaskExists(ctx context.Context, sctx sessionctx.Context, sqlDigest, planDigest string) (bool, error) {
	exec := sctx.GetSQLExecutor()
	rs, err := exec.ExecuteInternal(ctx, fmt.Sprintf("select * from mysql.plan_replayer_task where sql_digest = '%v' and plan_digest = '%v'",
		sqlDigest, planDigest))
	if err != nil {
		return false, err
	}
	if rs == nil {
		return false, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) > 0 {
		return true, nil
	}
	return false, nil
}

// PlanReplayerStatusRecord indicates record in mysql.plan_replayer_status
type PlanReplayerStatusRecord struct {
	SQLDigest    string
	PlanDigest   string
	OriginSQL    string
	Token        string
	FailedReason string
}

// PlanReplayerDumpTask wrap the params for plan replayer dump
type PlanReplayerDumpTask struct {
	replayer.PlanReplayerTaskKey

	// tmp variables stored during the query
	TblStats map[int64]any

	// variables used to dump the plan
	StartTS           uint64
	SessionBindings   []bindinfo.Bindings
	EncodedPlan       string
	SessionVars       *variable.SessionVars
	ExecStmts         []ast.StmtNode
	Analyze           bool
	HistoricalStatsTS uint64
	DebugTrace        []any

	FileName string
	Zf       *os.File

	// IsCapture indicates whether the task is from capture
	IsCapture bool
	// IsContinuesCapture indicates whether the task is from continues capture
	IsContinuesCapture bool
}
