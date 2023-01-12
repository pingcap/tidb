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
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// dumpFileGcChecker is used to gc dump file in circle
// For now it is used by `plan replayer` and `trace plan` statement
type dumpFileGcChecker struct {
	sync.Mutex
	gcLease time.Duration
	paths   []string
	sctx    sessionctx.Context
}

// GetPlanReplayerDirName returns plan replayer directory path.
// The path is related to the process id.
func GetPlanReplayerDirName() string {
	tidbLogDir := filepath.Dir(config.GetGlobalConfig().Log.File.Filename)
	return filepath.Join(tidbLogDir, "replayer")
}

func parseType(s string) string {
	return strings.Split(s, "_")[0]
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

func (p *dumpFileGcChecker) gcDumpFiles(t time.Duration) {
	p.Lock()
	defer p.Unlock()
	for _, path := range p.paths {
		p.gcDumpFilesByPath(path, t)
	}
}

func (p *dumpFileGcChecker) setupSctx(sctx sessionctx.Context) {
	p.sctx = sctx
}

func (p *dumpFileGcChecker) gcDumpFilesByPath(path string, t time.Duration) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Warn("[dumpFileGcChecker] open plan replayer directory failed", zap.Error(err))
		}
	}

	gcTime := time.Now().Add(-t)
	for _, f := range files {
		fileName := f.Name()
		createTime, err := parseTime(fileName)
		if err != nil {
			logutil.BgLogger().Error("[dumpFileGcChecker] parseTime failed", zap.Error(err), zap.String("filename", fileName))
			continue
		}
		isPlanReplayer := parseType(fileName) == "replayer"
		if !createTime.After(gcTime) {
			err := os.Remove(filepath.Join(path, f.Name()))
			if err != nil {
				logutil.BgLogger().Warn("[dumpFileGcChecker] remove file failed", zap.Error(err), zap.String("filename", fileName))
				continue
			}
			logutil.BgLogger().Info("dumpFileGcChecker successful", zap.String("filename", fileName))
			if isPlanReplayer && p.sctx != nil {
				deletePlanReplayerStatus(context.Background(), p.sctx, fileName)
			}
		}
	}
}

type planReplayerHandle struct {
	*planReplayerTaskCollectorHandle
	*planReplayerTaskDumpHandle
}

// HandlePlanReplayerDumpTask handle dump task
func (h *planReplayerHandle) HandlePlanReplayerDumpTask(task *PlanReplayerDumpTask) bool {
	success := h.dumpPlanReplayerDumpTask(task)
	if success {
		h.removeTask(task.PlanReplayerTaskKey)
	}
	return success
}

type planReplayerTaskCollectorHandle struct {
	taskMu struct {
		sync.RWMutex
		tasks map[PlanReplayerTaskKey]struct{}
	}
	ctx  context.Context
	sctx sessionctx.Context
}

func deletePlanReplayerStatus(ctx context.Context, sctx sessionctx.Context, token string) {
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	exec := sctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx1, fmt.Sprintf("delete from mysql.plan_replayer_status where token = %v", token))
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
	exec := sctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, fmt.Sprintf(
		"insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, fail_reason, instance) values ('%s','%s','%s','%s','%s')",
		record.SQLDigest, record.PlanDigest, record.OriginSQL, record.FailedReason, instance))
	if err != nil {
		logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
			zap.Error(err))
	}
}

func insertPlanReplayerSuccessStatusRecord(ctx context.Context, sctx sessionctx.Context, instance string, record PlanReplayerStatusRecord) {
	exec := sctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, fmt.Sprintf(
		"insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, token, instance) values ('%s','%s','%s','%s','%s')",
		record.SQLDigest, record.PlanDigest, record.OriginSQL, record.Token, instance))
	if err != nil {
		logutil.BgLogger().Warn("insert mysql.plan_replayer_status record failed",
			zap.Error(err))
	}
}

// CollectPlanReplayerTask collects all unhandled plan replayer task
func (h *planReplayerTaskCollectorHandle) CollectPlanReplayerTask() error {
	allKeys, err := h.collectAllPlanReplayerTask(h.ctx)
	if err != nil {
		return err
	}
	tasks := make([]PlanReplayerTaskKey, 0)
	for _, key := range allKeys {
		unhandled, err := checkUnHandledReplayerTask(h.ctx, h.sctx, key)
		if err != nil {
			return err
		}
		if unhandled {
			tasks = append(tasks, key)
		}
	}
	h.setupTasks(tasks)
	return nil
}

// GetTasks get all tasks
func (h *planReplayerTaskCollectorHandle) GetTasks() []PlanReplayerTaskKey {
	tasks := make([]PlanReplayerTaskKey, 0)
	h.taskMu.RLock()
	defer h.taskMu.RUnlock()
	for taskKey := range h.taskMu.tasks {
		tasks = append(tasks, taskKey)
	}
	return tasks
}

func (h *planReplayerTaskCollectorHandle) setupTasks(tasks []PlanReplayerTaskKey) {
	r := make(map[PlanReplayerTaskKey]struct{})
	for _, task := range tasks {
		r[task] = struct{}{}
	}
	h.taskMu.Lock()
	defer h.taskMu.Unlock()
	h.taskMu.tasks = r
}

func (h *planReplayerTaskCollectorHandle) removeTask(taskKey PlanReplayerTaskKey) {
	h.taskMu.Lock()
	defer h.taskMu.Unlock()
	delete(h.taskMu.tasks, taskKey)
}

func (h *planReplayerTaskCollectorHandle) collectAllPlanReplayerTask(ctx context.Context) ([]PlanReplayerTaskKey, error) {
	exec := h.sctx.(sqlexec.SQLExecutor)
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
	allKeys := make([]PlanReplayerTaskKey, 0, len(rows))
	for _, row := range rows {
		sqlDigest, planDigest := row.GetString(0), row.GetString(1)
		allKeys = append(allKeys, PlanReplayerTaskKey{
			SQLDigest:  sqlDigest,
			PlanDigest: planDigest,
		})
	}
	return allKeys, nil
}

type planReplayerTaskDumpHandle struct {
	ctx    context.Context
	sctx   sessionctx.Context
	taskCH chan *PlanReplayerDumpTask
}

<<<<<<< HEAD
// DrainTask drain a task for unit test
func (h *planReplayerTaskDumpHandle) DrainTask() *PlanReplayerDumpTask {
	return <-h.taskCH
=======
func (w *planReplayerTaskDumpWorker) run() {
	logutil.BgLogger().Info("planReplayerTaskDumpWorker started.")
	for task := range w.taskCH {
		w.handleTask(task)
	}
	logutil.BgLogger().Info("planReplayerTaskDumpWorker exited.")
>>>>>>> 22b43ff396 (domain: add timeout for updateStatsWorker exit process (#40434))
}

// HandlePlanReplayerDumpTask handled the task
func (h *planReplayerTaskDumpHandle) dumpPlanReplayerDumpTask(task *PlanReplayerDumpTask) (success bool) {
	taskKey := task.PlanReplayerTaskKey
	unhandled, err := checkUnHandledReplayerTask(h.ctx, h.sctx, taskKey)
	if err != nil {
		logutil.BgLogger().Warn("check plan replayer capture task failed",
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return false
	}
	// the task is processed, thus we directly skip it.
	if !unhandled {
		return true
	}

	file, fileName, err := GeneratePlanReplayerFile()
	if err != nil {
		logutil.BgLogger().Warn("generate plan replayer capture task file failed",
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return
	}
	task.Zf = file
	task.FileName = fileName
	task.EncodedPlan, _ = task.EncodePlan(task.SessionVars.StmtCtx, false)
	jsStats := make(map[int64]*handle.JSONTable)
	is := GetDomain(h.sctx).InfoSchema()
	for tblID, stat := range task.TblStats {
		tbl, ok := is.TableByID(tblID)
		if !ok {
			return false
		}
		schema, ok := is.SchemaByTable(tbl.Meta())
		if !ok {
			return false
		}
		r, err := handle.GenJSONTableFromStats(schema.Name.String(), tbl.Meta(), stat.(*statistics.Table))
		if err != nil {
			logutil.BgLogger().Warn("generate plan replayer capture task json stats failed",
				zap.String("sqlDigest", taskKey.SQLDigest),
				zap.String("planDigest", taskKey.PlanDigest),
				zap.Error(err))
			return false
		}
		jsStats[tblID] = r
	}
	err = DumpPlanReplayerInfo(h.ctx, h.sctx, task)
	if err != nil {
		logutil.BgLogger().Warn("dump plan replayer capture task result failed",
			zap.String("sqlDigest", taskKey.SQLDigest),
			zap.String("planDigest", taskKey.PlanDigest),
			zap.Error(err))
		return false
	}
	return true
}

// SendTask send dumpTask in background task handler
func (h *planReplayerTaskDumpHandle) SendTask(task *PlanReplayerDumpTask) {
	select {
	case h.taskCH <- task:
	default:
		// TODO: add metrics here
		// directly discard the task if the task channel is full in order not to block the query process
	}
}

func checkUnHandledReplayerTask(ctx context.Context, sctx sessionctx.Context, task PlanReplayerTaskKey) (bool, error) {
	exec := sctx.(sqlexec.SQLExecutor)
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
	exec := sctx.(sqlexec.SQLExecutor)
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

// PlanReplayerTaskKey indicates key of a plan replayer task
type PlanReplayerTaskKey struct {
	SQLDigest  string
	PlanDigest string
}

// PlanReplayerDumpTask wrap the params for plan replayer dump
type PlanReplayerDumpTask struct {
	PlanReplayerTaskKey

	// tmp variables stored during the query
	EncodePlan func(*stmtctx.StatementContext, bool) (string, string)
	TblStats   map[int64]interface{}

	// variables used to dump the plan
	SessionBindings []*bindinfo.BindRecord
	EncodedPlan     string
	SessionVars     *variable.SessionVars
	JSONTblStats    map[int64]*handle.JSONTable
	ExecStmts       []ast.StmtNode
	Analyze         bool

	FileName string
	Zf       *os.File
}

// GeneratePlanReplayerFile generates plan replayer file
func GeneratePlanReplayerFile() (*os.File, string, error) {
	path := GetPlanReplayerDirName()
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	fileName, err := generatePlanReplayerFileName()
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	zf, err := os.Create(filepath.Join(path, fileName))
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	return zf, fileName, err
}

func generatePlanReplayerFileName() (string, error) {
	// Generate key and create zip file
	time := time.Now().UnixNano()
	b := make([]byte, 16)
	//nolint: gosec
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	return fmt.Sprintf("replayer_%v_%v.zip", key, time), nil
}
