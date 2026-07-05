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

package session

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type nonTransactionalDMLColumnNameMeta struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Name   string `json:"name"`
}

type nonTransactionalDMLBoundaryMeta struct {
	Encoded   []byte `json:"encoded,omitempty"`
	Inclusive bool   `json:"inclusive"`
}

type nonTransactionalDMLDXFTaskMeta struct {
	JobID            string                            `json:"job_id"`
	ExecutableDML    string                            `json:"executable_dml"`
	DisplayDML       string                            `json:"display_dml"`
	DMLType          string                            `json:"dml_type"`
	DBName           string                            `json:"db_name"`
	CurrentDB        string                            `json:"current_db"`
	FromSQL          string                            `json:"from_sql"`
	HandleExprSQL    string                            `json:"handle_expr_sql"`
	OriginalWhereSQL string                            `json:"original_where_sql"`
	HandleKind       nonTransactionalDMLHandleKind     `json:"handle_kind"`
	HandleColumn     nonTransactionalDMLColumnNameMeta `json:"handle_column"`
	HandleFieldType  types.FieldType                   `json:"handle_field_type"`
	TableID          int64                             `json:"table_id"`
	PhysicalTableID  int64                             `json:"physical_table_id"`
	BatchSize        int                               `json:"batch_size"`
	SessionCtx       nonTransactionalDMLSessionContext `json:"session_ctx"`
	Ranges           []nonTransactionalDMLDXFRangeMeta `json:"ranges"`
}

type nonTransactionalDMLDXFRangeMeta struct {
	RangeID  int64                            `json:"range_id"`
	Lower    *nonTransactionalDMLBoundaryMeta `json:"lower,omitempty"`
	Upper    *nonTransactionalDMLBoundaryMeta `json:"upper,omitempty"`
	Scanned  uint64                           `json:"scanned,omitempty"`
	Affected uint64                           `json:"affected,omitempty"`
}

type nonTransactionalDMLScheduler struct {
	*scheduler.BaseScheduler
}

type nonTransactionalDMLTaskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

type nonTransactionalDMLStepExecutor struct {
	taskMeta *nonTransactionalDMLDXFTaskMeta
	taskMgr  *storage.TaskManager
	scanned  atomic.Uint64

	taskexecutor.BaseStepExecutor
}

type nonTransactionalDMLCleanUp struct{}

const nonTransactionalDMLDXFCancelTimeout = 30 * time.Second

func init() {
	registerNonTransactionalDMLDXFTask()
}

func registerNonTransactionalDMLDXFTask() {
	scheduler.RegisterSchedulerFactory(
		proto.NonTransactionalDML,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			sch := &nonTransactionalDMLScheduler{
				BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
			}
			sch.BaseScheduler.Extension = sch
			return sch
		},
	)
	taskexecutor.RegisterTaskType(
		proto.NonTransactionalDML,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			executor := &nonTransactionalDMLTaskExecutor{
				BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
			}
			executor.BaseTaskExecutor.Extension = executor
			return executor
		},
	)
	scheduler.RegisterSchedulerCleanUpFactory(
		proto.NonTransactionalDML,
		func() scheduler.CleanUpRoutine {
			return &nonTransactionalDMLCleanUp{}
		},
	)
}

func handleNonTransactionalDMLByDXF(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session,
	resolveCtx *resolve.Context, tableName *ast.TableName, shardColumnInfo *model.ColumnInfo,
	tableSources []*ast.TableSource) (recordSet sqlexec.RecordSet, retErr error) {
	startTime := time.Now()
	dmlType := strings.ToLower(ast.GetStmtLabel(stmt.DMLStmt))
	session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeDXF, dmlType, session_metrics.NonTransactionalDMLResultStart)
	defer func() {
		result := nonTransactionalDMLMetricsResult(retErr)
		session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeDXF, dmlType, result)
		session_metrics.NonTransactionalDMLDurationObserve(session_metrics.NonTransactionalDMLModeDXF, dmlType, result, time.Since(startTime).Seconds())
	}()
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return nil, errors.Annotate(err, "Non-transactional DML DXF mode requires distributed task framework")
	}
	if se.GetSessionVars().NonTransactionalIgnoreError {
		return nil, errors.New("Non-transactional DML DXF mode doesn't support tidb_nontransactional_ignore_error")
	}
	if err := checkNonTransactionalDMLRangeModeStatement(stmt, tableSources); err != nil {
		return nil, err
	}
	if stmt.DryRun != ast.NoDryRun {
		return nil, errors.New("Non-transactional DML DXF mode doesn't support dry run")
	}
	if stmt.Limit == 0 || stmt.Limit > uint64(math.MaxInt64) {
		return nil, errors.New("Non-transactional DML, batch size should be positive")
	}
	desc, err := buildNonTransactionalDMLHandleDescriptor(se, stmt, tableName, shardColumnInfo, tableSources)
	if err != nil {
		return nil, err
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContext(stmt, se, resolveCtx, tableName, desc, tableSources)
	if err != nil {
		return nil, err
	}
	rangeCtx.Mode = "dxf"
	ranges, err := buildNonTransactionalDMLRangeSpans(ctx, se, rangeCtx)
	if err != nil {
		return nil, err
	}
	taskMeta, err := buildNonTransactionalDMLDXFTaskMeta(rangeCtx, ranges)
	if err != nil {
		return nil, err
	}
	taskMetaBytes, err := json.Marshal(taskMeta)
	if err != nil {
		return nil, err
	}
	taskCtx := kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	taskKey := nonTransactionalDMLDXFTaskKey(taskMeta.JobID)
	task, err := dxfhandle.SubmitTask(taskCtx, taskKey,
		proto.NonTransactionalDML, nonTransactionalDMLRangeWorkerCount(rangeCtx.Concurrency, len(ranges)), "", 0, taskMetaBytes)
	if err != nil {
		return nil, err
	}
	logutil.Logger(ctx).Info("Non-transactional DML DXF task submitted",
		zap.Int64("task-id", task.ID),
		zap.String("job-id", taskMeta.JobID),
		zap.String("table", taskMeta.FromSQL),
		zap.Int("concurrency", task.Concurrency),
		zap.String("dml", taskMeta.DisplayDML))
	if err := waitNonTransactionalDMLDXFTask(taskCtx, task.ID, taskKey); err != nil {
		return nil, err
	}
	finishedTask, err := taskMgr.GetTaskByIDWithHistory(taskCtx, task.ID)
	if err != nil {
		return nil, err
	}
	if finishedTask.State != proto.TaskStateSucceed {
		return nil, errors.Errorf("Non-transactional DML DXF task stopped with state %s", finishedTask.State)
	}
	return buildNonTransactionalDMLDXFResults(taskCtx, se, taskMeta)
}

func waitNonTransactionalDMLDXFTask(ctx context.Context, taskID int64, taskKey string) error {
	err := dxfhandle.WaitTaskDoneOrPaused(ctx, taskID)
	if err == nil {
		return nil
	}
	if goerrors.Is(err, context.Canceled) || goerrors.Is(err, context.DeadlineExceeded) {
		return cancelNonTransactionalDMLDXFTask(taskKey, err)
	}
	return err
}

func cancelNonTransactionalDMLDXFTask(taskKey string, cause error) error {
	cancelCtx, cancel := context.WithTimeout(context.Background(), nonTransactionalDMLDXFCancelTimeout)
	defer cancel()
	cancelCtx = kv.WithInternalSourceType(cancelCtx, kv.InternalDistTask)
	if err := dxfhandle.CancelTask(cancelCtx, taskKey); err != nil {
		return errors.Annotatef(cause, "Non-transactional DML DXF task %s was interrupted, but cancellation failed: %v", taskKey, err)
	}
	if err := dxfhandle.WaitTaskDoneByKey(cancelCtx, taskKey); err != nil {
		return errors.Annotatef(cause, "Non-transactional DML DXF task %s was interrupted, but waiting for cancellation failed: %v", taskKey, err)
	}
	return cause
}

func buildNonTransactionalDMLDXFTaskMeta(rangeCtx *nonTransactionalDMLRangeContext,
	ranges []nonTransactionalDMLRangeSpan) (*nonTransactionalDMLDXFTaskMeta, error) {
	executableDML, err := restoreNonTransactionalDMLStmt(rangeCtx.Stmt.DMLStmt)
	if err != nil {
		return nil, err
	}
	rangeMetas := make([]nonTransactionalDMLDXFRangeMeta, 0, len(ranges))
	for i := range ranges {
		rangeMetas = append(rangeMetas, nonTransactionalDMLDXFRangeMeta{
			RangeID: int64(i + 1),
			Lower:   nonTransactionalDMLBoundaryMetaFromBoundary(ranges[i].lower),
			Upper:   nonTransactionalDMLBoundaryMetaFromBoundary(ranges[i].upper),
		})
	}
	redactMode := rangeCtx.SessionCtx.SysVars[variable.TiDBRedactLog]
	return &nonTransactionalDMLDXFTaskMeta{
		JobID:            rangeCtx.JobID,
		ExecutableDML:    executableDML,
		DisplayDML:       redact.String(redactMode, executableDML),
		DMLType:          rangeCtx.DMLType,
		DBName:           rangeCtx.DBName,
		CurrentDB:        rangeCtx.CurrentDB,
		FromSQL:          rangeCtx.FromSQL,
		HandleExprSQL:    rangeCtx.HandleExprSQL,
		OriginalWhereSQL: rangeCtx.OriginalWhereSQL,
		HandleKind:       rangeCtx.Descriptor.kind,
		HandleColumn:     nonTransactionalDMLColumnNameMetaFromAST(rangeCtx.Descriptor.columnName),
		HandleFieldType:  rangeCtx.Descriptor.fieldType,
		TableID:          rangeCtx.TableID,
		PhysicalTableID:  rangeCtx.PhysicalTableID,
		BatchSize:        rangeCtx.BatchSize,
		SessionCtx:       rangeCtx.SessionCtx,
		Ranges:           rangeMetas,
	}, nil
}

func buildNonTransactionalDMLDXFResults(ctx context.Context, se sessiontypes.Session,
	taskMeta *nonTransactionalDMLDXFTaskMeta) (sqlexec.RecordSet, error) {
	summary, err := summarizeNonTransactionalDMLCheckpoints(ctx, se, taskMeta.JobID)
	if err != nil {
		return nil, err
	}
	jobCount := int(summary.DoneRanges)
	if jobCount == 0 {
		jobCount = len(taskMeta.Ranges)
	}
	jobs := make([]job, 0, jobCount)
	for i := 1; i <= jobCount; i++ {
		jobs = append(jobs, job{jobID: i})
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

func nonTransactionalDMLDXFTaskKey(jobID string) string {
	return fmt.Sprintf("ntdml/%s", jobID)
}

func (s *nonTransactionalDMLScheduler) OnTick(context.Context, *proto.Task) {}

func (s *nonTransactionalDMLScheduler) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, step proto.Step) ([][]byte, error) {
	if step == proto.StepDone {
		return nil, nil
	}
	taskMeta, err := unmarshalNonTransactionalDMLDXFTaskMeta(task.Meta)
	if err != nil {
		return nil, err
	}
	metas := make([][]byte, 0, len(taskMeta.Ranges))
	for _, rangeMeta := range taskMeta.Ranges {
		metaBytes, err := json.Marshal(rangeMeta)
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

func (s *nonTransactionalDMLScheduler) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

func (s *nonTransactionalDMLScheduler) GetEligibleInstances(context.Context, *proto.Task) ([]string, error) {
	return nil, nil
}

func (s *nonTransactionalDMLScheduler) IsRetryableErr(err error) bool {
	return isNonTransactionalDMLRetryableError(err)
}

func (s *nonTransactionalDMLScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.NonTransactionalDMLStepRun
	case proto.NonTransactionalDMLStepRun:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

func (s *nonTransactionalDMLScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

func (*nonTransactionalDMLCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {
	taskMeta, err := unmarshalNonTransactionalDMLDXFTaskMeta(task.Meta)
	if err != nil {
		return err
	}
	if task.State != proto.TaskStateSucceed {
		return nil
	}
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return taskMgr.WithNewSession(func(ctxSe sessionctx.Context) error {
		se, ok := ctxSe.(sessiontypes.Session)
		if !ok {
			return errors.New("Non-transactional DML DXF cleanup requires a session executor")
		}
		return deleteNonTransactionalDMLCheckpoints(kv.WithInternalSourceType(ctx, kv.InternalDistTask), se, taskMeta.JobID)
	})
}

func (e *nonTransactionalDMLTaskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (e *nonTransactionalDMLTaskExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	taskMeta, err := unmarshalNonTransactionalDMLDXFTaskMeta(task.Meta)
	if err != nil {
		return nil, err
	}
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	return &nonTransactionalDMLStepExecutor{
		taskMeta: taskMeta,
		taskMgr:  taskMgr,
	}, nil
}

func (e *nonTransactionalDMLTaskExecutor) IsRetryableError(err error) bool {
	return isNonTransactionalDMLRetryableError(err)
}

func (e *nonTransactionalDMLStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logutil.Logger(ctx).Error("Non-transactional DML DXF subtask panicked",
				zap.Any("panic", recovered),
				zap.ByteString("stack", debug.Stack()))
			err = errors.Errorf("Non-transactional DML DXF subtask panicked: %v", recovered)
		}
	}()
	var rangeMeta nonTransactionalDMLDXFRangeMeta
	if err := json.Unmarshal(subtask.Meta, &rangeMeta); err != nil {
		return err
	}
	err = e.taskMgr.WithNewSession(func(ctxSe sessionctx.Context) error {
		se, ok := ctxSe.(sessiontypes.Session)
		if !ok {
			return errors.New("Non-transactional DML DXF executor requires a session executor")
		}
		return e.runSubtaskWithSession(kv.WithInternalSourceType(ctx, kv.InternalDistTask), se, &rangeMeta)
	})
	if err != nil {
		return err
	}
	subtask.Meta, err = json.Marshal(rangeMeta)
	return err
}

func (e *nonTransactionalDMLStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &execute.SubtaskSummary{RowCount: int64(e.scanned.Load())}
}

func (e *nonTransactionalDMLStepExecutor) runSubtaskWithSession(ctx context.Context, se sessiontypes.Session,
	rangeMeta *nonTransactionalDMLDXFRangeMeta) error {
	if err := applyNonTransactionalDMLSessionContext(se, e.taskMeta.SessionCtx); err != nil {
		return err
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContextFromDXFTaskMeta(e.taskMeta)
	if err != nil {
		return err
	}
	lower, err := rangeMeta.Lower.toBoundary(se.GetSessionVars().StmtCtx, rangeCtx.Descriptor)
	if err != nil {
		return err
	}
	upper, err := rangeMeta.Upper.toBoundary(se.GetSessionVars().StmtCtx, rangeCtx.Descriptor)
	if err != nil {
		return err
	}
	checkpoint, err := loadNonTransactionalDMLCheckpoint(ctx, se, e.taskMeta.JobID, rangeMeta.RangeID, rangeCtx.Descriptor)
	if err != nil {
		return err
	}
	if checkpoint != nil {
		switch checkpoint.Status {
		case nonTransactionalDMLCheckpointDone:
			if checkpoint.Checkpoint != nil {
				lower = cloneNonTransactionalDMLBoundaryPtr(checkpoint.Checkpoint)
				lower.inclusive = false
			}
			rangeMeta.Scanned = checkpoint.Scanned
			rangeMeta.Affected = checkpoint.Affected
			e.scanned.Store(checkpoint.Scanned)
		case nonTransactionalDMLCheckpointFailed:
			if checkpoint.ErrorClass != "retryable" {
				return errors.Errorf("Non-transactional DML DXF range %d has failed checkpoint: %s", rangeMeta.RangeID, checkpoint.ErrorText)
			}
			if err := deleteNonTransactionalDMLCheckpoint(ctx, se, e.taskMeta.JobID, rangeMeta.RangeID); err != nil {
				return err
			}
		}
	}
	for {
		scanned, err := scanNonTransactionalDMLRangeChunk(ctx, se, rangeCtx, lower, upper)
		if err != nil {
			return err
		}
		if scanned == nil {
			return nil
		}
		dmlSQL, err := buildNonTransactionalDMLRangeMutationSQL(rangeCtx, lower, &scanned.last)
		if err != nil {
			return err
		}
		affected, err := executeNonTransactionalDMLRangeChunkWithRetry(ctx, se, rangeCtx, rangeMeta.RangeID, lower, scanned, dmlSQL, rangeMeta.Scanned, rangeMeta.Affected)
		if err != nil {
			return err
		}
		rangeMeta.Scanned += uint64(scanned.size)
		rangeMeta.Affected += affected
		e.scanned.Store(rangeMeta.Scanned)
		nextLower := scanned.last
		nextLower.inclusive = false
		lower = &nextLower
	}
}

func unmarshalNonTransactionalDMLDXFTaskMeta(data []byte) (*nonTransactionalDMLDXFTaskMeta, error) {
	var taskMeta nonTransactionalDMLDXFTaskMeta
	if err := json.Unmarshal(data, &taskMeta); err != nil {
		return nil, err
	}
	if taskMeta.JobID == "" {
		return nil, errors.New("Non-transactional DML DXF task has empty job ID")
	}
	if taskMeta.BatchSize <= 0 {
		return nil, errors.New("Non-transactional DML DXF task has invalid batch size")
	}
	if taskMeta.HandleKind == "" {
		return nil, errors.New("Non-transactional DML DXF task has empty handle kind")
	}
	return &taskMeta, nil
}

func buildNonTransactionalDMLRangeContextFromDXFTaskMeta(taskMeta *nonTransactionalDMLDXFTaskMeta) (*nonTransactionalDMLRangeContext, error) {
	stmt, err := parseNonTransactionalDMLDXFExecutableDML(taskMeta)
	if err != nil {
		return nil, err
	}
	desc := &nonTransactionalDMLHandleDescriptor{
		kind:       taskMeta.HandleKind,
		tableInfo:  &model.TableInfo{ID: taskMeta.TableID},
		columnName: taskMeta.HandleColumn.toASTColumnName(),
		fieldType:  taskMeta.HandleFieldType,
	}
	return &nonTransactionalDMLRangeContext{
		Stmt:              stmt,
		Descriptor:        desc,
		SessionCtx:        taskMeta.SessionCtx,
		JobID:             taskMeta.JobID,
		Mode:              "dxf",
		DMLType:           taskMeta.DMLType,
		DBName:            taskMeta.DBName,
		CurrentDB:         taskMeta.CurrentDB,
		FromSQL:           taskMeta.FromSQL,
		HandleExprSQL:     taskMeta.HandleExprSQL,
		OriginalWhereSQL:  taskMeta.OriginalWhereSQL,
		OriginalCondition: stmt.DMLStmt.WhereExpr(),
		BatchSize:         taskMeta.BatchSize,
		TableID:           taskMeta.TableID,
		PhysicalTableID:   taskMeta.PhysicalTableID,
	}, nil
}

func parseNonTransactionalDMLDXFExecutableDML(taskMeta *nonTransactionalDMLDXFTaskMeta) (*ast.NonTransactionalDMLStmt, error) {
	parsed, err := parser.New().ParseOneStmt(taskMeta.ExecutableDML, "", "")
	if err != nil {
		return nil, err
	}
	dml, ok := parsed.(ast.ShardableDMLStmt)
	if !ok {
		return nil, errors.Errorf("Non-transactional DML DXF task stores unsupported DML type %T", parsed)
	}
	switch parsed.(type) {
	case *ast.DeleteStmt, *ast.UpdateStmt:
	default:
		return nil, errors.Errorf("Non-transactional DML DXF task stores unsupported DML type %T", parsed)
	}
	return &ast.NonTransactionalDMLStmt{
		DMLStmt:     dml,
		ShardColumn: taskMeta.HandleColumn.toASTColumnNamePtr(),
		Limit:       uint64(taskMeta.BatchSize),
	}, nil
}

func restoreNonTransactionalDMLStmt(stmt ast.StmtNode) (string, error) {
	var sb strings.Builder
	err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		return "", errors.Annotate(err, "Failed to restore non-transactional DML for DXF")
	}
	return sb.String(), nil
}

func nonTransactionalDMLColumnNameMetaFromAST(name ast.ColumnName) nonTransactionalDMLColumnNameMeta {
	return nonTransactionalDMLColumnNameMeta{
		Schema: name.Schema.O,
		Table:  name.Table.O,
		Name:   name.Name.O,
	}
}

func (m nonTransactionalDMLColumnNameMeta) toASTColumnName() ast.ColumnName {
	return ast.ColumnName{
		Schema: pmodel.NewCIStr(m.Schema),
		Table:  pmodel.NewCIStr(m.Table),
		Name:   pmodel.NewCIStr(m.Name),
	}
}

func (m nonTransactionalDMLColumnNameMeta) toASTColumnNamePtr() *ast.ColumnName {
	name := m.toASTColumnName()
	return &name
}

func nonTransactionalDMLBoundaryMetaFromBoundary(boundary *nonTransactionalDMLBoundary) *nonTransactionalDMLBoundaryMeta {
	if boundary == nil || !boundary.hasValue {
		return nil
	}
	return &nonTransactionalDMLBoundaryMeta{
		Encoded:   append([]byte(nil), boundary.encoded...),
		Inclusive: boundary.inclusive,
	}
}

func (m *nonTransactionalDMLBoundaryMeta) toBoundary(sc *stmtctx.StatementContext,
	desc *nonTransactionalDMLHandleDescriptor) (*nonTransactionalDMLBoundary, error) {
	if m == nil {
		return nil, nil
	}
	boundary, err := decodeNonTransactionalDMLBoundary(sc, desc, m.Encoded, m.Inclusive)
	if err != nil {
		return nil, err
	}
	return &boundary, nil
}
