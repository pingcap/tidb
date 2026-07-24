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

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const ctasLoadTaskStep = proto.StepOne

type ctasTaskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

type ctasStepExecutor struct {
	taskexecutor.BaseStepExecutor
	param taskexecutor.Param
}

type ctasScheduler struct {
	*scheduler.BaseScheduler
	meta ddl.CTASLoadSubtaskMeta
}

type ctasSessionGetter interface {
	GetSession(connectionID uint64) (any, bool)
}

var (
	_ taskexecutor.TaskExecutor = (*ctasTaskExecutor)(nil)
	_ scheduler.Scheduler       = (*ctasScheduler)(nil)
)

// NewCTASTaskExecutor creates the submitter-side executor for CTAS load subtasks.
func NewCTASTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
	e := &ctasTaskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

// NewCTASScheduler creates the scheduler for the single-subtask CTAS load task.
func NewCTASScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	s := &ctasScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
	}
	s.BaseScheduler.Extension = s
	return s
}

func newCTASDistTaskError(detail string) error {
	return exeerrors.ErrCTASDistTask.GenWithStackByArgs(detail)
}

func newCTASDistTaskErrorf(format string, args ...any) error {
	return newCTASDistTaskError(fmt.Sprintf(format, args...))
}

func validateCTASLoadTaskMeta(meta *ddl.CTASLoadSubtaskMeta) error {
	if meta == nil {
		return newCTASDistTaskError("ctas task meta is empty")
	}
	if meta.CTASSnapshotTS == 0 {
		return newCTASDistTaskError("ctas snapshot ts is empty")
	}
	if meta.CTASSql == "" {
		return newCTASDistTaskError("ctas sql is empty")
	}
	if meta.CTASSubmitterExecID == "" {
		return newCTASDistTaskError("ctas submitter exec id is empty")
	}
	if meta.CTASSubmitterConnID == 0 {
		return newCTASDistTaskError("ctas submitter conn id is empty")
	}
	return nil
}

func (*ctasTaskExecutor) IsIdempotent(*proto.Subtask) bool {
	// The CTAS load path can mutate the target table before reporting success, so
	// rerunning a leftover running subtask is not safe.
	return false
}

func (e *ctasTaskExecutor) GetStepExecutor(*proto.Task) (execute.StepExecutor, error) {
	return &ctasStepExecutor{param: e.Param}, nil
}

func (*ctasTaskExecutor) IsRetryableError(error) bool {
	return false
}

func (e *ctasStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	var meta ddl.CTASLoadSubtaskMeta
	if err := json.Unmarshal(subtask.Meta, &meta); err != nil {
		return errors.Trace(err)
	}
	if err := validateCTASLoadTaskMeta(&meta); err != nil {
		return err
	}
	return e.runOnSubmitter(ctx, &meta)
}

func (e *ctasStepExecutor) runOnSubmitter(ctx context.Context, meta *ddl.CTASLoadSubtaskMeta) error {
	if CreateSession == nil || CloseSession == nil {
		return newCTASDistTaskError("CTAS disttask session helpers are not initialized")
	}

	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	var sourceSctx sessionctx.Context
	if err := taskMgr.WithNewSession(func(seedSctx sessionctx.Context) error {
		var err error
		sourceSctx, err = lookupCTASSubmitterSession(seedSctx, meta.CTASSubmitterConnID)
		return err
	}); err != nil {
		return err
	}
	if sourceSctx == nil {
		return newCTASDistTaskError("CTAS submitter session lookup returned nil")
	}

	selectSctx, err := CreateSession(sourceSctx)
	if err != nil {
		return err
	}
	defer CloseSession(selectSctx)
	if err = cloneCTASSessionState(ctx, sourceSctx, selectSctx); err != nil {
		return err
	}

	writeSctx, err := CreateSession(sourceSctx)
	if err != nil {
		return err
	}
	defer CloseSession(writeSctx)
	if err = cloneCTASSessionState(ctx, sourceSctx, writeSctx); err != nil {
		return err
	}

	return runCTASLoadSubtask(ctx, selectSctx, writeSctx, meta)
}

func lookupCTASSubmitterSession(baseSctx sessionctx.Context, connID uint64) (sessionctx.Context, error) {
	dom := domain.GetDomain(baseSctx)
	if dom == nil {
		return nil, newCTASDistTaskError("domain is unavailable for CTAS disttask")
	}
	sm := dom.InfoSyncer().GetSessionManager()
	if sm == nil {
		return nil, newCTASDistTaskError("session manager is unavailable for CTAS disttask")
	}
	getter, ok := sm.(ctasSessionGetter)
	if !ok {
		return nil, newCTASDistTaskError("session manager does not support CTAS submitter lookup")
	}
	rawSession, ok := getter.GetSession(connID)
	if !ok || rawSession == nil {
		return nil, newCTASDistTaskErrorf("CTAS submitter session %d not found", connID)
	}
	sctx, ok := rawSession.(sessionctx.Context)
	if !ok {
		return nil, newCTASDistTaskError("CTAS submitter session has unexpected type")
	}
	return sctx, nil
}

func cloneCTASSessionState(ctx context.Context, src, dst sessionctx.Context) error {
	srcVars := src.GetSessionVars()
	dstVars := dst.GetSessionVars()

	var states sessionstates.SessionStates
	if err := srcVars.EncodeSessionStates(ctx, &states); err != nil {
		return err
	}
	if err := dstVars.DecodeSessionStates(ctx, &states); err != nil {
		return err
	}

	systemVars := make(map[string]string)
	for _, sv := range variable.GetSysVars() {
		switch {
		case sv.HasNoneScope(), !sv.HasSessionScope(), sv.ReadOnly:
			continue
		}
		val, keep, err := srcVars.GetSessionStatesSystemVar(sv.Name)
		if err != nil {
			return err
		}
		if keep {
			systemVars[sv.Name] = val
		}
	}

	for _, name := range variable.OrderByDependency(systemVars) {
		if err := dstVars.SetSystemVar(name, systemVars[name]); err != nil {
			return err
		}
	}
	dstVars.User = srcVars.User
	dstVars.ResourceGroupName = srcVars.ResourceGroupName
	dstVars.LocalTemporaryTables = srcVars.LocalTemporaryTables
	dstVars.TemporaryTableData = srcVars.TemporaryTableData
	dstVars.TxnCtx.TemporaryTables = srcVars.TxnCtx.TemporaryTables
	return nil
}

func prepareCTASSelectSnapshot(
	selectSctx sessionctx.Context,
	snapshotTS uint64,
) (infoschema.InfoSchema, error) {
	dom := domain.GetDomain(selectSctx)
	if dom == nil {
		return nil, newCTASDistTaskError("domain is unavailable when preparing CTAS select snapshot")
	}
	snapshotIS, err := dom.GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	return temptable.AttachLocalTemporaryTableInfoSchema(selectSctx, snapshotIS), nil
}

func installCTASSelectSnapshot(
	selectSctx sessionctx.Context,
	snapshotTS uint64,
	snapshotIS infoschema.InfoSchema,
) {
	vars := selectSctx.GetSessionVars()
	vars.SnapshotTS = snapshotTS
	vars.SnapshotInfoschema = snapshotIS
}

func lookupCTASWriteTarget(
	writeSctx sessionctx.Context,
	meta *ddl.CTASLoadSubtaskMeta,
) (table.Table, *model.DBInfo, error) {
	latestIS, ok := sessiontxn.GetTxnManager(writeSctx).GetTxnInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return nil, nil, newCTASDistTaskError("latest info schema is unavailable for CTAS execution")
	}
	targetTable, ok := latestIS.TableByID(context.Background(), meta.TableID)
	if !ok {
		return nil, nil, newCTASDistTaskErrorf("CTAS target table %d not found", meta.TableID)
	}
	dbInfo, ok := latestIS.SchemaByID(meta.SchemaID)
	if !ok {
		return nil, nil, newCTASDistTaskErrorf("CTAS target schema %d not found", meta.SchemaID)
	}
	return targetTable, dbInfo, nil
}

func resetCTASWriteSnapshot(writeSctx sessionctx.Context) {
	vars := writeSctx.GetSessionVars()
	vars.SnapshotTS = 0
	vars.SnapshotInfoschema = nil
}

func runCTASLoadSubtask(
	ctx context.Context,
	selectSctx sessionctx.Context,
	writeSctx sessionctx.Context,
	meta *ddl.CTASLoadSubtaskMeta,
) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)

	createStmt, err := parseCTASStmt(selectSctx, meta.CTASSql)
	if err != nil {
		return err
	}

	selectStmt, ok := createStmt.Select.(ast.StmtNode)
	if !ok {
		return newCTASDistTaskError("CTAS subtask does not contain a supported SELECT statement")
	}

	snapshotIS, err := prepareCTASSelectSnapshot(selectSctx, meta.CTASSnapshotTS)
	if err != nil {
		return err
	}

	installCTASSelectSnapshot(selectSctx, meta.CTASSnapshotTS, snapshotIS)

	targetTable, dbInfo, err := lookupCTASWriteTarget(writeSctx, meta)
	if err != nil {
		return err
	}

	resetCTASWriteSnapshot(writeSctx)

	finishSelectStmt, err := prepareCTASSessionForStmt(ctx, selectSctx, selectStmt)
	if err != nil {
		return err
	}
	defer finishSelectStmt()

	selectExec, selectPlan, selectColumns, err := buildCTASSelectExecutor(ctx, selectSctx, selectStmt, snapshotIS)
	if err != nil {
		return err
	}

	useImportMode, err := shouldUseImportForCTAS(ctx, writeSctx)
	if err != nil {
		return err
	}

	var writeExec exec.Executor
	var writeStmt ast.StmtNode
	if useImportMode {
		if meta.CTASOnDuplicate != ast.OnDuplicateKeyHandlingError {
			return newCTASDistTaskErrorf("CTAS import mode does not support on-duplicate mode %v", meta.CTASOnDuplicate)
		}
		writeExec, writeStmt, err = buildCTASImportWriteExecutor(
			writeSctx, dbInfo, targetTable, selectPlan, selectExec, selectColumns,
		)
		if err != nil {
			return err
		}
	} else {
		writeExec, writeStmt, err = buildCTASInsertWriteExecutor(
			writeSctx, targetTable, selectExec, selectColumns, meta,
		)
		if err != nil {
			return err
		}
	}

	finishWriteStmt, err := prepareCTASSessionForStmt(ctx, writeSctx, writeStmt)
	if err != nil {
		return err
	}
	defer finishWriteStmt()

	return executeCTASWriteExecutor(ctx, writeSctx, writeExec, meta)
}

type ctasTxnContextPreparer interface {
	PrepareTxnCtx(context.Context) error
}

func prepareCTASSessionForStmt(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode) (func(), error) {
	if preparer, ok := sctx.(ctasTxnContextPreparer); ok {
		if err := preparer.PrepareTxnCtx(ctx); err != nil {
			return nil, err
		}
	}
	if err := ResetContextOfStmt(sctx, stmt); err != nil {
		return nil, err
	}
	txnManager := sessiontxn.GetTxnManager(sctx)
	if err := txnManager.OnStmtStart(ctx, stmt); err != nil {
		return nil, err
	}
	return txnManager.OnStmtEnd, nil
}

func buildCTASSelectExecutor(
	ctx context.Context,
	selectSctx sessionctx.Context,
	selectStmt ast.StmtNode,
	snapshotIS infoschema.InfoSchema,
) (exec.Executor, base.PhysicalPlan, []*ast.ColumnName, error) {
	nodeW := resolve.NewNodeW(selectStmt)
	if err := plannercore.Preprocess(
		ctx,
		selectSctx,
		nodeW,
		plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: snapshotIS}),
	); err != nil {
		return nil, nil, nil, err
	}
	selectPlan, outputNames, err := planner.Optimize(ctx, selectSctx, nodeW, snapshotIS)
	if err != nil {
		return nil, nil, nil, err
	}
	selectPhysicalPlan, ok := selectPlan.(base.PhysicalPlan)
	if !ok {
		return nil, nil, nil, newCTASDistTaskErrorf("unexpected CTAS select plan type %T", selectPlan)
	}
	builder := newExecutorBuilder(ctx, selectSctx, snapshotIS, nil)
	selectExec := builder.build(selectPlan)
	if builder.err != nil {
		return nil, nil, nil, builder.err
	}
	if selectExec == nil {
		return nil, nil, nil, newCTASDistTaskError("failed to build CTAS select executor")
	}
	insertColumns := make([]*ast.ColumnName, 0, len(outputNames))
	for _, outputName := range outputNames {
		insertColumns = append(insertColumns, &ast.ColumnName{Name: outputName.ColName})
	}
	return selectExec, selectPhysicalPlan, insertColumns, nil
}

func buildCTASInsertWriteExecutor(
	writeSctx sessionctx.Context,
	targetTable table.Table,
	selectExec exec.Executor,
	selectColumns []*ast.ColumnName,
	meta *ddl.CTASLoadSubtaskMeta,
) (exec.Executor, ast.StmtNode, error) {

	insertStmt := &ast.InsertStmt{
		IsReplace: meta.CTASOnDuplicate == ast.OnDuplicateKeyHandlingReplace,
		IgnoreErr: meta.CTASOnDuplicate == ast.OnDuplicateKeyHandlingIgnore,
		Columns:   selectColumns,
		Select:    &ast.SelectStmt{},
	}

	baseExec := exec.NewBaseExecutor(writeSctx, nil, 0, selectExec)
	baseExec.SetInitCap(chunk.ZeroCapacity)

	insertValues := &InsertValues{
		BaseExecutor: baseExec,
		Table:        targetTable,
		SelectExec:   selectExec,
		ignoreErr:    insertStmt.IgnoreErr,
		Columns:      insertStmt.Columns,
	}
	if err := insertValues.initInsertColumns(); err != nil {
		return nil, nil, err
	}
	insertValues.rowLen = len(insertValues.insertColumns)
	if selectExec.Schema().Len() != insertValues.rowLen {
		return nil, nil, newCTASDistTaskErrorf("CTAS select column count %d does not match target insert columns %d",
			selectExec.Schema().Len(), insertValues.rowLen)
	}

	switch meta.CTASOnDuplicate {
	case ast.OnDuplicateKeyHandlingError, ast.OnDuplicateKeyHandlingIgnore:
		return &InsertExec{InsertValues: insertValues}, insertStmt, nil
	case ast.OnDuplicateKeyHandlingReplace:
		return &ReplaceExec{InsertValues: insertValues}, insertStmt, nil
	default:
		return nil, nil, newCTASDistTaskErrorf("unsupported CTAS on-duplicate mode %v", meta.CTASOnDuplicate)
	}
}

func buildCTASImportWriteExecutor(
	writeSctx sessionctx.Context,
	dbInfo *model.DBInfo,
	targetTable table.Table,
	selectPlan base.PhysicalPlan,
	selectExec exec.Executor,
	selectColumns []*ast.ColumnName,
) (exec.Executor, ast.StmtNode, error) {
	columnsAndUserVars := make([]*ast.ColumnNameOrUserVar, 0, len(selectColumns))
	for _, col := range selectColumns {
		columnsAndUserVars = append(columnsAndUserVars, &ast.ColumnNameOrUserVar{ColumnName: col})
	}
	importStmt := &ast.ImportIntoStmt{
		Table:              &ast.TableName{Schema: dbInfo.Name, Name: targetTable.Meta().Name},
		ColumnsAndUserVars: columnsAndUserVars,
		Select:             &ast.SelectStmt{},
	}
	importPlan := (&plannercore.ImportInto{
		Table: &resolve.TableNameW{
			TableName: importStmt.Table,
			DBInfo:    dbInfo,
			TableInfo: targetTable.Meta(),
		},
		ColumnsAndUserVars: columnsAndUserVars,
		SelectPlan:         selectPlan,
	}).Init(writeSctx.GetPlanCtx())
	baseExec := exec.NewBaseExecutor(writeSctx, nil, 0, selectExec)
	baseExec.SetInitCap(chunk.ZeroCapacity)

	importExec, err := newImportIntoExec(baseExec, selectExec, writeSctx, importPlan, targetTable, true)
	if err != nil {
		return nil, nil, err
	}
	return importExec, importStmt, nil
}

func shouldUseImportForCTAS(ctx context.Context, sctx sessionctx.Context) (bool, error) {
	val, err := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, variable.TiDBCreateFromSelectUsingImport)
	if err != nil {
		return false, err
	}
	return variable.TiDBOptOn(val), nil
}

func executeCTASWriteExecutor(
	ctx context.Context,
	writeSctx sessionctx.Context,
	writeExec exec.Executor,
	meta *ddl.CTASLoadSubtaskMeta,
) (err error) {
	logger := logutil.Logger(ctx).With(
		zap.Int64("job-id", meta.JobID),
		zap.Int64("table-id", meta.TableID),
		zap.Uint64("snapshot-ts", meta.CTASSnapshotTS),
		zap.Uint64("submitter-conn-id", meta.CTASSubmitterConnID),
	)
	logger.Info("start CTAS load subtask")

	committed := false
	defer func() {
		if !committed {
			writeSctx.StmtRollback(ctx, false)
			writeSctx.RollbackTxn(ctx)
		}
	}()
	defer func() {
		if closeErr := exec.Close(writeExec); closeErr != nil {
			logger.Error("close CTAS write executor failed", zap.Error(closeErr))
		}
	}()

	if err = exec.Open(ctx, writeExec); err != nil {
		return err
	}
	if err = exec.Next(ctx, writeExec, exec.NewFirstChunk(writeExec)); err != nil {
		return err
	}

	writeSctx.StmtCommit(ctx)
	if err = writeSctx.CommitTxn(ctx); err != nil {
		return err
	}
	committed = true

	logger.Info("finish CTAS load subtask")
	return nil
}

func parseCTASStmt(selectSctx sessionctx.Context, sql string) (*ast.CreateTableStmt, error) {
	p := parser.New()
	sessionVars := selectSctx.GetSessionVars()
	p.SetSQLMode(sessionVars.SQLMode)

	chs, coll := sessionVars.GetCharsetInfo()
	cli, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.CharacterSetClient)
	if err != nil {
		return nil, err
	}
	stmts, _, err := p.ParseSQL(
		sql,
		parser.CharsetClient(cli),
		parser.CharsetConnection(chs),
		parser.CollationConnection(coll),
	)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, newCTASDistTaskErrorf("expected exactly one CTAS statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, newCTASDistTaskErrorf("unexpected CTAS statement type %T", stmt)
	}
	if createStmt.Select == nil {
		return nil, newCTASDistTaskError("CTAS statement does not contain a SELECT clause")
	}
	return createStmt, nil
}

func (s *ctasScheduler) Init() error {
	if err := json.Unmarshal(s.GetTask().Meta, &s.meta); err != nil {
		return newCTASDistTaskErrorf("unmarshal CTAS task meta failed: %v", err)
	}
	if err := validateCTASLoadTaskMeta(&s.meta); err != nil {
		return err
	}
	return s.BaseScheduler.Init()
}

func (*ctasScheduler) OnTick(context.Context, *proto.Task) {}

func (s *ctasScheduler) OnNextSubtasksBatch(
	_ context.Context,
	_ storage.TaskHandle,
	_ *proto.Task,
	execIDs []string,
	nextStep proto.Step,
) ([][]byte, error) {
	if nextStep != ctasLoadTaskStep {
		return nil, newCTASDistTaskErrorf("unexpected CTAS next step %s", proto.Step2Str(proto.CTAS, nextStep))
	}
	if len(execIDs) != 1 || execIDs[0] != s.meta.CTASSubmitterExecID {
		return nil, newCTASDistTaskErrorf("CTAS must run on submitter %s, got eligible execs %v",
			s.meta.CTASSubmitterExecID, execIDs)
	}

	// The select may read local temporary tables that only exist in the submitter
	// session, so CTAS must materialize exactly one subtask on the submitter node.
	return [][]byte{bytes.Clone(s.GetTask().Meta)}, nil
}

func (*ctasScheduler) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

func (s *ctasScheduler) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]string, error) {
	liveExecIDs, err := scheduler.GetLiveExecIDs(ctx)
	if err != nil {
		return nil, err
	}
	for _, execID := range liveExecIDs {
		if execID == s.meta.CTASSubmitterExecID {
			return []string{s.meta.CTASSubmitterExecID}, nil
		}
	}
	return nil, newCTASDistTaskErrorf("CTAS submitter exec %s is unavailable", s.meta.CTASSubmitterExecID)
}

func (*ctasScheduler) IsRetryableErr(error) bool {
	return false
}

func (*ctasScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return ctasLoadTaskStep
	case ctasLoadTaskStep:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

func (*ctasScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}
