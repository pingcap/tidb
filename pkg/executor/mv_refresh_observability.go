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
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type mvRefreshObserveStepKind int

const (
	mvRefreshObserveStepMeta mvRefreshObserveStepKind = iota
	mvRefreshObserveStepFastMerge
	mvRefreshObserveStepCompleteDelete
	mvRefreshObserveStepCompleteInsert
)

type mvRefreshObserveStep struct {
	ID   string
	Name string
	Kind mvRefreshObserveStepKind
}

type mvRefreshStepObserver interface {
	OnStepStart(step mvRefreshObserveStep, at time.Time)
	OnStepEnd(step mvRefreshObserveStep, at time.Time, err error)
	OnStepPlanRows(step mvRefreshObserveStep, rows [][]string)
}

type mvRefreshObserveStepRuntime struct {
	StartAt  time.Time
	EndAt    time.Time
	Err      error
	PlanRows [][]string
}

type mvRefreshObserveCollector struct {
	stepRuntime map[string]*mvRefreshObserveStepRuntime
}

func newMVRefreshObserveCollector() *mvRefreshObserveCollector {
	return &mvRefreshObserveCollector{
		stepRuntime: make(map[string]*mvRefreshObserveStepRuntime),
	}
}

func (c *mvRefreshObserveCollector) ensureStep(step mvRefreshObserveStep) *mvRefreshObserveStepRuntime {
	runtime, ok := c.stepRuntime[step.ID]
	if !ok {
		runtime = &mvRefreshObserveStepRuntime{}
		c.stepRuntime[step.ID] = runtime
	}
	return runtime
}

func (c *mvRefreshObserveCollector) OnStepStart(step mvRefreshObserveStep, at time.Time) {
	runtime := c.ensureStep(step)
	runtime.StartAt = at
}

func (c *mvRefreshObserveCollector) OnStepEnd(step mvRefreshObserveStep, at time.Time, err error) {
	runtime := c.ensureStep(step)
	runtime.EndAt = at
	runtime.Err = err
}

func (c *mvRefreshObserveCollector) OnStepPlanRows(step mvRefreshObserveStep, rows [][]string) {
	runtime := c.ensureStep(step)
	runtime.PlanRows = clonePlanRows(rows)
}

func (c *mvRefreshObserveCollector) getStepRuntime(step mvRefreshObserveStep) *mvRefreshObserveStepRuntime {
	return c.stepRuntime[step.ID]
}

var (
	mvRefreshObserveStepTxnBegin = mvRefreshObserveStep{
		Name: "TXN_BEGIN",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepLockRefreshInfo = mvRefreshObserveStep{
		Name: "LOCK_REFRESH_INFO",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepInsertHistRunning = mvRefreshObserveStep{
		Name: "INSERT_HIST_RUNNING",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepDataChangeFastMerge = mvRefreshObserveStep{
		Name: "DATA_CHANGE_FAST_MERGE",
		Kind: mvRefreshObserveStepFastMerge,
	}
	mvRefreshObserveStepDataChangeCompleteDelete = mvRefreshObserveStep{
		Name: "DATA_CHANGE_COMPLETE_DELETE",
		Kind: mvRefreshObserveStepCompleteDelete,
	}
	mvRefreshObserveStepDataChangeCompleteInsert = mvRefreshObserveStep{
		Name: "DATA_CHANGE_COMPLETE_INSERT",
		Kind: mvRefreshObserveStepCompleteInsert,
	}
	mvRefreshObserveStepPersistRefreshInfo = mvRefreshObserveStep{
		Name: "PERSIST_REFRESH_INFO",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepTxnCommit = mvRefreshObserveStep{
		Name: "TXN_COMMIT",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepFinalizeHist = mvRefreshObserveStep{
		Name: "FINALIZE_HIST",
		Kind: mvRefreshObserveStepMeta,
	}
)

type mvRefreshStepSet struct {
	txnBegin                 mvRefreshObserveStep
	lockRefreshInfo          mvRefreshObserveStep
	insertHistRunning        mvRefreshObserveStep
	dataChangeFastMerge      mvRefreshObserveStep
	dataChangeCompleteDelete mvRefreshObserveStep
	dataChangeCompleteInsert mvRefreshObserveStep
	persistRefreshInfo       mvRefreshObserveStep
	txnCommit                mvRefreshObserveStep
	finalizeHist             mvRefreshObserveStep
	steps                    []mvRefreshObserveStep
}

func mvRefreshStepID(idx int) string {
	return fmt.Sprintf("S%02d", idx)
}

func newMVRefreshStepSet(refreshType ast.RefreshMaterializedViewType) (mvRefreshStepSet, error) {
	var set mvRefreshStepSet
	steps := make([]mvRefreshObserveStep, 0, 8)
	nextID := 1
	appendStep := func(step mvRefreshObserveStep) mvRefreshObserveStep {
		clone := step
		clone.ID = mvRefreshStepID(nextID)
		nextID++
		steps = append(steps, clone)
		return clone
	}

	set.txnBegin = appendStep(mvRefreshObserveStepTxnBegin)
	set.lockRefreshInfo = appendStep(mvRefreshObserveStepLockRefreshInfo)
	set.insertHistRunning = appendStep(mvRefreshObserveStepInsertHistRunning)

	switch refreshType {
	case ast.RefreshMaterializedViewTypeFast:
		set.dataChangeFastMerge = appendStep(mvRefreshObserveStepDataChangeFastMerge)
	case ast.RefreshMaterializedViewTypeComplete:
		set.dataChangeCompleteDelete = appendStep(mvRefreshObserveStepDataChangeCompleteDelete)
		set.dataChangeCompleteInsert = appendStep(mvRefreshObserveStepDataChangeCompleteInsert)
	default:
		return mvRefreshStepSet{}, errors.New("unknown REFRESH MATERIALIZED VIEW type")
	}

	set.persistRefreshInfo = appendStep(mvRefreshObserveStepPersistRefreshInfo)
	set.txnCommit = appendStep(mvRefreshObserveStepTxnCommit)
	set.finalizeHist = appendStep(mvRefreshObserveStepFinalizeHist)
	set.steps = steps
	return set, nil
}

var planIDSuffixPattern = regexp.MustCompile(`_(\d+)\b`)

// RefreshMaterializedViewDryRunExec executes "REFRESH MATERIALIZED VIEW ... DRY RUN".
// It does not execute the refresh. It only builds and returns the planned refresh steps.
type RefreshMaterializedViewDryRunExec struct {
	exec.BaseExecutor

	stmt *ast.RefreshMaterializedViewStmt
	is   infoschema.InfoSchema

	rows   []string
	cursor int
}

// Next implements the Executor Next interface.
func (e *RefreshMaterializedViewDryRunExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		rows, err := e.generateRows(ctx)
		if err != nil {
			return err
		}
		e.rows = rows
	}

	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		req.AppendString(0, e.rows[i])
	}
	e.cursor += numCurRows
	return nil
}

func (e *RefreshMaterializedViewDryRunExec) generateRows(ctx context.Context) ([]string, error) {
	if e.stmt == nil {
		return nil, errors.New("dry run refresh materialized view: missing statement")
	}

	steps, err := buildMVRefreshObserveSteps(e.stmt)
	if err != nil {
		return nil, err
	}

	var completeDeleteRows [][]string
	var completeInsertRows [][]string
	if e.stmt.Type == ast.RefreshMaterializedViewTypeComplete {
		completeDeleteRows, completeInsertRows, err = e.buildCompleteRefreshPlanRows(ctx)
		if err != nil {
			return nil, err
		}
	}

	rows := make([]string, 0, len(steps)*2)
	for _, step := range steps {
		rows = append(rows, fmt.Sprintf("[%s %s]", step.ID, step.Name))

		var stepRows [][]string
		switch step.Kind {
		case mvRefreshObserveStepFastMerge:
			stepRows, err = e.buildFastMergePlanRows(ctx)
		case mvRefreshObserveStepCompleteDelete:
			stepRows = completeDeleteRows
		case mvRefreshObserveStepCompleteInsert:
			stepRows = completeInsertRows
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(stepRows) == 0 {
			continue
		}
		rows = append(rows, formatMVRefreshDryRunPlanRows(stepRows)...)
	}
	return rows, nil
}

func (e *RefreshMaterializedViewDryRunExec) buildFastMergePlanRows(ctx context.Context) ([][]string, error) {
	refreshStmt := cloneRefreshMaterializedViewStmt(e.stmt)
	refreshStmt.ObserveType = ast.RefreshMaterializedViewObserveNone
	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt:                  refreshStmt,
		LastSuccessfulRefreshReadTSO: 0,
	}
	return e.renderPlanRowsForInternalStmt(ctx, implementStmt)
}

func (e *RefreshMaterializedViewDryRunExec) buildCompleteRefreshPlanRows(ctx context.Context) ([][]string, [][]string, error) {
	if e.stmt == nil {
		return nil, nil, errors.New("dry run refresh materialized view: missing statement")
	}

	refreshStmt := cloneRefreshMaterializedViewStmt(e.stmt)
	refreshStmt.ObserveType = ast.RefreshMaterializedViewObserveNone
	refreshExec := &RefreshMaterializedViewExec{
		BaseExecutor: exec.NewBaseExecutor(e.Ctx(), nil, 0),
	}
	schemaName, tblInfo, err := refreshExec.resolveRefreshMaterializedViewTarget(refreshStmt)
	if err != nil {
		return nil, nil, err
	}

	deleteSQL := sqlescape.MustEscapeSQL("DELETE FROM %n.%n", schemaName.O, refreshStmt.ViewName.Name.O)
	deleteRows, err := e.renderPlanRowsForInternalSQL(ctx, deleteSQL)
	if err != nil {
		return nil, nil, err
	}

	insertPrefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName.O, refreshStmt.ViewName.Name.O)
	/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
	insertSQL := insertPrefix + tblInfo.MaterializedView.SQLContent
	insertRows, err := e.renderPlanRowsForInternalSQL(ctx, insertSQL)
	if err != nil {
		return nil, nil, err
	}
	return deleteRows, insertRows, nil
}

func (e *RefreshMaterializedViewDryRunExec) renderPlanRowsForInternalStmt(ctx context.Context, stmt ast.StmtNode) ([][]string, error) {
	internalSctx, err := e.GetSysSession()
	if err != nil {
		return nil, err
	}
	defer e.ReleaseSysSession(ctx, internalSctx)

	restore := enableMVRefreshObserveMaintenanceFlags(internalSctx)
	defer restore()

	is := e.is
	if is == nil {
		var ok bool
		is, ok = e.Ctx().GetDomainInfoSchema().(infoschema.InfoSchema)
		if !ok {
			return nil, errors.New("dry run refresh materialized view: invalid infoschema")
		}
	}
	nodeW := resolve.NewNodeW(stmt)
	plan, _, err := plannercore.OptimizeAstNode(ctx, internalSctx, nodeW, is)
	if err != nil {
		return nil, err
	}
	explain := &plannercore.Explain{
		TargetPlan: plan,
		Format:     types.ExplainFormatBrief,
		Analyze:    false,
	}
	if plan != nil {
		explain.SetSCtx(plan.SCtx())
	}
	if err := explain.RenderResult(); err != nil {
		return nil, err
	}
	return explain.Rows, nil
}

func (e *RefreshMaterializedViewDryRunExec) renderPlanRowsForInternalSQL(ctx context.Context, sql string) ([][]string, error) {
	internalSctx, err := e.GetSysSession()
	if err != nil {
		return nil, err
	}
	defer e.ReleaseSysSession(ctx, internalSctx)

	restore := enableMVRefreshObserveMaintenanceFlags(internalSctx)
	defer restore()

	planSQL := fmt.Sprintf("EXPLAIN FORMAT='%s' %s", types.ExplainFormatBrief, sql)
	rs, err := internalSctx.GetSQLExecutor().ExecuteInternal(ctx, planSQL)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	fields := rs.Fields()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1024)
	closeErr := rs.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(rows) == 0 {
		return nil, nil
	}

	result := make([][]string, len(rows))
	for i, row := range rows {
		rowStr := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				rowStr[j] = "<nil>"
				continue
			}
			datum := row.GetDatum(j, &fields[j].Column.FieldType)
			value, err := datum.ToString()
			if err != nil {
				return nil, err
			}
			rowStr[j] = value
		}
		result[i] = rowStr
	}
	return result, nil
}

func formatMVRefreshDryRunPlanRows(rows [][]string) []string {
	if len(rows) == 0 {
		return nil
	}
	ret := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}

		id := planIDSuffixPattern.ReplaceAllString(row[0], "")
		parts := make([]string, 0, 5)
		parts = append(parts, id)

		var estRows string
		var task string
		var accessObj string
		var operatorInfo string
		if len(row) > 1 {
			estRows = strings.TrimSpace(row[1])
		}
		if len(row) > 2 {
			task = strings.TrimSpace(row[2])
		}
		if len(row) >= 5 {
			accessObj = row[3]
			operatorInfo = row[4]
		} else if len(row) >= 4 {
			operatorInfo = row[len(row)-1]
		}

		if estRows != "" {
			parts = append(parts, "estRows:"+estRows)
		}
		if task != "" {
			parts = append(parts, "task:"+task)
		}

		accessObj = normalizeDryRunPlanField(accessObj)
		operatorInfo = normalizeDryRunPlanField(operatorInfo)
		if accessObj != "" {
			parts = append(parts, accessObj)
		}
		if operatorInfo != "" {
			parts = append(parts, operatorInfo)
		}

		ret = append(ret, "  "+strings.Join(parts, " | "))
	}
	return ret
}

func normalizeDryRunPlanField(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "N/A") {
		return ""
	}
	return value
}

// RefreshMaterializedViewProfileExec executes "REFRESH MATERIALIZED VIEW ... WITH PROFILE".
// It executes the refresh and returns step-level runtime information.
type RefreshMaterializedViewProfileExec struct {
	exec.BaseExecutor

	stmt *ast.RefreshMaterializedViewStmt
	is   infoschema.InfoSchema

	rows   []string
	cursor int
}

// Next implements the Executor Next interface.
func (e *RefreshMaterializedViewProfileExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		rows, err := e.generateRows(ctx)
		if err != nil {
			return err
		}
		e.rows = rows
	}

	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		req.AppendString(0, e.rows[i])
	}
	e.cursor += numCurRows
	return nil
}

func (e *RefreshMaterializedViewProfileExec) generateRows(ctx context.Context) ([]string, error) {
	if e.stmt == nil {
		return nil, errors.New("profile refresh materialized view: missing statement")
	}

	steps, err := buildMVRefreshObserveSteps(e.stmt)
	if err != nil {
		return nil, err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)

	collector := newMVRefreshObserveCollector()
	refreshStmt := cloneRefreshMaterializedViewStmt(e.stmt)
	refreshStmt.ObserveType = ast.RefreshMaterializedViewObserveNone
	refreshExec := &RefreshMaterializedViewExec{
		BaseExecutor:          exec.NewBaseExecutor(e.Ctx(), nil, 0),
		stmt:                  refreshStmt,
		stepObserver:          collector,
		planFormatForObserver: types.ExplainFormatBrief,
	}
	if err := refreshExec.executeRefreshMaterializedView(ctx, refreshStmt); err != nil {
		return nil, err
	}

	rows := make([]string, 0, len(steps)*2)
	for _, step := range steps {
		runtime := collector.getStepRuntime(step)
		rows = append(rows, formatMVRefreshProfileStepHeader(step, runtime))
		if runtime == nil || len(runtime.PlanRows) == 0 {
			continue
		}
		rows = append(rows, formatMVRefreshProfilePlanRows(runtime.PlanRows)...)
	}
	return rows, nil
}

func formatMVRefreshProfileStepHeader(step mvRefreshObserveStep, runtime *mvRefreshObserveStepRuntime) string {
	info := buildMVRefreshAnalyzeExecutionInfo(runtime)
	if info == "" {
		return fmt.Sprintf("[%s %s]", step.ID, step.Name)
	}
	return fmt.Sprintf("[%s %s] %s", step.ID, step.Name, info)
}

func buildMVRefreshAnalyzeExecutionInfo(runtime *mvRefreshObserveStepRuntime) string {
	if runtime == nil || runtime.StartAt.IsZero() || runtime.EndAt.IsZero() {
		return ""
	}
	status := "success"
	if runtime.Err != nil {
		status = "failed"
	}
	return fmt.Sprintf("status:%s, time:%s", status, runtime.EndAt.Sub(runtime.StartAt))
}

func formatMVRefreshProfilePlanRows(rows [][]string) []string {
	if len(rows) == 0 {
		return nil
	}
	ret := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		id := planIDSuffixPattern.ReplaceAllString(row[0], "")
		parts := make([]string, 0, 9)
		parts = append(parts, id)

		var estRows string
		var actRows string
		var task string
		var accessObj string
		var execInfo string
		var operatorInfo string
		var memInfo string
		var diskInfo string
		if len(row) > 1 {
			estRows = strings.TrimSpace(row[1])
		}
		if len(row) > 2 {
			actRows = strings.TrimSpace(row[2])
		}
		if len(row) > 3 {
			task = strings.TrimSpace(row[3])
		}
		if len(row) > 4 {
			accessObj = row[4]
		}
		if len(row) > 5 {
			execInfo = row[5]
		}
		if len(row) > 6 {
			operatorInfo = row[6]
		} else if len(row) > 1 {
			operatorInfo = row[len(row)-1]
		}
		if len(row) > 7 {
			memInfo = row[7]
		}
		if len(row) > 8 {
			diskInfo = row[8]
		}

		if estRows != "" {
			parts = append(parts, "estRows:"+estRows)
		}
		if actRows != "" {
			parts = append(parts, "actRows:"+actRows)
		}
		if task != "" {
			parts = append(parts, "task:"+task)
		}

		accessObj = normalizeDryRunPlanField(accessObj)
		execInfo = normalizeDryRunPlanField(execInfo)
		operatorInfo = normalizeDryRunPlanField(operatorInfo)
		memInfo = normalizeDryRunPlanField(memInfo)
		diskInfo = normalizeDryRunPlanField(diskInfo)

		if accessObj != "" {
			parts = append(parts, accessObj)
		}
		if execInfo != "" {
			parts = append(parts, execInfo)
		}
		if operatorInfo != "" {
			parts = append(parts, operatorInfo)
		}
		if memInfo != "" {
			parts = append(parts, "memory:"+memInfo)
		}
		if diskInfo != "" {
			parts = append(parts, "disk:"+diskInfo)
		}

		ret = append(ret, "  "+strings.Join(parts, " | "))
	}
	return ret
}

func enableMVRefreshObserveMaintenanceFlags(sctx sessionctx.Context) func() {
	sessVars := sctx.GetSessionVars()
	origRestricted := sessVars.InRestrictedSQL
	origMaintenance := sessVars.InMaterializedViewMaintenance
	sessVars.InRestrictedSQL = true
	sessVars.InMaterializedViewMaintenance = true

	planVars := sctx.GetPlanCtx().GetSessionVars()
	origPlanRestricted := planVars.InRestrictedSQL
	origPlanMaintenance := planVars.InMaterializedViewMaintenance
	planVars.InRestrictedSQL = true
	planVars.InMaterializedViewMaintenance = true

	return func() {
		planVars.InRestrictedSQL = origPlanRestricted
		planVars.InMaterializedViewMaintenance = origPlanMaintenance
		sessVars.InRestrictedSQL = origRestricted
		sessVars.InMaterializedViewMaintenance = origMaintenance
	}
}

func cloneRefreshMaterializedViewStmt(stmt *ast.RefreshMaterializedViewStmt) *ast.RefreshMaterializedViewStmt {
	if stmt == nil {
		return nil
	}
	cloned := *stmt
	if stmt.ViewName != nil {
		viewName := *stmt.ViewName
		cloned.ViewName = &viewName
	}
	return &cloned
}

func clonePlanRows(rows [][]string) [][]string {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([][]string, len(rows))
	for i := range rows {
		cloned[i] = append([]string(nil), rows[i]...)
	}
	return cloned
}

func buildMVRefreshObserveSteps(stmt *ast.RefreshMaterializedViewStmt) ([]mvRefreshObserveStep, error) {
	if stmt == nil {
		return nil, errors.New("refresh materialized view: missing statement")
	}
	stepSet, err := newMVRefreshStepSet(stmt.Type)
	if err != nil {
		return nil, err
	}
	return stepSet.steps, nil
}
