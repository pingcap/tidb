// Copyright 2025 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pkg/errors"
)

var (
	_ WithTriggerSupport    = &InsertExec{}
	_ WithTriggerSupport    = &ReplaceExec{}
	_ WithTriggerSupport    = &DeleteExec{}
	_ WithTriggerSupport    = &UpdateExec{}
	_ tblctx.TriggerSupport = &TriggerExec{}
)

// WithTriggerSupport is an interface for executors that support triggers.
type WithTriggerSupport interface {
	GetTriggerExec() *TriggerExec
}

// TriggerExec is the executor for triggers.
type TriggerExec struct {
	ctx      context.Context
	rootExec *ExecStmt
	b        *executorBuilder
	p        *parser.Parser
	stmts    []*pendingStmt

	latestExec exec.Executor

	timings   []ast.TriggerTiming
	events    []ast.TriggerEvent
	tblInfos  []*model.TableInfo
	executors []exec.Executor
	oldData   [][]types.Datum
	newData   [][]types.Datum
	updated   [][]bool
}

func (e *TriggerExec) validate(newTbl *model.TableInfo) error {
	for _, tbl := range e.tblInfos {
		if tbl.ID == newTbl.ID {
			return exeerrors.ErrCantUpdateUsedTableInSfOrTrg.FastGenByArgs(newTbl.Name.O)
		}
	}
	return nil
}

func (e *TriggerExec) pushCtx(
	timing ast.TriggerTiming,
	event ast.TriggerEvent,
	oldData, newData []types.Datum, updated []bool,
	tblInfo *model.TableInfo,
) {
	e.timings = append(e.timings, timing)
	e.events = append(e.events, event)
	e.tblInfos = append(e.tblInfos, tblInfo)
	e.executors = append(e.executors, e.latestExec)
	sctx := e.rootExec.Ctx.GetSessionVars().StmtCtx
	sctx.TriggerCtx.InTrigger = true
	if len(sctx.TriggerCtx.OldData) > 0 {
		e.oldData = append(e.oldData, sctx.TriggerCtx.OldData)
	}
	if len(sctx.TriggerCtx.NewData) > 0 {
		e.newData = append(e.newData, sctx.TriggerCtx.NewData)
	}
	if len(sctx.TriggerCtx.Updated) > 0 {
		e.updated = append(e.updated, sctx.TriggerCtx.Updated)
	}
	sctx.TriggerCtx.OldData = deepCopyDatum(oldData)
	sctx.TriggerCtx.NewData = deepCopyDatum(newData)
	sctx.TriggerCtx.Updated = updated
	sctx.TriggerCtx.TableInfo = tblInfo
	sctx.TriggerCtx.HasTriggerCascades = true
	if len(oldData) > 0 {
		intest.Assert(len(tblInfo.Columns) == len(oldData))
	}
	if len(newData) > 0 {
		intest.Assert(len(tblInfo.Columns) == len(newData))
	}
}

func (e *TriggerExec) popCtx() {
	e.timings = e.timings[:len(e.timings)-1]
	e.tblInfos = e.tblInfos[:len(e.tblInfos)-1]
	e.events = e.events[:len(e.events)-1]
	e.executors = e.executors[:len(e.executors)-1]
	sctx := e.rootExec.Ctx.GetSessionVars().StmtCtx
	if len(e.timings) == 0 {
		sctx.TriggerCtx.InTrigger = false
		sctx.TriggerCtx.OldData = nil
		sctx.TriggerCtx.NewData = nil
		sctx.TriggerCtx.Updated = nil
		sctx.TriggerCtx.TableInfo = nil
		sctx.TriggerCtx.HasTriggerCascades = false
	} else {
		sctx.TriggerCtx.TableInfo = e.currentTableInfo()
		if len(e.oldData) > 0 {
			sctx.TriggerCtx.OldData = e.oldData[len(e.oldData)-1]
			e.oldData = e.oldData[:len(e.oldData)-1]
		} else {
			sctx.TriggerCtx.OldData = nil
		}
		if len(e.newData) > 0 {
			sctx.TriggerCtx.NewData = e.newData[len(e.newData)-1]
			e.newData = e.newData[:len(e.newData)-1]
		} else {
			sctx.TriggerCtx.NewData = nil
		}
		if len(e.updated) > 0 {
			sctx.TriggerCtx.Updated = e.updated[len(e.updated)-1]
			e.updated = e.updated[:len(e.updated)-1]
		} else {
			sctx.TriggerCtx.Updated = nil
		}
	}
}

func (e *TriggerExec) currentTiming() ast.TriggerTiming {
	return e.timings[len(e.timings)-1]
}

func (e *TriggerExec) currentTableInfo() *model.TableInfo {
	return e.tblInfos[len(e.tblInfos)-1]
}

func (e *TriggerExec) currentEvent() ast.TriggerEvent {
	return e.events[len(e.events)-1]
}

func (e *TriggerExec) currentExec() exec.Executor {
	return e.executors[len(e.executors)-1]
}

func (e *TriggerExec) currentStmt() *pendingStmt {
	if len(e.stmts) == 0 {
		return nil
	}
	return e.stmts[0]
}

func (e *TriggerExec) castNewData() error {
	triggerCtx := e.rootExec.Ctx.GetSessionVars().StmtCtx.TriggerCtx
	if len(triggerCtx.Updated) == 0 {
		return nil
	}
	tblInfo := triggerCtx.TableInfo
	for i, updated := range triggerCtx.Updated {
		if !updated {
			continue
		}
		casted, err := table.CastColumnValue(e.rootExec.Ctx.GetExprCtx(), triggerCtx.NewData[i], tblInfo.Columns[i], false, false)
		if err != nil {
			return err
		}
		triggerCtx.NewData[i] = casted
	}
	return nil
}

type pendingStmt struct {
	stmt    ast.StmtNode
	sqlMode mysql.SQLMode
}

func newTriggerExec(ctx context.Context, b *executorBuilder, a *ExecStmt) *TriggerExec {
	return &TriggerExec{
		ctx:      ctx,
		rootExec: a,
		b:        b,
	}
}

func (e *TriggerExec) initTriggerExec(
	currentExec exec.Executor,
	tblID2Table map[int64]table.Table,
	event ast.TriggerEvent,
) *TriggerExec {
	hasTrigger := false
	for _, tbl := range tblID2Table {
		if len(filterTriggers(tbl.Meta().Triggers, event)) > 0 {
			hasTrigger = true
			break
		}
	}
	if !hasTrigger {
		return nil
	}
	e.latestExec = currentExec
	e.b.ctx.GetSessionVars().StmtCtx.TriggerCtx.Exec = e
	e.p = parser.New()
	return e
}

func (e *TriggerExec) initTriggerExecWithOneTable(
	currentExec exec.Executor,
	tbl table.Table,
	event ...ast.TriggerEvent) *TriggerExec {
	trigs := filterTriggers(tbl.Meta().Triggers, event...)
	hasTrigger := len(trigs) > 0
	if !hasTrigger {
		return nil
	}
	e.latestExec = currentExec
	e.b.ctx.GetSessionVars().StmtCtx.TriggerCtx.Exec = e
	e.p = parser.New()
	return e
}

func filterTriggers(infos []*model.TriggerInfo, events ...ast.TriggerEvent) []*model.TriggerInfo {
	tmp := make([]*model.TriggerInfo, 0)
	for _, info := range infos {
		if slices.Contains(events, info.Event) {
			tmp = append(tmp, info)
		}

	}
	return tmp
}

func orderTriggersForExecution(trigs []*model.TriggerInfo) []*model.TriggerInfo {
	if len(trigs) <= 1 {
		return trigs
	}
	sorted := slices.Clone(trigs)
	slices.SortFunc(sorted, func(t1, t2 *model.TriggerInfo) int {
		if t1.CreatedTimestamp < t2.CreatedTimestamp {
			return -1
		}
		if t1.CreatedTimestamp > t2.CreatedTimestamp {
			return 1
		}
		if t1.Name.L < t2.Name.L {
			return -1
		}
		if t1.Name.L > t2.Name.L {
			return 1
		}
		return 0
	})

	ordered := make([]*model.TriggerInfo, 0, len(sorted))
	triggerPos := make(map[string]int, len(sorted))
	for _, trig := range sorted {
		insertPos := len(ordered)
		if trig.Order.OrderType != ast.TriggerOrderNone && trig.Order.OtherTriggerName.L != "" {
			pos, ok := triggerPos[trig.Order.OtherTriggerName.L]
			if ok {
				switch trig.Order.OrderType {
				case ast.TriggerOrderPrecedes:
					insertPos = pos
				case ast.TriggerOrderFollows:
					insertPos = pos + 1
				}
			}
		}

		if insertPos == len(ordered) {
			ordered = append(ordered, trig)
		} else {
			ordered = append(ordered, nil)
			copy(ordered[insertPos+1:], ordered[insertPos:])
			ordered[insertPos] = trig
		}
		for i := insertPos; i < len(ordered); i++ {
			triggerPos[ordered[i].Name.L] = i
		}
	}
	return ordered
}

func (e *TriggerExec) collectActivateTriggers() error {
	if len(e.timings) == 0 {
		return nil
	}
	e.stmts = make([]*pendingStmt, 0)
	trigs := orderTriggersForExecution(e.matchedTrigger(e.currentTableInfo(), e.currentTiming(), e.currentEvent()))
	for _, trig := range trigs {
		stmts, _, err := e.p.ParseSQL(trig.CreateSQL)
		if err != nil {
			return err
		}
		intest.Assert(len(stmts) == 1)
		createTriggerStmt, ok := stmts[0].(*ast.CreateTriggerStmt)
		if !ok {
			return errors.Errorf("stmt %T is not *ast.CreateTriggerStmt", stmts[0])
		}
		v := createTriggerStmt.TriggerBody
		e.stmts = append(e.stmts, &pendingStmt{stmt: v, sqlMode: trig.SQLMode})
	}
	return nil
}

func (e *TriggerExec) buildNextExecutor(ctx context.Context) (exec.Executor, error) {
	if len(e.timings) == 0 {
		return nil, nil
	}

	firstStmt := e.stmts[0]
	e.stmts = e.stmts[1:]
	stmtNode := firstStmt.stmt
	switch v := stmtNode.(type) {
	case *ast.SelectStmt:
		if v.SelectIntoOpt == nil {
			return nil, exeerrors.ErrSpNoRetset.FastGenByArgs("TRIGGER")
		}
	case *ast.SetOprStmt, *ast.ExplainStmt, *ast.ShowStmt:
		return nil, exeerrors.ErrSpNoRetset.FastGenByArgs("TRIGGER")
	}

	sctx := e.b.ctx
	nodeW := resolve.NewNodeW(stmtNode)

	err := plannercore.Preprocess(ctx, sctx, nodeW)
	if err != nil {
		return nil, err
	}
	finalPlan, err := planner.OptimizeForForeignKeyCascade(ctx, sctx.GetPlanCtx(), nodeW, e.b.is)
	if err != nil {
		return nil, err
	}
	newExec := e.b.build(finalPlan)
	return newExec, e.b.err
}

// OnInsertBefore handles BEFORE INSERT triggers.
func (e *TriggerExec) OnInsertBefore(tblInfo *model.TableInfo, dt []types.Datum, updated []bool) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingBefore, ast.TriggerEventInsert) {
		return nil
	}
	sctx := e.rootExec.Ctx.GetSessionVars().StmtCtx
	e.pushCtx(ast.TriggerTimingBefore, ast.TriggerEventInsert, nil, dt, updated, tblInfo)
	defer e.popCtx()
	err := e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
	if err != nil {
		return err
	}
	for i := range dt {
		dt[i] = sctx.TriggerCtx.NewData[i]
	}
	return nil
}

// OnInsertAfter handles AFTER INSERT triggers.
func (e *TriggerExec) OnInsertAfter(tblInfo *model.TableInfo, dt []types.Datum) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingAfter, ast.TriggerEventInsert) {
		return nil
	}
	e.pushCtx(ast.TriggerTimingAfter, ast.TriggerEventInsert, nil, dt, nil, tblInfo)
	defer e.popCtx()
	err := e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
	if err != nil {
		return err
	}
	return nil
}

// OnUpdateBefore handles BEFORE UPDATE triggers.
func (e *TriggerExec) OnUpdateBefore(tblInfo *model.TableInfo, oldDt []types.Datum, newDt []types.Datum) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingBefore, ast.TriggerEventUpdate) {
		return nil
	}
	e.pushCtx(ast.TriggerTimingBefore, ast.TriggerEventUpdate, oldDt, newDt, make([]bool, len(newDt)), tblInfo)
	defer e.popCtx()
	sctx := e.rootExec.Ctx.GetSessionVars().StmtCtx
	err := e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
	if err != nil {
		return err
	}
	for i := range newDt {
		newDt[i] = sctx.TriggerCtx.NewData[i]
	}
	return nil
}

// OnUpdateAfter handles AFTER UPDATE triggers.
func (e *TriggerExec) OnUpdateAfter(tblInfo *model.TableInfo, oldDt []types.Datum, newDt []types.Datum) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingAfter, ast.TriggerEventUpdate) {
		return nil
	}
	e.pushCtx(ast.TriggerTimingAfter, ast.TriggerEventUpdate, oldDt, newDt, nil, tblInfo)
	defer e.popCtx()
	return e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
}

// OnDeleteBefore handles BEFORE DELETE triggers.
func (e *TriggerExec) OnDeleteBefore(tblInfo *model.TableInfo, dt []types.Datum) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingBefore, ast.TriggerEventDelete) {
		return nil
	}
	e.pushCtx(ast.TriggerTimingBefore, ast.TriggerEventDelete, dt, nil, nil, tblInfo)
	defer e.popCtx()
	return e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
}

// OnDeleteAfter handles AFTER DELETE triggers.
func (e *TriggerExec) OnDeleteAfter(tblInfo *model.TableInfo, dt []types.Datum) error {
	if err := e.validate(tblInfo); err != nil {
		return err
	}
	if !e.foundTrigger(tblInfo, ast.TriggerTimingAfter, ast.TriggerEventDelete) {
		return nil
	}
	e.pushCtx(ast.TriggerTimingAfter, ast.TriggerEventDelete, dt, nil, nil, tblInfo)
	defer e.popCtx()
	return e.rootExec.handleStmtTrigger(e.ctx, e.currentExec())
}

func (e *TriggerExec) foundTrigger(tblInfo *model.TableInfo, timing ast.TriggerTiming, event ast.TriggerEvent) bool {
	for _, trig := range tblInfo.Triggers {
		if trig.Timing == timing && trig.Event == event {
			return true
		}
	}
	return false
}

func (e *TriggerExec) matchedTrigger(tblInfo *model.TableInfo, timing ast.TriggerTiming, event ast.TriggerEvent) []*model.TriggerInfo {
	ret := make([]*model.TriggerInfo, 0)
	for _, trig := range tblInfo.Triggers {
		if trig.Timing == timing && trig.Event == event {
			ret = append(ret, trig)
		}
	}
	return ret
}

func deepCopyDatum(dt []types.Datum) []types.Datum {
	if len(dt) == 0 {
		return nil
	}
	val := make([]types.Datum, len(dt))
	copy(val, dt)
	return val
}
