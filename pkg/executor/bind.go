// Copyright 2019 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// SQLBindExec represents a bind executor.
type SQLBindExec struct {
	exec.BaseExecutor

	isGlobal  bool
	sqlBindOp plannercore.SQLBindOpType
	details   []*plannercore.SQLBindOpDetail
}

// Next implements the Executor Next interface.
func (e *SQLBindExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	switch e.sqlBindOp {
	case plannercore.OpSQLBindCreate:
		return e.createSQLBind()
	case plannercore.OpSQLBindDrop:
		return e.dropSQLBind()
	case plannercore.OpSQLBindDropByDigest:
		return e.dropSQLBindByDigest()
	case plannercore.OpFlushBindings:
		return e.flushBindings()
	case plannercore.OpCaptureBindings:
		e.captureBindings()
	case plannercore.OpEvolveBindings:
		return nil // not support yet
	case plannercore.OpReloadBindings:
		return e.reloadBindings()
	case plannercore.OpSetBindingStatus:
		return e.setBindingStatus()
	case plannercore.OpSetBindingStatusByDigest:
		return e.setBindingStatusByDigest()
	default:
		return errors.Errorf("unsupported SQL bind operation: %v", e.sqlBindOp)
	}
	return nil
}

func (e *SQLBindExec) dropSQLBind() error {
	if len(e.details) != 1 {
		return errors.New("SQLBindExec: dropSQLBind should only have one SQLBindOpDetail")
	}
	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		err := handle.DropSessionBinding([]string{e.details[0].SQLDigest})
		return err
	}
	affectedRows, err := domain.GetDomain(e.Ctx()).BindHandle().DropGlobalBinding([]string{e.details[0].SQLDigest})
	e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(affectedRows)
	return err
}

func (e *SQLBindExec) dropSQLBindByDigest() error {
	sqlDigests := make([]string, 0, len(e.details))
	for _, detail := range e.details {
		if detail.SQLDigest == "" {
			return errors.New("SQLBindExec: dropSQLBindByDigest shouldn't contain empty SQLDigest")
		}
		sqlDigests = append(sqlDigests, detail.SQLDigest)
	}
	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		err := handle.DropSessionBinding(sqlDigests)
		return err
	}
	affectedRows, err := domain.GetDomain(e.Ctx()).BindHandle().DropGlobalBinding(sqlDigests)
	e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(affectedRows)
	return err
}

func (e *SQLBindExec) setBindingStatus() error {
	if len(e.details) != 1 {
		return errors.New("SQLBindExec: setBindingStatus should only have one SQLBindOpDetail")
	}
	_, sqlDigest := parser.NormalizeDigestForBinding(e.details[0].NormdOrigSQL)
	ok, err := domain.GetDomain(e.Ctx()).BindHandle().SetGlobalBindingStatus(e.details[0].NewStatus, sqlDigest.String())
	if err == nil && !ok {
		warningMess := errors.NewNoStackError("There are no bindings can be set the status. Please check the SQL text")
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(warningMess)
	}
	return err
}

func (e *SQLBindExec) setBindingStatusByDigest() error {
	if len(e.details) != 1 {
		return errors.New("SQLBindExec: setBindingStatusByDigest should only have one SQLBindOpDetail")
	}
	ok, err := domain.GetDomain(e.Ctx()).BindHandle().SetGlobalBindingStatus(
		e.details[0].NewStatus,
		e.details[0].SQLDigest,
	)
	if err == nil && !ok {
		warningMess := errors.NewNoStackError("There are no bindings can be set the status. Please check the SQL text")
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(warningMess)
	}
	return err
}

func (e *SQLBindExec) createSQLBind() error {
	// For audit log, SQLBindExec execute "explain" statement internally, save and recover stmtctx
	// is necessary to avoid 'create binding' been recorded as 'explain'.
	saveStmtCtx := e.Ctx().GetSessionVars().StmtCtx
	defer func() {
		// But we need to restore the SET_VAR's setting.
		for name, val := range e.Ctx().GetSessionVars().StmtCtx.SetVarHintRestore {
			saveStmtCtx.AddSetVarHintRestore(name, val)
		}
		e.Ctx().GetSessionVars().StmtCtx = saveStmtCtx
	}()

	bindings := make([]*bindinfo.Binding, 0, len(e.details))
	for _, detail := range e.details {
		binding := bindinfo.Binding{
			OriginalSQL: detail.NormdOrigSQL,
			Db:          detail.Db,
			BindSQL:     detail.BindSQL,
			Charset:     detail.Charset,
			Collation:   detail.Collation,
			Status:      bindinfo.Enabled,
			Source:      detail.Source,
			SQLDigest:   detail.SQLDigest,
			PlanDigest:  detail.PlanDigest,
		}
		bindings = append(bindings, &binding)
	}

	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		return handle.CreateSessionBinding(e.Ctx(), bindings)
	}
	return domain.GetDomain(e.Ctx()).BindHandle().CreateGlobalBinding(e.Ctx(), bindings)
}

func (e *SQLBindExec) flushBindings() error {
	return domain.GetDomain(e.Ctx()).BindHandle().FlushGlobalBindings()
}

func (e *SQLBindExec) captureBindings() {
	domain.GetDomain(e.Ctx()).BindHandle().CaptureBaselines()
}

func (e *SQLBindExec) reloadBindings() error {
	return domain.GetDomain(e.Ctx()).BindHandle().LoadFromStorageToCache(true)
}
