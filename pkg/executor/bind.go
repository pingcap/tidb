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
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// SQLBindExec represents a bind executor.
type SQLBindExec struct {
	exec.BaseExecutor

	sqlBindOp    plannercore.SQLBindOpType
	normdOrigSQL string
	bindSQL      string
	charset      string
	collation    string
	db           string
	isGlobal     bool
	isUniversal  bool // for universal binding
	bindAst      ast.StmtNode
	newStatus    string
	source       string // by manual or from history, only in create stmt
	sqlDigest    string
	planDigest   string
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
	var bindInfo *bindinfo.Binding
	if e.bindSQL != "" {
		bindInfo = &bindinfo.Binding{
			BindSQL:   e.bindSQL,
			Charset:   e.charset,
			Collation: e.collation,
		}
	}
	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		err := handle.DropSessionBinding(e.normdOrigSQL, e.db, bindInfo)
		return err
	}
	affectedRows, err := domain.GetDomain(e.Ctx()).BindHandle().DropGlobalBinding(e.normdOrigSQL, e.db, bindInfo)
	e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(affectedRows)
	return err
}

func (e *SQLBindExec) dropSQLBindByDigest() error {
	if e.sqlDigest == "" {
		return errors.New("sql digest is empty")
	}
	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		err := handle.DropSessionBindingByDigest(e.sqlDigest)
		return err
	}
	affectedRows, err := domain.GetDomain(e.Ctx()).BindHandle().DropGlobalBindingByDigest(e.sqlDigest)
	e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(affectedRows)
	return err
}

func (e *SQLBindExec) setBindingStatus() error {
	var bindInfo *bindinfo.Binding
	if e.bindSQL != "" {
		bindInfo = &bindinfo.Binding{
			BindSQL:   e.bindSQL,
			Charset:   e.charset,
			Collation: e.collation,
		}
	}
	ok, err := domain.GetDomain(e.Ctx()).BindHandle().SetGlobalBindingStatus(e.normdOrigSQL, bindInfo, e.newStatus)
	if err == nil && !ok {
		warningMess := errors.NewNoStackError("There are no bindings can be set the status. Please check the SQL text")
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(warningMess)
	}
	return err
}

func (e *SQLBindExec) setBindingStatusByDigest() error {
	ok, err := domain.GetDomain(e.Ctx()).BindHandle().SetGlobalBindingStatusByDigest(e.newStatus, e.sqlDigest)
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

	bindingType := bindinfo.TypeNormal
	if e.isUniversal {
		bindingType = bindinfo.TypeUniversal
	}

	bindInfo := bindinfo.Binding{
		BindSQL:    e.bindSQL,
		Charset:    e.charset,
		Collation:  e.collation,
		Status:     bindinfo.Enabled,
		Source:     e.source,
		SQLDigest:  e.sqlDigest,
		PlanDigest: e.planDigest,
		Type:       bindingType,
	}
	record := &bindinfo.BindRecord{
		OriginalSQL: e.normdOrigSQL,
		Db:          e.db,
		Bindings:    []bindinfo.Binding{bindInfo},
	}
	if !e.isGlobal {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		return handle.CreateSessionBinding(e.Ctx(), record)
	}
	return domain.GetDomain(e.Ctx()).BindHandle().CreateGlobalBinding(e.Ctx(), record)
}

func (e *SQLBindExec) flushBindings() error {
	return domain.GetDomain(e.Ctx()).BindHandle().FlushGlobalBindings()
}

func (e *SQLBindExec) captureBindings() {
	domain.GetDomain(e.Ctx()).BindHandle().CaptureBaselines()
}

func (e *SQLBindExec) reloadBindings() error {
	return domain.GetDomain(e.Ctx()).BindHandle().ReloadGlobalBindings()
}
