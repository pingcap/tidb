// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
)

func (cc *clientConn) onExtensionConnEvent(tp extension.ConnEventTp, err error) {
	if cc.extensions == nil {
		return
	}

	var connInfo *variable.ConnectionInfo
	var activeRoles []*auth.RoleIdentity
	if ctx := cc.getCtx(); ctx != nil {
		sessVars := ctx.GetSessionVars()
		connInfo = sessVars.ConnectionInfo
		activeRoles = sessVars.ActiveRoles
	}

	if connInfo == nil {
		connInfo = cc.connectInfo()
	}

	info := &extension.ConnEventInfo{
		ConnectionInfo: connInfo,
		ActiveRoles:    activeRoles,
		Error:          err,
	}

	cc.extensions.OnConnectionEvent(tp, info)
}

func (cc *clientConn) onExtensionStmtEnd(node interface{}, stmtCtxValid bool, err error, args ...expression.Expression) {
	if !cc.extensions.HasStmtEventListeners() {
		return
	}

	ctx := cc.getCtx()
	if ctx == nil {
		return
	}

	tp := extension.StmtSuccess
	if err != nil {
		tp = extension.StmtError
	}

	sessVars := ctx.GetSessionVars()
	info := &stmtEventInfo{
		sessVars: sessVars,
		err:      err,
	}

	switch stmt := node.(type) {
	case *ast.ExecuteStmt:
		info.executeStmt = stmt
		info.stmtNode = stmt
	case PreparedStatement:
		info.executeStmtID = uint32(stmt.ID())
		prepared, _ := sessVars.GetPreparedStmtByID(info.executeStmtID)
		info.executeStmt = &ast.ExecuteStmt{
			PrepStmt:   prepared,
			BinaryArgs: args,
		}
		info.stmtNode = info.executeStmt
	case ast.StmtNode:
		info.stmtNode = stmt
	}

	if stmtCtxValid {
		info.sc = sessVars.StmtCtx
	} else {
		info.sc = &stmtctx.StatementContext{}
	}
	cc.extensions.OnStmtEvent(tp, info)
}

// onSQLParseFailed will be called when sql parse failed
func (cc *clientConn) onExtensionSQLParseFailed(sql string, err error) {
	if !cc.extensions.HasStmtEventListeners() {
		return
	}

	cc.extensions.OnStmtEvent(extension.StmtError, &stmtEventInfo{
		sessVars:        cc.getCtx().GetSessionVars(),
		err:             err,
		failedParseText: sql,
	})
}

func (cc *clientConn) onExtensionBinaryExecuteEnd(prep PreparedStatement, args []expression.Expression, stmtCtxValid bool, err error) {
	cc.onExtensionStmtEnd(prep, stmtCtxValid, err, args...)
}

type stmtEventInfo struct {
	sessVars *variable.SessionVars
	sc       *stmtctx.StatementContext
	stmtNode ast.StmtNode
	// execute info
	executeStmtID         uint32
	executeStmt           *ast.ExecuteStmt
	executePreparedCached bool
	executePreparedCache  *core.PlanCacheStmt
	// error will only be valid when the stmt is failed
	err error
	// failedParseText will only present on parse failed
	failedParseText string
}

func (e *stmtEventInfo) ConnectionInfo() *variable.ConnectionInfo {
	return e.sessVars.ConnectionInfo
}

func (e *stmtEventInfo) StmtNode() ast.StmtNode {
	return e.stmtNode
}

func (e *stmtEventInfo) ExecuteStmtNode() *ast.ExecuteStmt {
	return e.executeStmt
}

func (e *stmtEventInfo) ExecutePreparedStmt() ast.StmtNode {
	if cache := e.ensureExecutePreparedCache(); cache != nil {
		return cache.PreparedAst.Stmt
	}
	return nil
}

func (e *stmtEventInfo) PreparedParams() []types.Datum {
	return e.sessVars.PreparedParams
}

func (e *stmtEventInfo) OriginalText() string {
	if sql := e.ensureStmtContextOriginalSQL(); sql != "" {
		return sql
	}

	if e.executeStmtID != 0 {
		return binaryExecuteStmtText(e.executeStmtID)
	}

	return e.failedParseText
}

func (e *stmtEventInfo) SQLDigest() (normalized string, digest *parser.Digest) {
	if sql := e.ensureStmtContextOriginalSQL(); sql != "" {
		return e.sc.SQLDigest()
	}

	if e.executeStmtID != 0 {
		return binaryExecuteStmtText(e.executeStmtID), nil
	}

	return e.failedParseText, nil
}

func (e *stmtEventInfo) User() *auth.UserIdentity {
	return e.sessVars.User
}

func (e *stmtEventInfo) ActiveRoles() []*auth.RoleIdentity {
	return e.sessVars.ActiveRoles
}

func (e *stmtEventInfo) CurrentDB() string {
	return e.sessVars.CurrentDB
}

func (e *stmtEventInfo) AffectedRows() uint64 {
	if e.sc == nil || e.err != nil {
		return 0
	}
	return e.sc.AffectedRows()
}

func (e *stmtEventInfo) RelatedTables() []stmtctx.TableEntry {
	if e.sc == nil {
		return nil
	}
	return e.sc.Tables
}

func (e *stmtEventInfo) GetError() error {
	return e.err
}

func (e *stmtEventInfo) ensureExecutePreparedCache() *core.PlanCacheStmt {
	if e.executeStmt == nil {
		return nil
	}

	if !e.executePreparedCached {
		e.executePreparedCache, _ = core.GetPreparedStmt(e.executeStmt, e.sessVars)
		e.executePreparedCached = true
	}

	return e.executePreparedCache
}

func (e *stmtEventInfo) ensureStmtContextOriginalSQL() string {
	if e.sc == nil {
		return ""
	}

	if sql := e.sc.OriginalSQL; sql != "" {
		return sql
	}

	if planCache := e.ensureExecutePreparedCache(); planCache != nil {
		e.sc.OriginalSQL = planCache.PreparedAst.Stmt.Text()
		e.sc.InitSQLDigest(planCache.NormalizedSQL, planCache.SQLDigest)
	}

	if e.sc.OriginalSQL == "" && e.executeStmtID == 0 {
		e.sc.OriginalSQL = e.stmtNode.Text()
	}

	return e.sc.OriginalSQL
}

func binaryExecuteStmtText(id uint32) string {
	return fmt.Sprintf("BINARY EXECUTE (ID %d)", id)
}
