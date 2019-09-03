// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"context"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, error) {
<<<<<<< HEAD
	if _, containTiKV := sctx.GetSessionVars().GetIsolationReadEngines()[kv.TiKV]; containTiKV {
		fp := plannercore.TryFastPlan(sctx, node)
		if fp != nil {
			return fp, nil
=======
	fp := plannercore.TryFastPlan(sctx, node)
	if fp != nil {
		if !isPointGetWithoutDoubleRead(sctx, fp) {
			sctx.PrepareTxnFuture(ctx)
		}
		return fp, nil
	}

	sctx.PrepareTxnFuture(ctx)

	var oriHint *bindinfo.HintsSet
	if stmtNode, ok := node.(ast.StmtNode); ok {
		oriHint = addHint(sctx, stmtNode)
	}
	plan, err := optimize(ctx, sctx, node, is)
	// Restore the original hint in case of prepare stmt.
	if oriHint != nil {
		node = bindinfo.BindHint(node.(ast.StmtNode), oriHint)
		if err != nil {
			handleInvalidBindRecord(ctx, sctx, node.(ast.StmtNode))
>>>>>>> dffe293... *: not send tso request when point get with max tso (#11981)
		}
	}

	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	hintProcessor := &plannercore.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder := plannercore.NewPlanBuilder(sctx, is, hintProcessor)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, err
		}
	}

	if err := plannercore.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, err
	}

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		return cascades.FindBestPlan(sctx, logic)
	}
	return plannercore.DoOptimize(ctx, builder.GetOptFlag(), logic)
}

<<<<<<< HEAD
=======
func extractSelectAndNormalizeDigest(stmtNode ast.StmtNode) (*ast.SelectStmt, string, string) {
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			normalizeExplainSQL := parser.Normalize(x.Text())
			idx := strings.Index(normalizeExplainSQL, "select")
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestHash(normalizeSQL)
			return x.Stmt.(*ast.SelectStmt), normalizeSQL, hash
		}
	case *ast.SelectStmt:
		normalizedSQL, hash := parser.NormalizeDigest(x.Text())
		return x, normalizedSQL, hash
	}
	return nil, "", ""
}

func addHint(ctx sessionctx.Context, stmtNode ast.StmtNode) *bindinfo.HintsSet {
	// When the domain is initializing, the bind will be nil.
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return nil
	}
	selectStmt, normalizedSQL, hash := extractSelectAndNormalizeDigest(stmtNode)
	if selectStmt == nil {
		return nil
	}
	return addHintForSelect(ctx, selectStmt, normalizedSQL, hash)
}

func addHintForSelect(ctx sessionctx.Context, stmt ast.StmtNode, normdOrigSQL, hash string) *bindinfo.HintsSet {
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord != nil {
		if bindRecord.Status == bindinfo.Invalid {
			return nil
		}
		if bindRecord.Status == bindinfo.Using {
			metrics.BindUsageCounter.WithLabelValues(metrics.ScopeSession).Inc()
			oriHint := bindinfo.CollectHint(stmt)
			bindinfo.BindHint(stmt, bindRecord.HintsSet)
			return oriHint
		}
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	if bindRecord != nil {
		metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Inc()
		oriHint := bindinfo.CollectHint(stmt)
		bindinfo.BindHint(stmt, bindRecord.HintsSet)
		return oriHint
	}
	return nil
}

func handleInvalidBindRecord(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) {
	selectStmt, normdOrigSQL, hash := extractSelectAndNormalizeDigest(stmtNode)
	if selectStmt == nil {
		return
	}
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindMeta := sessionHandle.GetBindRecord(normdOrigSQL, sctx.GetSessionVars().CurrentDB)
	if bindMeta != nil {
		bindMeta.Status = bindinfo.Invalid
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	bindMeta = globalHandle.GetBindRecord(hash, normdOrigSQL, sctx.GetSessionVars().CurrentDB)
	if bindMeta == nil {
		bindMeta = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	if bindMeta != nil {
		record := &bindinfo.BindRecord{
			OriginalSQL: bindMeta.OriginalSQL,
			BindSQL:     bindMeta.BindSQL,
			Db:          sctx.GetSessionVars().CurrentDB,
			Charset:     bindMeta.Charset,
			Collation:   bindMeta.Collation,
			Status:      bindinfo.Invalid,
		}

		err := sessionHandle.AddBindRecord(record)
		if err != nil {
			logutil.Logger(ctx).Warn("handleInvalidBindRecord failed", zap.Error(err))
		}

		globalHandle := domain.GetDomain(sctx).BindHandle()
		dropBindRecord := &bindinfo.BindRecord{
			OriginalSQL: bindMeta.OriginalSQL,
			Db:          bindMeta.Db,
		}
		globalHandle.AddDropInvalidBindTask(dropBindRecord)
	}
}

// isPointGetWithoutDoubleRead returns true when meets following conditions:
//  1. ctx is auto commit tagged.
//  2. plan is point get by pk.
func isPointGetWithoutDoubleRead(ctx sessionctx.Context, p plannercore.Plan) bool {
	if !ctx.GetSessionVars().IsAutocommit() {
		return false
	}

	v, ok := p.(*plannercore.PointGetPlan)
	return ok && v.IndexInfo == nil
}

>>>>>>> dffe293... *: not send tso request when point get with max tso (#11981)
func init() {
	plannercore.OptimizeAstNode = Optimize
}
