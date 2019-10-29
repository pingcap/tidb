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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error) {
	fp := plannercore.TryFastPlan(sctx, node)
	if fp != nil {
		if !isPointGetWithoutDoubleRead(sctx, fp) {
			sctx.PrepareTxnFuture(ctx)
		}
		return fp, fp.OutputNames(), nil
	}

	sctx.PrepareTxnFuture(ctx)

	var oriHint *bindinfo.HintsSet
	if sctx.GetSessionVars().UsePlanBaselines {
		if stmtNode, ok := node.(ast.StmtNode); ok {
			oriHint = addHint(sctx, stmtNode)
		}
	}
	plan, names, err := optimize(ctx, sctx, node, is)
	// Restore the original hint in case of prepare stmt.
	if oriHint != nil {
		node = bindinfo.BindHint(node.(ast.StmtNode), oriHint)
		if err != nil {
			handleInvalidBindRecord(ctx, sctx, node.(ast.StmtNode))
		}
	}
	if err == nil || oriHint == nil {
		return plan, names, err
	}
	// Reoptimize after restore the original hint.
	return optimize(ctx, sctx, node, is)
}

func optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error) {
	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	hintProcessor := &plannercore.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder := plannercore.NewPlanBuilder(sctx, is, hintProcessor)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, err
		}
	}

	if err := plannercore.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, p.OutputNames(), err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, names, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		finalPlan, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, err
	}
	finalPlan, err := plannercore.DoOptimize(ctx, builder.GetOptFlag(), logic)
	return finalPlan, names, err
}

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
		binding := bindRecord.FirstUsingBinding()
		if binding == nil {
			return nil
		}
		metrics.BindUsageCounter.WithLabelValues(metrics.ScopeSession).Inc()
		oriHint := bindinfo.CollectHint(stmt)
		bindinfo.BindHint(stmt, binding.Hint)
		return oriHint
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	if bindRecord != nil {
		metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Inc()
		oriHint := bindinfo.CollectHint(stmt)
		bindinfo.BindHint(stmt, bindRecord.FirstUsingBinding().Hint)
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
	bindRecord := sessionHandle.GetBindRecord(normdOrigSQL, sctx.GetSessionVars().CurrentDB)
	if bindRecord != nil {
		bindRecord.FirstUsingBinding().Status = bindinfo.Invalid
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, sctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	if bindRecord != nil {
		binding := *bindRecord.FirstUsingBinding()
		binding.Status = bindinfo.Invalid
		record := &bindinfo.BindRecord{
			OriginalSQL: bindRecord.OriginalSQL,
			Db:          sctx.GetSessionVars().CurrentDB,
			Bindings:    []bindinfo.Binding{binding},
		}

		err := sessionHandle.AddBindRecord(nil, nil, record)
		if err != nil {
			logutil.Logger(ctx).Warn("handleInvalidBindRecord failed", zap.Error(err))
		}

		globalHandle := domain.GetDomain(sctx).BindHandle()
		dropBindRecord := &bindinfo.BindRecord{
			OriginalSQL: bindRecord.OriginalSQL,
			Db:          bindRecord.Db,
			Bindings:    []bindinfo.Binding{binding},
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

// OptimizeExecStmt to optimize prepare statement protocol "execute" statement
// this is a short path ONLY does things filling prepare related params
// for point select like plan which does not need extra things
func OptimizeExecStmt(ctx context.Context, sctx sessionctx.Context,
	execAst *ast.ExecuteStmt, is infoschema.InfoSchema) (plannercore.Plan, error) {
	var err error
	builder := plannercore.NewPlanBuilder(sctx, is, nil)
	p, err := builder.Build(ctx, execAst)
	if err != nil {
		return nil, err
	}
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err = execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return execPlan.Plan, err
	}
	err = errors.Errorf("invalid result plan type, should be Execute")
	return nil, err
}

// GenHintsFromSQL is used to generate hints from SQL and inject the hints into original SQL.
func GenHintsFromSQL(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (string, error) {
	err := plannercore.Preprocess(sctx, node, is)
	if err != nil {
		return "", err
	}
	oldValue := sctx.GetSessionVars().UsePlanBaselines
	// Disable baseline to avoid binding hints.
	sctx.GetSessionVars().UsePlanBaselines = false
	p, _, err := Optimize(ctx, sctx, node, is)
	sctx.GetSessionVars().UsePlanBaselines = oldValue
	if err != nil {
		return "", err
	}
	return plannercore.GenHintsFromPhysicalPlan(p), nil
}

func init() {
	plannercore.OptimizeAstNode = Optimize
	bindinfo.GenHintsFromSQL = GenHintsFromSQL
}
