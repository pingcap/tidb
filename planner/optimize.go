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
	"math"
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

	bestPlan, names, _, err := optimize(ctx, sctx, node, is)
	if err != nil || !sctx.GetSessionVars().UsePlanBaselines {
		return bestPlan, names, err
	}
	stmtNode, ok := node.(ast.StmtNode)
	if !ok {
		return bestPlan, names, nil
	}
	bindRecord, scope := getBindRecord(sctx, stmtNode)
	if bindRecord == nil {
		return bestPlan, names, nil
	}
	bestPlanHint := plannercore.GenHintsFromPhysicalPlan(bestPlan)
	// If the best bestPlan is in baselines, just use it.
	if bindRecord.FindUsingBinding(bestPlanHint) != nil {
		return bestPlan, names, nil
	}
	bestCostAmongHints := math.MaxFloat64
	var bestPlanAmongHints plannercore.Plan
	originHints := bindinfo.CollectHint(stmtNode)
	// Try to find the best binding.
	for _, binding := range bindRecord.Bindings {
		if binding.Status != bindinfo.Using {
			continue
		}
		metrics.BindUsageCounter.WithLabelValues(scope).Inc()
		bindinfo.BindHint(stmtNode, binding.Hint)
		plan, _, cost, err := optimize(ctx, sctx, node, is)
		if err != nil {
			binding.Status = bindinfo.Invalid
			handleInvalidBindRecord(sctx, scope, bindinfo.BindRecord{
				OriginalSQL: bindRecord.OriginalSQL,
				Db:          bindRecord.Db,
				Bindings:    []bindinfo.Binding{binding},
			})
			continue
		}
		if cost < bestCostAmongHints {
			bestCostAmongHints = cost
			bestPlanAmongHints = plan
		}
	}
	// Restore the hint to avoid changing the stmt node.
	bindinfo.BindHint(stmtNode, originHints)
	// TODO: Evolve the plan baselines using best plan.
	if bestPlanAmongHints != nil {
		return bestPlanAmongHints, names, nil
	}
	return bestPlan, names, nil
}

func optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, float64, error) {
	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	hintProcessor := &plannercore.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder := plannercore.NewPlanBuilder(sctx, is, hintProcessor)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, 0, err
	}

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := plannercore.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, p.OutputNames(), 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, names, 0, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		finalPlan, cost, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, cost, err
	}
	finalPlan, cost, err := plannercore.DoOptimize(ctx, builder.GetOptFlag(), logic)
	return finalPlan, names, cost, err
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

func getBindRecord(ctx sessionctx.Context, stmt ast.StmtNode) (*bindinfo.BindRecord, string) {
	// When the domain is initializing, the bind will be nil.
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return nil, ""
	}
	selectStmt, normalizedSQL, hash := extractSelectAndNormalizeDigest(stmt)
	if selectStmt == nil {
		return nil, ""
	}
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(normalizedSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord != nil {
		if bindRecord.HasUsingBinding() {
			return bindRecord, metrics.ScopeSession
		}
		return nil, ""
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normalizedSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normalizedSQL, "")
	}
	return bindRecord, metrics.ScopeGlobal
}

func handleInvalidBindRecord(sctx sessionctx.Context, level string, bindRecord bindinfo.BindRecord) {
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	sessionHandle.DropBindRecord(&bindRecord)
	if level == metrics.ScopeSession {
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	globalHandle.AddDropInvalidBindTask(&bindRecord)
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
