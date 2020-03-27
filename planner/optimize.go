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
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
<<<<<<< HEAD
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, error) {
	fp := plannercore.TryFastPlan(sctx, node)
	if fp != nil {
		return fp, nil
=======
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error) {
	if _, isolationReadContainTiKV := sctx.GetSessionVars().GetIsolationReadEngines()[kv.TiKV]; isolationReadContainTiKV {
		fp := plannercore.TryFastPlan(sctx, node)
		if fp != nil {
			if !useMaxTS(sctx, fp) {
				sctx.PrepareTSFuture(ctx)
			}
			return fp, fp.OutputNames(), nil
		}
	}

	sctx.PrepareTSFuture(ctx)

	tableHints := extractTableHintsFromStmtNode(node)
	stmtHints, warns := handleStmtHints(tableHints)
	defer func() {
		sctx.GetSessionVars().StmtCtx.StmtHints = stmtHints
		for _, warn := range warns {
			sctx.GetSessionVars().StmtCtx.AppendWarning(warn)
		}
	}()
	sctx.GetSessionVars().StmtCtx.StmtHints = stmtHints
	bestPlan, names, _, err := optimize(ctx, sctx, node, is)
	if err != nil {
		return nil, nil, err
	}
	if !(sctx.GetSessionVars().UsePlanBaselines || sctx.GetSessionVars().EvolvePlanBaselines) {
		return bestPlan, names, nil
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
	binding := bindRecord.FindBinding(bestPlanHint)
	// If the best bestPlan is in baselines, just use it.
	if binding != nil && binding.Status == bindinfo.Using {
		if sctx.GetSessionVars().UsePlanBaselines {
			stmtHints, warns = handleStmtHints(binding.Hint.GetFirstTableHints())
		}
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
		curStmtHints, curWarns := handleStmtHints(binding.Hint.GetFirstTableHints())
		sctx.GetSessionVars().StmtCtx.StmtHints = curStmtHints
		plan, _, cost, err := optimize(ctx, sctx, node, is)
		if err != nil {
			binding.Status = bindinfo.Invalid
			handleInvalidBindRecord(ctx, sctx, scope, bindinfo.BindRecord{
				OriginalSQL: bindRecord.OriginalSQL,
				Db:          bindRecord.Db,
				Bindings:    []bindinfo.Binding{binding},
			})
			continue
		}
		if cost < bestCostAmongHints {
			if sctx.GetSessionVars().UsePlanBaselines {
				stmtHints, warns = curStmtHints, curWarns
			}
			bestCostAmongHints = cost
			bestPlanAmongHints = plan
		}
>>>>>>> c1e44a7... planner: don't choose point get when none tikv in isolation read (#15147)
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

func init() {
	plannercore.OptimizeAstNode = Optimize
}
