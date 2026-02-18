// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	semv1 "github.com/pingcap/tidb/pkg/util/sem"
)

const (
	// TraceFormatRow indicates row tracing format.
	TraceFormatRow = "row"
	// TraceFormatJSON indicates json tracing format.
	TraceFormatJSON = "json"
	// TraceFormatLog indicates log tracing format.
	TraceFormatLog = "log"

	// TracePlanTargetEstimation indicates CE trace target for optimizer trace.
	TracePlanTargetEstimation = "estimation"
	// TracePlanTargetDebug indicates debug trace target for optimizer trace.
	TracePlanTargetDebug = "debug"
)

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (base.Plan, error) {
	p := &Trace{
		StmtNode:             trace.Stmt,
		ResolveCtx:           b.resolveCtx,
		Format:               trace.Format,
		OptimizerTrace:       trace.TracePlan,
		OptimizerTraceTarget: trace.TracePlanTarget,
	}
	// TODO: forbid trace plan if the statement isn't select read-only statement
	if trace.TracePlan {
		switch trace.TracePlanTarget {
		case "":
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "Dump_link", mysql.TypeVarchar, 128))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		case TracePlanTargetEstimation:
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "CE_trace", mysql.TypeVarchar, mysql.MaxBlobWidth))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		case TracePlanTargetDebug:
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "Debug_trace", mysql.TypeVarchar, mysql.MaxBlobWidth))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		default:
			return nil, errors.New("trace plan target should only be 'estimation'")
		}
		return p, nil
	}
	switch trace.Format {
	case TraceFormatRow:
		schema := newColumnsWithNames(3)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	case TraceFormatJSON:
		schema := newColumnsWithNames(1)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	case TraceFormatLog:
		schema := newColumnsWithNames(4)
		schema.Append(buildColumnWithName("", "time", mysql.TypeTimestamp, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "event", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "tags", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "spanName", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	default:
		return nil, errors.New("trace format should be one of 'row', 'log' or 'json'")
	}
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan base.Plan, format string, explainBriefBinary string, analyze, explore bool, execStmt ast.StmtNode, runtimeStats *execdetails.RuntimeStatsColl, sqlDigest string) (base.Plan, error) {
	format = strings.ToLower(format)
	if format == types.ExplainFormatTrueCardCost && !analyze {
		return nil, errors.Errorf("'explain format=%v' cannot work without 'analyze', please use 'explain analyze format=%v'", format, format)
	}

	p := &Explain{
		TargetPlan:       targetPlan,
		Format:           format,
		Analyze:          analyze,
		Explore:          explore,
		SQLDigest:        sqlDigest,
		ExecStmt:         execStmt,
		BriefBinaryPlan:  explainBriefBinary,
		RuntimeStatsColl: runtimeStats,
	}
	p.SetSCtx(b.ctx)
	return p, p.prepareSchema()
}

// buildExplainFor gets *last* (maybe running or finished) query plan from connection #connection id.
// See https://dev.mysql.com/doc/refman/8.0/en/explain-for-connection.html.
func (b *PlanBuilder) buildExplainFor(explainFor *ast.ExplainForStmt) (base.Plan, error) {
	processInfo, ok := b.ctx.GetSessionManager().GetProcessInfo(explainFor.ConnectionID)
	if !ok {
		return nil, plannererrors.ErrNoSuchThread.GenWithStackByArgs(explainFor.ConnectionID)
	}
	if b.ctx.GetSessionVars() != nil && b.ctx.GetSessionVars().User != nil {
		if b.ctx.GetSessionVars().User.Username != processInfo.User {
			err := plannererrors.ErrAccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username, b.ctx.GetSessionVars().User.Hostname)
			// Different from MySQL's behavior and document.
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
	}

	targetPlan, ok := processInfo.Plan.(base.Plan)
	explainForFormat := strings.ToLower(explainFor.Format)
	if !ok || targetPlan == nil {
		return &Explain{Format: explainForFormat}, nil
	}
	// TODO: support other explain formats for connection
	if explainForFormat != types.ExplainFormatBrief && explainForFormat != types.ExplainFormatROW && explainForFormat != types.ExplainFormatVerbose {
		return nil, errors.Errorf("explain format '%s' for connection is not supported now", explainForFormat)
	}
	return b.buildExplainPlan(targetPlan, explainForFormat, processInfo.BriefBinaryPlan, false, false, nil, processInfo.RuntimeStatsColl, "")
}

// getHintedStmtThroughPlanDigest gets the hinted SQL like `select /*+ ... */ * from t where ...` from `stmt_summary`
// for `explain [analyze] <plan_digest>` statements.
func getHintedStmtThroughPlanDigest(ctx base.PlanContext, planDigest string) (stmt ast.StmtNode, err error) {
	err = domain.GetDomain(ctx).AdvancedSysSessionPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			defer func(warnings []stmtctx.SQLWarn) {
				sctx.GetSessionVars().StmtCtx.SetWarnings(warnings)
			}(sctx.GetSessionVars().StmtCtx.GetWarnings())
			// The warnings will be broken in fetchRecordFromClusterStmtSummary(), so we need to save and restore it to make the
			// warnings for repeated SQL Digest work.
			schema, query, planHint, characterSet, collation, err := fetchRecordFromClusterStmtSummary(sctx.GetPlanCtx(), planDigest)
			if err != nil {
				return err
			}
			if query == "" {
				return errors.NewNoStackErrorf("can't find any plans for '%s'", planDigest)
			}

			p := parser.New()
			originNode, err := p.ParseOneStmt(query, characterSet, collation)
			if err != nil {
				return errors.NewNoStackErrorf("failed to parse SQL for Plan Digest: %v", planDigest)
			}
			hintedSQL := bindinfo.GenerateBindingSQL(originNode, planHint, schema)
			stmt, err = p.ParseOneStmt(hintedSQL, characterSet, collation)
			return err
		})
	})
	return
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (base.Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}

	if explain.PlanDigest != "" { // explain [analyze] <SQLDigest>
		hintedStmt, err := getHintedStmtThroughPlanDigest(b.ctx, explain.PlanDigest)
		if err != nil {
			return nil, err
		}
		explain.Stmt = hintedStmt
	}

	sctx, err := AsSctx(b.ctx)
	if err != nil {
		return nil, err
	}

	stmtCtx := sctx.GetSessionVars().StmtCtx
	var targetPlan base.Plan
	if explain.Stmt != nil && !explain.Explore {
		nodeW := resolve.NewNodeWWithCtx(explain.Stmt, b.resolveCtx)
		if stmtCtx.ExplainFormat == types.ExplainFormatPlanCache {
			targetPlan, _, err = OptimizeAstNode(ctx, sctx, nodeW, b.is)
		} else {
			targetPlan, _, err = OptimizeAstNodeNoCache(ctx, sctx, nodeW, b.is)
		}
		if err != nil {
			return nil, err
		}
	}

	return b.buildExplainPlan(targetPlan, explain.Format, "", explain.Analyze, explain.Explore, explain.Stmt, nil, explain.SQLDigest)
}

func (b *PlanBuilder) buildSelectInto(ctx context.Context, sel *ast.SelectStmt) (base.Plan, error) {
	if semv1.IsEnabled() {
		return nil, plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("SELECT INTO")
	}
	selectIntoInfo := sel.SelectIntoOpt
	sel.SelectIntoOpt = nil
	sctx, err := AsSctx(b.ctx)
	if err != nil {
		return nil, err
	}
	nodeW := resolve.NewNodeWWithCtx(sel, b.resolveCtx)
	targetPlan, _, err := OptimizeAstNodeNoCache(ctx, sctx, nodeW, b.is)
	if err != nil {
		return nil, err
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.FilePriv, "", "", "", plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("FILE"))
	return &SelectInto{
		TargetPlan:     targetPlan,
		IntoOpt:        selectIntoInfo,
		LineFieldsInfo: NewLineFieldsInfo(selectIntoInfo.FieldsInfo, selectIntoInfo.LinesInfo),
	}, nil
}
