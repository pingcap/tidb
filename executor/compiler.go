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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/zap"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (_ *ExecStmt, err error) {
	r, ctx := tracing.StartRegionEx(ctx, "executor.Compile")
	defer r.End()

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if str, ok := r.(string); !ok || !strings.Contains(str, memory.PanicMemoryExceed) {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("compile SQL panic", zap.String("SQL", stmtNode.Text()), zap.Stack("stack"), zap.Any("recover", r))
	}()

	c.Ctx.GetSessionVars().StmtCtx.IsReadOnly = plannercore.IsReadOnly(stmtNode, c.Ctx.GetSessionVars())

	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(ctx, c.Ctx,
		stmtNode,
		plannercore.WithPreprocessorReturn(ret),
		plannercore.InitTxnContextProvider,
	)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("assertTxnManagerInCompile", func() {
		sessiontxn.RecordAssert(c.Ctx, "assertTxnManagerInCompile", true)
		sessiontxn.AssertTxnManagerInfoSchema(c.Ctx, ret.InfoSchema)
		if ret.LastSnapshotTS != 0 {
			staleread.AssertStmtStaleness(c.Ctx, true)
			sessiontxn.AssertTxnManagerReadTS(c.Ctx, ret.LastSnapshotTS)
		}
	})

	is := sessiontxn.GetTxnManager(c.Ctx).GetTxnInfoSchema()
	sessVars := c.Ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	// handle the execute statement
	var (
		pointPlanShortPathOK bool
		preparedObj          *plannercore.PlanCacheStmt
	)

	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if preparedObj, err = plannercore.GetPreparedStmt(execStmt, sessVars); err != nil {
			return nil, err
		}
		if pointPlanShortPathOK, err = plannercore.IsPointPlanShortPathOK(c.Ctx, is, preparedObj); err != nil {
			return nil, err
		}
	}
	finalPlan, names, err := planner.Optimize(ctx, c.Ctx, stmtNode, is)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("assertStmtCtxIsStaleness", func(val failpoint.Value) {
		staleread.AssertStmtStaleness(c.Ctx, val.(bool))
	})

	if preparedObj != nil {
		CountStmtNode(preparedObj.PreparedAst.Stmt, sessVars.InRestrictedSQL)
	} else {
		CountStmtNode(stmtNode, sessVars.InRestrictedSQL)
	}
	var lowerPriority bool
	if c.Ctx.GetSessionVars().StmtCtx.Priority == mysql.NoPriority {
		lowerPriority = needLowerPriority(finalPlan)
	}
	stmtCtx.SetPlan(finalPlan)
	stmt := &ExecStmt{
		GoCtx:         ctx,
		InfoSchema:    is,
		Plan:          finalPlan,
		LowerPriority: lowerPriority,
		Text:          stmtNode.Text(),
		StmtNode:      stmtNode,
		Ctx:           c.Ctx,
		OutputNames:   names,
		Ti:            &TelemetryInfo{},
	}
	if pointPlanShortPathOK {
		if ep, ok := stmt.Plan.(*plannercore.Execute); ok {
			if pointPlan, ok := ep.Plan.(*plannercore.PointGetPlan); ok {
				stmtCtx.SetPlan(stmt.Plan)
				stmtCtx.SetPlanDigest(preparedObj.NormalizedPlan, preparedObj.PlanDigest)
				stmt.Plan = pointPlan
				stmt.PsStmt = preparedObj
			} else {
				// invalid the previous cached point plan
				preparedObj.PreparedAst.CachedPlan = nil
			}
		}
	}
	if err = sessiontxn.OptimizeWithPlanAndThenWarmUp(c.Ctx, stmt.Plan); err != nil {
		return nil, err
	}
	return stmt, nil
}

// needLowerPriority checks whether it's needed to lower the execution priority
// of a query.
// If the estimated output row count of any operator in the physical plan tree
// is greater than the specific threshold, we'll set it to lowPriority when
// sending it to the coprocessor.
func needLowerPriority(p plannercore.Plan) bool {
	switch x := p.(type) {
	case plannercore.PhysicalPlan:
		return isPhysicalPlanNeedLowerPriority(x)
	case *plannercore.Execute:
		return needLowerPriority(x.Plan)
	case *plannercore.Insert:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.Delete:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.Update:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	}
	return false
}

func isPhysicalPlanNeedLowerPriority(p plannercore.PhysicalPlan) bool {
	expensiveThreshold := int64(config.GetGlobalConfig().Log.ExpensiveThreshold)
	if int64(p.StatsCount()) > expensiveThreshold {
		return true
	}

	for _, child := range p.Children() {
		if isPhysicalPlanNeedLowerPriority(child) {
			return true
		}
	}

	return false
}

// CountStmtNode records the number of statements with the same type.
func CountStmtNode(stmtNode ast.StmtNode, inRestrictedSQL bool) {
	if inRestrictedSQL {
		return
	}

	typeLabel := ast.GetStmtLabel(stmtNode)

	if config.GetGlobalConfig().Status.RecordQPSbyDB || config.GetGlobalConfig().Status.RecordDBLabel {
		dbLabels := getStmtDbLabel(stmtNode)
		switch {
		case config.GetGlobalConfig().Status.RecordQPSbyDB:
			for dbLabel := range dbLabels {
				metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel).Inc()
			}
		case config.GetGlobalConfig().Status.RecordDBLabel:
			for dbLabel := range dbLabels {
				metrics.StmtNodeCounter.WithLabelValues(typeLabel, dbLabel).Inc()
			}
		}
	} else {
		metrics.StmtNodeCounter.WithLabelValues(typeLabel, "").Inc()
	}
}

func getStmtDbLabel(stmtNode ast.StmtNode) map[string]struct{} {
	dbLabelSet := make(map[string]struct{})

	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		if x.Table != nil {
			dbLabel := x.Table.Schema.O
			dbLabelSet[dbLabel] = struct{}{}
		}
	case *ast.CreateIndexStmt:
		if x.Table != nil {
			dbLabel := x.Table.Schema.O
			dbLabelSet[dbLabel] = struct{}{}
		}
	case *ast.CreateTableStmt:
		if x.Table != nil {
			dbLabel := x.Table.Schema.O
			dbLabelSet[dbLabel] = struct{}{}
		}
	case *ast.InsertStmt:
		var dbLabels []string
		if x.Table != nil {
			dbLabels = getDbFromResultNode(x.Table.TableRefs)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
		dbLabels = getDbFromResultNode(x.Select)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
	case *ast.DropIndexStmt:
		if x.Table != nil {
			dbLabel := x.Table.Schema.O
			dbLabelSet[dbLabel] = struct{}{}
		}
	case *ast.DropTableStmt:
		tables := x.Tables
		for _, table := range tables {
			dbLabel := table.Schema.O
			if _, ok := dbLabelSet[dbLabel]; !ok {
				dbLabelSet[dbLabel] = struct{}{}
			}
		}
	case *ast.SelectStmt:
		dbLabels := getDbFromResultNode(x)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
	case *ast.UpdateStmt:
		if x.TableRefs != nil {
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
	case *ast.DeleteStmt:
		if x.TableRefs != nil {
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
	case *ast.CreateBindingStmt:
		var resNode ast.ResultSetNode
		var tableRef *ast.TableRefsClause
		if x.OriginNode != nil {
			switch n := x.OriginNode.(type) {
			case *ast.SelectStmt:
				tableRef = n.From
			case *ast.DeleteStmt:
				tableRef = n.TableRefs
			case *ast.UpdateStmt:
				tableRef = n.TableRefs
			case *ast.InsertStmt:
				tableRef = n.Table
			}
			if tableRef != nil {
				resNode = tableRef.TableRefs
			} else {
				resNode = nil
			}
			dbLabels := getDbFromResultNode(resNode)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}

		if len(dbLabelSet) == 0 && x.HintedNode != nil {
			switch n := x.HintedNode.(type) {
			case *ast.SelectStmt:
				tableRef = n.From
			case *ast.DeleteStmt:
				tableRef = n.TableRefs
			case *ast.UpdateStmt:
				tableRef = n.TableRefs
			case *ast.InsertStmt:
				tableRef = n.Table
			}
			if tableRef != nil {
				resNode = tableRef.TableRefs
			} else {
				resNode = nil
			}
			dbLabels := getDbFromResultNode(resNode)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
	}

	return dbLabelSet
}

func getDbFromResultNode(resultNode ast.ResultSetNode) []string { // may have duplicate db name
	var dbLabels []string

	if resultNode == nil {
		return dbLabels
	}

	switch x := resultNode.(type) {
	case *ast.TableSource:
		return getDbFromResultNode(x.Source)
	case *ast.SelectStmt:
		if x.From != nil {
			return getDbFromResultNode(x.From.TableRefs)
		}
	case *ast.TableName:
		if x.DBInfo != nil {
			dbLabels = append(dbLabels, x.DBInfo.Name.O)
		}
	case *ast.Join:
		if x.Left != nil {
			dbs := getDbFromResultNode(x.Left)
			if dbs != nil {
				dbLabels = append(dbLabels, dbs...)
			}
		}

		if x.Right != nil {
			dbs := getDbFromResultNode(x.Right)
			if dbs != nil {
				dbLabels = append(dbLabels, dbs...)
			}
		}
	}

	return dbLabels
}
