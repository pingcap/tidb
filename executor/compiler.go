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

	"github.com/opentracing/opentracing-go"
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
)

var (
	stmtNodeCounterUse      = metrics.StmtNodeCounter.WithLabelValues("Use")
	stmtNodeCounterShow     = metrics.StmtNodeCounter.WithLabelValues("Show")
	stmtNodeCounterBegin    = metrics.StmtNodeCounter.WithLabelValues("Begin")
	stmtNodeCounterCommit   = metrics.StmtNodeCounter.WithLabelValues("Commit")
	stmtNodeCounterRollback = metrics.StmtNodeCounter.WithLabelValues("Rollback")
	stmtNodeCounterInsert   = metrics.StmtNodeCounter.WithLabelValues("Insert")
	stmtNodeCounterReplace  = metrics.StmtNodeCounter.WithLabelValues("Replace")
	stmtNodeCounterDelete   = metrics.StmtNodeCounter.WithLabelValues("Delete")
	stmtNodeCounterUpdate   = metrics.StmtNodeCounter.WithLabelValues("Update")
	stmtNodeCounterSelect   = metrics.StmtNodeCounter.WithLabelValues("Select")
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	ret := &plannercore.PreprocessorReturn{}
	pe := &plannercore.PreprocessExecuteISUpdate{ExecuteInfoSchemaUpdate: planner.GetExecuteForUpdateReadIS, Node: stmtNode}
	err := plannercore.Preprocess(c.Ctx,
		stmtNode,
		plannercore.WithPreprocessorReturn(ret),
		plannercore.WithExecuteInfoSchemaUpdate(pe),
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
	finalPlan, names, err := planner.Optimize(ctx, c.Ctx, stmtNode, is)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("assertStmtCtxIsStaleness", func(val failpoint.Value) {
		staleread.AssertStmtStaleness(c.Ctx, val.(bool))
	})

	CountStmtNode(stmtNode, c.Ctx.GetSessionVars().InRestrictedSQL)
	var lowerPriority bool
	if c.Ctx.GetSessionVars().StmtCtx.Priority == mysql.NoPriority {
		lowerPriority = needLowerPriority(finalPlan)
	}
	return &ExecStmt{
		GoCtx:            ctx,
		ReplicaReadScope: ret.ReadReplicaScope,
		InfoSchema:       is,
		Plan:             finalPlan,
		LowerPriority:    lowerPriority,
		Text:             stmtNode.Text(),
		StmtNode:         stmtNode,
		Ctx:              c.Ctx,
		OutputNames:      names,
		Ti:               &TelemetryInfo{},
	}, nil
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

	typeLabel := GetStmtLabel(stmtNode)
	switch typeLabel {
	case "Use":
		stmtNodeCounterUse.Inc()
	case "Show":
		stmtNodeCounterShow.Inc()
	case "Begin":
		stmtNodeCounterBegin.Inc()
	case "Commit":
		stmtNodeCounterCommit.Inc()
	case "Rollback":
		stmtNodeCounterRollback.Inc()
	case "Insert":
		stmtNodeCounterInsert.Inc()
	case "Replace":
		stmtNodeCounterReplace.Inc()
	case "Delete":
		stmtNodeCounterDelete.Inc()
	case "Update":
		stmtNodeCounterUpdate.Inc()
	case "Select":
		stmtNodeCounterSelect.Inc()
	default:
		metrics.StmtNodeCounter.WithLabelValues(typeLabel).Inc()
	}

	if !config.GetGlobalConfig().Status.RecordQPSbyDB {
		return
	}

	dbLabels := getStmtDbLabel(stmtNode)
	for dbLabel := range dbLabels {
		metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel).Inc()
	}
}

func getStmtDbLabel(stmtNode ast.StmtNode) map[string]struct{} {
	dbLabelSet := make(map[string]struct{})

	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateIndexStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateTableStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.InsertStmt:
		dbLabels := getDbFromResultNode(x.Table.TableRefs)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
		dbLabels = getDbFromResultNode(x.Select)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
	case *ast.DropIndexStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
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
		dbLabels = append(dbLabels, x.DBInfo.Name.O)
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

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		return "AnalyzeTable"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.ChangeStmt:
		return "Change"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CompactTableStmt:
		return "CompactTable"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateViewStmt:
		return "CreateView"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropTableStmt:
		if x.IsView {
			return "DropView"
		}
		return "DropTable"
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			return "DescTable"
		}
		if x.Analyze {
			return "ExplainAnalyzeSQL"
		}
		return "ExplainSQL"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "Rollback"
	case *ast.SelectStmt:
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateTableStmt:
		return "TruncateTable"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.GrantStmt:
		return "Grant"
	case *ast.RevokeStmt:
		return "Revoke"
	case *ast.DeallocateStmt:
		return "Deallocate"
	case *ast.ExecuteStmt:
		return "Execute"
	case *ast.PrepareStmt:
		return "Prepare"
	case *ast.UseStmt:
		return "Use"
	case *ast.CreateBindingStmt:
		return "CreateBinding"
	case *ast.IndexAdviseStmt:
		return "IndexAdvise"
	case *ast.DropBindingStmt:
		return "DropBinding"
	case *ast.TraceStmt:
		return "Trace"
	case *ast.ShutdownStmt:
		return "Shutdown"
	}
	return "other"
}
