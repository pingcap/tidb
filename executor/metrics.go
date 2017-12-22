// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	stmtNodeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_node_total",
			Help:      "Counter of StmtNode.",
		}, []string{"type"})
	expensiveQueryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "expensive_query_total",
			Help:      "Counter of expensive query.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(stmtNodeCounter)
	prometheus.MustRegister(expensiveQueryCounter)
}

func stmtCount(node ast.StmtNode, p plan.Plan, inRestrictedSQL bool) bool {
	var isExpensive bool
	stmtLabel := StatementLabel(node, p, &isExpensive)
	if !inRestrictedSQL && stmtLabel != IGNORE {
		stmtNodeCounter.WithLabelValues(stmtLabel).Inc()
	}
	return isExpensive
}

const (
	// IGNORE is a special label to identify the situations we want to ignore.
	IGNORE = "Ignore"
	// Select represents select statements.
	Select = "Select"
	// AlterTable represents alter table statements.
	AlterTable = "AlterTable"
	// AnalyzeTable represents analyze table statements.
	AnalyzeTable = "AnalyzeTable"
	// Begin represents begin statements.
	Begin = "Begin"
	// Commit represents commit statements.
	Commit = "Commit"
	// CreateDatabase represents create database statements.
	CreateDatabase = "CreateDatabase"
	// CreateIndex represents create index statements.
	CreateIndex = "CreateIndex"
	// CreateTable represents create table statements.
	CreateTable = "CreateTable"
	// CreateUser represents create user statements.
	CreateUser = "CreateUser"
	// Delete represents delete statements.
	Delete = "Delete"
	// DropDatabase represents drop database statements.
	DropDatabase = "DropDatabase"
	// DropIndex represents drop index statements.
	DropIndex = "DropIndex"
	// DropTable represents drop table statements.
	DropTable = "DropTable"
	// Explain represents explain statements.
	Explain = "Explain"
	// Replace represents replace statements.
	Replace = "Replace"
	// Insert represents insert statements.
	Insert = "Insert"
	// LoadDataStmt represents load data statements.
	LoadDataStmt = "LoadData"
	// RollBack represents roll back statements.
	RollBack = "RollBack"
	// Set represents set statements.
	Set = "Set"
	// Show represents show statements.
	Show = "Show"
	// TruncateTable represents truncate table statements.
	TruncateTable = "TruncateTable"
	// Update represents update statements.
	Update = "Update"
	// Grant represents grant statements.
	Grant = "Grant"
	// Revoke represents revoke statements.
	Revoke = "Revoke"
)

// StatementLabel generates a label for a statement.
func StatementLabel(node ast.StmtNode, p plan.Plan, isExpensive *bool) string {
	switch x := node.(type) {
	case *ast.AlterTableStmt:
		return AlterTable
	case *ast.AnalyzeTableStmt:
		return AnalyzeTable
	case *ast.BeginStmt:
		return Begin
	case *ast.CommitStmt:
		return Commit
	case *ast.CreateDatabaseStmt:
		return CreateDatabase
	case *ast.CreateIndexStmt:
		return CreateIndex
	case *ast.CreateTableStmt:
		return CreateTable
	case *ast.CreateUserStmt:
		return CreateUser
	case *ast.DeleteStmt:
		return getDeleteStmtLabel(x, p, isExpensive)
	case *ast.DropDatabaseStmt:
		return DropDatabase
	case *ast.DropIndexStmt:
		return DropIndex
	case *ast.DropTableStmt:
		return DropTable
	case *ast.ExplainStmt:
		return Explain
	case *ast.InsertStmt:
		if x.IsReplace {
			return Replace
		}
		return Insert
	case *ast.LoadDataStmt:
		return LoadDataStmt
	case *ast.RollbackStmt:
		return RollBack
	case *ast.SelectStmt:
		return getSelectStmtLabel(x, p, isExpensive)
	case *ast.SetStmt, *ast.SetPwdStmt:
		return Set
	case *ast.ShowStmt:
		return Show
	case *ast.TruncateTableStmt:
		return TruncateTable
	case *ast.UpdateStmt:
		return getUpdateStmtLabel(x, p, isExpensive)
	case *ast.GrantStmt:
		return Grant
	case *ast.RevokeStmt:
		return Revoke
	case *ast.DeallocateStmt, *ast.ExecuteStmt, *ast.PrepareStmt, *ast.UseStmt:
		return IGNORE
	}
	return "other"
}

func getSelectStmtLabel(stmt *ast.SelectStmt, p plan.Plan, isExpensive *bool) string {
	var attributes stmtAttributes
	attributes.fromSelectStmt(stmt)
	attributes.fromPlan(p)
	stmtLabel := Select + attributes.toLabel()
	attributes.logExpensiveStmt(stmtLabel, stmt.Text(), isExpensive)
	return stmtLabel
}

func getDeleteStmtLabel(stmt *ast.DeleteStmt, p plan.Plan, isExpensive *bool) string {
	var attributes stmtAttributes
	attributes.fromDeleteStmt(stmt)
	attributes.fromPlan(p)
	stmtLabel := Delete + attributes.toLabel()
	attributes.logExpensiveStmt(stmtLabel, stmt.Text(), isExpensive)
	return stmtLabel
}

func getUpdateStmtLabel(stmt *ast.UpdateStmt, p plan.Plan, isExpensive *bool) string {
	var attributes stmtAttributes
	attributes.fromUpdateStmt(stmt)
	attributes.fromPlan(p)
	stmtLabel := Update + attributes.toLabel()
	attributes.logExpensiveStmt(stmtLabel, stmt.Text(), isExpensive)
	return stmtLabel
}

type stmtAttributes struct {
	hasJoin        bool
	hasApply       bool
	hasAggregate   bool
	hasIndexDouble bool
	hasIndexScan   bool
	hasTableScan   bool
	hasRange       bool
	hasOrder       bool
	hasLimit       bool
	isSystemTable  bool
}

func (pa *stmtAttributes) fromSelectStmt(stmt *ast.SelectStmt) {
	if stmt.Limit != nil {
		pa.hasLimit = true
	}
	if stmt.OrderBy != nil {
		pa.hasOrder = true
	}
}

func (pa *stmtAttributes) fromDeleteStmt(stmt *ast.DeleteStmt) {
	if stmt.Limit != nil {
		pa.hasLimit = true
	}
	if stmt.Order != nil {
		pa.hasOrder = true
	}
}

func (pa *stmtAttributes) fromUpdateStmt(stmt *ast.UpdateStmt) {
	if stmt.Limit != nil {
		pa.hasLimit = true
	}
	if stmt.Order != nil {
		pa.hasOrder = true
	}
}

func (pa *stmtAttributes) fromPlan(p plan.Plan) {
	switch x := p.(type) {
	case *plan.PhysicalApply:
		pa.hasApply = true
	case *plan.PhysicalHashAgg, *plan.PhysicalStreamAgg:
		pa.hasAggregate = true
	case *plan.PhysicalHashJoin:
		pa.hasJoin = true
	case *plan.PhysicalTableScan:
		pa.hasTableScan = true
		if len(x.AccessCondition) > 0 {
			pa.hasRange = true
		}
		pa.setIsSystemTable(x.DBName)
	case *plan.PhysicalIndexScan:
		pa.hasIndexScan = true
		if len(x.AccessCondition) > 0 {
			pa.hasRange = true
		}
		pa.setIsSystemTable(x.DBName)
	case *plan.Insert:
		if x.SelectPlan != nil {
			pa.fromPlan(x.SelectPlan)
		}
	case *plan.Delete:
		pa.fromPlan(x.SelectPlan)
	case *plan.Update:
		pa.fromPlan(x.SelectPlan)
	case *plan.PhysicalTableReader:
		for _, child := range x.TablePlans {
			pa.fromPlan(child)
		}
	case *plan.PhysicalIndexReader:
		for _, child := range x.IndexPlans {
			pa.fromPlan(child)
		}
	case *plan.PhysicalIndexLookUpReader:
		for _, child := range x.IndexPlans {
			pa.fromPlan(child)
		}
		for _, child := range x.TablePlans {
			pa.fromPlan(child)
		}
		pa.hasIndexDouble = true
	}
	children := p.Children()
	for _, child := range children {
		pa.fromPlan(child)
	}
}

func (pa *stmtAttributes) setIsSystemTable(dbName model.CIStr) {
	if dbName.L == mysql.SystemDB {
		pa.isSystemTable = true
	}
}

const (
	attrSimple    = "Simple"
	attrFull      = "Full"
	attrRange     = "Range"
	attrTable     = "Table"
	attrIndex     = "Index"
	attrIndexOnly = "IndexOnly"
	attrJoin      = "Join"
	attrApply     = "Apply"
	attrLimit     = "Limit"
	attrOrder     = "Order"
	attrAggregate = "Agg"
)

// toLabel ...
// Not all attributes is used to create the label, because we don't want too many labels.
func (pa *stmtAttributes) toLabel() string {
	var attrs []string
	// First attribute.
	if pa.hasJoin {
		attrs = append(attrs, attrJoin)
	} else if pa.hasApply {
		attrs = append(attrs, attrApply)
	} else if pa.hasAggregate {
		attrs = append(attrs, attrAggregate)
	} else if pa.hasIndexDouble {
		attrs = append(attrs, attrIndex)
	} else if pa.hasIndexScan {
		attrs = append(attrs, attrIndexOnly)
	} else if pa.hasTableScan {
		attrs = append(attrs, attrTable)
	} else {
		attrs = append(attrs, attrSimple)
	}
	// Second attribute.
	if pa.hasRange {
		attrs = append(attrs, attrRange)
	} else {
		attrs = append(attrs, attrFull)
	}
	// Third attribute, optional.
	if pa.hasOrder {
		attrs = append(attrs, attrOrder)
	}
	// Fourth attribute, optional.
	if pa.hasLimit {
		attrs = append(attrs, attrLimit)
	}
	return strings.Join(attrs, "")
}

func (pa *stmtAttributes) isExpensiveStmt() bool {
	if pa.hasRange || pa.hasLimit || pa.isSystemTable {
		return false
	}
	if !pa.hasIndexScan && !pa.hasTableScan && !pa.hasIndexDouble {
		return false
	}
	return true
}

func (pa *stmtAttributes) logExpensiveStmt(stmtLabel string, sql string, isExpensive *bool) {
	if pa.isExpensiveStmt() {
		const logSQLLen = 1024
		if len(sql) > logSQLLen {
			sql = sql[:logSQLLen] + fmt.Sprintf("len(%d)", len(sql))
		}
		log.Warnf("[EXPENSIVE_QUERY] %s", sql)
		expensiveQueryCounter.WithLabelValues(stmtLabel).Inc()
		*isExpensive = true
	}
}
