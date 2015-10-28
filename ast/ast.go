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
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ast is the abstract syntax tree parsed from a SQL statement by parser.
// It can be analysed and transformed by optimizer.
package ast

import (
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

// Node is the basic element of the AST.
// Interfaces embed Node should have 'Node' name suffix.
type Node interface {
	// Accept accepts Visitor to visit itself.
	// The returned node should replace original node.
	// ok returns false to stop visiting.
	Accept(v Visitor) (node Node, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Node.
	SetText(text string)
}

// ExprNode is a node that can be evaluated.
// Name of implementations should have 'Expr' suffix.
type ExprNode interface {
	// Node is embeded in ExprNode.
	Node
	// IsStatic means it can be evaluated independently.
	IsStatic() bool
	// SetType sets evaluation type to the expression.
	SetType(tp *types.FieldType)
	// GetType gets the evaluation type of the expression.
	GetType() *types.FieldType
}

// FuncNode represents function call expression node.
type FuncNode interface {
	ExprNode
	functionExpression()
}

// StmtNode represents statement node.
// Name of implementations should have 'Stmt' suffix.
type StmtNode interface {
	Node
	statement()
}

// DDLNode represents DDL statement node.
type DDLNode interface {
	StmtNode
	ddlStatement()
}

// DMLNode represents DML statement node.
type DMLNode interface {
	StmtNode
	dmlStatement()
}

// ResultField is computed from a parsed select statement.
type ResultField struct {
	Column       *model.ColumnInfo
	ColumnAsName model.CIStr
	Table        *model.TableInfo
	TableAsName  model.CIStr
	DBName       model.CIStr
}

// ResultSetNode interface has ResultFields property which is computed and set by visitor.
// Implementations include SelectStmt, SubqueryExpr, TableSource, TableName and Join.
type ResultSetNode interface {
	Node
	// GetResultFields gets result fields of the result set node.
	GetResultFields() []*ResultField
	// SetResultFields sets result fields of the result set node.
	SetResultFields(fields []*ResultField)
}

// Visitor visits a Node.
type Visitor interface {
	// VisitEnter is called before children nodes is visited.
	// skipChildren returns true means children nodes should be skipped,
	// this is useful when work is done in Enter and there is no need to visit children.
	// ok returns false to stop visiting.
	Enter(n Node) (skipChildren bool, ok bool)
	// VisitLeave is called after children nodes has been visited.
	// ok returns false to stop visiting.
	Leave(n Node) (node Node, ok bool)
}
