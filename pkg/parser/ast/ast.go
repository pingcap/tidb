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
	"io"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// Node is the basic element of the AST.
// Interfaces embed Node should have 'Node' name suffix.
type Node interface {
	// Restore returns the sql text from ast tree
	Restore(ctx *format.RestoreCtx) error
	// Accept accepts Visitor to visit itself.
	// The returned node should replace original node.
	// ok returns false to stop visiting.
	//
	// Implementation of this method should first call visitor.Enter,
	// assign the returned node to its method receiver, if skipChildren returns true,
	// children should be skipped. Otherwise, call its children in particular order that
	// later elements depends on former elements. Finally, return visitor.Leave.
	Accept(v Visitor) (node Node, ok bool)
	// Text returns the utf8 encoding text of the element.
	Text() string
	// OriginalText returns the original text of the element.
	OriginalText() string
	// SetText sets original text to the Node.
	SetText(enc charset.Encoding, text string)
	// SetOriginTextPosition set the start offset of this node in the origin text.
	// Only be called when `parser.lexer.skipPositionRecording` equals to false.
	SetOriginTextPosition(offset int)
	// OriginTextPosition get the start offset of this node in the origin text.
	OriginTextPosition() int
}

// Flags indicates whether an expression contains certain types of expression.
const (
	FlagConstant       uint64 = 0
	FlagHasParamMarker uint64 = 1 << iota
	FlagHasFunc
	FlagHasReference
	FlagHasAggregateFunc
	FlagHasSubquery
	FlagHasVariable
	FlagHasDefault
	FlagPreEvaluated
	FlagHasWindowFunc
)

// ExprNode is a node that can be evaluated.
// Name of implementations should have 'Expr' suffix.
type ExprNode interface {
	// Node is embedded in ExprNode.
	Node
	// SetType sets evaluation type to the expression.
	SetType(tp *types.FieldType)
	// GetType gets the evaluation type of the expression.
	GetType() *types.FieldType
	// SetFlag sets flag to the expression.
	// Flag indicates whether the expression contains
	// parameter marker, reference, aggregate function...
	SetFlag(flag uint64)
	// GetFlag returns the flag of the expression.
	GetFlag() uint64

	// Format formats the AST into a writer.
	Format(w io.Writer)
}

// OptBinary is used for parser.
type OptBinary struct {
	IsBinary bool
	Charset  string
}

// OptVectorType represents the element type of the vector.
type VectorElementType struct {
	Tp byte // Only FLOAT and DOUBLE is accepted.
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

// ResultSetNode interface has a ResultFields property, represents a Node that returns result set.
// Implementations include SelectStmt, SubqueryExpr, TableSource, TableName, Join and SetOprStmt.
type ResultSetNode interface {
	Node

	resultSet()
}

// SensitiveStmtNode overloads StmtNode and provides a SecureText method.
type SensitiveStmtNode interface {
	StmtNode
	// SecureText is different from Text that it hide password information.
	SecureText() string
}

// Visitor visits a Node.
type Visitor interface {
	// Enter is called before children nodes are visited.
	// The returned node must be the same type as the input node n.
	// skipChildren returns true means children nodes should be skipped,
	// this is useful when work is done in Enter and there is no need to visit children.
	Enter(n Node) (node Node, skipChildren bool)
	// Leave is called after children nodes have been visited.
	// The returned node's type can be different from the input node if it is a ExprNode,
	// Non-expression node must be the same type as the input node n.
	// ok returns false to stop visiting.
	Leave(n Node) (node Node, ok bool)
}

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode StmtNode) string {
	switch x := stmtNode.(type) {
	case *AlterTableStmt:
		return "AlterTable"
	case *AnalyzeTableStmt:
		return "AnalyzeTable"
	case *BeginStmt:
		return "Begin"
	case *ChangeStmt:
		return "Change"
	case *CommitStmt:
		return "Commit"
	case *CompactTableStmt:
		return "CompactTable"
	case *CreateDatabaseStmt:
		return "CreateDatabase"
	case *CreateIndexStmt:
		return "CreateIndex"
	case *CreateTableStmt:
		return "CreateTable"
	case *CreateViewStmt:
		return "CreateView"
	case *CreateUserStmt:
		return "CreateUser"
	case *DeleteStmt:
		return "Delete"
	case *DropDatabaseStmt:
		return "DropDatabase"
	case *DropIndexStmt:
		return "DropIndex"
	case *DropTableStmt:
		if x.IsView {
			return "DropView"
		}
		return "DropTable"
	case *ExplainStmt:
		if _, ok := x.Stmt.(*ShowStmt); ok {
			return "DescTable"
		}
		if x.Analyze {
			return "ExplainAnalyzeSQL"
		}
		return "ExplainSQL"
	case *InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ImportIntoStmt:
		return "ImportInto"
	case *LoadDataStmt:
		return "LoadData"
	case *RollbackStmt:
		return "Rollback"
	case *SelectStmt:
		return "Select"
	case *SetStmt, *SetPwdStmt:
		return "Set"
	case *ShowStmt:
		return "Show"
	case *TruncateTableStmt:
		return "TruncateTable"
	case *UpdateStmt:
		return "Update"
	case *GrantStmt:
		return "Grant"
	case *RevokeStmt:
		return "Revoke"
	case *DeallocateStmt:
		return "Deallocate"
	case *ExecuteStmt:
		return "Execute"
	case *PrepareStmt:
		return "Prepare"
	case *UseStmt:
		return "Use"
	case *CreateBindingStmt:
		return "CreateBinding"
	case *DropBindingStmt:
		return "DropBinding"
	case *TraceStmt:
		return "Trace"
	case *ShutdownStmt:
		return "Shutdown"
	case *SavepointStmt:
		return "Savepoint"
	case *OptimizeTableStmt:
		return "Optimize"
	}
	return "other"
}
