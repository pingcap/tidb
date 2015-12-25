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

package ast

import "github.com/pingcap/tidb/mysql"

var (
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &PrepareStmt{}
	_ StmtNode = &DeallocateStmt{}
	_ StmtNode = &ExecuteStmt{}
	_ StmtNode = &ShowStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &UseStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &SetCharsetStmt{}
	_ StmtNode = &SetPwdStmt{}
	_ StmtNode = &CreateUserStmt{}
	_ StmtNode = &DoStmt{}
	_ StmtNode = &GrantStmt{}

	_ Node = &VariableAssignment{}
)

// FloatOpt is used for parsing floating-point type option from SQL.
// TODO: add reference doc.
type FloatOpt struct {
	Flen    int
	Decimal int
}

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// AuthString/HashString can be empty, so we need to decide which one to use.
	ByAuthString bool
	AuthString   string
	HashString   string
	// TODO: support auth_plugin
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt StmtNode
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*ExplainStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(DMLNode)
	return v.Leave(n)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	Name    string
	SQLText string
	SQLVar  *VariableExpr
}

// Accept implements Node Accept interface.
func (n *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*PrepareStmt)
	if n.SQLVar != nil {
		node, ok := n.SQLVar.Accept(v)
		if !ok {
			return n, false
		}
		n.SQLVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
}

// Accept implements Node Accept interface.
func (n *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*DeallocateStmt)
	return v.Leave(n)
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name      string
	UsingVars []ExprNode
}

// Accept implements Node Accept interface.
func (n *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*ExecuteStmt)
	for i, val := range n.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// ShowStmtType is the type for SHOW statement.
type ShowStmtType int

// Show statement types.
const (
	ShowNone = iota
	ShowEngines
	ShowDatabases
	ShowTables
	ShowTableStatus
	ShowColumns
	ShowWarnings
	ShowCharset
	ShowVariables
	ShowStatus
	ShowCollation
	ShowCreateTable
	ShowGrants
	ShowTriggers
	ShowProcedureStatus
	ShowIndex
)

// ShowStmt is a statement to provide information about databases, tables, columns and so on.
// See: https://dev.mysql.com/doc/refman/5.7/en/show.html
type ShowStmt struct {
	dmlNode

	Tp     ShowStmtType // Databases/Tables/Columns/....
	DBName string
	Table  *TableName  // Used for showing columns.
	Column *ColumnName // Used for `desc table column`.
	Flag   int         // Some flag parsed from sql, such as FULL.
	Full   bool
	User   string // Used for show grants.

	// Used by show variables
	GlobalScope bool
	Pattern     *PatternLikeExpr
	Where       ExprNode
}

// Accept implements Node Accept interface.
func (n *ShowStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*ShowStmt)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	if n.Column != nil {
		node, ok := n.Column.Accept(v)
		if !ok {
			return n, false
		}
		n.Column = node.(*ColumnName)
	}
	if n.Pattern != nil {
		node, ok := n.Pattern.Accept(v)
		if !ok {
			return n, false
		}
		n.Pattern = node.(*PatternLikeExpr)
	}
	if n.Where != nil {
		node, ok := n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*BeginStmt)
	return v.Leave(n)
}

// CommitStmt is a statement to commit the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*CommitStmt)
	return v.Leave(n)
}

// RollbackStmt is a statement to roll back the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*RollbackStmt)
	return v.Leave(n)
}

// UseStmt is a statement to use the DBName database as the current database.
// See: https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Accept implements Node Accept interface.
func (n *UseStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*UseStmt)
	return v.Leave(n)
}

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	node
	Name     string
	Value    ExprNode
	IsGlobal bool
	IsSystem bool
}

// Accept implements Node interface.
func (n *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*VariableAssignment)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node Accept interface.
func (n *SetStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*SetStmt)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

// SetCharsetStmt is a statement to assign values to character and collation variables.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
type SetCharsetStmt struct {
	stmtNode

	Charset string
	Collate string
}

// Accept implements Node Accept interface.
func (n *SetCharsetStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*SetCharsetStmt)
	return v.Leave(n)
}

// SetPwdStmt is a statement to assign a password to user account.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-password.html
type SetPwdStmt struct {
	stmtNode

	User     string
	Password string
}

// Accept implements Node Accept interface.
func (n *SetPwdStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*SetPwdStmt)
	return v.Leave(n)
}

// UserSpec is used for parsing create user statement.
type UserSpec struct {
	User    string
	AuthOpt *AuthOption
}

// CreateUserStmt creates user account.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-user.html
type CreateUserStmt struct {
	stmtNode

	IfNotExists bool
	Specs       []*UserSpec
}

// Accept implements Node Accept interface.
func (n *CreateUserStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*CreateUserStmt)
	return v.Leave(n)
}

// DoStmt is the struct for DO statement.
type DoStmt struct {
	stmtNode

	Exprs []ExprNode
}

// Accept implements Node Accept interface.
func (n *DoStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*DoStmt)
	for i, val := range n.Exprs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Exprs[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// AdminStmtType is the type for admin statement.
type AdminStmtType int

// Admin statement types.
const (
	AdminShowDDL = iota
	AdminCheckTable
)

// AdminStmt is the struct for Admin statement.
type AdminStmt struct {
	stmtNode

	Tp     AdminStmtType
	Tables []*TableName
}

// Accept implements Node Accpet interface.
func (n *AdminStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}

	n = newNod.(*AdminStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}

	return v.Leave(n)
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	node
	Priv mysql.PrivilegeType
	Cols []*ColumnName
}

// Accept implements Node Accept interface.
func (n *PrivElem) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*PrivElem)
	for i, val := range n.Cols {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// ObjectTypeType is the type for object type.
type ObjectTypeType int

const (
	// ObjectTypeNone is for empty object type.
	ObjectTypeNone ObjectTypeType = iota
	// ObjectTypeTable means the following object is a table.
	ObjectTypeTable
)

// GrantLevelType is the type for grant level.
type GrantLevelType int

const (
	// GrantLevelNone is the dummy const for default value.
	GrantLevelNone GrantLevelType = iota
	// GrantLevelGlobal means the privileges are administrative or apply to all databases on a given server.
	GrantLevelGlobal
	// GrantLevelDB means the privileges apply to all objects in a given database.
	GrantLevelDB
	// GrantLevelTable means the privileges apply to all columns in a given table.
	GrantLevelTable
)

// GrantLevel is used for store the privilege scope.
type GrantLevel struct {
	Level     GrantLevelType
	DBName    string
	TableName string
}

// GrantStmt is the struct for GRANT statement.
type GrantStmt struct {
	stmtNode

	Privs      []*PrivElem
	ObjectType ObjectTypeType
	Level      *GrantLevel
	Users      []*UserSpec
}

// Accept implements Node Accept interface.
func (n *GrantStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNod)
	}
	n = newNod.(*GrantStmt)
	for i, val := range n.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(n)
}
