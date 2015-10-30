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
func (nod *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ExplainStmt)
	node, ok := nod.Stmt.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Stmt = node.(DMLNode)
	return v.Leave(nod)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	InPrepare bool // true for prepare mode, false for use mode
	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	SQLText   string
	SQLVar    *VariableExpr
}

// Accept implements Node Accept interface.
func (nod *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PrepareStmt)
	node, ok := nod.SQLVar.Accept(v)
	if !ok {
		return nod, false
	}
	nod.SQLVar = node.(*VariableExpr)
	return v.Leave(nod)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
	ID   uint32 // For binary protocol, there is no Name but only ID.
}

// Accept implements Node Accept interface.
func (nod *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DeallocateStmt)
	return v.Leave(nod)
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	UsingVars []ExprNode
}

// Accept implements Node Accept interface.
func (nod *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ExecuteStmt)
	for i, val := range nod.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(nod)
}

// ShowStmtType is the type for SHOW statement.
type ShowStmtType int

// Show statement types.
const (
	ShowNone = iota
	ShowEngines
	ShowDatabases
	ShowTables
	ShowColumns
	ShowWarnings
	ShowCharset
	ShowVariables
	ShowCollation
	ShowCreateTable
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

	// Used by show variables
	GlobalScope bool
	Pattern     *PatternLikeExpr
	Where       ExprNode
}

// Accept implements Node Accept interface.
func (nod *ShowStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ShowStmt)
	if nod.Table != nil {
		node, ok := nod.Table.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Table = node.(*TableName)
	}
	if nod.Column != nil {
		node, ok := nod.Column.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Column = node.(*ColumnName)
	}
	if nod.Pattern != nil {
		node, ok := nod.Pattern.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Pattern = node.(*PatternLikeExpr)
	}
	if nod.Where != nil {
		node, ok := nod.Where.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Where = node.(ExprNode)
	}
	return v.Leave(nod)
}

// BeginStmt is a statement to start a new transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (nod *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*BeginStmt)
	return v.Leave(nod)
}

// CommitStmt is a statement to commit the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (nod *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CommitStmt)
	return v.Leave(nod)
}

// RollbackStmt is a statement to roll back the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (nod *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*RollbackStmt)
	return v.Leave(nod)
}

// UseStmt is a statement to use the DBName database as the current database.
// See: https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Accept implements Node Accept interface.
func (nod *UseStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*UseStmt)
	return v.Leave(nod)
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
func (nod *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*VariableAssignment)
	node, ok := nod.Value.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Value = node.(ExprNode)
	return v.Leave(nod)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node Accept interface.
func (nod *SetStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SetStmt)
	for i, val := range nod.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(nod)
}

// SetCharsetStmt is a statement to assign values to character and collation variables.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
type SetCharsetStmt struct {
	stmtNode

	Charset string
	Collate string
}

// Accept implements Node Accept interface.
func (nod *SetCharsetStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SetCharsetStmt)
	return v.Leave(nod)
}

// SetPwdStmt is a statement to assign a password to user account.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-password.html
type SetPwdStmt struct {
	stmtNode

	User     string
	Password string
}

// Accept implements Node Accept interface.
func (nod *SetPwdStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SetPwdStmt)
	return v.Leave(nod)
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
func (nod *CreateUserStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CreateUserStmt)
	return v.Leave(nod)
}

// DoStmt is the struct for DO statement.
type DoStmt struct {
	stmtNode

	Exprs []ExprNode
}

// Accept implements Node Accept interface.
func (nod *DoStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DoStmt)
	for i, val := range nod.Exprs {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Exprs[i] = node.(ExprNode)
	}
	return v.Leave(nod)
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	node
	Priv mysql.PrivilegeType
	Cols []*ColumnName
}

// Accept implements Node Accept interface.
func (nod *PrivElem) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PrivElem)
	for i, val := range nod.Cols {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Cols[i] = node.(*ColumnName)
	}
	return v.Leave(nod)
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
func (nod *GrantStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*GrantStmt)
	for i, val := range nod.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(nod)
}
