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

import (
	"sync"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// Thread-safe common strings cache to reduce allocations
var (
	commonStrings = map[string]string{
		"":         "",
		"SELECT":   "SELECT",
		"FROM":     "FROM",
		"WHERE":    "WHERE",
		"INSERT":   "INSERT",
		"UPDATE":   "UPDATE",
		"DELETE":   "DELETE",
		"*":        "*",
		"1":        "1",
		"0":        "0",
		"NULL":     "NULL",
		"DEFAULT":  "DEFAULT",
		"AND":      "AND",
		"OR":       "OR",
		"NOT":      "NOT",
		"IN":       "IN",
		"EXISTS":   "EXISTS",
		"ORDER":    "ORDER",
		"BY":       "BY",
		"GROUP":    "GROUP",
		"HAVING":   "HAVING",
		"LIMIT":    "LIMIT",
		"OFFSET":   "OFFSET",
		"JOIN":     "JOIN",
		"LEFT":     "LEFT",
		"RIGHT":    "RIGHT",
		"INNER":    "INNER",
		"OUTER":    "OUTER",
		"ON":       "ON",
		"AS":       "AS",
		"COUNT":    "COUNT",
		"SUM":      "SUM",
		"AVG":      "AVG",
		"MAX":      "MAX",
		"MIN":      "MIN",
		"DISTINCT": "DISTINCT",
		"UNION":    "UNION",
		"ALL":      "ALL",
		"CREATE":   "CREATE",
		"TABLE":    "TABLE",
		"INDEX":    "INDEX",
		"DROP":     "DROP",
		"ALTER":    "ALTER",
		"ADD":      "ADD",
		"COLUMN":   "COLUMN",
		"PRIMARY":  "PRIMARY",
		"KEY":      "KEY",
		"FOREIGN":  "FOREIGN",
		"REFERENCES": "REFERENCES",
		"UNIQUE":   "UNIQUE",
		"CONSTRAINT": "CONSTRAINT",
		"CHECK":    "CHECK",
		"VALUES":   "VALUES",
		"INTO":     "INTO",
		"SET":      "SET",
	}
	commonStringsLock sync.RWMutex
)

// node is the struct implements Node interface except for Accept method.
// Node implementations should embed it in.
type node struct {
	utf8Text string
	enc      charset.Encoding
	once     *sync.Once

	text   string
	offset int
}

// SetOriginTextPosition implements Node interface.
func (n *node) SetOriginTextPosition(offset int) {
	n.offset = offset
}

// OriginTextPosition implements Node interface.
func (n *node) OriginTextPosition() int {
	return n.offset
}

// SetText implements Node interface with thread-safe string interning.
func (n *node) SetText(enc charset.Encoding, text string) {
	n.enc = enc
	// Thread-safe lookup in common strings cache
	commonStringsLock.RLock()
	if cached, exists := commonStrings[text]; exists {
		commonStringsLock.RUnlock()
		n.text = cached
	} else {
		commonStringsLock.RUnlock()
		n.text = text
	}
	n.once = &sync.Once{}
}

// Text implements Node interface.
func (n *node) Text() string {
	if n.once == nil {
		return n.text
	}
	n.once.Do(func() {
		if n.enc == nil {
			n.utf8Text = n.text
			return
		}
		utf8Lit, _ := n.enc.Transform(nil, charset.HackSlice(n.text), charset.OpDecodeReplace)
		n.utf8Text = charset.HackString(utf8Lit)
	})
	return n.utf8Text
}

// OriginalText implements Node interface.
func (n *node) OriginalText() string {
	return n.text
}

// stmtNode implements StmtNode interface.
// Statement implementations should embed it in.
type stmtNode struct {
	node
}

// statement implements StmtNode interface.
func (sn *stmtNode) statement() {}

// ddlNode implements DDLNode interface.
// DDL implementations should embed it in.
type ddlNode struct {
	stmtNode
}

// ddlStatement implements DDLNode interface.
func (dn *ddlNode) ddlStatement() {}

// dmlNode is the struct implements DMLNode interface.
// DML implementations should embed it in.
type dmlNode struct {
	stmtNode
}

// dmlStatement implements DMLNode interface.
func (dn *dmlNode) dmlStatement() {}

// exprNode is the struct implements Expression interface.
// Expression implementations should embed it in.
type exprNode struct {
	node
	Type types.FieldType
	flag uint64
}

// TexprNode is exported for parser driver.
type TexprNode = exprNode

// SetType implements ExprNode interface.
func (en *exprNode) SetType(tp *types.FieldType) {
	en.Type = *tp
}

// GetType implements ExprNode interface.
func (en *exprNode) GetType() *types.FieldType {
	return &en.Type
}

// SetFlag implements ExprNode interface.
func (en *exprNode) SetFlag(flag uint64) {
	en.flag = flag
}

// GetFlag implements ExprNode interface.
func (en *exprNode) GetFlag() uint64 {
	return en.flag
}

type funcNode struct {
	exprNode
}

// functionExpression implements FunctionNode interface.
func (fn *funcNode) functionExpression() {}
