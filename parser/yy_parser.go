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

package parser

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Error instances.
var (
	ErrSyntax = terror.ClassParser.New(CodeSyntaxErr, "syntax error")
)

// Error codes.
const (
	CodeSyntaxErr terror.ErrCode = iota + 1
)

// Parse parses a query string to raw ast.StmtNode.
// If charset and collation is "", default charset and collation will be used.
func Parse(sql, charset, collation string) ([]ast.StmtNode, error) {
	if charset == "" {
		charset = mysql.DefaultCharset
	}
	if collation == "" {
		collation = mysql.DefaultCollationName
	}
	l := NewLexer(sql)
	l.SetCharsetInfo(charset, collation)
	yyParse(l)
	if len(l.Errors()) != 0 {
		return nil, errors.Trace(l.Errors()[0])
	}
	return l.Stmts(), nil
}

// ParseOne parses a query and return the ast.StmtNode.
// The query must has one statement, otherwise ErrSyntax is returned.
func ParseOne(sql, charset, collation string) (ast.StmtNode, error) {
	stmts, err := Parse(sql, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(stmts) != 1 {
		return nil, ErrSyntax
	}
	return stmts[0], nil
}
