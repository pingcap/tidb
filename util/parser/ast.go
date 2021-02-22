// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

// GetDefaultDB checks if all columns in the AST have explicit DBName. If not, return specified DBName.
func GetDefaultDB(sel ast.StmtNode, dbName string) string {
	implicitDB := &implicitDatabase{}
	sel.Accept(implicitDB)
	if implicitDB.hasImplicit {
		return dbName
	}
	return ""
}

type implicitDatabase struct {
	hasImplicit bool
}

func (i *implicitDatabase) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.TableName:
		if x.Schema.L == "" {
			i.hasImplicit = true
		}
		return in, true
	}
	return in, i.hasImplicit
}

func (i *implicitDatabase) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// RestoreWithDefaultDB returns restore strings for StmtNode with defaultDB
func RestoreWithDefaultDB(node ast.StmtNode, defaultDB string) string {
	var sb strings.Builder
	// Three flags for restore with default DB:
	// 1. RestoreStringSingleQuotes specifies to use single quotes to surround the string;
	// 2. RestoreSpacesAroundBinaryOperation specifies to add space around binary operation;
	// 3. RestoreStringWithoutDefaultCharset specifies to not print default charset before string;
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreSpacesAroundBinaryOperation|format.RestoreStringWithoutDefaultCharset, &sb)
	ctx.DefaultDB = defaultDB
	if err := node.Restore(ctx); err != nil {
		return ""
	}
	return sb.String()
}
