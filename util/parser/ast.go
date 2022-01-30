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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GetDefaultDB checks if all tables in the AST have explicit DBName. If not, return specified DBName.
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

func findTablePos(s, t string) int {
	l := 0
	for i := range s {
		if s[i] == ' ' || s[i] == ',' {
			if len(t) == i-l && strings.Compare(s[l:i], t) == 0 {
				return l
			}
			l = i + 1
		}
	}
	if len(t) == len(s)-l && strings.Compare(s[l:], t) == 0 {
		return l
	}
	return -1
}

// SimpleCases captures simple SQL statements and uses string replacement instead of `restore` to improve performance.
// See https://github.com/pingcap/tidb/issues/22398.
func SimpleCases(node ast.StmtNode, defaultDB, origin string) (s string, ok bool) {
	if len(origin) == 0 {
		return "", false
	}
	insert, ok := node.(*ast.InsertStmt)
	if !ok {
		return "", false
	}
	if insert.Select != nil || insert.Setlist != nil || insert.OnDuplicate != nil || (insert.TableHints != nil && len(insert.TableHints) != 0) {
		return "", false
	}
	join := insert.Table.TableRefs
	if join.Tp != 0 || join.Right != nil {
		return "", false
	}
	ts, ok := join.Left.(*ast.TableSource)
	if !ok {
		return "", false
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return "", false
	}
	parenPos := strings.Index(origin, "(")
	if parenPos == -1 {
		return "", false
	}
	if strings.Contains(origin[:parenPos], ".") {
		return origin, true
	}
	lower := strings.ToLower(origin[:parenPos])
	pos := findTablePos(lower, tn.Name.L)
	if pos == -1 {
		return "", false
	}
	var builder strings.Builder
	builder.WriteString(origin[:pos])
	if tn.Schema.String() != "" {
		builder.WriteString(tn.Schema.String())
	} else {
		builder.WriteString(defaultDB)
	}
	builder.WriteString(".")
	builder.WriteString(origin[pos:])
	return builder.String(), true
}

// RestoreWithDefaultDB returns restore strings for StmtNode with defaultDB
// This function is customized for SQL bind usage.
func RestoreWithDefaultDB(node ast.StmtNode, defaultDB, origin string) string {
	if s, ok := SimpleCases(node, defaultDB, origin); ok {
		return s
	}
	var sb strings.Builder
	// Three flags for restore with default DB:
	// 1. RestoreStringSingleQuotes specifies to use single quotes to surround the string;
	// 2. RestoreSpacesAroundBinaryOperation specifies to add space around binary operation;
	// 3. RestoreStringWithoutCharset specifies to not print charset before string;
	// 4. RestoreNameBackQuotes specifies to use back quotes to surround the name;
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreSpacesAroundBinaryOperation|format.RestoreStringWithoutCharset|format.RestoreNameBackQuotes, &sb)
	ctx.DefaultDB = defaultDB
	if err := node.Restore(ctx); err != nil {
		logutil.BgLogger().Debug("[sql-bind] restore SQL failed", zap.Error(err))
		return ""
	}
	return sb.String()
}
