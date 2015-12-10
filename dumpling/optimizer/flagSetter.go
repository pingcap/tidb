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

package optimizer

import "github.com/pingcap/tidb/ast"

func setFlag(n ast.Node) {

}

type flagSetter struct {

}

func (f *flagSetter) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (f *flagSetter) Leave(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.ValueExpr:
	case *ast.BetweenExpr:
		x.SetFlag(x.Expr.GetFlag() | x.Left.GetFlag() | x.Right.GetFlag())
	case *ast.BinaryOperationExpr:
		x.SetFlag(x.L.GetFlag() | x.R.GetFlag())
	case *ast.CaseExpr:
		f.caseExpr(x)
	case *ast.SubqueryExpr:
		x.SetFlag(ast.FlagHasSubquery)
	case *ast.CompareSubqueryExpr:
		x.SetFlag(x.L.GetFlag() | x.R.GetFlag())
	case *ast.ColumnNameExpr:
		x.SetFlag(ast.FlagHasColumnName)
	case *ast.DefaultExpr:
	case *ast.ExistsSubqueryExpr:
		x.SetFlag(x.Sel.GetFlag())
	case *ast.PatternInExpr:
		f.patternIn(x)
	case *ast.IsNullExpr:
		x.SetFlag(x.Expr.GetFlag())
	case *ast.IsTruthExpr:
		x.SetFlag(x.Expr.GetFlag())
	case *ast.PatternLikeExpr:
		f.patternLike(x)
	}

	return in, true
}

func (f *flagSetter) caseExpr(x *ast.CaseExpr) {
	var flag uint64
	if x.Value != nil {
		flag |= x.Value.GetFlag()
	}
	for _, val := range x.WhenClauses {
		flag |= val.Expr.GetFlag()
		flag |= val.Result.GetFlag()
	}
	if x.ElseClause != nil {
		flag |= x.ElseClause.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) patternIn(x *ast.PatternInExpr) {
	flag := x.Expr.GetFlag()
	for _, val := range x.List {
		flag |= val.GetFlag()
	}
	if x.Sel != nil {
		flag |= x.Sel.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) patternLike(x *ast.PatternLikeExpr) {
	flag := x.Pattern.GetFlag()
	if x.Expr != nil {
		flag |= x.Expr.GetFlag()
	}
	x.SetFlag(flag)
}