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
	var setter flagSetter
	n.Accept(&setter)
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
		x.SetFlag(ast.FlagHasReference)
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
	case *ast.ParamMarkerExpr:
		x.SetFlag(ast.FlagHasParamMarker)
	case *ast.ParenthesesExpr:
		x.SetFlag(x.Expr.GetFlag())
	case *ast.PositionExpr:
		x.SetFlag(ast.FlagHasReference)
	case *ast.PatternRegexpExpr:
		f.patternRegexp(x)
	case *ast.RowExpr:
		f.row(x)
	case *ast.UnaryOperationExpr:
		x.SetFlag(x.V.GetFlag())
	case *ast.ValuesExpr:
		x.SetFlag(ast.FlagHasReference)
	case *ast.VariableExpr:
		x.SetFlag(ast.FlagHasVariable)
	case *ast.FuncCallExpr:
		f.funcCall(x)
	case *ast.FuncExtractExpr:
		x.SetFlag(ast.FlagHasFunc | x.Date.GetFlag())
	case *ast.FuncConvertExpr:
		x.SetFlag(ast.FlagHasFunc | x.Expr.GetFlag())
	case *ast.FuncCastExpr:
		x.SetFlag(ast.FlagHasFunc | x.Expr.GetFlag())
	case *ast.FuncSubstringExpr:
		f.funcSubstring(x)
	case *ast.FuncSubstringIndexExpr:
		f.funcSubstringIndex(x)
	case *ast.FuncLocateExpr:
		f.funcLocate(x)
	case *ast.FuncTrimExpr:
		f.funcTrim(x)
	case *ast.FuncDateArithExpr:
		f.funcDateArith(x)
	case *ast.AggregateFuncExpr:
		f.aggregateFunc(x)
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

func (f *flagSetter) patternRegexp(x *ast.PatternRegexpExpr) {
	flag := x.Pattern.GetFlag()
	if x.Expr != nil {
		flag |= x.Expr.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) row(x *ast.RowExpr) {
	var flag uint64
	for _, val := range x.Values {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcCall(x *ast.FuncCallExpr) {
	flag := ast.FlagHasFunc
	for _, val := range x.Args {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcSubstring(x *ast.FuncSubstringExpr) {
	flag := ast.FlagHasFunc | x.StrExpr.GetFlag() | x.Pos.GetFlag()
	if x.Len != nil {
		flag |= x.Len.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcSubstringIndex(x *ast.FuncSubstringIndexExpr) {
	flag := ast.FlagHasFunc | x.StrExpr.GetFlag()
	if x.Delim != nil {
		flag |= x.Delim.GetFlag()
	}
	if x.Count != nil {
		flag |= x.Count.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcLocate(x *ast.FuncLocateExpr) {
	flag := ast.FlagHasFunc | x.Str.GetFlag()
	if x.SubStr != nil {
		flag |= x.SubStr.GetFlag()
	}
	if x.Pos != nil {
		flag |= x.Pos.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcTrim(x *ast.FuncTrimExpr) {
	flag := ast.FlagHasFunc | x.Str.GetFlag()
	if x.RemStr != nil {
		flag |= x.RemStr.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcDateArith(x *ast.FuncDateArithExpr) {
	flag := ast.FlagHasFunc | x.Date.GetFlag()
	if x.Interval != nil {
		flag |= x.Interval.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) aggregateFunc(x *ast.AggregateFuncExpr) {
	flag := ast.FlagHasAggregateFunc
	for _, val := range x.Args {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}
