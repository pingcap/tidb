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

package expression

type Visitor interface {
	VisitBetween(b *Between) (Expression, error)
	VisitBinaryOperation(o *BinaryOperation) (Expression, error)
	VisitCall(c *Call) (Expression, error)
	VisitCompareSubQuery(cs *CompareSubQuery) (Expression, error)
	VisitDefault(d *Default) (Expression, error)
	VisitFunctionCase(f *FunctionCase) (Expression, error)
	VisitFunctionCast(f *FunctionCast) (Expression, error)
	VisitFunctionConvert(f *FunctionConvert) (Expression, error)
	VisitFunctionSubstring(ss *FunctionSubstring) (Expression, error)
	VisitExistsSubQuery(es *ExistsSubQuery) (Expression, error)
	VisitIdent(i *Ident) (Expression, error)
	VisitIsNull(is *IsNull) (Expression, error)
	VisitIsTruth(is *IsTruth) (Expression, error)
	VisitParamMaker(pm *ParamMarker) (Expression, error)
	VisitPatternIn(n *PatternIn) (Expression, error)
	VisitPatternLike(p *PatternLike) (Expression, error)
	VisitPatternRegexp(p *PatternRegexp) (Expression, error)
	VisitPExpr(p *PExpr) (Expression, error)
	VisitPosition(p *Position) (Expression, error)
	VisitRow(r *Row) (Expression, error)
	VisitSubQuery(sq SubQuery) (Expression, error)
	VisitUnaryOpeartion(u *UnaryOperation) (Expression, error)
	VisitValue(v Value) (Expression, error)
	VisitValues(v *Values) (Expression, error)
	VisitVariable(v *Variable) (Expression, error)
	VisitWhenClause(w *WhenClause) (Expression, error)
}
