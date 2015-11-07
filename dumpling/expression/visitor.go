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

import "github.com/juju/errors"

// Visitor represents a visitor pattern.
type Visitor interface {
	// VisitBetween visits Between expression.
	VisitBetween(b *Between) (Expression, error)

	// VisitBinaryOperation visits BinaryOperation expression.
	VisitBinaryOperation(o *BinaryOperation) (Expression, error)

	// VisitCall visits Call expression.
	VisitCall(c *Call) (Expression, error)

	// VisitCompareSubQuery visits CompareSubQuery expression.
	VisitCompareSubQuery(cs *CompareSubQuery) (Expression, error)

	// VisitDefault visits Default expression.
	VisitDefault(d *Default) (Expression, error)

	// VisitFunctionCase visits FunctionCase expression.
	VisitFunctionCase(f *FunctionCase) (Expression, error)

	// VisitFunctionCast visits FunctionCast expression.
	VisitFunctionCast(f *FunctionCast) (Expression, error)

	// VisitFunctionConvert visits FunctionConvert expression.
	VisitFunctionConvert(f *FunctionConvert) (Expression, error)

	// VisitFunctionSubstring visits FunctionSubstring expression.
	VisitFunctionSubstring(ss *FunctionSubstring) (Expression, error)

	// VisitFunctionSubstringIndex visits FunctionSubstringIndex expression.
	VisitFunctionSubstringIndex(ss *FunctionSubstringIndex) (Expression, error)

	// VisitFunctionLocate visits FunctionLocate expression.
	VisitFunctionLocate(ss *FunctionLocate) (Expression, error)

	// VisitExistsSubQuery visits ExistsSubQuery expression.
	VisitExistsSubQuery(es *ExistsSubQuery) (Expression, error)

	// VisitIdent visits Ident expression.
	VisitIdent(i *Ident) (Expression, error)

	// VisitIsNull visits IsNull expression.
	VisitIsNull(is *IsNull) (Expression, error)

	// VisitIsTruth visits IsTruth expression.
	VisitIsTruth(is *IsTruth) (Expression, error)

	// VisitParamMaker visits ParamMarker expression.
	VisitParamMaker(pm *ParamMarker) (Expression, error)

	// VisitPatternIn visits PatternIn expression.
	VisitPatternIn(n *PatternIn) (Expression, error)

	// VisitPatternLike visits PatternLike expression.
	VisitPatternLike(p *PatternLike) (Expression, error)

	// VisitPatternRegexp visits PatternRegexp expression.
	VisitPatternRegexp(p *PatternRegexp) (Expression, error)

	// VisitPExpr visits PExpr expression.
	VisitPExpr(p *PExpr) (Expression, error)

	// VisitPosition visits Position expression.
	VisitPosition(p *Position) (Expression, error)

	// VisitRow visits Row expression.
	VisitRow(r *Row) (Expression, error)

	// VisitSubQuery visits SubQuery expression.
	VisitSubQuery(sq SubQuery) (Expression, error)

	// VisitUnaryOperation visits UnaryOperation expression.
	VisitUnaryOperation(u *UnaryOperation) (Expression, error)

	// VisitValue visits Value expression.
	VisitValue(v Value) (Expression, error)

	// VisitValues visits Values expression.
	VisitValues(v *Values) (Expression, error)

	// VisitVariable visits Variable expression.
	VisitVariable(v *Variable) (Expression, error)

	// VisitWhenClause visits WhenClause expression.
	VisitWhenClause(w *WhenClause) (Expression, error)

	// VisitExtract visits Extract expression.
	VisitExtract(v *Extract) (Expression, error)

	// VisitFunctionTrim visits FunctionTrim expression.
	VisitFunctionTrim(v *FunctionTrim) (Expression, error)

	// VisitDateArith visits DateArith expression.
	VisitDateArith(da *DateArith) (Expression, error)
}

// BaseVisitor is the base implementation of Visitor.
// It traverses the expression tree and call expression's Accept function.
// It can not be used directly.
// A specific Visitor implementation can embed it in and only implements
// desired methods to do the job.
type BaseVisitor struct {
	V Visitor
}

// VisitBetween implements Visitor interface.
func (bv *BaseVisitor) VisitBetween(b *Between) (Expression, error) {
	var err error
	b.Expr, err = b.Expr.Accept(bv.V)
	if err != nil {
		return b, errors.Trace(err)
	}
	b.Left, err = b.Left.Accept(bv.V)
	if err != nil {
		return b, errors.Trace(err)
	}
	b.Right, err = b.Right.Accept(bv.V)
	if err != nil {
		return b, errors.Trace(err)
	}
	return b, nil
}

// VisitBinaryOperation implements Visitor interface.
func (bv *BaseVisitor) VisitBinaryOperation(o *BinaryOperation) (Expression, error) {
	var err error
	o.L, err = o.L.Accept(bv.V)
	if err != nil {
		return o, errors.Trace(err)
	}
	o.R, err = o.R.Accept(bv.V)
	if err != nil {
		return o, errors.Trace(err)
	}
	return o, nil
}

// VisitCall implements Visitor interface.
func (bv *BaseVisitor) VisitCall(c *Call) (Expression, error) {
	var err error
	for i := range c.Args {
		c.Args[i], err = c.Args[i].Accept(bv.V)
		if err != nil {
			return c, errors.Trace(err)
		}
	}
	return c, nil
}

// VisitCompareSubQuery implements Visitor interface.
func (bv *BaseVisitor) VisitCompareSubQuery(cs *CompareSubQuery) (Expression, error) {
	var err error
	cs.L, err = cs.L.Accept(bv.V)
	if err != nil {
		return cs, errors.Trace(err)
	}
	_, err = cs.R.Accept(bv.V)
	if err != nil {
		return cs, errors.Trace(err)
	}
	return cs, nil
}

// VisitDefault implements Visitor interface.
func (bv *BaseVisitor) VisitDefault(d *Default) (Expression, error) {
	return d, nil
}

// VisitExistsSubQuery implements Visitor interface.
func (bv *BaseVisitor) VisitExistsSubQuery(es *ExistsSubQuery) (Expression, error) {
	var err error
	_, err = es.Sel.Accept(bv.V)
	if err != nil {
		return es, errors.Trace(err)
	}
	return es, nil
}

// VisitFunctionCase implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionCase(f *FunctionCase) (Expression, error) {
	var err error
	if f.Value != nil {
		f.Value, err = f.Value.Accept(bv.V)
		if err != nil {
			return f, errors.Trace(err)
		}
	}
	for i := range f.WhenClauses {
		_, err = f.WhenClauses[i].Accept(bv.V)
		if err != nil {
			return f, errors.Trace(err)
		}
	}
	if f.ElseClause != nil {
		f.ElseClause, err = f.ElseClause.Accept(bv.V)
		if err != nil {
			return f, errors.Trace(err)
		}
	}
	return f, nil
}

//VisitFunctionCast implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionCast(f *FunctionCast) (Expression, error) {
	var err error
	f.Expr, err = f.Expr.Accept(bv.V)
	if err != nil {
		return f, errors.Trace(err)
	}
	return f, nil
}

// VisitFunctionConvert implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionConvert(f *FunctionConvert) (Expression, error) {
	var err error
	f.Expr, err = f.Expr.Accept(bv.V)
	if err != nil {
		return f, errors.Trace(err)
	}
	return f, nil
}

// VisitFunctionSubstring implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionSubstring(ss *FunctionSubstring) (Expression, error) {
	var err error
	ss.StrExpr, err = ss.StrExpr.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	ss.Pos, err = ss.Pos.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	if ss.Len == nil {
		return ss, nil
	}
	ss.Len, err = ss.Len.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	return ss, nil
}

// VisitFunctionSubstringIndex implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionSubstringIndex(ss *FunctionSubstringIndex) (Expression, error) {
	var err error
	ss.StrExpr, err = ss.StrExpr.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	ss.Delim, err = ss.Delim.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	if ss.Count == nil {
		return ss, nil
	}
	ss.Count, err = ss.Count.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	return ss, nil
}

// VisitFunctionLocate implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionLocate(ss *FunctionLocate) (Expression, error) {
	var err error
	ss.Str, err = ss.Str.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	ss.SubStr, err = ss.SubStr.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	if ss.Pos == nil {
		return ss, nil
	}
	ss.Pos, err = ss.Pos.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	return ss, nil
}

// VisitIdent implements Visitor interface.
func (bv *BaseVisitor) VisitIdent(i *Ident) (Expression, error) {
	return i, nil
}

// VisitIsNull implements Visitor interface.
func (bv *BaseVisitor) VisitIsNull(is *IsNull) (Expression, error) {
	var err error
	is.Expr, err = is.Expr.Accept(bv.V)
	if err != nil {
		return is, errors.Trace(err)
	}
	return is, nil
}

// VisitIsTruth implements Visitor interface.
func (bv *BaseVisitor) VisitIsTruth(is *IsTruth) (Expression, error) {
	var err error
	is.Expr, err = is.Expr.Accept(bv.V)
	if err != nil {
		return is, errors.Trace(err)
	}
	return is, nil
}

// VisitParamMaker implements Visitor interface.
func (bv *BaseVisitor) VisitParamMaker(pm *ParamMarker) (Expression, error) {
	if pm.Expr == nil {
		return pm, nil
	}
	var err error
	pm.Expr, err = pm.Expr.Accept(bv.V)
	if err != nil {
		return pm, errors.Trace(err)
	}
	return pm, nil
}

// VisitPatternIn implements Visitor interface.
func (bv *BaseVisitor) VisitPatternIn(n *PatternIn) (Expression, error) {
	var err error
	n.Expr, err = n.Expr.Accept(bv.V)
	if err != nil {
		return n, errors.Trace(err)
	}
	if n.Sel != nil {
		_, err = n.Sel.Accept(bv.V)
		if err != nil {
			return n, errors.Trace(err)
		}
	}
	for i := range n.List {
		n.List[i], err = n.List[i].Accept(bv.V)
		if err != nil {
			return n, errors.Trace(err)
		}
	}
	return n, nil
}

// VisitPatternLike implements Visitor interface.
func (bv *BaseVisitor) VisitPatternLike(p *PatternLike) (Expression, error) {
	var err error
	p.Expr, err = p.Expr.Accept(bv.V)
	if err != nil {
		return p, errors.Trace(err)
	}
	p.Pattern, err = p.Pattern.Accept(bv.V)
	if err != nil {
		return p, errors.Trace(err)
	}
	return p, nil
}

// VisitPatternRegexp implements Visitor interface.
func (bv *BaseVisitor) VisitPatternRegexp(p *PatternRegexp) (Expression, error) {
	var err error
	p.Expr, err = p.Expr.Accept(bv.V)
	if err != nil {
		return p, errors.Trace(err)
	}
	p.Pattern, err = p.Pattern.Accept(bv.V)
	if err != nil {
		return p, errors.Trace(err)
	}
	return p, nil
}

// VisitPExpr implements Visitor interface.
func (bv *BaseVisitor) VisitPExpr(p *PExpr) (Expression, error) {
	var err error
	p.Expr, err = p.Expr.Accept(bv.V)
	if err != nil {
		return p, errors.Trace(err)
	}
	return p, nil
}

// VisitPosition implements Visitor interface.
func (bv *BaseVisitor) VisitPosition(p *Position) (Expression, error) {
	return p, nil
}

// VisitRow implements Visitor interface.
func (bv *BaseVisitor) VisitRow(r *Row) (Expression, error) {
	var err error
	for i := range r.Values {
		r.Values[i], err = r.Values[i].Accept(bv.V)
		if err != nil {
			return r, errors.Trace(err)
		}
	}
	return r, nil
}

// VisitSubQuery implements Visitor interface.
func (bv *BaseVisitor) VisitSubQuery(sq SubQuery) (Expression, error) {
	return sq, nil
}

// VisitUnaryOperation implements Visitor interface.
func (bv *BaseVisitor) VisitUnaryOperation(u *UnaryOperation) (Expression, error) {
	var err error
	u.V, err = u.V.Accept(bv.V)
	if err != nil {
		return u, errors.Trace(err)
	}
	return u, nil
}

// VisitValue implements Visitor interface.
func (bv *BaseVisitor) VisitValue(v Value) (Expression, error) {
	return v, nil
}

// VisitValues implements Visitor interface.
func (bv *BaseVisitor) VisitValues(v *Values) (Expression, error) {
	return v, nil
}

// VisitVariable implements Visitor interface.
func (bv *BaseVisitor) VisitVariable(v *Variable) (Expression, error) {
	return v, nil
}

// VisitWhenClause implements Visitor interface.
func (bv *BaseVisitor) VisitWhenClause(w *WhenClause) (Expression, error) {
	var err error
	w.Expr, err = w.Expr.Accept(bv.V)
	if err != nil {
		return w, errors.Trace(err)
	}
	w.Result, err = w.Result.Accept(bv.V)
	if err != nil {
		return w, errors.Trace(err)
	}
	return w, nil
}

// VisitExtract implements Visitor interface.
func (bv *BaseVisitor) VisitExtract(v *Extract) (Expression, error) {
	var err error
	v.Date, err = v.Date.Accept(bv.V)
	return v, errors.Trace(err)
}

// VisitFunctionTrim implements Visitor interface.
func (bv *BaseVisitor) VisitFunctionTrim(ss *FunctionTrim) (Expression, error) {
	var err error
	ss.Str, err = ss.Str.Accept(bv.V)
	if err != nil {
		return ss, errors.Trace(err)
	}
	if ss.RemStr != nil {
		ss.RemStr, err = ss.RemStr.Accept(bv.V)
		if err != nil {
			return ss, errors.Trace(err)
		}
	}
	return ss, nil
}

// VisitDateArith implements Visitor interface.
func (bv *BaseVisitor) VisitDateArith(da *DateArith) (Expression, error) {
	var err error
	da.Date, err = da.Date.Accept(bv.V)
	if err != nil {
		return da, errors.Trace(err)
	}

	da.Interval, err = da.Interval.Accept(bv.V)
	if err != nil {
		return da, errors.Trace(err)
	}

	return da, nil
}
