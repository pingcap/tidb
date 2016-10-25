// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

var (
	inequalityFuncs = map[string]string{
		ast.LT:   ast.LT,
		ast.GT:   ast.GT,
		ast.LE:   ast.LE,
		ast.GE:   ast.GE,
		ast.NE:   ast.NE,
		ast.Like: ast.Like,
	}
)

func addSelection(p Plan, child LogicalPlan, conditions []expression.Expression, allocator *idAllocator) error {
	selection := &Selection{
		Conditions:      conditions,
		baseLogicalPlan: newBaseLogicalPlan(Sel, allocator)}
	selection.self = selection
	selection.initID()
	selection.SetSchema(child.GetSchema().DeepCopy())
	return InsertPlan(p, child, selection)
}

// columnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func columnSubstitute(expr expression.Expression, schema expression.Schema, newExprs []expression.Expression) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		id := schema.GetIndex(v)
		if id == -1 {
			log.Errorf("Can't find columns %s in schema %s", v, schema)
		}
		return newExprs[id]
	case *expression.ScalarFunction:
		for i, arg := range v.Args {
			v.Args[i] = columnSubstitute(arg, schema, newExprs)
		}
	}
	return expr
}

// propagateConstant propagate constant values of equality predicates and inequality predicates in a condition.
func propagateConstant(conditions []expression.Expression) []expression.Expression {
	if len(conditions) == 0 {
		return conditions
	}
	// Propagate constants in equality predicates.
	// e.g. for condition: "a = b and b = c and c = a and a = 1";
	// we propagate constant as the following step:
	// first: "1 = b and b = c and c = 1 and a = 1";
	// next:  "1 = b and 1 = c and c = 1 and a = 1";
	// next:  "1 = b and 1 = c and 1 = 1 and a = 1";
	// next:  "1 = b and 1 = c and a = 1";

	// e.g for condition: "a = b and b = c and b = 2 and a = 1";
	// we propagate constant as the following step:
	// first: "a = 2 and 2 = c and b = 2 and a = 1";
	// next:  "a = 2 and 2 = c and b = 2 and 2 = 1";
	// next:  "0"
	isSource := make([]bool, len(conditions))
	type transitiveEqualityPredicate map[string]*expression.Constant // transitive equality predicates between one column and one constant
	for {
		equalities := make(transitiveEqualityPredicate, 0)
		for i, getOneEquality := 0, false; i < len(conditions) && !getOneEquality; i++ {
			if isSource[i] {
				continue
			}
			expr, ok := conditions[i].(*expression.ScalarFunction)
			if !ok {
				continue
			}
			// process the included OR conditions recursively to do the same for CNF item.
			switch expr.FuncName.L {
			case ast.OrOr:
				expressions := expression.SplitDNFItems(conditions[i])
				var newExpression []expression.Expression
				for _, v := range expressions {
					newExpression = append(newExpression, propagateConstant([]expression.Expression{v})...)
				}
				conditions[i] = expression.ComposeDNFCondition(newExpression)
				isSource[i] = true
			case ast.AndAnd:
				newExpression := propagateConstant(expression.SplitCNFItems(conditions[i]))
				conditions[i] = expression.ComposeCNFCondition(newExpression)
				isSource[i] = true
			case ast.EQ:
				var (
					col *expression.Column
					val *expression.Constant
				)
				leftConst, leftIsConst := expr.Args[0].(*expression.Constant)
				rightConst, rightIsConst := expr.Args[1].(*expression.Constant)
				leftCol, leftIsCol := expr.Args[0].(*expression.Column)
				rightCol, rightIsCol := expr.Args[1].(*expression.Column)
				if rightIsConst && leftIsCol {
					col = leftCol
					val = rightConst
				} else if leftIsConst && rightIsCol {
					col = rightCol
					val = leftConst
				} else {
					continue
				}
				equalities[string(col.HashCode())] = val
				isSource[i] = true
				getOneEquality = true
			}
		}
		if len(equalities) == 0 {
			break
		}
		for i := 0; i < len(conditions); i++ {
			if isSource[i] {
				continue
			}
			if len(equalities) != 0 {
				conditions[i] = constantSubstitute(equalities, conditions[i])
			}
		}
	}
	// Propagate transitive inequality predicates.
	// e.g for conditions "a = b and c = d and a = c and g = h and b > 0 and e != 0 and g like 'abc'",
	//     we propagate constant as the following step:
	// 1. build multiple equality predicates(mep):
	//    =(a, b, c, d), =(g, h).
	// 2. extract inequality predicates between one constant and one column,
	//    and rewrite them using the root column of a multiple equality predicate:
	//    b > 0, e != 0, g like 'abc' ==> a > 0, g like 'abc'.
	//    ATTENTION: here column 'e' doesn't belong to any mep, so we skip "e != 0".
	// 3. propagate constants in these inequality predicates, and we finally get:
	//    "a = b and c = d and a = c and e = f and g = h and e != 0 and a > 0 and b > 0 and c > 0 and d > 0 and g like 'abc' and h like 'abc' ".
	multipleEqualities := make(map[*expression.Column]*expression.Column, 0)
	for _, cond := range conditions { // build multiple equality predicates.
		expr, ok := cond.(*expression.ScalarFunction)
		if ok && expr.FuncName.L == ast.EQ {
			left, ok1 := expr.Args[0].(*expression.Column)
			right, ok2 := expr.Args[1].(*expression.Column)
			if ok1 && ok2 {
				UnionColumns(left, right, multipleEqualities)
			}
		}
	}
	if len(multipleEqualities) == 0 {
		return conditions
	}

	type inequalityFactor struct {
		FuncName string
		Factor   []*expression.Constant
	}
	type transitiveInEqualityPredicate map[string][]inequalityFactor // transitive inequality predicates between one column and one constant.
	inequalities := make(transitiveInEqualityPredicate, 0)
	for i := 0; i < len(conditions); i++ { // extract inequality predicates.
		var (
			column   *expression.Column
			equalCol *expression.Column // the root column corresponding to a column in a multiple equality predicate.
			val      *expression.Constant
			funcName string
		)
		expr, ok := conditions[i].(*expression.ScalarFunction)
		if !ok {
			continue
		}
		funcName, ok = inequalityFuncs[expr.FuncName.L]
		if !ok {
			continue
		}
		leftConst, leftIsConst := expr.Args[0].(*expression.Constant)
		rightConst, rightIsConst := expr.Args[1].(*expression.Constant)
		leftCol, leftIsCol := expr.Args[0].(*expression.Column)
		rightCol, rightIsCol := expr.Args[1].(*expression.Column)
		if rightIsConst && leftIsCol {
			column = leftCol
			val = rightConst
		} else if leftIsConst && rightIsCol {
			column = rightCol
			val = leftConst
		} else {
			continue
		}
		equalCol, ok = multipleEqualities[column]
		if !ok { // no need to propagate inequality predicates whose column is only equal to itself.
			continue
		}
		colHashCode := string(equalCol.HashCode())
		if funcName == ast.Like { // func 'LIKE' need 3 input arguments, so here we handle it alone.
			inequalities[colHashCode] = append(inequalities[colHashCode], inequalityFactor{FuncName: ast.Like, Factor: []*expression.Constant{val, expr.Args[2].(*expression.Constant)}})
		} else {
			inequalities[colHashCode] = append(inequalities[colHashCode], inequalityFactor{FuncName: funcName, Factor: []*expression.Constant{val}})
		}
		conditions = append(conditions[:i], conditions[i+1:]...)
		i--
	}
	if len(inequalities) == 0 {
		return conditions
	}
	for k, v := range multipleEqualities { // propagate constants in inequality predicates.
		for _, x := range inequalities[string(v.HashCode())] {
			funcName, factors := x.FuncName, x.Factor
			if funcName == ast.Like {
				for i := 0; i < len(factors); i += 2 {
					newFunc, _ := expression.NewFunction(funcName, types.NewFieldType(mysql.TypeTiny), k, factors[i], factors[i+1])
					conditions = append(conditions, newFunc)
				}
			} else {
				for i := 0; i < len(factors); i++ {
					newFunc, _ := expression.NewFunction(funcName, types.NewFieldType(mysql.TypeTiny), k, factors[i])
					conditions = append(conditions, newFunc)
					i++
				}
			}
		}
	}
	return conditions
}

// UnionColumns uses union-find to build multiple equality predicates.
func UnionColumns(leftExpr *expression.Column, rightExpr *expression.Column, multipleEqualities map[*expression.Column]*expression.Column) {
	rootOfLeftExpr, ok1 := multipleEqualities[leftExpr]
	rootOfRightExpr, ok2 := multipleEqualities[rightExpr]
	if !ok1 && !ok2 {
		multipleEqualities[leftExpr] = leftExpr
		multipleEqualities[rightExpr] = leftExpr
	} else if ok1 && !ok2 {
		multipleEqualities[rightExpr] = rootOfLeftExpr
	} else if !ok1 && ok2 {
		multipleEqualities[leftExpr] = rootOfRightExpr
	} else if !rootOfLeftExpr.Equal(rootOfRightExpr) {
		for k, v := range multipleEqualities {
			if v.Equal(rootOfRightExpr) {
				multipleEqualities[k] = rootOfLeftExpr
			}
		}
	}
}

// constantSubstitute substitute column expression in a condition by an equivalent constant.
func constantSubstitute(equalities map[string]*expression.Constant, condition expression.Expression) expression.Expression {
	switch expr := condition.(type) {
	case *expression.Column:
		if v, ok := equalities[string(expr.HashCode())]; ok {
			return v
		}
	case *expression.ScalarFunction:
		for i, arg := range expr.Args {
			expr.Args[i] = constantSubstitute(equalities, arg)
		}
		if _, ok := evaluator.Funcs[expr.FuncName.L]; ok {
			condition, _ = expression.NewFunction(expr.FuncName.L, expr.RetType, expr.Args...)
		}
		return condition
	}
	return condition
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Selection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retP LogicalPlan, err error) {
	retConditions, child, err1 := p.GetChildByIndex(0).(LogicalPlan).PredicatePushDown(propagateConstant(append(p.Conditions, predicates...)))
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	if len(retConditions) > 0 {
		p.Conditions = retConditions
		retP = p
	} else {
		err1 = RemovePlan(p)
		if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
		retP = child
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *TableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Join) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	err = outerJoinSimplify(p, predicates)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	groups, valid := tryToGetJoinGroup(p)
	if valid {
		e := joinReOrderSolver{allocator: p.allocator}
		e.reorderJoin(groups, predicates)
		newJoin := e.resultJoin
		parent := p.parents[0]
		newJoin.SetParents(parent)
		parent.ReplaceChild(p, newJoin)
		return newJoin.PredicatePushDown(predicates)
	}
	var leftCond, rightCond []expression.Expression
	retPlan = p
	leftPlan := p.GetChildByIndex(0).(LogicalPlan)
	rightPlan := p.GetChildByIndex(1).(LogicalPlan)
	var (
		equalCond                              []*expression.ScalarFunction
		leftPushCond, rightPushCond, otherCond []expression.Expression
	)
	if p.JoinType != InnerJoin {
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan)
	} else {
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		if len(tempCond) != 0 {
			tempCond = append(tempCond, predicates...)
			equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(propagateConstant(tempCond), leftPlan, rightPlan)
		} else { // "on" is not used.
			equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan)
		}
	}
	switch p.JoinType {
	case LeftOuterJoin, SemiJoinWithAux:
		rightCond = p.RightConditions
		p.RightConditions = nil
		leftCond = leftPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		leftCond = p.LeftConditions
		p.LeftConditions = nil
		rightCond = rightPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin:
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan)
		leftCond = propagateConstant(append(p.LeftConditions, leftPushCond...))
		rightCond = propagateConstant(append(p.RightConditions, rightPushCond...))
		p.LeftConditions = nil
		p.RightConditions = nil
	case InnerJoin:
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	}
	leftRet, _, err1 := leftPlan.PredicatePushDown(leftCond)
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	rightRet, _, err2 := rightPlan.PredicatePushDown(rightCond)
	if err2 != nil {
		return nil, nil, errors.Trace(err2)
	}
	if len(leftRet) > 0 {
		err2 = addSelection(p, leftPlan, leftRet, p.allocator)
		if err2 != nil {
			return nil, nil, errors.Trace(err2)
		}
	}
	if len(rightRet) > 0 {
		err2 = addSelection(p, rightPlan, rightRet, p.allocator)
		if err2 != nil {
			return nil, nil, errors.Trace(err2)
		}
	}
	return
}

// outerJoinSimplify simplifies outer join.
func outerJoinSimplify(p *Join, predicates []expression.Expression) error {
	var innerTable, outerTable LogicalPlan
	child1 := p.GetChildByIndex(0).(LogicalPlan)
	child2 := p.GetChildByIndex(1).(LogicalPlan)
	var fullConditions []expression.Expression
	if p.JoinType == LeftOuterJoin {
		innerTable = child2
		outerTable = child1
	} else if p.JoinType == RightOuterJoin || p.JoinType == InnerJoin {
		innerTable = child1
		outerTable = child2
	} else {
		return nil
	}
	// first simplify embedded outer join.
	// When trying to simplify an embedded outer join operation in a query,
	// we must take into account the join condition for the embedding outer join together with the WHERE condition.
	if innerPlan, ok := innerTable.(*Join); ok {
		fullConditions = concatOnAndWhereConds(p, predicates)
		err := outerJoinSimplify(innerPlan, fullConditions)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if outerPlan, ok := outerTable.(*Join); ok {
		if fullConditions != nil {
			fullConditions = concatOnAndWhereConds(p, predicates)
		}
		err := outerJoinSimplify(outerPlan, fullConditions)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if p.JoinType == InnerJoin {
		return nil
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		isOk, err := isNullRejected(innerTable.GetSchema(), expr)
		if err != nil {
			return errors.Trace(err)
		}
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
	return nil
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(schema expression.Schema, expr expression.Expression) (bool, error) {
	result, err := calculateResultOfExpression(schema, expr)
	if err != nil {
		return false, errors.Trace(err)
	}
	x, ok := result.(*expression.Constant)
	if !ok {
		return false, nil
	}
	if x.Value.IsNull() {
		return true, nil
	} else if isTrue, err := x.Value.ToBool(); err != nil || isTrue == 0 {
		return true, errors.Trace(err)
	}
	return false, nil
}

// calculateResultOfExpression set inner table columns in a expression as null and calculate the finally result of the scalar function.
func calculateResultOfExpression(schema expression.Schema, expr expression.Expression) (expression.Expression, error) {
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		var err error
		args := make([]expression.Expression, len(x.Args))
		for i, arg := range x.Args {
			args[i], err = calculateResultOfExpression(schema, arg)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return expression.NewFunction(x.FuncName.L, types.NewFieldType(mysql.TypeTiny), args...)
	case *expression.Column:
		if schema.GetIndex(x) == -1 {
			return x, nil
		}
		constant := &expression.Constant{Value: types.Datum{}}
		constant.Value.SetNull()
		return constant, nil
	default:
		return x.DeepCopy(), nil
	}
}

// concatOnAndWhereConds concatenate ON conditions with WHERE conditions.
func concatOnAndWhereConds(join *Join, predicates []expression.Expression) []expression.Expression {
	equalConds, leftConds, rightConds, otherConds := join.EqualConditions, join.LeftConditions, join.RightConditions, join.OtherConditions
	ans := make([]expression.Expression, 0, len(equalConds)+len(leftConds)+len(rightConds)+len(predicates))
	for _, v := range equalConds {
		ans = append(ans, v)
	}
	ans = append(ans, leftConds...)
	ans = append(ans, rightConds...)
	ans = append(ans, otherConds...)
	ans = append(ans, predicates...)
	return ans
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Projection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	var push []expression.Expression
	for _, cond := range predicates {
		canSubstitute := true
		extractedCols, _ := extractColumn(cond, nil, nil)
		for _, col := range extractedCols {
			id := p.GetSchema().GetIndex(col)
			if _, ok := p.Exprs[id].(*expression.ScalarFunction); ok {
				canSubstitute = false
				break
			}
		}
		if canSubstitute {
			push = append(push, columnSubstitute(cond, p.GetSchema(), p.Exprs))
		} else {
			ret = append(ret, cond)
		}
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	restConds, _, err1 := child.PredicatePushDown(propagateConstant(push))
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	if len(restConds) > 0 {
		err1 = addSelection(p, child, restConds, p.allocator)
		if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Union) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	for _, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		for _, cond := range predicates {
			newCond := columnSubstitute(cond.DeepCopy(), p.GetSchema(), expression.Schema2Exprs(proj.GetSchema()))
			newExprs = append(newExprs, newCond)
		}
		retCond, _, err := proj.(LogicalPlan).PredicatePushDown(newExprs)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(retCond) != 0 {
			addSelection(p, proj.(LogicalPlan), retCond, p.allocator)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Aggregation) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// TODO: implement aggregation push down.
	var condsToPush []expression.Expression
	for _, cond := range predicates {
		if _, ok := cond.(*expression.Constant); ok {
			condsToPush = append(condsToPush, cond)
		}
	}
	p.baseLogicalPlan.PredicatePushDown(condsToPush)
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Apply) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	var push []expression.Expression
	for _, cond := range predicates {
		extractedCols, _ := extractColumn(cond, nil, nil)
		canPush := true
		for _, col := range extractedCols {
			if child.GetSchema().GetIndex(col) == -1 {
				canPush = false
				break
			}
		}
		if canPush {
			push = append(push, cond)
		} else {
			ret = append(ret, cond)
		}
	}
	childRet, _, err := child.PredicatePushDown(push)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return append(ret, childRet...), p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Limit) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// Limit forbids any condition to push down.
	_, _, err := p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Sort) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Trim) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *MaxOneRow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Exists) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Distinct) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Insert) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *SelectLock) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Update) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Delete) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}
