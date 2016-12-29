// Copyright 2016 PingCAP, Inc.
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

package xeval

import (
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalTwoBoolChildren(expr *tipb.Expr) (leftBool, rightBool int64, err error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if left.IsNull() {
		leftBool = compareResultNull
	} else {
		leftBool, err = left.ToBool(e.sc)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	}
	if right.IsNull() {
		rightBool = compareResultNull
	} else {
		rightBool, err = right.ToBool(e.sc)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	}
	return
}

func (e *Evaluator) evalCompareOps(expr *tipb.Expr) (types.Datum, error) {
	switch op := expr.GetTp(); op {
	case tipb.ExprType_NullEQ:
		return e.evalNullEQ(expr)
	case tipb.ExprType_Like:
		return e.evalLike(expr)
	case tipb.ExprType_In:
		return e.evalIn(expr)
	}

	cmp, err := e.compareTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == compareResultNull {
		return types.Datum{}, nil
	}
	switch op := expr.GetTp(); op {
	case tipb.ExprType_LT:
		return e.evalLT(cmp)
	case tipb.ExprType_LE:
		return e.evalLE(cmp)
	case tipb.ExprType_EQ:
		return e.evalEQ(cmp)
	case tipb.ExprType_NE:
		return e.evalNE(cmp)
	case tipb.ExprType_GT:
		return e.evalGT(cmp)
	case tipb.ExprType_GE:
		return e.evalGE(cmp)
	default:
		return types.Datum{}, errors.Errorf("Unknown binop type: %v", op)
	}
}

func (e *Evaluator) compareTwoChildren(expr *tipb.Expr) (int, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if left.IsNull() || right.IsNull() {
		return compareResultNull, nil
	}
	return left.CompareDatum(e.sc, right)
}

func (e *Evaluator) evalLT(cmp int) (types.Datum, error) {
	if cmp < 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalLE(cmp int) (types.Datum, error) {
	if cmp <= 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalEQ(cmp int) (types.Datum, error) {
	if cmp == 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalNE(cmp int) (types.Datum, error) {
	if cmp != 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalGE(cmp int) (types.Datum, error) {
	if cmp >= 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalGT(cmp int) (types.Datum, error) {
	if cmp > 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalNullEQ(expr *tipb.Expr) (types.Datum, error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	cmp, err := left.CompareDatum(e.sc, right)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if cmp == 0 {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

func (e *Evaluator) evalLike(expr *tipb.Expr) (types.Datum, error) {
	target, pattern, err := e.evalTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if target.IsNull() || pattern.IsNull() {
		return types.Datum{}, nil
	}
	targetStr, err := target.ToString()
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	patternStr, err := pattern.ToString()
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if containsAlphabet(patternStr) {
		patternStr = strings.ToLower(patternStr)
		targetStr = strings.ToLower(targetStr)
	}
	mType, trimmedPattern := matchType(patternStr)
	var matched bool
	switch mType {
	case matchExact:
		matched = targetStr == trimmedPattern
	case matchPrefix:
		matched = strings.HasPrefix(targetStr, trimmedPattern)
	case matchSuffix:
		matched = strings.HasSuffix(targetStr, trimmedPattern)
	case matchMiddle:
		matched = strings.Index(targetStr, trimmedPattern) != -1
	}
	if matched {
		return types.NewIntDatum(1), nil
	}
	return types.NewIntDatum(0), nil
}

const (
	matchExact  = 1
	matchPrefix = 2
	matchSuffix = 3
	matchMiddle = 4
)

func matchType(pattern string) (tp int, trimmed string) {
	switch len(pattern) {
	case 0:
		return matchExact, pattern
	case 1:
		if pattern[0] == '%' {
			return matchMiddle, ""
		}
		return matchExact, pattern
	default:
		first := pattern[0]
		last := pattern[len(pattern)-1]
		if first == '%' {
			if last == '%' {
				return matchMiddle, pattern[1 : len(pattern)-1]
			}
			return matchSuffix, pattern[1:]
		}
		if last == '%' {
			return matchPrefix, pattern[:len(pattern)-1]
		}
		return matchExact, pattern
	}

}

func containsAlphabet(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return true
		}
	}
	return false
}

func (e *Evaluator) evalIn(expr *tipb.Expr) (types.Datum, error) {
	if len(expr.Children) != 2 {
		return types.Datum{}, ErrInvalid.Gen("IN need 2 operands, got %d", len(expr.Children))
	}
	target, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if target.IsNull() {
		return types.Datum{}, nil
	}
	valueListExpr := expr.Children[1]
	if valueListExpr.GetTp() != tipb.ExprType_ValueList {
		return types.Datum{}, ErrInvalid.Gen("the second child should be value list type")
	}
	decoded, err := e.decodeValueList(valueListExpr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	in, err := e.checkIn(target, decoded.values)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if in {
		return types.NewDatum(1), nil
	}
	if decoded.hasNull {
		return types.Datum{}, nil
	}
	return types.NewDatum(0), nil
}

// The value list is in sorted order so we can do a binary search.
func (e *Evaluator) checkIn(target types.Datum, list []types.Datum) (bool, error) {
	var outerErr error
	n := sort.Search(len(list), func(i int) bool {
		val := list[i]
		cmp, err := val.CompareDatum(e.sc, target)
		if err != nil {
			outerErr = errors.Trace(err)
			return false
		}
		return cmp >= 0
	})
	if outerErr != nil {
		return false, errors.Trace(outerErr)
	}
	if n < 0 || n >= len(list) {
		return false, nil
	}
	cmp, err := list[n].CompareDatum(e.sc, target)
	if err != nil {
		return false, errors.Trace(err)
	}
	return cmp == 0, nil
}

func (e *Evaluator) decodeValueList(valueListExpr *tipb.Expr) (*decodedValueList, error) {
	if len(valueListExpr.Val) == 0 {
		// Empty value list.
		return &decodedValueList{}, nil
	}
	if e.valueLists == nil {
		e.valueLists = make(map[*tipb.Expr]*decodedValueList)
	}
	decoded := e.valueLists[valueListExpr]
	if decoded != nil {
		return decoded, nil
	}
	list, err := codec.Decode(valueListExpr.Val, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var hasNull bool
	for _, v := range list {
		if v.IsNull() {
			hasNull = true
		}
	}
	decoded = &decodedValueList{values: list, hasNull: hasNull}
	e.valueLists[valueListExpr] = decoded
	return decoded, nil
}
