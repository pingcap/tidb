// Copyright 2019 PingCAP, Inc.
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

package core

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// extractHelper contains some common utililty functions for all extractor.
// define an individual struct instead of a bunch of un-exported functions
// to avoid polluting the global scope of current package.
type extractHelper struct {
	enableScalarPushDown bool
	pushedDownFuncs      map[string]func(string) string

	// Store whether the extracted strings for a specific column are converted to lower case
	extractLowerString map[string]bool
}

func (extractHelper) extractColInConsExpr(ctx base.PlanContext, extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
	args := expr.GetArgs()
	col, isCol := args[0].(*expression.Column)
	if !isCol {
		return "", nil
	}
	name, found := extractCols[col.UniqueID]
	if !found {
		return "", nil
	}
	// All expressions in IN must be a constant
	// SELECT * FROM t1 WHERE c IN ('1', '2')
	results := make([]types.Datum, 0, len(args[1:]))
	for _, arg := range args[1:] {
		constant, ok := arg.(*expression.Constant)
		if !ok || constant.DeferredExpr != nil {
			return "", nil
		}
		v := constant.Value
		if constant.ParamMarker != nil {
			var err error
			v, err = constant.ParamMarker.GetUserVar(ctx.GetExprCtx().GetEvalCtx())
			intest.AssertNoError(err, "fail to get param")
			if err != nil {
				logutil.BgLogger().Warn("fail to get param", zap.Error(err))
				return "", nil
			}
		}
		results = append(results, v)
	}
	return name.ColName.L, results
}

func (helper *extractHelper) setColumnPushedDownFn(
	colNameL string,
	extractCols map[int64]*types.FieldName,
	expr *expression.ScalarFunction,
) {
	scalar := helper.extractColBinaryOpScalarFunc(extractCols, expr)
	if scalar == nil {
		return
	}
	switch scalar.FuncName.L {
	case ast.Lower:
		helper.pushedDownFuncs = make(map[string]func(string) string)
		helper.pushedDownFuncs[colNameL] = strings.ToLower
	case ast.Upper:
		helper.pushedDownFuncs = make(map[string]func(string) string)
		helper.pushedDownFuncs[colNameL] = strings.ToUpper
	}
}

func (extractHelper) isPushDownSupported(fnNameL string) bool {
	return slices.Contains([]string{ast.Lower, ast.Upper}, fnNameL)
}

// extractColBinaryOpScalarFunc extract the scalar function from a binary operation. For example,
// `eq(lower(col), "constant")` returns `lower`.
func (extractHelper) extractColBinaryOpScalarFunc(
	extractCols map[int64]*types.FieldName,
	expr *expression.ScalarFunction,
) (sf *expression.ScalarFunction) {
	args := expr.GetArgs()
	var constIdx int
	// c = 'rhs'
	// 'lhs' = c
	for i := range 2 {
		_, isConst := args[i].(*expression.Constant)
		if isConst {
			constIdx = i
			break
		}
	}
	scalar, isScalar := args[1-constIdx].(*expression.ScalarFunction)
	if !isScalar {
		return nil
	}
	args = scalar.GetArgs()
	if len(args) != 1 {
		return nil
	}
	col, isCol := args[0].(*expression.Column)
	if !isCol {
		return nil
	}
	_, found := extractCols[col.UniqueID]
	if !found {
		return nil
	}
	return scalar
}

func (helper *extractHelper) tryToFindInnerColAndIdx(args []expression.Expression) (innerCol *expression.Column, colIdx int) {
	if !helper.enableScalarPushDown {
		return nil, -1
	}
	var scalar *expression.ScalarFunction
	for i := range 2 {
		var isScalar bool
		scalar, isScalar = args[i].(*expression.ScalarFunction)
		if isScalar {
			colIdx = i
			break
		}
	}
	if scalar != nil {
		args := scalar.GetArgs()
		if len(args) != 1 {
			return nil, -1
		}
		col, isCol := args[0].(*expression.Column)
		if !isCol {
			return nil, -1
		}
		if !helper.isPushDownSupported(scalar.FuncName.L) {
			return nil, -1
		}
		return col, colIdx
	}
	return nil, -1
}

func (helper *extractHelper) extractColBinaryOpConsExpr(
	ctx base.PlanContext,
	extractCols map[int64]*types.FieldName,
	expr *expression.ScalarFunction,
) (string, []types.Datum) {
	args := expr.GetArgs()
	var col *expression.Column
	var colIdx int
	// c = 'rhs'
	// 'lhs' = c
	for i := range 2 {
		var isCol bool
		col, isCol = args[i].(*expression.Column)
		if isCol {
			colIdx = i
			break
		}
	}

	innerCol, innerColIdx := helper.tryToFindInnerColAndIdx(args)
	if innerCol != nil {
		col, colIdx = innerCol, innerColIdx
	}
	if col == nil {
		return "", nil
	}

	name, found := extractCols[col.UniqueID]
	if !found {
		return "", nil
	}

	// The `lhs/rhs` of EQ expression must be a constant
	// SELECT * FROM t1 WHERE c='rhs'
	// SELECT * FROM t1 WHERE 'lhs'=c
	constant, ok := args[1-colIdx].(*expression.Constant)
	if !ok || constant.DeferredExpr != nil {
		return "", nil
	}
	v := constant.Value
	if constant.ParamMarker != nil {
		var err error
		v, err = constant.ParamMarker.GetUserVar(ctx.GetExprCtx().GetEvalCtx())
		intest.AssertNoError(err, "fail to get param")
		if err != nil {
			logutil.BgLogger().Warn("fail to get param", zap.Error(err))
			return "", nil
		}
	}
	return name.ColName.L, []types.Datum{v}
}

// extract the OR expression, e.g:
// SELECT * FROM t1 WHERE c1='a' OR c1='b' OR c1='c'
func (helper *extractHelper) extractColOrExpr(ctx base.PlanContext, extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
	args := expr.GetArgs()
	lhs, ok := args[0].(*expression.ScalarFunction)
	if !ok {
		return "", nil
	}
	rhs, ok := args[1].(*expression.ScalarFunction)
	if !ok {
		return "", nil
	}
	// Define an inner function to avoid populate the outer scope
	var extract = func(extractCols map[int64]*types.FieldName, fn *expression.ScalarFunction) (string, []types.Datum) {
		switch helper.getStringFunctionName(fn) {
		case ast.EQ:
			return helper.extractColBinaryOpConsExpr(ctx, extractCols, fn)
		case ast.LogicOr:
			return helper.extractColOrExpr(ctx, extractCols, fn)
		case ast.In:
			return helper.extractColInConsExpr(ctx, extractCols, fn)
		default:
			return "", nil
		}
	}
	lhsColName, lhsDatums := extract(extractCols, lhs)
	if lhsColName == "" {
		return "", nil
	}
	rhsColName, rhsDatums := extract(extractCols, rhs)
	if lhsColName == rhsColName {
		return lhsColName, append(lhsDatums, rhsDatums...)
	}
	return "", nil
}

// merges `lhs` and `datums` with CNF logic
// 1. Returns `datums` set if the `lhs` is an empty set
// 2. Returns the intersection of `datums` and `lhs` if the `lhs` is not an empty set
func (extractHelper) merge(lhs set.StringSet, datums []types.Datum, toLower bool) set.StringSet {
	tmpNodeTypes := set.NewStringSet()
	for _, datum := range datums {
		s, err := datum.ToString()
		if err != nil {
			return nil
		}
		if toLower {
			s = strings.ToLower(s)
		}
		tmpNodeTypes.Insert(s)
	}
	if len(lhs) > 0 {
		return lhs.Intersection(tmpNodeTypes)
	}
	return tmpNodeTypes
}

func (helper *extractHelper) extractCol(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractColName string,
	valueToLower bool,
) (
	remained []expression.Expression,
	skipRequest bool,
	result set.StringSet,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	result = set.NewStringSet()
	extractCols := helper.findColumn(schema, names, extractColName)
	if len(extractCols) == 0 {
		return predicates, false, result
	}

	// We should use INTERSECTION of sets because of the predicates is CNF array
	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			remained = append(remained, expr)
			continue
		}
		var colName string
		var datums []types.Datum // the memory of datums should not be reused, they will be put into result.
		switch helper.getStringFunctionName(fn) {
		case ast.EQ:
			helper.enableScalarPushDown = true
			colName, datums = helper.extractColBinaryOpConsExpr(ctx, extractCols, fn)
			if colName == extractColName {
				helper.setColumnPushedDownFn(colName, extractCols, fn)
			}
			helper.enableScalarPushDown = false
		case ast.In:
			colName, datums = helper.extractColInConsExpr(ctx, extractCols, fn)
		case ast.LogicOr:
			colName, datums = helper.extractColOrExpr(ctx, extractCols, fn)
		}
		if colName == extractColName {
			result = helper.merge(result, datums, valueToLower)
			skipRequest = len(result) == 0
		} else {
			remained = append(remained, expr)
		}
		// There are no data if the low-level executor skip request, so the filter can be droped
		if skipRequest {
			remained = remained[:0]
			break
		}
	}

	if helper.extractLowerString == nil {
		helper.extractLowerString = make(map[string]bool)
	}
	helper.extractLowerString[extractColName] = valueToLower
	return
}

// extracts the string pattern column, e.g:
// SELECT * FROM t WHERE c LIKE '%a%'
// SELECT * FROM t WHERE c LIKE '%a%' AND c REGEXP '.*xxx.*'
// SELECT * FROM t WHERE c LIKE '%a%' OR c REGEXP '.*xxx.*'
func (helper *extractHelper) extractLikePatternCol(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractColName string,
	toLower bool,
	needLike2Regexp bool,
) (
	remained []expression.Expression,
	patterns []string,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	extractCols := helper.findColumn(schema, names, extractColName)
	if len(extractCols) == 0 {
		return predicates, nil
	}

	// We use a string array to save multiple patterns because the Golang and Rust don't
	// support perl-like CNF regular expression: (?=expr1)(?=expr2).
	// e.g:
	// SELECT * FROM t WHERE c LIKE '%a%' AND c LIKE '%b%' AND c REGEXP 'gc.*[0-9]{10,20}'
	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			remained = append(remained, expr)
			continue
		}

		var canBuildPattern bool
		var pattern string
		// We use '|' to combine DNF regular expression: .*a.*|.*b.*
		// e.g:
		// SELECT * FROM t WHERE c LIKE '%a%' OR c LIKE '%b%'
		if fn.FuncName.L == ast.LogicOr && !toLower {
			canBuildPattern, pattern = helper.extractOrLikePattern(ctx, fn, extractColName, extractCols, needLike2Regexp)
		} else {
			canBuildPattern, pattern = helper.extractLikePattern(ctx, fn, extractColName, extractCols, needLike2Regexp)
		}
		if canBuildPattern && toLower {
			pattern = strings.ToLower(pattern)
		}
		if canBuildPattern {
			patterns = append(patterns, pattern)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

func (helper extractHelper) extractOrLikePattern(
	ctx base.PlanContext,
	orFunc *expression.ScalarFunction,
	extractColName string,
	extractCols map[int64]*types.FieldName,
	needLike2Regexp bool,
) (
	ok bool,
	pattern string,
) {
	predicates := expression.SplitDNFItems(orFunc)
	if len(predicates) == 0 {
		return false, ""
	}

	patternBuilder := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		fn, ok := predicate.(*expression.ScalarFunction)
		if !ok {
			return false, ""
		}

		ok, partPattern := helper.extractLikePattern(ctx, fn, extractColName, extractCols, needLike2Regexp)
		if !ok {
			return false, ""
		}
		patternBuilder = append(patternBuilder, partPattern)
	}
	return true, strings.Join(patternBuilder, "|")
}

func (helper extractHelper) extractLikePattern(
	ctx base.PlanContext,
	fn *expression.ScalarFunction,
	extractColName string,
	extractCols map[int64]*types.FieldName,
	needLike2Regexp bool,
) (
	ok bool,
	pattern string,
) {
	var colName string
	var datums []types.Datum
	switch fn.FuncName.L {
	case ast.EQ, ast.Like, ast.Ilike, ast.Regexp, ast.RegexpLike:
		colName, datums = helper.extractColBinaryOpConsExpr(ctx, extractCols, fn)
	}
	if colName != extractColName {
		return false, ""
	}
	switch fn.FuncName.L {
	case ast.EQ:
		return true, "^" + regexp.QuoteMeta(datums[0].GetString()) + "$"
	case ast.Like, ast.Ilike:
		if needLike2Regexp {
			return true, stringutil.CompileLike2Regexp(datums[0].GetString())
		}
		return true, datums[0].GetString()
	case ast.Regexp, ast.RegexpLike:
		return true, datums[0].GetString()
	default:
		return false, ""
	}
}

func (extractHelper) findColumn(schema *expression.Schema, names []*types.FieldName, colName string) map[int64]*types.FieldName {
	extractCols := make(map[int64]*types.FieldName)
	for i, name := range names {
		if name.ColName.L == colName {
			extractCols[schema.Columns[i].UniqueID] = name
		}
	}
	return extractCols
}

// getTimeFunctionName is used to get the (time) function name.
// For the expression that push down to the coprocessor, the function name is different with normal compare function,
// Then getTimeFunctionName will do a sample function name convert.
// Currently, this is used to support query `CLUSTER_SLOW_QUERY` at any time.
func (extractHelper) getTimeFunctionName(fn *expression.ScalarFunction) string {
	switch fn.Function.PbCode() {
	case tipb.ScalarFuncSig_GTTime:
		return ast.GT
	case tipb.ScalarFuncSig_GETime:
		return ast.GE
	case tipb.ScalarFuncSig_LTTime:
		return ast.LT
	case tipb.ScalarFuncSig_LETime:
		return ast.LE
	case tipb.ScalarFuncSig_EQTime:
		return ast.EQ
	default:
		return fn.FuncName.L
	}
}

// getStringFunctionName is used to get the (string) function name.
// For the expression that push down to the coprocessor, the function name is different with normal compare function,
// Then getStringFunctionName will do a sample function name convert.
// Currently, this is used to support query `CLUSTER_STMT_SUMMARY` at any string.
func (extractHelper) getStringFunctionName(fn *expression.ScalarFunction) string {
	switch fn.Function.PbCode() {
	case tipb.ScalarFuncSig_GTString:
		return ast.GT
	case tipb.ScalarFuncSig_GEString:
		return ast.GE
	case tipb.ScalarFuncSig_LTString:
		return ast.LT
	case tipb.ScalarFuncSig_LEString:
		return ast.LE
	case tipb.ScalarFuncSig_EQString:
		return ast.EQ
	default:
		return fn.FuncName.L
	}
}

// extracts the time range column, e.g:
// SELECT * FROM t WHERE time='2019-10-10 10:10:10'
// SELECT * FROM t WHERE time>'2019-10-10 10:10:10' AND time<'2019-10-11 10:10:10'
func (helper extractHelper) extractTimeRange(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractColName string,
	timezone *time.Location,
) (
	remained []expression.Expression,
	startTime int64, // unix timestamp in nanoseconds
	endTime int64,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	extractCols := helper.findColumn(schema, names, extractColName)
	if len(extractCols) == 0 {
		return predicates, startTime, endTime
	}

	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			remained = append(remained, expr)
			continue
		}

		var colName string
		var datums []types.Datum
		fnName := helper.getTimeFunctionName(fn)
		switch fnName {
		case ast.GT, ast.GE, ast.LT, ast.LE, ast.EQ:
			colName, datums = helper.extractColBinaryOpConsExpr(ctx, extractCols, fn)
		}

		if colName == extractColName {
			timeType := types.NewFieldType(mysql.TypeDatetime)
			timeType.SetDecimal(6)
			timeDatum, err := datums[0].ConvertTo(ctx.GetSessionVars().StmtCtx.TypeCtx(), timeType)
			if err != nil || timeDatum.Kind() == types.KindNull {
				remained = append(remained, expr)
				continue
			}

			mysqlTime := timeDatum.GetMysqlTime()
			timestamp := time.Date(mysqlTime.Year(),
				time.Month(mysqlTime.Month()),
				mysqlTime.Day(),
				mysqlTime.Hour(),
				mysqlTime.Minute(),
				mysqlTime.Second(),
				mysqlTime.Microsecond()*1000,
				timezone,
			).UnixNano()

			switch fnName {
			case ast.EQ:
				startTime = max(startTime, timestamp)
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = min(endTime, timestamp)
				}
			case ast.GT:
				// FixMe: add 1ms is not absolutely correct here, just because the log search precision is millisecond.
				startTime = max(startTime, timestamp+int64(time.Millisecond))
			case ast.GE:
				startTime = max(startTime, timestamp)
			case ast.LT:
				if endTime == 0 {
					endTime = timestamp - int64(time.Millisecond)
				} else {
					endTime = min(endTime, timestamp-int64(time.Millisecond))
				}
			case ast.LE:
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = min(endTime, timestamp)
				}
			default:
				remained = append(remained, expr)
			}
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

func (extractHelper) parseQuantiles(quantileSet set.StringSet) []float64 {
	quantiles := make([]float64, 0, len(quantileSet))
	for k := range quantileSet {
		v, err := strconv.ParseFloat(k, 64)
		if err != nil {
			// ignore the parse error won't affect result.
			continue
		}
		quantiles = append(quantiles, v)
	}
	slices.Sort(quantiles)
	return quantiles
}

func (extractHelper) parseUint64(uint64Set set.StringSet) []uint64 {
	uint64s := make([]uint64, 0, len(uint64Set))
	for k := range uint64Set {
		v, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			// ignore the parse error won't affect result.
			continue
		}
		uint64s = append(uint64s, v)
	}
	slices.Sort(uint64s)
	return uint64s
}

func (helper extractHelper) extractCols(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	excludeCols set.StringSet,
	valueToLower bool) ([]expression.Expression, bool, map[string]set.StringSet) {
	cols := map[string]set.StringSet{}
	remained := predicates
	skipRequest := false
	// Extract the label columns.
	for _, name := range names {
		if excludeCols.Exist(name.ColName.L) {
			continue
		}
		var values set.StringSet
		remained, skipRequest, values = helper.extractCol(ctx, schema, names, remained, name.ColName.L, valueToLower)
		if skipRequest {
			return nil, true, nil
		}
		if len(values) == 0 {
			continue
		}
		cols[name.ColName.L] = values
	}
	return remained, skipRequest, cols
}

func (extractHelper) convertToTime(t int64) time.Time {
	if t == 0 || t == math.MaxInt64 {
		return time.Now()
	}
	return time.Unix(0, t)
}

func (extractHelper) convertToBoolSlice(uint64Slice []uint64) []bool {
	if len(uint64Slice) == 0 {
		return []bool{false, true}
	}
	var res []bool
	// use to keep res unique
	b := make(map[bool]struct{}, 2)
	for _, l := range uint64Slice {
		tmpBool := l == 1
		_, ok := b[tmpBool]
		if !ok {
			b[tmpBool] = struct{}{}
			res = append(res, tmpBool)
		}
	}
	return res
}
