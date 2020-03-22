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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// MemTablePredicateExtractor is used to extract some predicates from `WHERE` clause
// and push the predicates down to the data retrieving on reading memory table stage.
//
// e.g:
// SELECT * FROM cluster_config WHERE type='tikv' AND instance='192.168.1.9:2379'
// We must request all components in the cluster via HTTP API for retrieving
// configurations and filter them by `type/instance` columns.
//
// The purpose of defining a `MemTablePredicateExtractor` is to optimize this
// 1. Define a `ClusterConfigTablePredicateExtractor`
// 2. Extract the `type/instance` columns on the logic optimizing stage and save them via fields.
// 3. Passing the extractor to the `ClusterReaderExecExec` executor
// 4. Executor sends requests to the target components instead of all of the components
type MemTablePredicateExtractor interface {
	// Extracts predicates which can be pushed down and returns the remained predicates
	Extract(sessionctx.Context, *expression.Schema, []*types.FieldName, []expression.Expression) (remained []expression.Expression)
	explainInfo(p *PhysicalMemTable) string
}

// extractHelper contains some common utililty functions for all extractor.
// define an individual struct instead of a bunch of un-exported functions
// to avoid polluting the global scope of current package.
type extractHelper struct{}

func (helper extractHelper) extractColInConsExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
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
	var results []types.Datum
	for _, arg := range args[1:] {
		constant, ok := arg.(*expression.Constant)
		if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
			return "", nil
		}
		results = append(results, constant.Value)
	}
	return name.ColName.L, results
}

func (helper extractHelper) extractColBinaryOpConsExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
	args := expr.GetArgs()
	var col *expression.Column
	var colIdx int
	// c = 'rhs'
	// 'lhs' = c
	for i := 0; i < 2; i++ {
		var isCol bool
		col, isCol = args[i].(*expression.Column)
		if isCol {
			colIdx = i
			break
		}
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
	if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
		return "", nil
	}
	return name.ColName.L, []types.Datum{constant.Value}
}

// extract the OR expression, e.g:
// SELECT * FROM t1 WHERE c1='a' OR c1='b' OR c1='c'
func (helper extractHelper) extractColOrExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
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
		switch fn.FuncName.L {
		case ast.EQ:
			return helper.extractColBinaryOpConsExpr(extractCols, fn)
		case ast.LogicOr:
			return helper.extractColOrExpr(extractCols, fn)
		case ast.In:
			return helper.extractColInConsExpr(extractCols, fn)
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
func (helper extractHelper) merge(lhs set.StringSet, datums []types.Datum, toLower bool) set.StringSet {
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

func (helper extractHelper) extractCol(
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
			continue
		}
		var colName string
		var datums []types.Datum
		switch fn.FuncName.L {
		case ast.EQ:
			colName, datums = helper.extractColBinaryOpConsExpr(extractCols, fn)
		case ast.In:
			colName, datums = helper.extractColInConsExpr(extractCols, fn)
		case ast.LogicOr:
			colName, datums = helper.extractColOrExpr(extractCols, fn)
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
	return
}

// extracts the string pattern column, e.g:
// SELECT * FROM t WHERE c LIKE '%a%'
// SELECT * FROM t WHERE c LIKE '%a%' AND c REGEXP '.*xxx.*'
// SELECT * FROM t WHERE c LIKE '%a%' OR c REGEXP '.*xxx.*'
func (helper extractHelper) extractLikePatternCol(
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractColName string,
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
		if fn.FuncName.L == ast.LogicOr {
			canBuildPattern, pattern = helper.extractOrLikePattern(fn, extractColName, extractCols)
		} else {
			canBuildPattern, pattern = helper.extractLikePattern(fn, extractColName, extractCols)
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
	orFunc *expression.ScalarFunction,
	extractColName string,
	extractCols map[int64]*types.FieldName,
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

		ok, partPattern := helper.extractLikePattern(fn, extractColName, extractCols)
		if !ok {
			return false, ""
		}
		patternBuilder = append(patternBuilder, partPattern)
	}
	return true, strings.Join(patternBuilder, "|")
}

func (helper extractHelper) extractLikePattern(
	fn *expression.ScalarFunction,
	extractColName string,
	extractCols map[int64]*types.FieldName,
) (
	ok bool,
	pattern string,
) {
	var colName string
	var datums []types.Datum
	switch fn.FuncName.L {
	case ast.EQ, ast.Like, ast.Regexp:
		colName, datums = helper.extractColBinaryOpConsExpr(extractCols, fn)
	}
	if colName == extractColName {
		switch fn.FuncName.L {
		case ast.EQ:
			return true, "^" + regexp.QuoteMeta(datums[0].GetString()) + "$"
		case ast.Like:
			return true, stringutil.CompileLike2Regexp(datums[0].GetString())
		case ast.Regexp:
			return true, datums[0].GetString()
		default:
			return false, ""
		}
	} else {
		return false, ""
	}
}

func (helper extractHelper) findColumn(schema *expression.Schema, names []*types.FieldName, colName string) map[int64]*types.FieldName {
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
func (helper extractHelper) getTimeFunctionName(fn *expression.ScalarFunction) string {
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

// extracts the time range column, e.g:
// SELECT * FROM t WHERE time='2019-10-10 10:10:10'
// SELECT * FROM t WHERE time>'2019-10-10 10:10:10' AND time<'2019-10-11 10:10:10'
func (helper extractHelper) extractTimeRange(
	ctx sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractColName string,
	timezone *time.Location,
) (
	remained []expression.Expression,
	// unix timestamp in millisecond
	startTime int64,
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
			colName, datums = helper.extractColBinaryOpConsExpr(extractCols, fn)
		}

		if colName == extractColName {
			timeType := types.NewFieldType(mysql.TypeDatetime)
			timeType.Decimal = 3
			timeDatum, err := datums[0].ConvertTo(ctx.GetSessionVars().StmtCtx, timeType)
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
			).UnixNano() / int64(time.Millisecond)

			switch fnName {
			case ast.EQ:
				startTime = mathutil.MaxInt64(startTime, timestamp)
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp)
				}
			case ast.GT:
				startTime = mathutil.MaxInt64(startTime, timestamp+1)
			case ast.GE:
				startTime = mathutil.MaxInt64(startTime, timestamp)
			case ast.LT:
				if endTime == 0 {
					endTime = timestamp - 1
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp-1)
				}
			case ast.LE:
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp)
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

func (helper extractHelper) parseQuantiles(quantileSet set.StringSet) []float64 {
	quantiles := make([]float64, 0, len(quantileSet))
	for k := range quantileSet {
		v, err := strconv.ParseFloat(k, 64)
		if err != nil {
			// ignore the parse error won't affect result.
			continue
		}
		quantiles = append(quantiles, v)
	}
	sort.Float64s(quantiles)
	return quantiles
}

func (helper extractHelper) extractCols(
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
		remained, skipRequest, values = helper.extractCol(schema, names, remained, name.ColName.L, valueToLower)
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

func (helper extractHelper) convertToTime(t int64) time.Time {
	if t == 0 || t == math.MaxInt64 {
		return time.Now()
	}
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
}

// ClusterTableExtractor is used to extract some predicates of cluster table.
type ClusterTableExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE type='tikv'
	// 2. SELECT * FROM cluster_config WHERE type in ('tikv', 'tidb')
	NodeTypes set.StringSet

	// Instances represents all components instances we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE instance='192.168.1.7:2379'
	// 2. SELECT * FROM cluster_config WHERE type in ('192.168.1.7:2379', '192.168.1.9:2379')
	Instances set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *ClusterTableExtractor) Extract(_ sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, typeSkipRequest, nodeTypes := e.extractCol(schema, names, predicates, "type", true)
	remained, addrSkipRequest, instances := e.extractCol(schema, names, remained, "instance", false)
	e.SkipRequest = typeSkipRequest || addrSkipRequest
	e.NodeTypes = nodeTypes
	e.Instances = instances
	return remained
}

func (e *ClusterTableExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}

// ClusterLogTableExtractor is used to extract some predicates of `cluster_config`
type ClusterLogTableExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE type='tikv'
	// 2. SELECT * FROM cluster_log WHERE type in ('tikv', 'tidb')
	NodeTypes set.StringSet

	// Instances represents all components instances we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE instance='192.168.1.7:2379'
	// 2. SELECT * FROM cluster_log WHERE instance in ('192.168.1.7:2379', '192.168.1.9:2379')
	Instances set.StringSet

	// StartTime represents the beginning time of log message
	// e.g: SELECT * FROM cluster_log WHERE time>'2019-10-10 10:10:10.999'
	StartTime int64
	// EndTime represents the ending time of log message
	// e.g: SELECT * FROM cluster_log WHERE time<'2019-10-11 10:10:10.999'
	EndTime int64
	// Pattern is used to filter the log message
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE message like '%gc%'
	// 2. SELECT * FROM cluster_log WHERE message regexp '.*'
	Patterns  []string
	LogLevels set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *ClusterLogTableExtractor) Extract(
	ctx sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `type/instance` columns
	remained, typeSkipRequest, nodeTypes := e.extractCol(schema, names, predicates, "type", true)
	remained, addrSkipRequest, instances := e.extractCol(schema, names, remained, "instance", false)
	remained, levlSkipRequest, logLevels := e.extractCol(schema, names, remained, "level", true)
	e.SkipRequest = typeSkipRequest || addrSkipRequest || levlSkipRequest
	e.NodeTypes = nodeTypes
	e.Instances = instances
	e.LogLevels = logLevels
	if e.SkipRequest {
		return nil
	}

	remained, startTime, endTime := e.extractTimeRange(ctx, schema, names, remained, "time", time.Local)
	if endTime == 0 {
		endTime = math.MaxInt64
	}
	e.StartTime = startTime
	e.EndTime = endTime
	e.SkipRequest = startTime > endTime

	if e.SkipRequest {
		return nil
	}

	remained, patterns := e.extractLikePatternCol(schema, names, remained, "message")
	e.Patterns = patterns
	return remained
}

func (e *ClusterLogTableExtractor) explainInfo(p *PhysicalMemTable) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	r := new(bytes.Buffer)
	st, et := e.GetTimeRange(false)
	if st > 0 {
		st := time.Unix(0, st*1e6)
		r.WriteString(fmt.Sprintf("start_time:%v, ", st.In(p.ctx.GetSessionVars().StmtCtx.TimeZone).Format(MetricTableTimeFormat)))
	}
	if et < math.MaxInt64 {
		et := time.Unix(0, et*1e6)
		r.WriteString(fmt.Sprintf("end_time:%v, ", et.In(p.ctx.GetSessionVars().StmtCtx.TimeZone).Format(MetricTableTimeFormat)))
	}
	if len(e.NodeTypes) > 0 {
		r.WriteString(fmt.Sprintf("node_types:[%s], ", extractStringFromStringSet(e.Instances)))
	}
	if len(e.Instances) > 0 {
		r.WriteString(fmt.Sprintf("instances:[%s], ", extractStringFromStringSet(e.Instances)))
	}
	if len(e.LogLevels) > 0 {
		r.WriteString(fmt.Sprintf("log_levels:[%s], ", extractStringFromStringSet(e.LogLevels)))
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// GetTimeRange extract startTime and endTime
func (e *ClusterLogTableExtractor) GetTimeRange(isFailpointTestModeSkipCheck bool) (int64, int64) {
	startTime := e.StartTime
	endTime := e.EndTime
	if endTime == 0 {
		endTime = math.MaxInt64
	}
	if !isFailpointTestModeSkipCheck {
		// Just search the recent half an hour logs if the user doesn't specify the start time
		const defaultSearchLogDuration = 30 * time.Minute / time.Millisecond
		if startTime == 0 {
			if endTime == math.MaxInt64 {
				startTime = time.Now().UnixNano()/int64(time.Millisecond) - int64(defaultSearchLogDuration)
			} else {
				startTime = endTime - int64(defaultSearchLogDuration)
			}
		}
	}
	return startTime, endTime
}

// MetricTableExtractor is used to extract some predicates of metrics_schema tables.
type MetricTableExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool
	// StartTime represents the beginning time of metric data.
	StartTime time.Time
	// EndTime represents the ending time of metric data.
	EndTime time.Time
	// LabelConditions represents the label conditions of metric data.
	LabelConditions map[string]set.StringSet
	Quantiles       []float64
}

func newMetricTableExtractor() *MetricTableExtractor {
	e := &MetricTableExtractor{}
	e.StartTime, e.EndTime = e.getTimeRange(0, 0)
	return e
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *MetricTableExtractor) Extract(
	ctx sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `quantile` columns
	remained, skipRequest, quantileSet := e.extractCol(schema, names, predicates, "quantile", true)
	e.Quantiles = e.parseQuantiles(quantileSet)
	e.SkipRequest = skipRequest
	if e.SkipRequest {
		return nil
	}

	// Extract the `time` columns
	remained, startTime, endTime := e.extractTimeRange(ctx, schema, names, remained, "time", ctx.GetSessionVars().StmtCtx.TimeZone)
	e.StartTime, e.EndTime = e.getTimeRange(startTime, endTime)
	e.SkipRequest = e.StartTime.After(e.EndTime)
	if e.SkipRequest {
		return nil
	}

	excludeCols := set.NewStringSet("quantile", "time", "value")
	_, skipRequest, extractCols := e.extractCols(schema, names, remained, excludeCols, false)
	e.SkipRequest = skipRequest
	if e.SkipRequest {
		return nil
	}
	e.LabelConditions = extractCols
	// For some metric, the metric reader can't use the predicate, so keep all label conditions remained.
	return remained
}

func (e *MetricTableExtractor) getTimeRange(start, end int64) (time.Time, time.Time) {
	const defaultMetricQueryDuration = 10 * time.Minute
	var startTime, endTime time.Time
	if start == 0 && end == 0 {
		endTime = time.Now()
		return endTime.Add(-defaultMetricQueryDuration), endTime
	}
	if start != 0 {
		startTime = e.convertToTime(start)
	}
	if end != 0 {
		endTime = e.convertToTime(end)
	}
	if start == 0 {
		startTime = endTime.Add(-defaultMetricQueryDuration)
	}
	if end == 0 {
		endTime = startTime.Add(defaultMetricQueryDuration)
	}
	return startTime, endTime
}

func (e *MetricTableExtractor) explainInfo(p *PhysicalMemTable) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	promQL := e.GetMetricTablePromQL(p.ctx, p.Table.Name.L)
	startTime, endTime := e.StartTime, e.EndTime
	step := time.Second * time.Duration(p.ctx.GetSessionVars().MetricSchemaStep)
	return fmt.Sprintf("PromQL:%v, start_time:%v, end_time:%v, step:%v",
		promQL,
		startTime.In(p.ctx.GetSessionVars().StmtCtx.TimeZone).Format(MetricTableTimeFormat),
		endTime.In(p.ctx.GetSessionVars().StmtCtx.TimeZone).Format(MetricTableTimeFormat),
		step,
	)
}

// GetMetricTablePromQL uses to get the promQL of metric table.
func (e *MetricTableExtractor) GetMetricTablePromQL(sctx sessionctx.Context, lowerTableName string) string {
	quantiles := e.Quantiles
	def, err := infoschema.GetMetricTableDef(lowerTableName)
	if err != nil {
		return ""
	}
	if len(quantiles) == 0 {
		quantiles = []float64{def.Quantile}
	}
	var buf bytes.Buffer
	for i, quantile := range quantiles {
		promQL := def.GenPromQL(sctx, e.LabelConditions, quantile)
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(promQL)
	}
	return buf.String()
}

// MetricSummaryTableExtractor is used to extract some predicates of metrics_schema tables.
type MetricSummaryTableExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest  bool
	MetricsNames set.StringSet
	Quantiles    []float64
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *MetricSummaryTableExtractor) Extract(
	_ sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	remained, quantileSkip, quantiles := e.extractCol(schema, names, predicates, "quantile", false)
	remained, metricsNameSkip, metricsNames := e.extractCol(schema, names, predicates, "metrics_name", true)
	e.SkipRequest = quantileSkip || metricsNameSkip
	e.Quantiles = e.parseQuantiles(quantiles)
	e.MetricsNames = metricsNames
	return remained
}

func (e *MetricSummaryTableExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}

// InspectionResultTableExtractor is used to extract some predicates of `inspection_result`
type InspectionResultTableExtractor struct {
	extractHelper
	// SkipInspection means the where clause always false, we don't need to request any component
	SkipInspection bool
	// Rules represents rules applied to, and we should apply all inspection rules if there is no rules specified
	// e.g: SELECT * FROM inspection_result WHERE rule in ('ddl', 'config')
	Rules set.StringSet
	// Items represents items applied to, and we should apply all inspection item if there is no rules specified
	// e.g: SELECT * FROM inspection_result WHERE item in ('ddl.lease', 'raftstore.threadpool')
	Items set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InspectionResultTableExtractor) Extract(
	_ sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `rule/item` columns
	remained, ruleSkip, rules := e.extractCol(schema, names, predicates, "rule", true)
	remained, itemSkip, items := e.extractCol(schema, names, remained, "item", true)
	e.SkipInspection = ruleSkip || itemSkip
	e.Rules = rules
	e.Items = items
	return remained
}

func (e *InspectionResultTableExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}

// InspectionSummaryTableExtractor is used to extract some predicates of `inspection_summary`
type InspectionSummaryTableExtractor struct {
	extractHelper
	// SkipInspection means the where clause always false, we don't need to request any component
	SkipInspection bool
	// Rules represents rules applied to, and we should apply all inspection rules if there is no rules specified
	// e.g: SELECT * FROM inspection_summary WHERE rule in ('ddl', 'config')
	Rules       set.StringSet
	MetricNames set.StringSet
	Quantiles   []float64
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InspectionSummaryTableExtractor) Extract(
	_ sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `rule` columns
	remained, ruleSkip, rules := e.extractCol(schema, names, predicates, "rule", true)
	// Extract the `metric_name` columns
	remained, metricNameSkip, metricNames := e.extractCol(schema, names, predicates, "metrics_name", true)
	// Extract the `quantile` columns
	remained, quantileSkip, quantileSet := e.extractCol(schema, names, predicates, "quantile", false)
	e.SkipInspection = ruleSkip || quantileSkip || metricNameSkip
	e.Rules = rules
	e.Quantiles = e.parseQuantiles(quantileSet)
	e.MetricNames = metricNames
	return remained
}

func (e *InspectionSummaryTableExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}

// InspectionRuleTableExtractor is used to extract some predicates of `inspection_rules`
type InspectionRuleTableExtractor struct {
	extractHelper

	SkipRequest bool
	Types       set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InspectionRuleTableExtractor) Extract(
	_ sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `type` columns
	remained, tpSkip, tps := e.extractCol(schema, names, predicates, "type", true)
	e.SkipRequest = tpSkip
	e.Types = tps
	return remained
}

func (e *InspectionRuleTableExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}

// SlowQueryExtractor is used to extract some predicates of `slow_query`
type SlowQueryExtractor struct {
	extractHelper

	SkipRequest bool
	StartTime   time.Time
	EndTime     time.Time
	// Enable is true means the executor should use the time range to locate the slow-log file that need to be parsed.
	// Enable is false, means the executor should keep the behavior compatible with before, which is only parse the
	// current slow-log file.
	Enable bool
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *SlowQueryExtractor) Extract(
	ctx sessionctx.Context,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, startTime, endTime := e.extractTimeRange(ctx, schema, names, predicates, "time", ctx.GetSessionVars().StmtCtx.TimeZone)
	e.setTimeRange(startTime, endTime)
	e.SkipRequest = e.Enable && e.StartTime.After(e.EndTime)
	if e.SkipRequest {
		return nil
	}
	return remained
}

func (e *SlowQueryExtractor) setTimeRange(start, end int64) {
	const defaultSlowQueryDuration = 24 * time.Hour
	var startTime, endTime time.Time
	if start == 0 && end == 0 {
		return
	}
	if start != 0 {
		startTime = e.convertToTime(start)
	}
	if end != 0 {
		endTime = e.convertToTime(end)
	}
	if start == 0 {
		startTime = endTime.Add(-defaultSlowQueryDuration)
	}
	if end == 0 {
		endTime = startTime.Add(defaultSlowQueryDuration)
	}
	e.StartTime, e.EndTime = startTime, endTime
	e.Enable = true
}

func (e *SlowQueryExtractor) explainInfo(p *PhysicalMemTable) string {
	return ""
}
