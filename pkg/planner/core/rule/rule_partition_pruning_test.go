// Copyright 2018 PingCAP, Inc.
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

package rule_test

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/planner/core" // for init the expression.BuildSimpleExpression
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/testkit/ddlhelper"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCanBePrune(t *testing.T) {
	// For the following case:
	// CREATE TABLE t1 ( recdate  DATETIME NOT NULL )
	// PARTITION BY RANGE( TO_DAYS(recdate) ) (
	// 	PARTITION p0 VALUES LESS THAN ( TO_DAYS('2007-03-08') ),
	// 	PARTITION p1 VALUES LESS THAN ( TO_DAYS('2007-04-01') )
	// );
	// SELECT * FROM t1 WHERE recdate < '2007-03-08 00:00:00';
	// SELECT * FROM t1 WHERE recdate > '2018-03-08 00:00:00';

	tc := prepareTestCtx(t, "create table t (d datetime not null)", "to_days(d)")
	lessThan := rule.LessThanDataInt{Data: []int64{733108, 733132}, Maxvalue: false}
	pruner := &rule.RangePruner{LessThan: lessThan, Col: tc.col, PartFn: tc.fn, Monotonous: rule.MonotoneModeNonStrict}

	queryExpr := tc.expr("d < '2000-03-08 00:00:00'")
	result := rule.PartitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, rule.GetFullRange(len(lessThan.Data)))
	require.True(t, slices.Equal(result, rule.PartitionRangeOR{{Start: 0, End: 1}}))

	queryExpr = tc.expr("d > '2018-03-08 00:00:00'")
	result = rule.PartitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, rule.GetFullRange(len(lessThan.Data)))
	require.True(t, slices.Equal(result, rule.PartitionRangeOR{}))

	// For the following case:
	// CREATE TABLE quarterly_report_status (
	// 	report_id INT NOT NULL,
	// 	report_status VARCHAR(20) NOT NULL,
	// 	report_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)
	// PARTITION BY RANGE (UNIX_TIMESTAMP(report_updated)) (
	// 	PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2008-01-01 00:00:00')),
	// 	PARTITION p1 VALUES LESS THAN (UNIX_TIMESTAMP('2008-04-01 00:00:00')),
	// 	PARTITION p2 VALUES LESS THAN (UNIX_TIMESTAMP('2010-01-01 00:00:00')),
	// 	PARTITION p3 VALUES LESS THAN (MAXVALUE)
	// );
	tc = prepareTestCtx(t, "create table t (report_updated timestamp)", "unix_timestamp(report_updated)")
	lessThan = rule.LessThanDataInt{Data: []int64{1199145600, 1207008000, 1262304000, 0}, Maxvalue: true}
	pruner = &rule.RangePruner{LessThan: lessThan, Col: tc.col, PartFn: tc.fn, Monotonous: rule.MonotoneModeStrict}

	queryExpr = tc.expr("report_updated > '2008-05-01 00:00:00'")
	result = rule.PartitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, rule.GetFullRange(len(lessThan.Data)))
	require.True(t, slices.Equal(result, rule.PartitionRangeOR{{Start: 2, End: 4}}))

	queryExpr = tc.expr("report_updated > unix_timestamp('2008-05-01 00:00:00')")
	rule.PartitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, rule.GetFullRange(len(lessThan.Data)))
	// TODO: Uncomment the check after fixing issue https://github.com/pingcap/tidb/issues/12028
	// require.True(t, slices.Equal(result, rule.PartitionRangeOR{{Start: 2, End: 4}}))
	// report_updated > unix_timestamp('2008-05-01 00:00:00') is converted to gt(t.t.report_updated, <nil>)
	// Because unix_timestamp('2008-05-01 00:00:00') is fold to constant int 1564761600, and compare it with timestamp (report_updated)
	// need to convert 1564761600 to a timestamp, during that step, an error happen and the result is set to <nil>
}

func TestPruneUseBinarySearchSigned(t *testing.T) {
	lessThan := rule.LessThanDataInt{Data: []int64{-3, 4, 7, 11, 14, 17, 0}, Maxvalue: true, Unsigned: false}
	cases := []struct {
		input  rule.DataForPrune
		result rule.PartitionRange
	}{
		{input: rule.DataForPrune{Op: ast.EQ, C: 66}, result: rule.PartitionRange{Start: 6, End: 7}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 14}, result: rule.PartitionRange{Start: 5, End: 6}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 10}, result: rule.PartitionRange{Start: 3, End: 4}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 3}, result: rule.PartitionRange{Start: 1, End: 2}},
		{input: rule.DataForPrune{Op: ast.EQ, C: -4}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.LT, C: 66}, result: rule.PartitionRange{End: 7}},
		{input: rule.DataForPrune{Op: ast.LT, C: 14}, result: rule.PartitionRange{End: 5}},
		{input: rule.DataForPrune{Op: ast.LT, C: 10}, result: rule.PartitionRange{End: 4}},
		{input: rule.DataForPrune{Op: ast.LT, C: 3}, result: rule.PartitionRange{End: 2}},
		{input: rule.DataForPrune{Op: ast.LT, C: -4}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.GE, C: 66}, result: rule.PartitionRange{Start: 6, End: 7}},
		{input: rule.DataForPrune{Op: ast.GE, C: 14}, result: rule.PartitionRange{Start: 5, End: 7}},
		{input: rule.DataForPrune{Op: ast.GE, C: 10}, result: rule.PartitionRange{Start: 3, End: 7}},
		{input: rule.DataForPrune{Op: ast.GE, C: 3}, result: rule.PartitionRange{Start: 1, End: 7}},
		{input: rule.DataForPrune{Op: ast.GE, C: -4}, result: rule.PartitionRange{End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: 66}, result: rule.PartitionRange{Start: 6, End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: 14}, result: rule.PartitionRange{Start: 5, End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: 10}, result: rule.PartitionRange{Start: 4, End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: 3}, result: rule.PartitionRange{Start: 2, End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: 2}, result: rule.PartitionRange{Start: 1, End: 7}},
		{input: rule.DataForPrune{Op: ast.GT, C: -4}, result: rule.PartitionRange{Start: 1, End: 7}},
		{input: rule.DataForPrune{Op: ast.LE, C: 66}, result: rule.PartitionRange{End: 7}},
		{input: rule.DataForPrune{Op: ast.LE, C: 14}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.LE, C: 10}, result: rule.PartitionRange{End: 4}},
		{input: rule.DataForPrune{Op: ast.LE, C: 3}, result: rule.PartitionRange{End: 2}},
		{input: rule.DataForPrune{Op: ast.LE, C: -4}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.IsNull}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: "illegal"}, result: rule.PartitionRange{End: 7}},
	}

	for i, ca := range cases {
		start, end := rule.PruneUseBinarySearch(lessThan, ca.input)
		require.Equalf(t, ca.result.Start, start, "fail = %d", i)
		require.Equalf(t, ca.result.End, end, "fail = %d", i)
	}
}

func TestPruneUseBinarySearchUnSigned(t *testing.T) {
	lessThan := rule.LessThanDataInt{Data: []int64{4, 7, 11, 14, 17, 0}, Maxvalue: true, Unsigned: true}
	cases := []struct {
		input  rule.DataForPrune
		result rule.PartitionRange
	}{
		{input: rule.DataForPrune{Op: ast.EQ, C: 66}, result: rule.PartitionRange{Start: 5, End: 6}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 14}, result: rule.PartitionRange{Start: 4, End: 5}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 10}, result: rule.PartitionRange{Start: 2, End: 3}},
		{input: rule.DataForPrune{Op: ast.EQ, C: 3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.EQ, C: -3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.LT, C: 66}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.LT, C: 14}, result: rule.PartitionRange{End: 4}},
		{input: rule.DataForPrune{Op: ast.LT, C: 10}, result: rule.PartitionRange{End: 3}},
		{input: rule.DataForPrune{Op: ast.LT, C: 3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.LT, C: -3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.GE, C: 66}, result: rule.PartitionRange{Start: 5, End: 6}},
		{input: rule.DataForPrune{Op: ast.GE, C: 14}, result: rule.PartitionRange{Start: 4, End: 6}},
		{input: rule.DataForPrune{Op: ast.GE, C: 10}, result: rule.PartitionRange{Start: 2, End: 6}},
		{input: rule.DataForPrune{Op: ast.GE, C: 3}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.GE, C: -3}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: 66}, result: rule.PartitionRange{Start: 5, End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: 14}, result: rule.PartitionRange{Start: 4, End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: 10}, result: rule.PartitionRange{Start: 3, End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: 3}, result: rule.PartitionRange{Start: 1, End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: 2}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.GT, C: -3}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.LE, C: 66}, result: rule.PartitionRange{End: 6}},
		{input: rule.DataForPrune{Op: ast.LE, C: 14}, result: rule.PartitionRange{End: 5}},
		{input: rule.DataForPrune{Op: ast.LE, C: 10}, result: rule.PartitionRange{End: 3}},
		{input: rule.DataForPrune{Op: ast.LE, C: 3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.LE, C: -3}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: ast.IsNull}, result: rule.PartitionRange{End: 1}},
		{input: rule.DataForPrune{Op: "illegal"}, result: rule.PartitionRange{End: 6}},
	}

	for i, ca := range cases {
		start, end := rule.PruneUseBinarySearch(lessThan, ca.input)
		require.Equalf(t, ca.result.Start, start, "fail = %d", i)
		require.Equalf(t, ca.result.End, end, "fail = %d", i)
	}
}

type testCtx struct {
	require *require.Assertions
	sctx    *mock.Context
	schema  *expression.Schema
	columns []*expression.Column
	names   types.NameSlice
	col     *expression.Column
	fn      *expression.ScalarFunction
}

func prepareBenchCtx(createTable string, partitionExpr string) *testCtx {
	p := parser.New()
	stmt, err := p.ParseOneStmt(createTable, "", "")
	if err != nil {
		return nil
	}
	sctx := mock.NewContext()
	tblInfo, err := ddlhelper.BuildTableInfoFromASTForTest(stmt.(*ast.CreateTableStmt))
	if err != nil {
		return nil
	}
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(sctx, ast.NewCIStr("t"), tblInfo.Name, tblInfo.Cols(), tblInfo)
	if err != nil {
		return nil
	}
	schema := expression.NewSchema(columns...)

	col, fn, _, err := rule.MakePartitionByFnCol(sctx, columns, names, partitionExpr)
	if err != nil {
		return nil
	}
	return &testCtx{
		require: nil,
		sctx:    sctx,
		schema:  schema,
		columns: columns,
		names:   names,
		col:     col,
		fn:      fn,
	}
}

func prepareTestCtx(t *testing.T, createTable string, partitionExpr string) *testCtx {
	p := parser.New()
	stmt, err := p.ParseOneStmt(createTable, "", "")
	require.NoError(t, err)
	sctx := mock.NewContext()
	tblInfo, err := ddlhelper.BuildTableInfoFromASTForTest(stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(sctx, ast.NewCIStr("t"), tblInfo.Name, tblInfo.Cols(), tblInfo)
	require.NoError(t, err)
	schema := expression.NewSchema(columns...)

	col, fn, _, err := rule.MakePartitionByFnCol(sctx, columns, names, partitionExpr)
	require.NoError(t, err)
	return &testCtx{
		require: require.New(t),
		sctx:    sctx,
		schema:  schema,
		columns: columns,
		names:   names,
		col:     col,
		fn:      fn,
	}
}

func (tc *testCtx) expr(expr string) expression.Expression {
	res, err := expression.ParseSimpleExpr(tc.sctx, expr, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
	tc.require.NoError(err)
	return res
}

func TestPartitionRangeForExpr(t *testing.T) {
	tc := prepareTestCtx(t, "create table t (a int)", "a")
	lessThan := rule.LessThanDataInt{Data: []int64{4, 7, 11, 14, 17, 0}, Maxvalue: true}
	pruner := &rule.RangePruner{LessThan: lessThan, Col: tc.columns[0], Monotonous: rule.MonotoneModeInvalid}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a < 2 and a > 10", rule.PartitionRangeOR{}},
		{"a > 3", rule.PartitionRangeOR{{Start: 1, End: 6}}},
		{"a < 3", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a >= 11", rule.PartitionRangeOR{{Start: 3, End: 6}}},
		{"a > 11", rule.PartitionRangeOR{{Start: 3, End: 6}}},
		{"a < 11", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"a = 16", rule.PartitionRangeOR{{Start: 4, End: 5}}},
		{"a > 66", rule.PartitionRangeOR{{Start: 5, End: 6}}},
		{"a > 2 and a < 10", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"a < 2 or a >= 15", rule.PartitionRangeOR{{Start: 0, End: 1}, {Start: 4, End: 6}}},
		{"a is null", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"12 > a", rule.PartitionRangeOR{{Start: 0, End: 4}}},
		{"4 <= a", rule.PartitionRangeOR{{Start: 1, End: 6}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(lessThan.Length())
		result = rule.PartitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v", ca.input)
	}
}

func TestPartitionRangeOperation(t *testing.T) {
	testIntersectionRange := []struct {
		input1 rule.PartitionRangeOR
		input2 rule.PartitionRange
		result rule.PartitionRangeOR
	}{
		{input1: rule.PartitionRangeOR{{Start: 0, End: 3}, {Start: 6, End: 12}},
			input2: rule.PartitionRange{Start: 4, End: 7},
			result: rule.PartitionRangeOR{{Start: 6, End: 7}}},
		{input1: rule.PartitionRangeOR{{Start: 0, End: 5}},
			input2: rule.PartitionRange{Start: 6, End: 7},
			result: rule.PartitionRangeOR{}},
		{input1: rule.PartitionRangeOR{{Start: 0, End: 4}, {Start: 6, End: 7}, {Start: 8, End: 11}},
			input2: rule.PartitionRange{Start: 3, End: 9},
			result: rule.PartitionRangeOR{{Start: 3, End: 4}, {Start: 6, End: 7}, {Start: 8, End: 9}}},
	}
	for i, ca := range testIntersectionRange {
		result := ca.input1.IntersectionRange(ca.input2.Start, ca.input2.End)
		require.Truef(t, slices.Equal(ca.result, result), "fail = %d", i)
	}

	testIntersection := []struct {
		input1 rule.PartitionRangeOR
		input2 rule.PartitionRangeOR
		result rule.PartitionRangeOR
	}{
		{input1: rule.PartitionRangeOR{{Start: 0, End: 3}, {Start: 6, End: 12}},
			input2: rule.PartitionRangeOR{{Start: 4, End: 7}},
			result: rule.PartitionRangeOR{{Start: 6, End: 7}}},
		{input1: rule.PartitionRangeOR{{Start: 4, End: 7}},
			input2: rule.PartitionRangeOR{{Start: 0, End: 3}, {Start: 6, End: 12}},
			result: rule.PartitionRangeOR{{Start: 6, End: 7}}},
		{input1: rule.PartitionRangeOR{{Start: 4, End: 7}, {Start: 8, End: 10}},
			input2: rule.PartitionRangeOR{{Start: 0, End: 5}, {Start: 6, End: 12}},
			result: rule.PartitionRangeOR{{Start: 4, End: 5}, {Start: 6, End: 7}, {Start: 8, End: 10}}},
	}
	for i, ca := range testIntersection {
		result := ca.input1.Intersection(ca.input2)
		require.Truef(t, slices.Equal(ca.result, result), "fail = %d", i)
	}

	testUnion := []struct {
		input1 rule.PartitionRangeOR
		input2 rule.PartitionRangeOR
		result rule.PartitionRangeOR
	}{
		{input1: rule.PartitionRangeOR{{Start: 0, End: 1}, {Start: 2, End: 7}},
			input2: rule.PartitionRangeOR{{Start: 3, End: 5}},
			result: rule.PartitionRangeOR{{Start: 0, End: 1}, {Start: 2, End: 7}}},
		{input1: rule.PartitionRangeOR{{Start: 2, End: 7}},
			input2: rule.PartitionRangeOR{{Start: 0, End: 3}, {Start: 4, End: 12}},
			result: rule.PartitionRangeOR{{Start: 0, End: 12}}},
		{input1: rule.PartitionRangeOR{{Start: 4, End: 7}, {Start: 8, End: 10}},
			input2: rule.PartitionRangeOR{{Start: 0, End: 5}},
			result: rule.PartitionRangeOR{{Start: 0, End: 7}, {Start: 8, End: 10}}},
	}
	for i, ca := range testUnion {
		result := ca.input1.Union(ca.input2)
		require.Truef(t, slices.Equal(ca.result, result), "fail = %d", i)
	}
}

func TestPartitionRangePruner2VarChar(t *testing.T) {
	tc := prepareTestCtx(t, "create table t (a varchar(32))", "a")
	lessThanDataInt := []string{"'c'", "'f'", "'h'", "'l'", "'t'", "maxvalue"}
	lessThan := make([][]*expression.Expression, len(lessThanDataInt))
	for i, str := range lessThanDataInt {
		e := make([]*expression.Expression, 0, 1)
		if strings.EqualFold(str, "MAXVALUE") {
			e = append(e, nil)
		} else {
			expr, err := expression.ParseSimpleExpr(tc.sctx, str, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
			require.NoError(t, err)
			e = append(e, &expr)
		}
		lessThan[i] = e
	}

	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a > 'g'", rule.PartitionRangeOR{{Start: 2, End: 6}}},
		{"a < 'h'", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"a >= 'm'", rule.PartitionRangeOR{{Start: 4, End: 6}}},
		{"a > 'm'", rule.PartitionRangeOR{{Start: 4, End: 6}}},
		{"a < 'f'", rule.PartitionRangeOR{{Start: 0, End: 2}}},
		{"a = 'c'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a > 't'", rule.PartitionRangeOR{{Start: 5, End: 6}}},
		{"a > 'c' and a < 'q'", rule.PartitionRangeOR{{Start: 1, End: 5}}},
		{"a < 'l' or a >= 'w'", rule.PartitionRangeOR{{Start: 0, End: 4}, {Start: 5, End: 6}}},
		{"a is null", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"'mm' > a", rule.PartitionRangeOR{{Start: 0, End: 5}}},
		{"'f' <= a", rule.PartitionRangeOR{{Start: 2, End: 6}}},
		{"'f' >= a", rule.PartitionRangeOR{{Start: 0, End: 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(len(lessThan))
		result = rule.PartitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v", ca.input)
	}
}

func TestPartitionRangePruner2CharWithCollation(t *testing.T) {
	tc := prepareTestCtx(t,
		"create table t (a char(32) collate utf8mb4_unicode_ci)",
		"a",
	)
	lessThanDataInt := []string{"'c'", "'F'", "'h'", "'L'", "'t'", "MAXVALUE"}
	lessThan := make([][]*expression.Expression, len(lessThanDataInt))
	for i, str := range lessThanDataInt {
		e := make([]*expression.Expression, 0, 1)
		if strings.EqualFold(str, "MAXVALUE") {
			e = append(e, nil)
		} else {
			expr, err := expression.ParseSimpleExpr(tc.sctx, str, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
			require.NoError(t, err)
			e = append(e, &expr)
		}
		lessThan[i] = e
	}

	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a > 'G'", rule.PartitionRangeOR{{Start: 2, End: 6}}},
		{"a > 'g'", rule.PartitionRangeOR{{Start: 2, End: 6}}},
		{"a < 'h'", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"a >= 'M'", rule.PartitionRangeOR{{Start: 4, End: 6}}},
		{"a > 'm'", rule.PartitionRangeOR{{Start: 4, End: 6}}},
		{"a < 'F'", rule.PartitionRangeOR{{Start: 0, End: 2}}},
		{"a = 'C'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a > 't'", rule.PartitionRangeOR{{Start: 5, End: 6}}},
		{"a > 'C' and a < 'q'", rule.PartitionRangeOR{{Start: 1, End: 5}}},
		{"a > 'c' and a < 'Q'", rule.PartitionRangeOR{{Start: 1, End: 5}}},
		{"a < 'l' or a >= 'W'", rule.PartitionRangeOR{{Start: 0, End: 4}, {Start: 5, End: 6}}},
		{"a is null", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"'Mm' > a", rule.PartitionRangeOR{{Start: 0, End: 5}}},
		{"'f' <= a", rule.PartitionRangeOR{{Start: 2, End: 6}}},
		{"'f' >= a", rule.PartitionRangeOR{{Start: 0, End: 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(len(lessThan))
		result = rule.PartitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v %v != %v", ca.input, ca.result, result)
	}
}

func TestPartitionRangePruner2Date(t *testing.T) {
	tc := prepareTestCtx(t,
		"create table t (a date)",
		"a",
	)
	lessThanDataInt := []string{
		"'19990601'",
		"'2000-05-01'",
		"'20080401'",
		"'2010-03-01'",
		"'20160201'",
		"'2020-01-01'",
		"MAXVALUE"}
	lessThan := make([][]*expression.Expression, len(lessThanDataInt))
	for i, str := range lessThanDataInt {
		e := make([]*expression.Expression, 0, 1)
		if strings.EqualFold(str, "MAXVALUE") {
			e = append(e, nil)
		} else {
			expr, err := expression.ParseSimpleExpr(tc.sctx, str, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
			require.NoError(t, err)
			e = append(e, &expr)
		}
		lessThan[i] = e
	}

	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a < '1943-02-12'", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a >= '19690213'", rule.PartitionRangeOR{{Start: 0, End: 7}}},
		{"a > '2003-03-13'", rule.PartitionRangeOR{{Start: 2, End: 7}}},
		{"a < '2006-02-03'", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"a = '20070707'", rule.PartitionRangeOR{{Start: 2, End: 3}}},
		{"a > '1949-10-10'", rule.PartitionRangeOR{{Start: 0, End: 7}}},
		{"a > '2016-02-01' and a < '20000103'", rule.PartitionRangeOR{}},
		{"a < '19691112' or a >= '2019-09-18'", rule.PartitionRangeOR{{Start: 0, End: 1}, {Start: 5, End: 7}}},
		{"a is null", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"'2003-02-27' >= a", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"'20141024' < a", rule.PartitionRangeOR{{Start: 4, End: 7}}},
		{"'2003-03-30' > a", rule.PartitionRangeOR{{Start: 0, End: 3}}},
		{"'2003-03-30' < a AND a < '20080808'", rule.PartitionRangeOR{{Start: 2, End: 4}}},
		{"a between '2003-03-30' AND '20080808'", rule.PartitionRangeOR{{Start: 2, End: 4}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(len(lessThan))
		result = rule.PartitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v, %v != %v", ca.input, ca.result, result)
	}
}

func TestPartitionRangeColumnsForExpr(t *testing.T) {
	tc := prepareTestCtx(t, "create table t (a int unsigned, b int, c int)", "a,b")
	lessThan := make([][]*expression.Expression, 0, 6)
	partDefs := [][]int64{{3, -99},
		{4, math.MinInt64},
		{4, 1},
		{4, 4},
		{4, 7},
		{4, 11}, // p5
		{4, 14},
		{4, 17},
		{4, -99},
		{7, 0},
		{11, -99}, // p10
		{14, math.MinInt64},
		{17, 17},
		{-99, math.MinInt64}}
	for i := range partDefs {
		l := make([]*expression.Expression, 0, 2)
		for j := range []int{0, 1} {
			v := partDefs[i][j]
			var e *expression.Expression
			if v == -99 {
				e = nil // MAXVALUE
			} else {
				expr, err := expression.ParseSimpleExpr(tc.sctx, strconv.FormatInt(v, 10), expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
				require.NoError(t, err)
				e = &expr
			}
			l = append(l, e)
		}
		lessThan = append(lessThan, l)
	}
	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns[:2]}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a < 1 and a > 1", rule.PartitionRangeOR{}},
		{"c = 3", rule.PartitionRangeOR{{Start: 0, End: len(partDefs)}}},
		{"b > 3 AND c = 3", rule.PartitionRangeOR{{Start: 0, End: len(partDefs)}}},
		{"a = 5 AND c = 3", rule.PartitionRangeOR{{Start: 9, End: 10}}},
		{"a = 4 AND c = 3", rule.PartitionRangeOR{{Start: 1, End: 9}}},
		{"b > 3", rule.PartitionRangeOR{{Start: 0, End: len(partDefs)}}},
		{"a > 3", rule.PartitionRangeOR{{Start: 1, End: len(partDefs)}}},
		{"a < 3", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a >= 11", rule.PartitionRangeOR{{Start: 10, End: len(partDefs)}}},
		{"a > 11", rule.PartitionRangeOR{{Start: 11, End: len(partDefs)}}},
		{"a > 4", rule.PartitionRangeOR{{Start: 9, End: len(partDefs)}}},
		{"a >= 4", rule.PartitionRangeOR{{Start: 1, End: len(partDefs)}}},
		{"a < 11", rule.PartitionRangeOR{{Start: 0, End: 11}}},
		{"a = 16", rule.PartitionRangeOR{{Start: 12, End: 13}}},
		{"a > 66", rule.PartitionRangeOR{{Start: 13, End: 14}}},
		{"a > 2 and a < 10", rule.PartitionRangeOR{{Start: 0, End: 11}}},
		{"a < 2 or a >= 15", rule.PartitionRangeOR{{Start: 0, End: 1}, {Start: 12, End: 14}}},
		{"a is null", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"12 > a", rule.PartitionRangeOR{{Start: 0, End: 12}}},
		{"4 <= a", rule.PartitionRangeOR{{Start: 1, End: 14}}},
		{"(a,b) < (4,4)", rule.PartitionRangeOR{{Start: 0, End: 4}}},
		{"(a,b) = (4,4)", rule.PartitionRangeOR{{Start: 4, End: 5}}},
		{"a < 4 OR (a = 4 AND b < 4)", rule.PartitionRangeOR{{Start: 0, End: 4}}},
		{"(a,b,c) < (4,4,4)", rule.PartitionRangeOR{{Start: 0, End: 5}}},
		{"a < 4 OR (a = 4 AND b < 4) OR (a = 4 AND b = 4 AND c < 4)", rule.PartitionRangeOR{{Start: 0, End: 5}}},
		{"(a,b,c) >= (4,7,4)", rule.PartitionRangeOR{{Start: 5, End: len(partDefs)}}},
		{"a > 4 or (a= 4 and b > 7) or (a = 4 and b = 7 and c >= 4)", rule.PartitionRangeOR{{Start: 5, End: len(partDefs)}}},
		{"(a,b,c) = (4,7,4)", rule.PartitionRangeOR{{Start: 5, End: 6}}},
		{"a < 2 and a > 10", rule.PartitionRangeOR{}},
		{"a < 1 and a > 1", rule.PartitionRangeOR{}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(len(lessThan))
		e := expression.SplitCNFItems(expr)
		result = rule.PartitionRangeForCNFExpr(tc.sctx, e, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v %v != %v", ca.input, ca.result, result)
	}
}

func TestPartitionRangeColumnsForExprWithSpecialCollation(t *testing.T) {
	tc := prepareTestCtx(t, "create table t (a varchar(255) COLLATE utf8mb4_0900_ai_ci, b varchar(255) COLLATE utf8mb4_unicode_ci)", "a,b")
	lessThan := make([][]*expression.Expression, 0, 6)
	partDefs := [][]string{
		{"'i'", "'i'"},
		{"MAXVALUE", "MAXVALUE"},
	}
	for i := range partDefs {
		l := make([]*expression.Expression, 0, 2)
		for j := range []int{0, 1} {
			v := partDefs[i][j]
			var e *expression.Expression
			if v == "MAXVALUE" {
				e = nil // MAXVALUE
			} else {
				expr, err := expression.ParseSimpleExpr(tc.sctx, v, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
				require.NoError(t, err)
				e = &expr
			}
			l = append(l, e)
		}
		lessThan = append(lessThan, l)
	}
	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns[:2]}
	cases := []struct {
		input  string
		result rule.PartitionRangeOR
	}{
		{"a = 'q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'Q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'a'", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a = 'A'", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a > 'a'", rule.PartitionRangeOR{{Start: 0, End: 2}}},
		{"a > 'q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'i' and b = 'q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'i' and b = 'Q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'i' and b = 'a'", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a = 'i' and b = 'A'", rule.PartitionRangeOR{{Start: 0, End: 1}}},
		{"a = 'i' and b > 'a'", rule.PartitionRangeOR{{Start: 0, End: 2}}},
		{"a = 'i' and b > 'q'", rule.PartitionRangeOR{{Start: 1, End: 2}}},
		{"a = 'i' or a = 'h'", rule.PartitionRangeOR{{Start: 0, End: 2}}},
		{"a = 'h' and a = 'j'", rule.PartitionRangeOR{}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := rule.GetFullRange(len(lessThan))
		e := expression.SplitCNFItems(expr)
		result = rule.PartitionRangeForCNFExpr(tc.sctx, e, pruner, result)
		require.Truef(t, slices.Equal(ca.result, result), "unexpected: %v %v != %v", ca.input, ca.result, result)
	}
}

func benchmarkRangeColumnsPruner(b *testing.B, parts int) {
	tc := prepareBenchCtx("create table t (a bigint unsigned, b int, c int)", "a")
	if tc == nil {
		panic("Failed to initialize benchmark")
	}
	lessThan := make([][]*expression.Expression, 0, parts)
	partDefs := make([][]int64, 0, parts)
	for i := range parts - 1 {
		partDefs = append(partDefs, []int64{int64(i * 10000)})
	}
	partDefs = append(partDefs, []int64{-99})
	for i := range partDefs {
		v := partDefs[i][0]
		var e *expression.Expression
		if v == -99 {
			e = nil // MAXVALUE
		} else {
			expr, err := expression.ParseSimpleExpr(tc.sctx, strconv.FormatInt(v, 10), expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
			if err != nil {
				panic(err.Error())
			}
			e = &expr
		}
		lessThan = append(lessThan, []*expression.Expression{e})
	}
	pruner := &rule.RangeColumnsPruner{LessThan: lessThan, PartCols: tc.columns[:1]}

	expr, err := expression.ParseSimpleExpr(tc.sctx, "a > 11000", expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
	if err != nil {
		panic(err.Error())
	}
	result := rule.GetFullRange(len(lessThan))
	e := expression.SplitCNFItems(expr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result[0] = rule.PartitionRange{Start: 0, End: parts}
		result = result[:1]
		result = rule.PartitionRangeForCNFExpr(tc.sctx, e, pruner, result)
	}
}

func BenchmarkRangeColumnsPruner2(b *testing.B) {
	benchmarkRangeColumnsPruner(b, 2)
}

func BenchmarkRangeColumnsPruner10(b *testing.B) {
	benchmarkRangeColumnsPruner(b, 10)
}

func BenchmarkRangeColumnsPruner100(b *testing.B) {
	benchmarkRangeColumnsPruner(b, 100)
}

func BenchmarkRangeColumnsPruner1000(b *testing.B) {
	benchmarkRangeColumnsPruner(b, 1000)
}

func BenchmarkRangeColumnsPruner8000(b *testing.B) {
	benchmarkRangeColumnsPruner(b, 8000)
}
