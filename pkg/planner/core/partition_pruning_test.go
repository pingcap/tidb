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

package core

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
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
	lessThan := lessThanDataInt{data: []int64{733108, 733132}, maxvalue: false}
	pruner := &rangePruner{lessThan, tc.col, tc.fn, monotoneModeNonStrict}

	queryExpr := tc.expr("d < '2000-03-08 00:00:00'")
	result := partitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, fullRange(len(lessThan.data)))
	require.True(t, equalPartitionRangeOR(result, partitionRangeOR{{0, 1}}))

	queryExpr = tc.expr("d > '2018-03-08 00:00:00'")
	result = partitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, fullRange(len(lessThan.data)))
	require.True(t, equalPartitionRangeOR(result, partitionRangeOR{}))

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
	lessThan = lessThanDataInt{data: []int64{1199145600, 1207008000, 1262304000, 0}, maxvalue: true}
	pruner = &rangePruner{lessThan, tc.col, tc.fn, monotoneModeStrict}

	queryExpr = tc.expr("report_updated > '2008-05-01 00:00:00'")
	result = partitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, fullRange(len(lessThan.data)))
	require.True(t, equalPartitionRangeOR(result, partitionRangeOR{{2, 4}}))

	queryExpr = tc.expr("report_updated > unix_timestamp('2008-05-01 00:00:00')")
	partitionRangeForCNFExpr(tc.sctx, []expression.Expression{queryExpr}, pruner, fullRange(len(lessThan.data)))
	// TODO: Uncomment the check after fixing issue https://github.com/pingcap/tidb/issues/12028
	// require.True(t, equalPartitionRangeOR(result, partitionRangeOR{{2, 4}}))
	// report_updated > unix_timestamp('2008-05-01 00:00:00') is converted to gt(t.t.report_updated, <nil>)
	// Because unix_timestamp('2008-05-01 00:00:00') is fold to constant int 1564761600, and compare it with timestamp (report_updated)
	// need to convert 1564761600 to a timestamp, during that step, an error happen and the result is set to <nil>
}

func TestPruneUseBinarySearchSigned(t *testing.T) {
	lessThan := lessThanDataInt{data: []int64{-3, 4, 7, 11, 14, 17, 0}, maxvalue: true, unsigned: false}
	cases := []struct {
		input  dataForPrune
		result partitionRange
	}{
		{dataForPrune{ast.EQ, 66, false}, partitionRange{6, 7}},
		{dataForPrune{ast.EQ, 14, false}, partitionRange{5, 6}},
		{dataForPrune{ast.EQ, 10, false}, partitionRange{3, 4}},
		{dataForPrune{ast.EQ, 3, false}, partitionRange{1, 2}},
		{dataForPrune{ast.EQ, -4, false}, partitionRange{0, 1}},
		{dataForPrune{ast.LT, 66, false}, partitionRange{0, 7}},
		{dataForPrune{ast.LT, 14, false}, partitionRange{0, 5}},
		{dataForPrune{ast.LT, 10, false}, partitionRange{0, 4}},
		{dataForPrune{ast.LT, 3, false}, partitionRange{0, 2}},
		{dataForPrune{ast.LT, -4, false}, partitionRange{0, 1}},
		{dataForPrune{ast.GE, 66, false}, partitionRange{6, 7}},
		{dataForPrune{ast.GE, 14, false}, partitionRange{5, 7}},
		{dataForPrune{ast.GE, 10, false}, partitionRange{3, 7}},
		{dataForPrune{ast.GE, 3, false}, partitionRange{1, 7}},
		{dataForPrune{ast.GE, -4, false}, partitionRange{0, 7}},
		{dataForPrune{ast.GT, 66, false}, partitionRange{6, 7}},
		{dataForPrune{ast.GT, 14, false}, partitionRange{5, 7}},
		{dataForPrune{ast.GT, 10, false}, partitionRange{4, 7}},
		{dataForPrune{ast.GT, 3, false}, partitionRange{2, 7}},
		{dataForPrune{ast.GT, 2, false}, partitionRange{1, 7}},
		{dataForPrune{ast.GT, -4, false}, partitionRange{1, 7}},
		{dataForPrune{ast.LE, 66, false}, partitionRange{0, 7}},
		{dataForPrune{ast.LE, 14, false}, partitionRange{0, 6}},
		{dataForPrune{ast.LE, 10, false}, partitionRange{0, 4}},
		{dataForPrune{ast.LE, 3, false}, partitionRange{0, 2}},
		{dataForPrune{ast.LE, -4, false}, partitionRange{0, 1}},
		{dataForPrune{ast.IsNull, 0, false}, partitionRange{0, 1}},
		{dataForPrune{"illegal", 0, false}, partitionRange{0, 7}},
	}

	for i, ca := range cases {
		start, end := pruneUseBinarySearch(lessThan, ca.input)
		require.Equalf(t, ca.result.start, start, "fail = %d", i)
		require.Equalf(t, ca.result.end, end, "fail = %d", i)
	}
}

func TestPruneUseBinarySearchUnSigned(t *testing.T) {
	lessThan := lessThanDataInt{data: []int64{4, 7, 11, 14, 17, 0}, maxvalue: true, unsigned: true}
	cases := []struct {
		input  dataForPrune
		result partitionRange
	}{
		{dataForPrune{ast.EQ, 66, false}, partitionRange{5, 6}},
		{dataForPrune{ast.EQ, 14, false}, partitionRange{4, 5}},
		{dataForPrune{ast.EQ, 10, false}, partitionRange{2, 3}},
		{dataForPrune{ast.EQ, 3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.EQ, -3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.LT, 66, false}, partitionRange{0, 6}},
		{dataForPrune{ast.LT, 14, false}, partitionRange{0, 4}},
		{dataForPrune{ast.LT, 10, false}, partitionRange{0, 3}},
		{dataForPrune{ast.LT, 3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.LT, -3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.GE, 66, false}, partitionRange{5, 6}},
		{dataForPrune{ast.GE, 14, false}, partitionRange{4, 6}},
		{dataForPrune{ast.GE, 10, false}, partitionRange{2, 6}},
		{dataForPrune{ast.GE, 3, false}, partitionRange{0, 6}},
		{dataForPrune{ast.GE, -3, false}, partitionRange{0, 6}},
		{dataForPrune{ast.GT, 66, false}, partitionRange{5, 6}},
		{dataForPrune{ast.GT, 14, false}, partitionRange{4, 6}},
		{dataForPrune{ast.GT, 10, false}, partitionRange{3, 6}},
		{dataForPrune{ast.GT, 3, false}, partitionRange{1, 6}},
		{dataForPrune{ast.GT, 2, false}, partitionRange{0, 6}},
		{dataForPrune{ast.GT, -3, false}, partitionRange{0, 6}},
		{dataForPrune{ast.LE, 66, false}, partitionRange{0, 6}},
		{dataForPrune{ast.LE, 14, false}, partitionRange{0, 5}},
		{dataForPrune{ast.LE, 10, false}, partitionRange{0, 3}},
		{dataForPrune{ast.LE, 3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.LE, -3, false}, partitionRange{0, 1}},
		{dataForPrune{ast.IsNull, 0, false}, partitionRange{0, 1}},
		{dataForPrune{"illegal", 0, false}, partitionRange{0, 6}},
	}

	for i, ca := range cases {
		start, end := pruneUseBinarySearch(lessThan, ca.input)
		require.Equalf(t, ca.result.start, start, "fail = %d", i)
		require.Equalf(t, ca.result.end, end, "fail = %d", i)
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
	tblInfo, err := ddlhelper.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	if err != nil {
		return nil
	}
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(sctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Cols(), tblInfo)
	if err != nil {
		return nil
	}
	schema := expression.NewSchema(columns...)

	col, fn, _, err := makePartitionByFnCol(sctx, columns, names, partitionExpr)
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
	tblInfo, err := ddlhelper.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(sctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Cols(), tblInfo)
	require.NoError(t, err)
	schema := expression.NewSchema(columns...)

	col, fn, _, err := makePartitionByFnCol(sctx, columns, names, partitionExpr)
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
	lessThan := lessThanDataInt{data: []int64{4, 7, 11, 14, 17, 0}, maxvalue: true}
	pruner := &rangePruner{lessThan, tc.columns[0], nil, monotoneModeInvalid}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a < 2 and a > 10", partitionRangeOR{}},
		{"a > 3", partitionRangeOR{{1, 6}}},
		{"a < 3", partitionRangeOR{{0, 1}}},
		{"a >= 11", partitionRangeOR{{3, 6}}},
		{"a > 11", partitionRangeOR{{3, 6}}},
		{"a < 11", partitionRangeOR{{0, 3}}},
		{"a = 16", partitionRangeOR{{4, 5}}},
		{"a > 66", partitionRangeOR{{5, 6}}},
		{"a > 2 and a < 10", partitionRangeOR{{0, 3}}},
		{"a < 2 or a >= 15", partitionRangeOR{{0, 1}, {4, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"12 > a", partitionRangeOR{{0, 4}}},
		{"4 <= a", partitionRangeOR{{1, 6}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := fullRange(lessThan.length())
		result = partitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "unexpected: %v", ca.input)
	}
}

func equalPartitionRangeOR(x, y partitionRangeOR) bool {
	if len(x) != len(y) {
		return false
	}
	for i := 0; i < len(x); i++ {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func TestPartitionRangeOperation(t *testing.T) {
	testIntersectionRange := []struct {
		input1 partitionRangeOR
		input2 partitionRange
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 3}, {6, 12}},
			input2: partitionRange{4, 7},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{0, 5}},
			input2: partitionRange{6, 7},
			result: partitionRangeOR{}},
		{input1: partitionRangeOR{{0, 4}, {6, 7}, {8, 11}},
			input2: partitionRange{3, 9},
			result: partitionRangeOR{{3, 4}, {6, 7}, {8, 9}}},
	}
	for i, ca := range testIntersectionRange {
		result := ca.input1.intersectionRange(ca.input2.start, ca.input2.end)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "fail = %d", i)
	}

	testIntersection := []struct {
		input1 partitionRangeOR
		input2 partitionRangeOR
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 3}, {6, 12}},
			input2: partitionRangeOR{{4, 7}},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{4, 7}},
			input2: partitionRangeOR{{0, 3}, {6, 12}},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{4, 7}, {8, 10}},
			input2: partitionRangeOR{{0, 5}, {6, 12}},
			result: partitionRangeOR{{4, 5}, {6, 7}, {8, 10}}},
	}
	for i, ca := range testIntersection {
		result := ca.input1.intersection(ca.input2)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "fail = %d", i)
	}

	testUnion := []struct {
		input1 partitionRangeOR
		input2 partitionRangeOR
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 1}, {2, 7}},
			input2: partitionRangeOR{{3, 5}},
			result: partitionRangeOR{{0, 1}, {2, 7}}},
		{input1: partitionRangeOR{{2, 7}},
			input2: partitionRangeOR{{0, 3}, {4, 12}},
			result: partitionRangeOR{{0, 12}}},
		{input1: partitionRangeOR{{4, 7}, {8, 10}},
			input2: partitionRangeOR{{0, 5}},
			result: partitionRangeOR{{0, 7}, {8, 10}}},
	}
	for i, ca := range testUnion {
		result := ca.input1.union(ca.input2)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "fail = %d", i)
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

	pruner := &rangeColumnsPruner{lessThan, tc.columns}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a > 'g'", partitionRangeOR{{2, 6}}},
		{"a < 'h'", partitionRangeOR{{0, 3}}},
		{"a >= 'm'", partitionRangeOR{{4, 6}}},
		{"a > 'm'", partitionRangeOR{{4, 6}}},
		{"a < 'f'", partitionRangeOR{{0, 2}}},
		{"a = 'c'", partitionRangeOR{{1, 2}}},
		{"a > 't'", partitionRangeOR{{5, 6}}},
		{"a > 'c' and a < 'q'", partitionRangeOR{{1, 5}}},
		{"a < 'l' or a >= 'w'", partitionRangeOR{{0, 4}, {5, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"'mm' > a", partitionRangeOR{{0, 5}}},
		{"'f' <= a", partitionRangeOR{{2, 6}}},
		{"'f' >= a", partitionRangeOR{{0, 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := fullRange(len(lessThan))
		result = partitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "unexpected: %v", ca.input)
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

	pruner := &rangeColumnsPruner{lessThan, tc.columns}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a > 'G'", partitionRangeOR{{2, 6}}},
		{"a > 'g'", partitionRangeOR{{2, 6}}},
		{"a < 'h'", partitionRangeOR{{0, 3}}},
		{"a >= 'M'", partitionRangeOR{{4, 6}}},
		{"a > 'm'", partitionRangeOR{{4, 6}}},
		{"a < 'F'", partitionRangeOR{{0, 2}}},
		{"a = 'C'", partitionRangeOR{{1, 2}}},
		{"a > 't'", partitionRangeOR{{5, 6}}},
		{"a > 'C' and a < 'q'", partitionRangeOR{{1, 5}}},
		{"a > 'c' and a < 'Q'", partitionRangeOR{{1, 5}}},
		{"a < 'l' or a >= 'W'", partitionRangeOR{{0, 4}, {5, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"'Mm' > a", partitionRangeOR{{0, 5}}},
		{"'f' <= a", partitionRangeOR{{2, 6}}},
		{"'f' >= a", partitionRangeOR{{0, 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := fullRange(len(lessThan))
		result = partitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "unexpected: %v %v != %v", ca.input, ca.result, result)
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

	pruner := &rangeColumnsPruner{lessThan, tc.columns}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a < '1943-02-12'", partitionRangeOR{{0, 1}}},
		{"a >= '19690213'", partitionRangeOR{{0, 7}}},
		{"a > '2003-03-13'", partitionRangeOR{{2, 7}}},
		{"a < '2006-02-03'", partitionRangeOR{{0, 3}}},
		{"a = '20070707'", partitionRangeOR{{2, 3}}},
		{"a > '1949-10-10'", partitionRangeOR{{0, 7}}},
		{"a > '2016-02-01' and a < '20000103'", partitionRangeOR{}},
		{"a < '19691112' or a >= '2019-09-18'", partitionRangeOR{{0, 1}, {5, 7}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"'2003-02-27' >= a", partitionRangeOR{{0, 3}}},
		{"'20141024' < a", partitionRangeOR{{4, 7}}},
		{"'2003-03-30' > a", partitionRangeOR{{0, 3}}},
		{"'2003-03-30' < a AND a < '20080808'", partitionRangeOR{{2, 4}}},
		{"a between '2003-03-30' AND '20080808'", partitionRangeOR{{2, 4}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := fullRange(len(lessThan))
		result = partitionRangeForExpr(tc.sctx, expr, pruner, result)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "unexpected: %v, %v != %v", ca.input, ca.result, result)
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
	pruner := &rangeColumnsPruner{lessThan, tc.columns[:2]}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a < 1 and a > 1", partitionRangeOR{}},
		{"c = 3", partitionRangeOR{{0, len(partDefs)}}},
		{"b > 3 AND c = 3", partitionRangeOR{{0, len(partDefs)}}},
		{"a = 5 AND c = 3", partitionRangeOR{{9, 10}}},
		{"a = 4 AND c = 3", partitionRangeOR{{1, 9}}},
		{"b > 3", partitionRangeOR{{0, len(partDefs)}}},
		{"a > 3", partitionRangeOR{{1, len(partDefs)}}},
		{"a < 3", partitionRangeOR{{0, 1}}},
		{"a >= 11", partitionRangeOR{{10, len(partDefs)}}},
		{"a > 11", partitionRangeOR{{11, len(partDefs)}}},
		{"a > 4", partitionRangeOR{{9, len(partDefs)}}},
		{"a >= 4", partitionRangeOR{{1, len(partDefs)}}},
		{"a < 11", partitionRangeOR{{0, 11}}},
		{"a = 16", partitionRangeOR{{12, 13}}},
		{"a > 66", partitionRangeOR{{13, 14}}},
		{"a > 2 and a < 10", partitionRangeOR{{0, 11}}},
		{"a < 2 or a >= 15", partitionRangeOR{{0, 1}, {12, 14}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"12 > a", partitionRangeOR{{0, 12}}},
		{"4 <= a", partitionRangeOR{{1, 14}}},
		// The expression is converted to 'if ...', see constructBinaryOpFunction, so not possible to break down to ranges
		{"(a,b) < (4,4)", partitionRangeOR{{0, 14}}},
		{"(a,b) = (4,4)", partitionRangeOR{{4, 5}}},
		{"a < 4 OR (a = 4 AND b < 4)", partitionRangeOR{{0, 4}}},
		// The expression is converted to 'if ...', see constructBinaryOpFunction, so not possible to break down to ranges
		{"(a,b,c) < (4,4,4)", partitionRangeOR{{0, 14}}},
		{"a < 4 OR (a = 4 AND b < 4) OR (a = 4 AND b = 4 AND c < 4)", partitionRangeOR{{0, 5}}},
		{"(a,b,c) >= (4,7,4)", partitionRangeOR{{0, len(partDefs)}}},
		{"(a,b,c) = (4,7,4)", partitionRangeOR{{5, 6}}},
		{"a < 2 and a > 10", partitionRangeOR{}},
		{"a < 1 and a > 1", partitionRangeOR{}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExpr(tc.sctx, ca.input, expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
		require.NoError(t, err)
		result := fullRange(len(lessThan))
		e := expression.SplitCNFItems(expr)
		result = partitionRangeForCNFExpr(tc.sctx, e, pruner, result)
		require.Truef(t, equalPartitionRangeOR(ca.result, result), "unexpected: %v %v != %v", ca.input, ca.result, result)
	}
}

func benchmarkRangeColumnsPruner(b *testing.B, parts int) {
	tc := prepareBenchCtx("create table t (a bigint unsigned, b int, c int)", "a")
	if tc == nil {
		panic("Failed to initialize benchmark")
	}
	lessThan := make([][]*expression.Expression, 0, parts)
	partDefs := make([][]int64, 0, parts)
	for i := 0; i < parts-1; i++ {
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
	pruner := &rangeColumnsPruner{lessThan, tc.columns[:1]}

	expr, err := expression.ParseSimpleExpr(tc.sctx, "a > 11000", expression.WithInputSchemaAndNames(tc.schema, tc.names, nil))
	if err != nil {
		panic(err.Error())
	}
	result := fullRange(len(lessThan))
	e := expression.SplitCNFItems(expr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result[0] = partitionRange{0, parts}
		result = result[:1]
		result = partitionRangeForCNFExpr(tc.sctx, e, pruner, result)
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
