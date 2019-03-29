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

package executor

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

type requiredRowsDataSource struct {
	baseExecutor
	totalRows int
	count     int
	ctx       sessionctx.Context

	expectedRowsRet []int
	numNextCalled   int
}

func newRequiredRowsDataSource(ctx sessionctx.Context, totalRows int, expectedRowsRet []int) *requiredRowsDataSource {
	// the schema of output is fixed now, which is [Double, Long]
	retTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLonglong)}
	cols := make([]*expression.Column, len(retTypes))
	for i := range retTypes {
		cols[i] = &expression.Column{Index: i, RetType: retTypes[i]}
	}
	schema := expression.NewSchema(cols...)
	baseExec := newBaseExecutor(ctx, schema, "")
	return &requiredRowsDataSource{baseExec, totalRows, 0, ctx, expectedRowsRet, 0}
}

func (r *requiredRowsDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	defer func() {
		rowsRet := req.NumRows()
		expected := r.expectedRowsRet[r.numNextCalled]
		if rowsRet != expected {
			panic(fmt.Sprintf("unexpected number of rows returned, obtain: %v, expected: %v", rowsRet, expected))
		}
		r.numNextCalled++
	}()

	req.Reset()
	if r.count > r.totalRows {
		return nil
	}
	required := mathutil.Min(req.RequiredRows(), r.totalRows-r.count)
	for i := 0; i < required; i++ {
		req.AppendRow(r.genOneRow())
	}
	r.count += required
	return nil
}

func (r *requiredRowsDataSource) genOneRow() chunk.Row {
	row := chunk.MutRowFromTypes(r.retTypes())
	for i := range r.retTypes() {
		row.SetValue(i, r.genValue(r.retTypes()[i]))
	}
	return row.ToRow()
}

func (r *requiredRowsDataSource) genValue(valType *types.FieldType) interface{} {
	switch valType.Tp {
	case mysql.TypeLong, mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeDouble:
		return rand.Float64()
	default:
		panic("not implement")
	}
}

func (r *requiredRowsDataSource) checkNumNextCalled() error {
	if r.numNextCalled != len(r.expectedRowsRet) {
		return fmt.Errorf("unexpected number of call on Next, obtain: %v, expected: %v",
			r.numNextCalled, len(r.expectedRowsRet))
	}
	return nil
}

func (s *testExecSuite) TestLimitRequiredRows(c *C) {
	maxChunkSize := defaultCtx().GetSessionVars().MaxChunkSize
	testCases := []struct {
		totalRows      int
		limitOffset    int
		limitCount     int
		requiredRows   []int
		expectedRows   []int
		expectedRowsDS []int
	}{
		{
			totalRows:      20,
			limitOffset:    0,
			limitCount:     10,
			requiredRows:   []int{3, 5, 1, 500, 500},
			expectedRows:   []int{3, 5, 1, 1, 0},
			expectedRowsDS: []int{3, 5, 1, 1},
		},
		{
			totalRows:      20,
			limitOffset:    0,
			limitCount:     25,
			requiredRows:   []int{9, 500},
			expectedRows:   []int{9, 11},
			expectedRowsDS: []int{9, 11},
		},
		{
			totalRows:      100,
			limitOffset:    50,
			limitCount:     30,
			requiredRows:   []int{10, 5, 10, 20},
			expectedRows:   []int{10, 5, 10, 5},
			expectedRowsDS: []int{60, 5, 10, 5},
		},
		{
			totalRows:      100,
			limitOffset:    101,
			limitCount:     10,
			requiredRows:   []int{10},
			expectedRows:   []int{0},
			expectedRowsDS: []int{100, 0},
		},
		{
			totalRows:      maxChunkSize + 20,
			limitOffset:    maxChunkSize + 1,
			limitCount:     10,
			requiredRows:   []int{3, 3, 3, 100},
			expectedRows:   []int{3, 3, 3, 1},
			expectedRowsDS: []int{maxChunkSize, 4, 3, 3, 1},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredRowsDataSource(sctx, testCase.totalRows, testCase.expectedRowsDS)
		exec := buildLimitExec(sctx, ds, testCase.limitOffset, testCase.limitCount)
		c.Assert(exec.Open(ctx), IsNil)
		chk := exec.newFirstChunk()
		for i := range testCase.requiredRows {
			chk.SetRequiredRows(testCase.requiredRows[i], sctx.GetSessionVars().MaxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumRows(), Equals, testCase.expectedRows[i])
		}
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildLimitExec(ctx sessionctx.Context, src Executor, offset, count int) Executor {
	n := mathutil.Min(count, ctx.GetSessionVars().MaxChunkSize)
	base := newBaseExecutor(ctx, src.Schema(), "", src)
	base.initCap = n
	limitExec := &LimitExec{
		baseExecutor: base,
		begin:        uint64(offset),
		end:          uint64(offset + count),
	}
	return limitExec
}

func defaultCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().MaxChunkSize = 1024
	return ctx
}
