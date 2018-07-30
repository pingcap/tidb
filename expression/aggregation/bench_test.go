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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"testing"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

func BenchmarkCreateContext(b *testing.B) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	fun := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false).GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetSessionVars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetContext(b *testing.B) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	fun := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false).GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetSessionVars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetSessionVars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}

func BenchmarkCreateDistinctContext(b *testing.B) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	fun := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, true).GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetSessionVars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetDistinctContext(b *testing.B) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	fun := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, true).GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetSessionVars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetSessionVars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}
