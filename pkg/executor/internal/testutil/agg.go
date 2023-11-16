// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// AggTestCase has a fixed schema (aggCol Double, groupBy LongLong).
type AggTestCase struct {
	ExecType         string // "hash" or "stream"
	AggFunc          string // sum, avg, count ....
	GroupByNDV       int    // the number of distinct group-by keys
	HasDistinct      bool
	Rows             int
	Concurrency      int
	DataSourceSorted bool
	Ctx              sessionctx.Context
}

func (a AggTestCase) Columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (a AggTestCase) String() string {
	return fmt.Sprintf("(execType:%v, aggFunc:%v, ndv:%v, hasDistinct:%v, rows:%v, concurrency:%v, sorted:%v)",
		a.ExecType, a.AggFunc, a.GroupByNDV, a.HasDistinct, a.Rows, a.Concurrency, a.DataSourceSorted)
}

func DefaultAggTestCase(exec string) *AggTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &AggTestCase{exec, ast.AggFuncSum, 1000, false, 10000000, 4, true, ctx}
}
