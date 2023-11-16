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
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// WindowTestCase has a fixed schema (col Double, partitionBy LongLong, rawData VarString(16), col LongLong).
type WindowTestCase struct {
	WindowFunc       string
	NumFunc          int // The number of windowFuncs. Default: 1.
	Frame            *core.WindowFrame
	Ndv              int // the number of distinct group-by keys
	Rows             int
	Concurrency      int
	Pipelined        int
	DataSourceSorted bool
	Ctx              sessionctx.Context
	RawDataSmall     string
	Columns          []*expression.Column // the columns of mock schema
}

func (a WindowTestCase) String() string {
	return fmt.Sprintf("(func:%v, aggColType:%s, numFunc:%v, ndv:%v, rows:%v, sorted:%v, concurrency:%v, pipelined:%v)",
		a.WindowFunc, a.Columns[0].RetType, a.NumFunc, a.Ndv, a.Rows, a.DataSourceSorted, a.Concurrency, a.Pipelined)
}

func DefaultWindowTestCase() *WindowTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &WindowTestCase{ast.WindowFuncRowNumber, 1, nil, 1000, 10000000, 1, 0, true, ctx, strings.Repeat("x", 16),
		[]*expression.Column{
			{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
			{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
			{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
		}}
}
