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
	Ctx              sessionctx.Context
	Frame            *core.WindowFrame
	WindowFunc       string
	RawDataSmall     string
	Columns          []*expression.Column
	NumFunc          int
	Ndv              int
	Rows             int
	Concurrency      int
	Pipelined        int
	DataSourceSorted bool
}

// String gets case content
func (a WindowTestCase) String() string {
	return fmt.Sprintf("(func:%v, aggColType:%s, numFunc:%v, ndv:%v, rows:%v, sorted:%v, concurrency:%v, pipelined:%v)",
		a.WindowFunc, a.Columns[0].RetType, a.NumFunc, a.Ndv, a.Rows, a.DataSourceSorted, a.Concurrency, a.Pipelined)
}

// DefaultWindowTestCase returns default window test case
func DefaultWindowTestCase() *WindowTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &WindowTestCase{
		WindowFunc:       ast.WindowFuncRowNumber,
		NumFunc:          1,
		Frame:            nil,
		Ndv:              1000,
		Rows:             10000000,
		Concurrency:      1,
		Pipelined:        0,
		DataSourceSorted: true,
		Ctx:              ctx,
		RawDataSmall:     strings.Repeat("x", 16),
		Columns: []*expression.Column{
			{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
			{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
			{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
		},
	}
}
