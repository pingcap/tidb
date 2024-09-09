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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// SortCase is the sort case
type SortCase struct {
	Ctx        sessionctx.Context
	OrderByIdx []int
	Ndvs       []int
	Rows       int
}

// Columns creates column
func (SortCase) Columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

// String gets case content
func (tc SortCase) String() string {
	return fmt.Sprintf("(rows:%v, orderBy:%v, ndvs: %v)", tc.Rows, tc.OrderByIdx, tc.Ndvs)
}

// DefaultSortTestCase returns default sort test case
func DefaultSortTestCase() *SortCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	tc := &SortCase{Rows: 300000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	return tc
}

// SortTestCaseWithMemoryLimit returns sort test case
func SortTestCaseWithMemoryLimit(bytesLimit int64) *SortCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MemTracker = memory.NewTracker(-1, bytesLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, bytesLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	tc := &SortCase{Rows: 300000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	return tc
}
