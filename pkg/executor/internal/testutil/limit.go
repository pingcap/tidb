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

type LimitCase struct {
	Rows                  int
	Offset                int
	Count                 int
	ChildUsedSchema       []bool
	UsingInlineProjection bool
	Ctx                   sessionctx.Context
}

func (tc LimitCase) Columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (tc LimitCase) String() string {
	return fmt.Sprintf("(rows:%v, offset:%v, count:%v, inline_projection:%v)",
		tc.Rows, tc.Offset, tc.Count, tc.UsingInlineProjection)
}

func DefaultLimitTestCase() *LimitCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	tc := &LimitCase{
		Rows:                  30000,
		Offset:                10000,
		Count:                 10000,
		ChildUsedSchema:       []bool{false, true},
		UsingInlineProjection: false,
		Ctx:                   ctx,
	}
	return tc
}
