// Copyright 2026 PingCAP, Inc.
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

package join

import (
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// The parent optimizer no longer selects IndexMergeJoin from its deprecated
// hint. Exercise the retained executor's key construction without reviving it.
func TestIndexMergeJoinLogicalLookupKeys(t *testing.T) {
	ctx := mock.NewContext()
	vars := ctx.GetSessionVars()
	vars.EnableReadBillingDemo = true
	vars.StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	lookup := &IndexLookUpMergeJoin{BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 1)}
	fieldTypes := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rows := chunk.NewList(fieldTypes, 8, 8)
	chk := chunk.NewChunkWithCapacity(fieldTypes, 8)
	for _, value := range []int64{101, 101, 205, 999} {
		chk.AppendInt64(0, value)
	}
	chk.AppendNull(0)
	rows.Add(chk)
	task := &lookUpMergeJoinTask{outerResult: rows}
	for i := range 5 {
		task.outerOrderIdx = append(task.outerOrderIdx, chunk.RowPtr{RowIdx: uint32(i)})
	}
	worker := &innerMergeWorker{
		lookup: lookup, ctx: ctx,
		outerMergeCtx: OuterMergeCtx{RowTypes: fieldTypes, KeyCols: []int{0}},
		InnerMergeCtx: InnerMergeCtx{
			RowTypes: fieldTypes, KeyCols: []int{0}, KeyCollators: []collate.Collator{collate.GetCollator("binary")},
		},
	}
	keys, err := worker.constructDatumLookupKeys(task)
	require.NoError(t, err)
	require.Len(t, keys, 4)
	require.Len(t, worker.dedupDatumLookUpKeys(keys), 3)
	count, present := lookup.RuntimeStats().GetLogicalLookupKeys()
	require.True(t, present)
	require.Equal(t, int64(4), count)

	vars.StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	lookup.BaseExecutor = exec.NewBaseExecutor(ctx, expression.NewSchema(), 2)
	task.outerMatch = [][]bool{{false, false, false, false, true}}
	keys, err = worker.constructDatumLookupKeys(task)
	require.NoError(t, err)
	require.Empty(t, keys)
	count, present = lookup.RuntimeStats().GetLogicalLookupKeys()
	require.True(t, present)
	require.Zero(t, count)
}
