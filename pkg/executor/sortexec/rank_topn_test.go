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

package sortexec_test

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// RankTopNSortCase is the sort case for RankTopN tests with string prefix key
type RankTopNSortCase struct {
	ctx                     sessionctx.Context
	rowCount                int
	cols                    []*expression.Column
	orderByIdx              []int
	prefixKeyFieldTypes     []expression.Expression
	prefixKeyFieldCollators []collate.Collator
	prefixKeyColIdxs        []int
	prefixKeyCharCounts     []int
}

func buildRankTopNDataSource(rankTopNCase *RankTopNSortCase, schema *expression.Schema) *testutil.MockDataSource {
	// Generate prefix-ordered data: rows are ordered by the first column (prefix key)
	opt := testutil.MockDataSourceParameters{
		DataSchema: schema,
		Rows:       rankTopNCase.rowCount,
		Ctx:        rankTopNCase.ctx,
		Ndvs:       []int{-2, 0}, // -2 means use provided data for column 0
		Datums:     make([][]any, 2),
	}

	// Generate prefix key data: strings that are pre-ordered.
	// Each prefix group has multiple rows; the group size is variable and each group
	// size is randomized in [1, 200].
	prefixData := make([]any, rankTopNCase.rowCount)
	groupIdx := 0
	buff := make([]byte, 5)
	for i := 0; i < rankTopNCase.rowCount; {
		groupSize := rand.Intn(200) + 1
		prefix := fmt.Sprintf("prefix_%05d", groupIdx)
		for j := 0; j < groupSize && i < rankTopNCase.rowCount; j++ {
			_, err := crand.Read(buff)
			if err != nil {
				panic("rand.Read returns error")
			}
			prefixData[i] = fmt.Sprintf("%s%s", prefix, base64.RawURLEncoding.EncodeToString(buff))
			i++
		}
		groupIdx++
	}
	opt.Datums[0] = prefixData

	return testutil.BuildMockDataSource(opt)
}

func buildRankTopNExec(rankTopNCase *RankTopNSortCase, dataSource *testutil.MockDataSource, offset uint64, count uint64) *sortexec.TopNExec {
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(rankTopNCase.ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(rankTopNCase.orderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}

	for _, idx := range rankTopNCase.orderByIdx {
		sortExec.ByItems = append(sortExec.ByItems, &plannerutil.ByItems{Expr: rankTopNCase.cols[idx]})
	}

	topNexec := &sortexec.TopNExec{
		SortExec:    sortExec,
		Limit:       &physicalop.PhysicalLimit{Offset: offset, Count: count},
		Concurrency: 5,
	}

	topNexec.SetPrefixKeyFieldsForTest(rankTopNCase.prefixKeyFieldTypes, rankTopNCase.prefixKeyColIdxs, rankTopNCase.prefixKeyCharCounts, rankTopNCase.prefixKeyFieldCollators)
	return topNexec
}

func checkRankTopNCorrectness(schema *expression.Schema, exe *sortexec.TopNExec, dataSource *testutil.MockDataSource, resultChunks []*chunk.Chunk, offset uint64, count uint64) bool {
	keyColumns, keyCmpFuncs, byItemsDesc := exe.GetSortMetaForTest()
	checker := newResultChecker(schema, keyColumns, keyCmpFuncs, byItemsDesc, dataSource.GenData)
	return checker.check(resultChunks, int64(offset), int64(count))
}

func rankTopNBasicCase(t *testing.T, sortCase *RankTopNSortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	exe := buildRankTopNExec(sortCase, dataSource, offset, count)
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)
	err := exe.Close()
	require.NoError(t, err)
	require.True(t, checkRankTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

func TestRankTopN(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	rankTopNCase := &RankTopNSortCase{
		rowCount:   rand.Intn(9000) + 1000,
		ctx:        ctx,
		orderByIdx: []int{0}, // Order by prefix key column and second column
		prefixKeyFieldTypes: []expression.Expression{
			&expression.Column{
				RetType: types.NewFieldType(mysql.TypeVarString),
				Index:   0,
			}},
		prefixKeyFieldCollators: []collate.Collator{collate.GetCollator("utf8mb4_bin")},
		prefixKeyColIdxs:        []int{0},
		prefixKeyCharCounts:     []int{12},
		cols: []*expression.Column{
			{Index: 0, RetType: types.NewFieldType(mysql.TypeVarString)},
			{Index: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}},
	}

	schema := expression.NewSchema(rankTopNCase.cols...)
	dataSource := buildRankTopNDataSource(rankTopNCase, schema)

	randNum := rand.Intn(100)
	var offset uint64
	var count uint64
	if randNum < 10 {
		offset = 0
	} else {
		offset = uint64(rand.Intn(1000))
	}

	if randNum < 5 {
		count = 0
	} else {
		count = uint64(rand.Intn(10000))
	}
	rankTopNBasicCase(t, rankTopNCase, schema, dataSource, offset, count)
}
