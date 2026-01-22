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
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// rankTopNCase is the sort case for RankTopN tests with string prefix key
type rankTopNCase struct {
	ctx                         sessionctx.Context
	rowCount                    int
	cols                        []*expression.Column
	orderByIdx                  []int
	truncateKeyExprs            []expression.Expression
	truncateKeyColIdxs          []int
	truncateKeyPrefixCharCounts []int
}

func buildRankTopNDataSource(rankTopNCase *rankTopNCase, schema *expression.Schema) *testutil.MockDataSource {
	// Generate prefix-ordered data: rows are ordered by the first column (prefix key)
	opt := testutil.MockDataSourceParameters{
		DataSchema: schema,
		Rows:       rankTopNCase.rowCount,
		Ctx:        rankTopNCase.ctx,
		Ndvs:       make([]int, len(rankTopNCase.truncateKeyExprs)+1),
		Datums:     make([][]any, len(rankTopNCase.truncateKeyExprs)+1),
	}

	for i := range rankTopNCase.truncateKeyExprs {
		// -2 means use provided data
		opt.Ndvs[i] = -2
	}

	outputs := make([]string, rankTopNCase.rowCount)

	opt.Ndvs[len(rankTopNCase.truncateKeyExprs)] = 0
	// Generate prefix key data: strings that are pre-ordered.
	// Each prefix group has multiple rows; the group size is variable and each group
	// size is randomized in [1, 200].
	for i, ft := range rankTopNCase.truncateKeyExprs {
		prefixData := make([]any, rankTopNCase.rowCount)
		groupIdx := 0
		var bufLen int
		if rankTopNCase.truncateKeyPrefixCharCounts[i] == -1 {
			bufLen = 0
		} else {
			bufLen = 5
		}
		buf := make([]byte, bufLen)
		isCI := false
		switch ft.GetType(nil).GetCollate() {
		case "utf8mb4_bin":
		case "utf8mb4_general_ci":
			isCI = true
		default:
			panic(fmt.Sprintf("Unconsidered collator %s", ft.GetType(nil).GetCollate()))
		}

		for i := 0; i < rankTopNCase.rowCount; {
			// groupSize := rand.Intn(10) + 1  // TODO(x)
			groupSize := rand.Intn(200) + 1
			for j := 0; j < groupSize && i < rankTopNCase.rowCount; j++ {
				_, err := crand.Read(buf)
				if err != nil {
					panic("rand.Read returns error")
				}
				if isCI && rand.Intn(10) < 5 {
					prefixData[i] = fmt.Sprintf("PREFIX前缀_%05d_%s", groupIdx, base64.RawURLEncoding.EncodeToString(buf))
				} else {
					prefixData[i] = fmt.Sprintf("prefix前缀_%05d_%s", groupIdx, base64.RawURLEncoding.EncodeToString(buf))
				}
				outputs[i] = fmt.Sprintf("%s, %s", outputs[i], prefixData[i])
				i++
			}
			groupIdx++
		}
		opt.Datums[i] = prefixData
	}

	// TODO(x) remove debug info
	// fmt.Println("---------- Origin Data ----------")
	// for _, output := range outputs {
	// 	fmt.Println(output)
	// }

	return testutil.BuildMockDataSource(opt)
}

func buildRankTopNExec(rankTopNCase *rankTopNCase, dataSource *testutil.MockDataSource, offset uint64, count uint64) *sortexec.TopNExec {
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(rankTopNCase.ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(rankTopNCase.orderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}

	for _, idx := range rankTopNCase.orderByIdx {
		sortExec.ByItems = append(sortExec.ByItems, &plannerutil.ByItems{Expr: rankTopNCase.cols[idx]})
	}
	// fmt.Printf("xzxdebug len(sortExec.ByItems): %d\n", len(sortExec.ByItems)) // TODO(x) remove debug info

	topNexec := &sortexec.TopNExec{
		SortExec:    sortExec,
		Limit:       &physicalop.PhysicalLimit{Offset: offset, Count: count},
		Concurrency: 5,
	}

	topNexec.SetTruncateKeyMetasForTest(rankTopNCase.truncateKeyExprs, rankTopNCase.truncateKeyColIdxs, rankTopNCase.truncateKeyPrefixCharCounts)
	return topNexec
}

func checkRankTopNCorrectness(schema *expression.Schema, exe *sortexec.TopNExec, dataSource *testutil.MockDataSource, resultChunks []*chunk.Chunk, offset uint64, count uint64) bool {
	keyColumns, keyCmpFuncs, byItemsDesc := exe.GetSortMetaForTest()
	checker := newResultChecker(schema, keyColumns, keyCmpFuncs, byItemsDesc, dataSource.GenData)
	return checker.check(resultChunks, int64(offset), int64(count))
}

func rankTopNBasicCase(t *testing.T, sortCase *rankTopNCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	exe := buildRankTopNExec(sortCase, dataSource, offset, count)
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)
	err := exe.Close()
	require.NoError(t, err)
	require.True(t, checkRankTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

func TestRankTopN(t *testing.T) {
	collationNames := []string{"utf8mb4_bin", "utf8mb4_general_ci"}
	ctx := mock.NewContext()
	rankTopNCases := make([]*rankTopNCase, 0)
	for _, collationName := range collationNames {
		truncateKeyField := types.NewFieldType(mysql.TypeVarString)
		truncateKeyField.SetCharset("utf8mb4")
		truncateKeyField.SetCollate(collationName)
		rankTopNCases = append(rankTopNCases, &rankTopNCase{
			rowCount:   rand.Intn(9000) + 1000,
			ctx:        ctx,
			orderByIdx: []int{0}, // Order by truncate key column
			truncateKeyExprs: []expression.Expression{
				&expression.Column{
					RetType: truncateKeyField,
					Index:   0,
				}},
			truncateKeyColIdxs:          []int{0},
			truncateKeyPrefixCharCounts: []int{14},
			cols: []*expression.Column{
				{Index: 0, RetType: truncateKeyField},
				{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}},
		})
		rankTopNCases = append(rankTopNCases, &rankTopNCase{
			rowCount:   rand.Intn(9000) + 1000,
			ctx:        ctx,
			orderByIdx: []int{0, 1}, // Order by truncate key column
			truncateKeyExprs: []expression.Expression{
				&expression.Column{
					RetType: truncateKeyField,
					Index:   0,
				},
				&expression.Column{
					RetType: truncateKeyField,
					Index:   1,
				}},
			truncateKeyColIdxs:          []int{0, 1},
			truncateKeyPrefixCharCounts: []int{-1, 12},
			cols: []*expression.Column{
				{Index: 0, RetType: truncateKeyField},
				{Index: 1, RetType: truncateKeyField},
				{Index: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}},
		})
	}

	for _, testCase := range rankTopNCases {
		ctx.GetSessionVars().InitChunkSize = 32
		ctx.GetSessionVars().MaxChunkSize = 32
		ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
		ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

		schema := expression.NewSchema(testCase.cols...)
		dataSource := buildRankTopNDataSource(testCase, schema)

		randNum := rand.Intn(100)
		var offset uint64
		var count uint64
		if randNum < 10 {
			offset = 0
		} else {
			offset = uint64(rand.Intn(3000))
		}

		if randNum < 5 {
			count = 0
		} else {
			count = uint64(rand.Intn(10000))
		}
		rankTopNBasicCase(t, testCase, schema, dataSource, offset, count)
	}
}
