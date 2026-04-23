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
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sort"
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
	truncateKeyPrefixCharCounts []int
}

// resultChecker validates TopN output rows against the sorted source rows.
type resultChecker struct {
	schema      *expression.Schema
	keyColumns  []int
	keyCmpFuncs []chunk.CompareFunc
	byItemsDesc []bool
	savedChunks []*chunk.Chunk
	rowPtrs     []chunk.RowPtr
}

func newResultChecker(schema *expression.Schema, keyColumns []int, keyCmpFuncs []chunk.CompareFunc, byItemsDesc []bool, savedChunks []*chunk.Chunk) *resultChecker {
	checker := resultChecker{}
	checker.schema = schema
	checker.keyColumns = keyColumns
	checker.keyCmpFuncs = keyCmpFuncs
	checker.byItemsDesc = byItemsDesc
	checker.savedChunks = savedChunks
	return &checker
}

func (r *resultChecker) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range r.keyColumns {
		cmpFunc := r.keyCmpFuncs[i]
		if cmpFunc != nil {
			cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
			if r.byItemsDesc[i] {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			} else if cmp > 0 {
				return false
			}
		}
	}
	return false
}

func (r *resultChecker) keyColumnsLess(i, j int) bool {
	rowI := r.savedChunks[r.rowPtrs[i].ChkIdx].GetRow(int(r.rowPtrs[i].RowIdx))
	rowJ := r.savedChunks[r.rowPtrs[j].ChkIdx].GetRow(int(r.rowPtrs[j].RowIdx))
	return r.lessRow(rowI, rowJ)
}

func (r *resultChecker) getSavedChunksRowNumber() int {
	rowNum := 0
	for _, chk := range r.savedChunks {
		rowNum += chk.NumRows()
	}
	return rowNum
}

func (r *resultChecker) initRowPtrs() {
	r.rowPtrs = make([]chunk.RowPtr, 0, r.getSavedChunksRowNumber())
	chunkNum := len(r.savedChunks)
	for chkIdx := range chunkNum {
		chk := r.savedChunks[chkIdx]
		for rowIdx := range chk.NumRows() {
			r.rowPtrs = append(r.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (r *resultChecker) check(resultChunks []*chunk.Chunk, offset int64, count int64) bool {
	if r.rowPtrs == nil {
		r.initRowPtrs()

		sort.Slice(r.rowPtrs, r.keyColumnsLess)
		if offset < 0 {
			offset = 0
		}
		if count < 0 {
			count = int64(len(r.rowPtrs)) - offset
		}

		start := min(int64(len(r.rowPtrs)), offset)
		end := min(int64(len(r.rowPtrs)), offset+count)
		r.rowPtrs = r.rowPtrs[start:end]
	}

	cursor := 0
	fieldTypes := make([]*types.FieldType, 0, len(r.schema.Columns))
	for _, col := range r.schema.Columns {
		fieldTypes = append(fieldTypes, col.GetType(nil))
	}

	totalResRowNum := 0
	for _, chk := range resultChunks {
		totalResRowNum += chk.NumRows()
	}
	if totalResRowNum != len(r.rowPtrs) {
		return false
	}

	for _, chk := range resultChunks {
		rowNum := chk.NumRows()
		for i := range rowNum {
			resRow := chk.GetRow(i)
			res := resRow.ToString(fieldTypes)

			expectRow := r.savedChunks[r.rowPtrs[cursor].ChkIdx].GetRow(int(r.rowPtrs[cursor].RowIdx))
			expect := expectRow.ToString(fieldTypes)
			if res != expect {
				return false
			}
			cursor++
		}
	}

	return true
}

func executeTopNExecutor(t *testing.T, exe *sortexec.TopNExec) []*chunk.Chunk {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)

	resultChunks := make([]*chunk.Chunk, 0)
	chk := exec.NewFirstChunk(exe)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
		resultChunks = append(resultChunks, chk.CopyConstruct())
	}
	return resultChunks
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

	topNexec := &sortexec.TopNExec{
		SortExec:    sortExec,
		Limit:       &physicalop.PhysicalLimit{Offset: offset, Count: count},
		Concurrency: 5,
	}

	topNexec.SetTruncateKeyMetasForTest(rankTopNCase.truncateKeyExprs, rankTopNCase.truncateKeyPrefixCharCounts)
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
