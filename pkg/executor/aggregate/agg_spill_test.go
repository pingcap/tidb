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

package aggregate_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// Chunk schema in this test file: | column0: string | column1: float64 |

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type resultsContainer struct {
	rows []chunk.Row
}

func (r *resultsContainer) add(chk *chunk.Chunk) {
	iter := chunk.NewIterator4Chunk(chk.CopyConstruct())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		r.rows = append(r.rows, row)
	}
}

func (r *resultsContainer) check(expectRes map[string]float64) bool {
	if len(r.rows) != len(expectRes) {
		return false
	}

	cols := getColumns()
	retFields := []*types.FieldType{cols[0].RetType, cols[1].RetType}
	for _, row := range r.rows {
		key := ""
		for i, field := range retFields {
			d := row.GetDatum(i, field)
			resStr, err := d.ToString()
			if err != nil {
				panic("Fail to convert to string")
			}

			if i == 0 {
				key = resStr
			} else {
				expectVal, ok := expectRes[key]
				if !ok {
					return false
				}
				if resStr != strconv.Itoa(int(expectVal)) {
					return false
				}
			}
		}
	}
	return true
}

func getRandString() string {
	b := make([]byte, 5)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func generateData(rowNum int, ndv int) ([]string, []float64) {
	keys := make([]string, 0)
	for i := 0; i < ndv; i++ {
		keys = append(keys, getRandString())
	}

	col0Data := make([]string, 0)
	col1Data := make([]float64, 0)

	// Generate data
	for i := 0; i < rowNum; i++ {
		key := keys[i%ndv]
		col0Data = append(col0Data, key)
		col1Data = append(col1Data, 1) // Always 1
	}

	// Shuffle data
	rand.Shuffle(rowNum, func(i, j int) {
		col0Data[i], col0Data[j] = col0Data[j], col0Data[i]
		// There is no need to shuffle col2Data as all of it's values are 1.
	})
	return col0Data, col1Data
}

func buildMockDataSource(opt testutil.MockDataSourceParameters, col0Data []string, col1Data []float64) *testutil.MockDataSource {
	baseExec := exec.NewBaseExecutor(opt.Ctx, opt.DataSchema, 0)
	mockDatasource := &testutil.MockDataSource{
		BaseExecutor: baseExec,
		ChunkPtr:     0,
		P:            opt,
		GenData:      nil,
		Chunks:       nil}

	maxChunkSize := mockDatasource.MaxChunkSize()
	rowNum := len(col0Data)
	mockDatasource.GenData = make([]*chunk.Chunk, (rowNum+maxChunkSize-1)/maxChunkSize)
	for i := range mockDatasource.GenData {
		mockDatasource.GenData[i] = chunk.NewChunkWithCapacity(exec.RetTypes(mockDatasource), maxChunkSize)
	}

	for i := 0; i < rowNum; i++ {
		chkIdx := i / maxChunkSize
		mockDatasource.GenData[chkIdx].AppendString(0, col0Data[i])
		mockDatasource.GenData[chkIdx].AppendFloat64(1, col1Data[i])
	}

	return mockDatasource
}

func generateResult(col0 []string, col1 []float64) map[string]float64 {
	result := make(map[string]float64, 0)
	length := len(col0)

	for i := 0; i < length; i++ {
		_, ok := result[col0[i]]
		if ok {
			result[col0[i]]++
		} else {
			result[col0[i]] = 1
		}
	}
	return result
}

func getColumns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeVarString)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
	}
}

func getSchema() *expression.Schema {
	return expression.NewSchema(getColumns()...)
}

func getMockDataSourceParameters(ctx sessionctx.Context) testutil.MockDataSourceParameters {
	return testutil.MockDataSourceParameters{
		DataSchema: getSchema(),
		Ctx:        ctx,
	}
}

func buildHashAggExecutor(t *testing.T, ctx sessionctx.Context, child exec.Executor) *aggregate.HashAggExec {
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", 5)); err != nil {
		t.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", 5)); err != nil {
		t.Fatal(err)
	}

	childCols := getColumns()
	schema := expression.NewSchema(childCols...)
	groupItems := []expression.Expression{childCols[0]}

	aggFirstRow, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{childCols[0]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggFunc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggFuncs := []*aggregation.AggFuncDesc{aggFirstRow, aggFunc}

	aggExec := &aggregate.HashAggExec{
		BaseExecutor:     exec.NewBaseExecutor(ctx, schema, 0, child),
		Sc:               ctx.GetSessionVars().StmtCtx,
		PartialAggFuncs:  make([]aggfuncs.AggFunc, 0, len(aggFuncs)),
		GroupByItems:     groupItems,
		IsUnparallelExec: false,
	}

	partialOrdinal := 0
	for i, aggDesc := range aggFuncs {
		ordinal := []int{partialOrdinal}
		partialOrdinal++
		partialAggDesc, finalDesc := aggDesc.Split(ordinal)
		partialAggFunc := aggfuncs.Build(ctx, partialAggDesc, i)
		finalAggFunc := aggfuncs.Build(ctx, finalDesc, i)
		aggExec.PartialAggFuncs = append(aggExec.PartialAggFuncs, partialAggFunc)
		aggExec.FinalAggFuncs = append(aggExec.FinalAggFuncs, finalAggFunc)
	}

	aggExec.SetChildren(0, child)
	return aggExec
}

func TestGetCorrectResult(t *testing.T) {
	hardLimitBytesNum := int64(1000000)

	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, hardLimitBytesNum)
	ctx.GetSessionVars().TrackAggregateMemoryUsage = true
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(false)`)
	rowNum := rand.Intn(300000)
	ndv := rand.Intn(100000)
	col1, col2 := generateData(rowNum, ndv)
	result := generateResult(col1, col2)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col1, col2)
	dataSource.PrepareChunks()
	aggExec := buildHashAggExecutor(t, ctx, dataSource)

	tmpCtx := context.Background()
	chk := exec.NewFirstChunk(aggExec)
	resContainer := resultsContainer{}
	aggExec.Open(tmpCtx)

	for {
		aggExec.Next(tmpCtx, chk)
		if chk.NumRows() == 0 {
			break
		}
		resContainer.add(chk)
		chk.Reset()
	}
	aggExec.Close()

	require.True(t, aggExec.IsSpillTriggeredForTest())
	require.True(t, resContainer.check(result))
}

func TestRandomFail(t *testing.T) {
	hardLimitBytesNum := int64(1000000)

	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 500
	ctx.GetSessionVars().MaxChunkSize = 500
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, hardLimitBytesNum)
	ctx.GetSessionVars().TrackAggregateMemoryUsage = true
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(true)`)
	rowNum := rand.Intn(300000)
	ndv := rand.Intn(100000)
	col1, col2 := generateData(rowNum, ndv)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col1, col2)

	// Test is successful when all sqls are not hung
	for i := 0; i < 200; i++ {
		dataSource.PrepareChunks()
		aggExec := buildHashAggExecutor(t, ctx, dataSource)
		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(aggExec)
		aggExec.Open(tmpCtx)
		for {
			aggExec.Next(tmpCtx, chk)
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
		aggExec.Close()
	}
}
