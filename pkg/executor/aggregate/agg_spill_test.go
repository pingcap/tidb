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
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		key := keys[rand.Intn(ndv)]
		col0Data = append(col0Data, key)
		col1Data = append(col1Data, float64(rand.Intn(10000000)))
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

func generateCMPFunc(fieldTypes []*types.FieldType) func(chunk.Row, chunk.Row) int {
	cmpFuncs := make([]chunk.CompareFunc, 0, len(fieldTypes))
	for _, colType := range fieldTypes {
		cmpFuncs = append(cmpFuncs, chunk.GetCompareFunc(colType))
	}

	cmp := func(rowI, rowJ chunk.Row) int {
		for i, cmpFunc := range cmpFuncs {
			cmp := cmpFunc(rowI, i, rowJ, i)
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}

	return cmp
}

func sortRows(rows []chunk.Row, fieldTypes []*types.FieldType) []chunk.Row {
	cmp := generateCMPFunc(fieldTypes)

	sort.Slice(rows, func(i, j int) bool {
		return cmp(rows[i], rows[j]) < 0
	})
	return rows
}

func generateResult(t *testing.T, ctx *mock.Context, dataSource *testutil.MockDataSource) []chunk.Row {
	aggExec := buildHashAggExecutor(t, ctx, dataSource)
	dataSource.PrepareChunks()
	tmpCtx := context.Background()
	resultRows := make([]chunk.Row, 0)
	aggExec.Open(tmpCtx)
	for {
		chk := exec.NewFirstChunk(aggExec)
		err := aggExec.Next(tmpCtx, chk)
		require.Equal(t, nil, err)
		if chk.NumRows() == 0 {
			break
		}

		rowNum := chk.NumRows()
		for i := 0; i < rowNum; i++ {
			resultRows = append(resultRows, chk.GetRow(i))
		}
	}
	aggExec.Close()

	require.False(t, aggExec.IsSpillTriggeredForTest())
	return sortRows(resultRows, getRetTypes())
}

func getRetTypes() []*types.FieldType {
	return []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
	}
}

func getColumns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeVarString)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
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

	var err error
	var aggFirstRow *aggregation.AggFuncDesc
	var aggSum *aggregation.AggFuncDesc
	var aggCount *aggregation.AggFuncDesc
	var aggAvg *aggregation.AggFuncDesc
	var aggMin *aggregation.AggFuncDesc
	var aggMax *aggregation.AggFuncDesc
	aggFirstRow, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{childCols[0]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggSum, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggCount, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggAvg, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncAvg, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggMin, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncMin, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggMax, err = aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{childCols[1]}, false)
	if err != nil {
		t.Fatal(err)
	}

	aggFuncs := []*aggregation.AggFuncDesc{aggFirstRow, aggSum, aggCount, aggAvg, aggMin, aggMax}

	aggExec := &aggregate.HashAggExec{
		BaseExecutor:     exec.NewBaseExecutor(ctx, schema, 0, child),
		Sc:               ctx.GetSessionVars().StmtCtx,
		PartialAggFuncs:  make([]aggfuncs.AggFunc, 0, len(aggFuncs)),
		FinalAggFuncs:    make([]aggfuncs.AggFunc, 0, len(aggFuncs)),
		GroupByItems:     groupItems,
		IsUnparallelExec: false,
	}

	partialOrdinal := 0
	for i, aggDesc := range aggFuncs {
		ordinal := []int{partialOrdinal}
		partialOrdinal++
		if aggDesc.Name == ast.AggFuncAvg {
			ordinal = append(ordinal, partialOrdinal+1)
			partialOrdinal++
		}
		partialAggDesc, finalDesc := aggDesc.Split(ordinal)
		partialAggFunc := aggfuncs.Build(ctx.GetExprCtx(), partialAggDesc, i)
		finalAggFunc := aggfuncs.Build(ctx.GetExprCtx(), finalDesc, i)
		aggExec.PartialAggFuncs = append(aggExec.PartialAggFuncs, partialAggFunc)
		aggExec.FinalAggFuncs = append(aggExec.FinalAggFuncs, finalAggFunc)
	}

	aggExec.SetChildren(0, child)
	return aggExec
}

func initCtx(ctx *mock.Context, newRootExceedAction *testutil.MockActionOnExceed, hardLimitBytesNum int64, chkSize int) {
	ctx.GetSessionVars().InitChunkSize = chkSize
	ctx.GetSessionVars().MaxChunkSize = chkSize
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, hardLimitBytesNum)
	ctx.GetSessionVars().TrackAggregateMemoryUsage = true
	ctx.GetSessionVars().EnableParallelHashaggSpill = true
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	ctx.GetSessionVars().MemTracker.SetActionOnExceed(newRootExceedAction)
}

func checkResult(expectResult []chunk.Row, actualResult []chunk.Row, retTypes []*types.FieldType) bool {
	if len(expectResult) != len(actualResult) {
		return false
	}

	rowNum := len(expectResult)
	for i := 0; i < rowNum; i++ {
		if expectResult[i].ToString(retTypes) != actualResult[i].ToString(retTypes) {
			return false
		}
	}

	return true
}

func executeCorrecResultTest(t *testing.T, ctx *mock.Context, aggExec *aggregate.HashAggExec, dataSource *testutil.MockDataSource, expectResult []chunk.Row) {
	if aggExec == nil {
		aggExec = buildHashAggExecutor(t, ctx, dataSource)
	}
	dataSource.PrepareChunks()
	tmpCtx := context.Background()
	resultRows := make([]chunk.Row, 0)
	aggExec.Open(tmpCtx)
	for {
		chk := exec.NewFirstChunk(aggExec)
		err := aggExec.Next(tmpCtx, chk)
		require.Equal(t, nil, err)
		if chk.NumRows() == 0 {
			break
		}

		rowNum := chk.NumRows()
		for i := 0; i < rowNum; i++ {
			resultRows = append(resultRows, chk.GetRow(i))
		}
	}
	aggExec.Close()

	require.True(t, aggExec.IsSpillTriggeredForTest())
	retTypes := getRetTypes()
	resultRows = sortRows(resultRows, retTypes)
	require.True(t, checkResult(expectResult, resultRows, retTypes))
}

func fallBackActionTest(t *testing.T) {
	newRootExceedAction := new(testutil.MockActionOnExceed)
	hardLimitBytesNum := int64(6000000)

	ctx := mock.NewContext()
	initCtx(ctx, newRootExceedAction, hardLimitBytesNum, 4096)

	// Consume lots of memory in advance to help to trigger fallback action.
	ctx.GetSessionVars().MemTracker.Consume(int64(float64(hardLimitBytesNum) * 0.799999))

	rowNum := 10000 + rand.Intn(10000)
	ndv := 5000 + rand.Intn(5000)
	col1, col2 := generateData(rowNum, ndv)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col1, col2)

	aggExec := buildHashAggExecutor(t, ctx, dataSource)
	dataSource.PrepareChunks()
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
	require.Less(t, 0, newRootExceedAction.GetTriggeredNum())
}

func randomFailTest(t *testing.T, ctx *mock.Context, aggExec *aggregate.HashAggExec, dataSource *testutil.MockDataSource) {
	if aggExec == nil {
		aggExec = buildHashAggExecutor(t, ctx, dataSource)
	}
	dataSource.PrepareChunks()
	tmpCtx := context.Background()
	chk := exec.NewFirstChunk(aggExec)
	aggExec.Open(tmpCtx)

	goRoutineWaiter := sync.WaitGroup{}
	goRoutineWaiter.Add(1)
	defer goRoutineWaiter.Wait()

	once := sync.Once{}

	go func() {
		time.Sleep(time.Duration(rand.Int31n(300)) * time.Millisecond)
		once.Do(func() {
			aggExec.Close()
		})
		goRoutineWaiter.Done()
	}()

	for {
		err := aggExec.Next(tmpCtx, chk)
		if err != nil {
			once.Do(func() {
				err = aggExec.Close()
				require.Equal(t, nil, err)
			})
			break
		}
		if chk.NumRows() == 0 {
			break
		}
		chk.Reset()
	}
	once.Do(func() {
		aggExec.Close()
	})
}

// sql: select col0, sum(col1), count(col1), avg(col1), min(col1), max(col1) from t group by t.col0;
func TestGetCorrectResult(t *testing.T) {
	newRootExceedAction := new(testutil.MockActionOnExceed)

	ctx := mock.NewContext()
	initCtx(ctx, newRootExceedAction, -1, 1024)

	rowNum := 100000
	ndv := 50000
	col0, col1 := generateData(rowNum, ndv)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col0, col1)
	result := generateResult(t, ctx, dataSource)

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/slowSomePartialWorkers", `return(true)`)
	require.NoError(t, err)
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/aggregate/slowSomePartialWorkers"))

	finished := atomic.Bool{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tracker := ctx.GetSessionVars().MemTracker
		for {
			if finished.Load() {
				break
			}
			// Mock consuming in another goroutine, so that we can test potential data race.
			tracker.Consume(1)
			time.Sleep(1 * time.Millisecond)
		}
		wg.Done()
	}()

	hardLimitBytesNum := int64(6000000)
	initCtx(ctx, newRootExceedAction, hardLimitBytesNum, 256)

	aggExec := buildHashAggExecutor(t, ctx, dataSource)
	for i := 0; i < 5; i++ {
		executeCorrecResultTest(t, ctx, nil, dataSource, result)
		executeCorrecResultTest(t, ctx, aggExec, dataSource, result)
	}

	finished.Store(true)
	wg.Wait()
}

func TestFallBackAction(t *testing.T) {
	for i := 0; i < 50; i++ {
		fallBackActionTest(t)
	}
}

func TestRandomFail(t *testing.T) {
	newRootExceedAction := new(testutil.MockActionOnExceed)
	hardLimitBytesNum := int64(5000000)

	ctx := mock.NewContext()
	initCtx(ctx, newRootExceedAction, hardLimitBytesNum, 32)

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest")
	failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError")
	rowNum := 100000 + rand.Intn(100000)
	ndv := 50000 + rand.Intn(50000)
	col1, col2 := generateData(rowNum, ndv)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col1, col2)

	finishChan := atomic.Bool{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tracker := ctx.GetSessionVars().MemTracker
		for {
			if finishChan.Load() {
				break
			}
			// Mock consuming in another goroutine, so that we can test potential data race.
			tracker.Consume(1)
			time.Sleep(3 * time.Millisecond)
		}
		wg.Done()
	}()

	// Test is successful when all sqls are not hung
	aggExec := buildHashAggExecutor(t, ctx, dataSource)
	for i := 0; i < 30; i++ {
		randomFailTest(t, ctx, nil, dataSource)
		randomFailTest(t, ctx, aggExec, dataSource)
	}

	finishChan.Store(true)
	wg.Wait()
}
