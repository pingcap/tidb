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

package aggregate

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
)

// Chunk schema in this test file: | column0: string | column1: int |

func checkResults(actualRes [][]interface{}, expectedRes map[string]string) bool {
	if len(actualRes) != len(expectedRes) {
		return false
	}

	var key string
	var expectVal string
	var actualVal string
	var ok bool
	for _, row := range actualRes {
		if len(row) != 2 {
			return false
		}

		key, ok = row[0].(string)
		if !ok {
			return false
		}

		expectVal, ok = expectedRes[key]
		if !ok {
			return false
		}

		actualVal, ok = row[1].(string)
		if !ok {
			return false
		}

		if expectVal != actualVal {
			return false
		}
	}
	return true
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func getRandString() string {
	strLen := rand.Intn(50) + 1 // At least 1
	b := make([]byte, strLen)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func generateData(rowNum int, ndv int) ([]string, []int64) {
	keys := make([]string, 0)
	for i := 0; i < ndv; i++ {
		keys = append(keys, getRandString())
	}

	col1Data := make([]string, 0)
	col2Data := make([]int64, 0)

	// Generate data
	for i := 0; i < rowNum; i++ {
		key := keys[i%ndv]
		col1Data = append(col1Data, key)
		col2Data = append(col2Data, 1) // Always 1
	}

	// Shuffle data
	rand.Shuffle(rowNum, func(i, j int) {
		col1Data[i], col1Data[j] = col1Data[j], col1Data[i]
		// There is no need to shuffle col2Data as all of it's values are 1.
	})
	return col1Data, col2Data
}

func buildMockDataSource(opt testutil.MockDataSourceParameters, col1Data []string, col2Data []int64) *testutil.MockDataSource {
	baseExec := exec.NewBaseExecutor(opt.Ctx, opt.DataSchema, 0)
	mockDatasource := &testutil.MockDataSource{
		BaseExecutor: baseExec,
		ChunkPtr:     0,
		P:            opt,
		GenData:      nil,
		Chunks:       nil}

	maxChunkSize := mockDatasource.MaxChunkSize()
	rowNum := len(col1Data)
	mockDatasource.GenData = make([]*chunk.Chunk, (rowNum+maxChunkSize-1)/maxChunkSize)
	for i := range mockDatasource.GenData {
		mockDatasource.GenData[i] = chunk.NewChunkWithCapacity(exec.RetTypes(mockDatasource), maxChunkSize)
	}

	for i := 0; i < rowNum; i++ {
		chkIdx := i / maxChunkSize
		mockDatasource.GenData[chkIdx].AppendString(0, col1Data[i])
		mockDatasource.GenData[chkIdx].AppendInt64(1, col2Data[i])
	}

	return mockDatasource
}

func generateResult(col1 []string, col2 []int64) map[string]int64 {
	result := make(map[string]int64, 0)
	length := len(col1)

	for i := 0; i < length; i++ {
		_, ok := result[col1[i]]
		if ok {
			result[col1[i]]++
		} else {
			result[col1[i]] = 1
		}
	}
	return result
}

func getColumns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeVarString)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
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

func buildHashAggExecutor(t *testing.T, ctx sessionctx.Context, child exec.Executor) *HashAggExec {
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

	aggExec := &HashAggExec{
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

type resultsContainer struct {
	rows []chunk.Row
}

func (r *resultsContainer) Add(chk *chunk.Chunk) {
	iter := chunk.NewIterator4Chunk(chk.CopyConstruct())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		r.rows = append(r.rows, row)
	}
}

func (r *resultsContainer) check(expectRes map[string]int64) bool {
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

func TestGetCorrectResult(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, -1)
	ctx.GetSessionVars().TrackAggregateMemoryUsage = true
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(false)`)
	rowNum := rand.Intn(3000) + 3000
	ndv := rand.Intn(50) + 50
	col1, col2 := generateData(rowNum, ndv)
	// result := generateResult(col1, col2)
	opt := getMockDataSourceParameters(ctx)
	dataSource := buildMockDataSource(opt, col1, col2)
	aggExec := buildHashAggExecutor(t, ctx, dataSource)

	tmpCtx := context.Background()
	aggExec.Open(tmpCtx)
	
}

func TestGetCorrectResultDeprecated(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(false)`)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.test_spill_bin;")
	tk.MustExec("create table test.test_spill_bin(k varchar(30), v int);")
	tk.MustExec("insert into test.test_spill_bin (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")
	tk.MustExec("drop table if exists test.test_spill_ci;")
	tk.MustExec("create table test.test_spill_ci(k varchar(30), v int) DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("insert into test.test_spill_ci (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")
	hardLimitBytesNum := 1000000
	tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%d;", hardLimitBytesNum))
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/triggerSpill", fmt.Sprintf("return(%d)", hardLimitBytesNum))

	// bin collation
	binCollationResult := make(map[string]string)
	binCollationResult["CC"] = "1"
	binCollationResult["aA"] = "2"
	binCollationResult["DD"] = "1"
	binCollationResult["Bb"] = "2"
	binCollationResult["ee"] = "1"
	binCollationResult["bb"] = "2"
	binCollationResult["BB"] = "2"
	binCollationResult["bB"] = "2"
	binCollationResult["Cc"] = "1"
	binCollationResult["Dd"] = "1"
	binCollationResult["dD"] = "1"
	binCollationResult["Aa"] = "2"
	binCollationResult["AA"] = "2"
	binCollationResult["aa"] = "2"
	binCollationResult["cC"] = "1"
	binCollationResult["dd"] = "1"
	binCollationResult["cc"] = "1"
	log.Info("xzxdebugtt")
	res := tk.MustQuery("select k, sum(v) from test_spill_bin group by k;")
	log.Info("xzxdebugtt")
	tk.RequireEqual(true, checkResults(res.Rows(), binCollationResult))

	// ci collation
	ciCollationResult := make(map[string]string)
	ciCollationResult["aa"] = "8"
	ciCollationResult["bb"] = "8"
	ciCollationResult["cc"] = "4"
	ciCollationResult["dd"] = "4"
	ciCollationResult["ee"] = "1"
	res = tk.MustQuery("select k, sum(v) from test_spill_ci group by k;")
	tk.RequireEqual(true, checkResults(res.Rows(), ciCollationResult))
}

// TODO maybe add more random fail?
func TestRandomFailDeprecated(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.test_spill_random_fail;")
	tk.MustExec("create table test.test_spill_random_fail(k varchar(30), v int);")
	tk.MustExec("insert into test.test_spill_random_fail (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")

	hardLimitBytesNum := 1000000
	tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%d;", hardLimitBytesNum))
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/triggerSpill", fmt.Sprintf("return(%d)", hardLimitBytesNum))
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(true)`)

	// Test is successful when all sqls are not hung
	for i := 0; i < 50; i++ {
		tk.ExecuteAndResultSetToResultWithCtx("select k, sum(v) from test_spill_random_fail group by k;")
	}
}
