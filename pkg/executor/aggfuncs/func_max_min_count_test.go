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

package aggfuncs_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func evalMaxMinCount(t *testing.T, funcName string, ft *types.FieldType, datums []types.Datum) int64 {
	ctx := mock.NewContext()
	args := []expression.Expression{&expression.Column{RetType: ft, Index: 0}}
	desc, err := aggregation.NewAggFuncDesc(ctx, funcName, args, false)
	require.NoError(t, err)

	aggFunc := aggfuncs.Build(ctx, desc, 0)
	require.NotNil(t, aggFunc)
	pr, _ := aggFunc.AllocPartialResult()

	src := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, len(datums))
	for i := range datums {
		d := datums[i]
		src.AppendDatum(0, &d)
	}
	iter := chunk.NewIterator4Chunk(src)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = aggFunc.UpdatePartialResult(ctx, []chunk.Row{row}, pr)
		require.NoError(t, err)
	}

	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	err = aggFunc.AppendFinalResult2Chunk(ctx, pr, resultChk)
	require.NoError(t, err)
	return resultChk.GetRow(0).GetInt64(0)
}

func buildDataByType(ft *types.FieldType, n int) []types.Datum {
	ret := make([]types.Datum, 0, n+1)
	gen := getDataGenFunc(ft)
	for i := 0; i < n; i++ {
		ret = append(ret, gen(i))
	}
	ret = append(ret, types.Datum{})
	return ret
}

func TestMaxMinCountAllMaxMinTypes(t *testing.T) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.AddFlag(mysql.UnsignedFlag)
	testTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		unsignedType,
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeDuration),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeEnum),
		types.NewFieldType(mysql.TypeSet),
	}

	for _, ft := range testTypes {
		data := buildDataByType(ft, 5)
		require.Equal(t, int64(1), evalMaxMinCount(t, ast.AggFuncMaxCount, ft, data))
		require.Equal(t, int64(1), evalMaxMinCount(t, ast.AggFuncMinCount, ft, data))
		require.Equal(t, int64(0), evalMaxMinCount(t, ast.AggFuncMaxCount, ft, nil))
		require.Equal(t, int64(0), evalMaxMinCount(t, ast.AggFuncMinCount, ft, nil))
	}
}

func TestMaxMinCountSpecialTypes(t *testing.T) {
	bitType := types.NewFieldType(mysql.TypeBit)
	bitData := []types.Datum{
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0x00}),
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0x01}),
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0x01}),
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0x02}),
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0x02}),
		{},
	}
	require.Equal(t, int64(2), evalMaxMinCount(t, ast.AggFuncMaxCount, bitType, bitData))
	require.Equal(t, int64(1), evalMaxMinCount(t, ast.AggFuncMinCount, bitType, bitData))

	vectorType := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vectorData := []types.Datum{
		types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{1, 1})),
		types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{2, 2})),
		types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{2, 2})),
		types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{3, 3})),
		types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{3, 3})),
		{},
	}
	require.Equal(t, int64(2), evalMaxMinCount(t, ast.AggFuncMaxCount, vectorType, vectorData))
	require.Equal(t, int64(1), evalMaxMinCount(t, ast.AggFuncMinCount, vectorType, vectorData))
}

func TestMaxMinCountDuplicateSemantics(t *testing.T) {
	intType := types.NewFieldType(mysql.TypeLonglong)
	data := []types.Datum{
		types.NewIntDatum(0),
		types.NewIntDatum(0),
		types.NewIntDatum(1),
		types.NewIntDatum(4),
		types.NewIntDatum(4),
		types.NewIntDatum(4),
		{},
	}
	require.Equal(t, int64(3), evalMaxMinCount(t, ast.AggFuncMaxCount, intType, data))
	require.Equal(t, int64(2), evalMaxMinCount(t, ast.AggFuncMinCount, intType, data))
}

func TestMergePartialResult4MaxMinCount(t *testing.T) {
	ctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	args := []expression.Expression{&expression.Column{RetType: intType, Index: 0}}

	buildAndCheck := func(funcName string, group1, group2 []types.Datum, expected int64) {
		desc, err := aggregation.NewAggFuncDesc(ctx, funcName, args, false)
		require.NoError(t, err)

		partialDesc, finalDesc := desc.Split([]int{0, 1})
		partialFunc := aggfuncs.Build(ctx, partialDesc, 0)
		finalFunc := aggfuncs.Build(ctx, finalDesc, 0)
		require.NotNil(t, partialFunc)
		require.NotNil(t, finalFunc)

		pr1, _ := partialFunc.AllocPartialResult()
		pr2, _ := partialFunc.AllocPartialResult()
		finalPr, _ := finalFunc.AllocPartialResult()

		fill := func(pr aggfuncs.PartialResult, datums []types.Datum) {
			src := chunk.NewChunkWithCapacity([]*types.FieldType{intType}, len(datums))
			for i := range datums {
				d := datums[i]
				src.AppendDatum(0, &d)
			}
			iter := chunk.NewIterator4Chunk(src)
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				_, err = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, pr)
				require.NoError(t, err)
			}
		}

		fill(pr1, group1)
		fill(pr2, group2)
		_, err = finalFunc.MergePartialResult(ctx, pr1, finalPr)
		require.NoError(t, err)
		_, err = finalFunc.MergePartialResult(ctx, pr2, finalPr)
		require.NoError(t, err)

		resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
		err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
		require.NoError(t, err)
		require.Equal(t, expected, resultChk.GetRow(0).GetInt64(0))
	}

	buildAndCheck(
		ast.AggFuncMaxCount,
		[]types.Datum{types.NewIntDatum(0), types.NewIntDatum(1), types.NewIntDatum(4), types.NewIntDatum(4)},
		[]types.Datum{types.NewIntDatum(2), types.NewIntDatum(4), types.NewIntDatum(4), {}},
		4,
	)
	buildAndCheck(
		ast.AggFuncMinCount,
		[]types.Datum{types.NewIntDatum(0), types.NewIntDatum(1), types.NewIntDatum(4), types.NewIntDatum(4)},
		[]types.Datum{types.NewIntDatum(2), types.NewIntDatum(4), types.NewIntDatum(4), {}},
		1,
	)

	strType := types.NewFieldType(mysql.TypeString)
	strType.SetCharset("utf8mb4")
	strType.SetCollate("utf8mb4_general_ci")
	strArgs := []expression.Expression{&expression.Column{RetType: strType, Index: 0}}
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncMaxCount, strArgs, false)
	require.NoError(t, err)

	partialDesc, finalDesc := desc.Split([]int{0, 1})
	require.Equal(t, mysql.TypeString, finalDesc.Args[0].GetType(nil).GetType())
	require.Equal(t, "utf8mb4_general_ci", finalDesc.Args[0].GetType(nil).GetCollate())

	partialFunc := aggfuncs.Build(ctx, partialDesc, 0)
	finalFunc := aggfuncs.Build(ctx, finalDesc, 0)
	require.NotNil(t, partialFunc)
	require.NotNil(t, finalFunc)

	pr1, _ := partialFunc.AllocPartialResult()
	pr2, _ := partialFunc.AllocPartialResult()
	finalPr, _ := finalFunc.AllocPartialResult()
	fillStr := func(pr aggfuncs.PartialResult, datums []types.Datum) {
		src := chunk.NewChunkWithCapacity([]*types.FieldType{strType}, len(datums))
		for i := range datums {
			d := datums[i]
			src.AppendDatum(0, &d)
		}
		iter := chunk.NewIterator4Chunk(src)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			_, err = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, pr)
			require.NoError(t, err)
		}
	}

	fillStr(pr1, []types.Datum{types.NewStringDatum("B"), types.NewStringDatum("a")})
	fillStr(pr2, []types.Datum{types.NewStringDatum("b"), types.NewStringDatum("a")})
	_, err = finalFunc.MergePartialResult(ctx, pr1, finalPr)
	require.NoError(t, err)
	_, err = finalFunc.MergePartialResult(ctx, pr2, finalPr)
	require.NoError(t, err)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	require.Equal(t, int64(2), resultChk.GetRow(0).GetInt64(0))
}

func TestMemMaxMinCount(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinCountIntSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinCountIntSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4MaxMinCountStringSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4MaxMinCountStringSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeJSON, 5,
			aggfuncs.DefPartialResult4MaxMinCountJSONSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeJSON, 5,
			aggfuncs.DefPartialResult4MaxMinCountJSONSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeEnum, 5,
			aggfuncs.DefPartialResult4MaxMinCountEnumSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeEnum, 5,
			aggfuncs.DefPartialResult4MaxMinCountEnumSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeSet, 5,
			aggfuncs.DefPartialResult4MaxMinCountSetSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeSet, 5,
			aggfuncs.DefPartialResult4MaxMinCountSetSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMaxCount, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinCountDurationSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMinCount, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinCountDurationSize, defaultUpdateMemDeltaGens, false),
	}

	for _, test := range tests {
		testAggMemFunc(t, test)
	}

	vectorType := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	ctx := mock.NewContext()
	args := []expression.Expression{&expression.Column{RetType: vectorType, Index: 0}}
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncMaxCount, args, false)
	require.NoError(t, err)
	aggFunc := aggfuncs.Build(ctx, desc, 0)
	_, memDelta := aggFunc.AllocPartialResult()
	require.Equal(t, aggfuncs.DefPartialResult4MaxMinCountVectorFloat32Size, memDelta)
}

func TestMaxMinCountSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (1),(1),(2),(2),(2),(null)")

	tk.MustQuery("select max_count(a), min_count(a) from t").Check(testkit.Rows("3 2"))
	tk.MustQuery("select max_count(a), min_count(a) from t where a is null").Check(testkit.Rows("0 0"))
	tk.MustQuery("select max_count(a) over (), min_count(a) over () from t limit 1").Check(testkit.Rows("3 2"))
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=4")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=4")
	rows := tk.MustQuery("explain analyze select /*+ hash_agg() */ a, max_count(a) from t group by a").Rows()
	hasParallelHashAgg := false
	for _, row := range rows {
		if strings.Contains(fmt.Sprint(row...), "partial_worker") {
			hasParallelHashAgg = true
			break
		}
	}
	require.True(t, hasParallelHashAgg)

	tk.MustContainErrMsg("select max_count(distinct a) from t", "You have an error in your SQL syntax")
	tk.MustContainErrMsg("select min_count(distinct a) from t", "You have an error in your SQL syntax")
}

func TestMaxMinCountSlidingWindow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, a int)")
	tk.MustExec("insert into t values (1,1),(2,1),(3,2),(4,2),(5,null),(6,2),(7,1)")

	tk.MustQuery(`
		select
			id,
			max_count(a) over (order by id rows between 1 preceding and current row),
			min_count(a) over (order by id rows between 1 preceding and current row)
		from t
		order by id;
	`).Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 1 1",
		"4 2 2",
		"5 1 1",
		"6 1 1",
		"7 1 1",
	))
}
