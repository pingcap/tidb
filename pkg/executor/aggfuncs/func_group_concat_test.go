// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func TestMergePartialResult4GroupConcat(t *testing.T) {
	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, "0 1 2 3 4", "2 3 4", "0 1 2 3 4 2 3 4")
	testMergePartialResult(t, test)
}

func TestGroupConcat(t *testing.T) {
	ctx := mock.NewContext()

	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, nil, "0 1 2 3 4")
	testAggFunc(t, test)

	test2 := buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00")
	test2.orderBy = true
	testMultiArgsAggFunc(t, ctx, test2)

	defer func() {
		err := ctx.GetSessionVars().SetSystemVar(variable.GroupConcatMaxLen, "1024")
		require.NoError(t, err)
	}()
	// minimum GroupConcatMaxLen is 4
	for i := 4; i <= 7; i++ {
		err := ctx.GetSessionVars().SetSystemVar(variable.GroupConcatMaxLen, fmt.Sprint(i))
		require.NoError(t, err)
		test2 = buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00"[:i])
		test2.orderBy = true
		testMultiArgsAggFunc(t, ctx, test2)
	}
}

func TestMemGroupConcat(t *testing.T) {
	multiArgsTest1 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatSize+aggfuncs.DefBytesBufferSize, groupConcatMultiArgsUpdateMemDeltaGens, false)
	multiArgsTest2 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatDistinctSize+aggfuncs.DefBytesBufferSize+hack.DefBucketMemoryUsageForSetString, groupConcatDistinctMultiArgsUpdateMemDeltaGens, true)

	multiArgsTest3 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderSize+aggfuncs.DefTopNRowsSize, groupConcatOrderMultiArgsUpdateMemDeltaGens, false)
	multiArgsTest3.multiArgsAggTest.orderBy = true
	multiArgsTest4 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderDistinctSize+aggfuncs.DefTopNRowsSize+hack.DefBucketMemoryUsageForSetString, groupConcatDistinctOrderMultiArgsUpdateMemDeltaGens, true)
	multiArgsTest4.multiArgsAggTest.orderBy = true

	multiArgsTests := []multiArgsAggMemTest{multiArgsTest1, multiArgsTest2, multiArgsTest3, multiArgsTest4}
	for i, test := range multiArgsTests {
		t.Run(fmt.Sprintf("%s_%d", test.multiArgsAggTest.funcName, i), func(t *testing.T) {
			testMultiArgsAggMemFunc(t, test)
		})
	}
}

func groupConcatMultiArgsUpdateMemDeltaGens(ctx sessionctx.Context, srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	buffer := new(bytes.Buffer)
	valBuffer := new(bytes.Buffer)
	for i := 0; i < srcChk.NumRows(); i++ {
		valBuffer.Reset()
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Cap() + valBuffer.Cap()
		if i != 0 {
			buffer.WriteString(separator)
		}
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			valBuffer.WriteString(curVal)
		}
		buffer.WriteString(valBuffer.String())
		memDelta := int64(buffer.Cap() + valBuffer.Cap() - oldMemSize)
		if i == 0 {
			memDelta += aggfuncs.DefBytesBufferSize
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatOrderMultiArgsUpdateMemDeltaGens(ctx sessionctx.Context, srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		buffer := new(bytes.Buffer)
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Cap()
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			buffer.WriteString(curVal)
		}
		memDelta := int64(buffer.Cap() - oldMemSize)
		for _, byItem := range byItems {
			fdt, _ := byItem.Expr.Eval(ctx.GetExprCtx().GetEvalCtx(), row)
			datumMem := aggfuncs.GetDatumMemSize(&fdt)
			memDelta += datumMem
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatDistinctMultiArgsUpdateMemDeltaGens(ctx sessionctx.Context, srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	buffer := new(bytes.Buffer)
	valsBuf := new(bytes.Buffer)
	var encodeBytesBuffer []byte
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valsBuf.Reset()
		oldMemSize := buffer.Cap() + valsBuf.Cap() + cap(encodeBytesBuffer)
		encodeBytesBuffer = encodeBytesBuffer[:0]
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			encodeBytesBuffer = codec.EncodeBytes(encodeBytesBuffer, hack.Slice(curVal))
			valsBuf.WriteString(curVal)
		}
		joinedVal := string(encodeBytesBuffer)
		if valSet.Exist(joinedVal) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valSet.Insert(joinedVal)
		if i != 0 {
			buffer.WriteString(separator)
		}
		buffer.WriteString(valsBuf.String())
		memDelta := int64(len(joinedVal) + (buffer.Cap() + valsBuf.Cap() + cap(encodeBytesBuffer) - oldMemSize))
		if i == 0 {
			memDelta += aggfuncs.DefBytesBufferSize
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatDistinctOrderMultiArgsUpdateMemDeltaGens(ctx sessionctx.Context, srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	var encodeBytesBuffer []byte
	for i := 0; i < srcChk.NumRows(); i++ {
		valsBuf := new(bytes.Buffer)
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valsBuf.Reset()
		encodeBytesBuffer = encodeBytesBuffer[:0]
		oldMemSize := valsBuf.Cap() + cap(encodeBytesBuffer)
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			encodeBytesBuffer = codec.EncodeBytes(encodeBytesBuffer, hack.Slice(curVal))
			valsBuf.WriteString(curVal)
		}
		joinedVal := string(encodeBytesBuffer)
		if valSet.Exist(joinedVal) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valSet.Insert(joinedVal)
		memDelta := int64(len(joinedVal) + (valsBuf.Cap() + cap(encodeBytesBuffer) - oldMemSize))
		for _, byItem := range byItems {
			fdt, _ := byItem.Expr.Eval(ctx.GetExprCtx().GetEvalCtx(), row)
			datumMem := aggfuncs.GetDatumMemSize(&fdt)
			memDelta += datumMem
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}
