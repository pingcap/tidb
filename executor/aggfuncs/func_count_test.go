// Copyright 2018 PingCAP, Inc.
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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
	"github.com/stretchr/testify/require"
)

func genApproxDistinctMergePartialResult(begin, end uint64) string {
	o := aggfuncs.NewPartialResult4ApproxCountDistinct()
	encodedBytes := make([]byte, 8)
	for i := begin; i < end; i++ {
		binary.LittleEndian.PutUint64(encodedBytes, i)
		x := farm.Hash64(encodedBytes)
		o.InsertHash64(x)
	}
	return string(o.Serialize())
}

func TestMergePartialResult4Count(t *testing.T) {
	tester := buildAggTester(ast.AggFuncCount, mysql.TypeLonglong, 5, 5, 3, 8)
	testMergePartialResult(t, tester)

	tester = buildAggTester(ast.AggFuncApproxCountDistinct, mysql.TypeLonglong, 5, genApproxDistinctMergePartialResult(0, 5), genApproxDistinctMergePartialResult(2, 5), 5)
	testMergePartialResult(t, tester)
}

func TestCount(t *testing.T) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncCount, mysql.TypeLonglong, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeFloat, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDouble, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeNewDecimal, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeString, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDate, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDuration, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeJSON, 5, 0, 5),
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.funcName, i), func(t *testing.T) {
			testAggFunc(t, test)
		})
	}
	tests2 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeLonglong, mysql.TypeLonglong}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeFloat, mysql.TypeFloat}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDouble, mysql.TypeDouble}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeNewDecimal, mysql.TypeNewDecimal}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDate, mysql.TypeDate}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDuration, mysql.TypeDuration}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeJSON, mysql.TypeJSON}, mysql.TypeLonglong, 5, 0, 5),
	}
	for i, test := range tests2 {
		t.Run(fmt.Sprintf("%s_%d", test.funcName, i), func(t *testing.T) {
			testMultiArgsAggFunc(t, mock.NewContext(), test)
		})
	}

	tests3 := []aggTest{
		buildAggTester(ast.AggFuncCount, mysql.TypeLonglong, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeFloat, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDouble, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeNewDecimal, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeString, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDate, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeDuration, 5, 0, 5),
		buildAggTester(ast.AggFuncCount, mysql.TypeJSON, 5, 0, 5),
	}
	for _, test := range tests3 {
		testAggFunc(t, test)
	}

	tests4 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeLonglong, mysql.TypeLonglong}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeFloat, mysql.TypeFloat}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDouble, mysql.TypeDouble}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeNewDecimal, mysql.TypeNewDecimal}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDate, mysql.TypeDate}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDuration, mysql.TypeDuration}, mysql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeJSON, mysql.TypeJSON}, mysql.TypeLonglong, 5, 0, 5),
	}

	for i, test := range tests4 {
		t.Run(fmt.Sprintf("%s_%d", test.funcName, i), func(t *testing.T) {
			testMultiArgsAggFunc(t, mock.NewContext(), test)
		})
	}
}

func TestMemCount(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncCount, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeFloat, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4CountDistinctIntSize+set.DefInt64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeFloat, 5,
			aggfuncs.DefPartialResult4CountDistinctRealSize+set.DefFloat64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4CountDistinctRealSize+set.DefFloat64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4CountDistinctDecimalSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4CountDistinctStringSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4CountWithDistinctSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4CountDistinctDurationSize+set.DefInt64SetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncCount, mysql.TypeJSON, 5,
			aggfuncs.DefPartialResult4CountWithDistinctSize+set.DefStringSetBucketMemoryUsage, distinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncApproxCountDistinct, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4ApproxCountDistinctSize, approxCountDistinctUpdateMemDeltaGens, true),
		buildAggMemTester(ast.AggFuncApproxCountDistinct, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4ApproxCountDistinctSize, approxCountDistinctUpdateMemDeltaGens, true),
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.aggTest.funcName, i), func(t *testing.T) {
			testAggMemFunc(t, test)
		})
	}
}

func TestWriteTime(t *testing.T) {
	tt, err := types.ParseDate(&(stmtctx.StatementContext{}), "2020-11-11")
	require.NoError(t, err)

	buf := make([]byte, 16)
	for i := range buf {
		buf[i] = uint8(255)
	}
	aggfuncs.WriteTime(buf, tt)
	for i := range buf {
		require.False(t, buf[i] == uint8(255))
	}
}

func BenchmarkCount(b *testing.B) {
	ctx := mock.NewContext()

	rowNum := 50000
	tests := []aggTest{
		buildAggTester(ast.AggFuncCount, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeFloat, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeDouble, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeNewDecimal, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeString, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeDate, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeDuration, rowNum, 0, rowNum),
		buildAggTester(ast.AggFuncCount, mysql.TypeJSON, rowNum, 0, rowNum),
	}
	for _, test := range tests {
		benchmarkAggFunc(b, ctx, test)
	}

	tests2 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeLonglong, mysql.TypeLonglong}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeFloat, mysql.TypeFloat}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDouble, mysql.TypeDouble}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeNewDecimal, mysql.TypeNewDecimal}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDate, mysql.TypeDate}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeDuration, mysql.TypeDuration}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{mysql.TypeJSON, mysql.TypeJSON}, mysql.TypeLonglong, rowNum, 0, rowNum),
	}
	for _, test := range tests2 {
		benchmarkMultiArgsAggFunc(b, ctx, test)
	}

	tests3 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeLonglong, mysql.TypeLonglong}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeFloat, mysql.TypeFloat}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDouble, mysql.TypeDouble}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeNewDecimal, mysql.TypeNewDecimal}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDate, mysql.TypeDate}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeDuration, mysql.TypeDuration}, mysql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{mysql.TypeJSON, mysql.TypeJSON}, mysql.TypeLonglong, rowNum, 0, rowNum),
	}
	for _, test := range tests3 {
		benchmarkMultiArgsAggFunc(b, ctx, test)
	}
}
