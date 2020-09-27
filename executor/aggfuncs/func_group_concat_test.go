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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"bytes"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

func (s *testSuite) TestMergePartialResult4GroupConcat(c *C) {
	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, "0 1 2 3 4", "2 3 4", "0 1 2 3 4 2 3 4")
	s.testMergePartialResult(c, test)
}

func (s *testSuite) TestGroupConcat(c *C) {
	test := buildAggTester(ast.AggFuncGroupConcat, mysql.TypeString, 5, nil, "0 1 2 3 4")
	s.testAggFunc(c, test)

	test2 := buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00")
	test2.orderBy = true
	s.testMultiArgsAggFunc(c, test2)

	defer variable.SetSessionSystemVar(s.ctx.GetSessionVars(), variable.GroupConcatMaxLen, types.NewStringDatum("1024"))
	// minimum GroupConcatMaxLen is 4
	for i := 4; i <= 7; i++ {
		variable.SetSessionSystemVar(s.ctx.GetSessionVars(), variable.GroupConcatMaxLen, types.NewStringDatum(fmt.Sprint(i)))
		test2 = buildMultiArgsAggTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5, nil, "44 33 22 11 00"[:i])
		test2.orderBy = true
		s.testMultiArgsAggFunc(c, test2)
	}
}

func (s *testSuite) TestMemGroupConcat(c *C) {
	test1 := buildAggMemTester(ast.AggFuncGroupConcat, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatSize, groupConcatUpdateMemDeltaGens, false)
	test2 := buildAggMemTester(ast.AggFuncGroupConcat, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatDistinctSize, groupConcatUpdateDistinctMemDeltaGens, true)
	test3 := buildAggMemTester(ast.AggFuncGroupConcat, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderSize, groupConcatUpdateOrderMemDeltaGens, false)
	test3.aggTest.orderBy = true
	test4 := buildAggMemTester(ast.AggFuncGroupConcat, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderDistinctSize, groupConcatUpdateOrderDistinctMemDeltaGens, true)
	test4.aggTest.orderBy = true
	tests := []aggMemTest{test1, test2, test3, test4}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

func groupConcatUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	buffer := new(bytes.Buffer)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Cap()
		if i != 0 {
			buffer.WriteString(" ")
		}
		curVal := row.GetString(0)
		buffer.WriteString(curVal)
		memDeltas = append(memDeltas, int64(buffer.Cap()-oldMemSize))
	}
	return memDeltas, nil
}

func groupConcatUpdateOrderMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	buffer := new(bytes.Buffer)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Len()
		if i != 0 {
			buffer.WriteString(" ")
		}
		curVal := row.GetString(0)
		buffer.WriteString(curVal)
		memDeltas = append(memDeltas, int64(buffer.Len()-oldMemSize))
	}
	return memDeltas, nil
}

func groupConcatUpdateDistinctMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	buffer := new(bytes.Buffer)
	var encodeBytesBuffer []byte
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		encodeBytesBuffer = encodeBytesBuffer[:0]
		curVal := row.GetString(0)
		encodeBytesBuffer = codec.EncodeBytes(encodeBytesBuffer, hack.Slice(curVal))
		joinedVal := string(encodeBytesBuffer)
		if valSet.Exist(joinedVal) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Cap()
		if i != 0 {
			buffer.WriteString(" ")
		}
		valSet.Insert(joinedVal)
		buffer.WriteString(curVal)
		memDeltas = append(memDeltas, int64(buffer.Cap()-oldMemSize+len(joinedVal)))
	}
	return memDeltas, nil
}

func groupConcatUpdateOrderDistinctMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	buffer := new(bytes.Buffer)
	var encodeBytesBuffer []byte
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		encodeBytesBuffer = encodeBytesBuffer[:0]
		curVal := row.GetString(0)
		encodeBytesBuffer = codec.EncodeBytes(encodeBytesBuffer, hack.Slice(curVal))
		joinedVal := string(encodeBytesBuffer)
		if valSet.Exist(joinedVal) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemSize := buffer.Len()
		if i != 0 {
			buffer.WriteString(" ")
		}
		valSet.Insert(joinedVal)
		buffer.WriteString(curVal)
		memDeltas = append(memDeltas, int64(buffer.Len()-oldMemSize+len(joinedVal)))
	}
	return memDeltas, nil
}
