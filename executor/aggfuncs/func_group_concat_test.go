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
	"github.com/pingcap/tidb/planner/util"
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
	multiArgsTest1 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatSize, groupConcatMultiArgsUpdateMemDeltaGens, false)
	multiArgsTest2 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatDistinctSize, groupConcatDistinctMultiArgsUpdateMemDeltaGens, true)

	multiArgsTest3 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderSize, groupConcatOrderMultiArgsUpdateMemDeltaGens, false)
	multiArgsTest3.multiArgsAggTest.orderBy = true
	multiArgsTest4 := buildMultiArgsAggMemTester(ast.AggFuncGroupConcat, []byte{mysql.TypeString, mysql.TypeString}, mysql.TypeString, 5,
		aggfuncs.DefPartialResult4GroupConcatOrderDistinctSize, groupConcatDistinctOrderMultiArgsUpdateMemDeltaGens, true)
	multiArgsTest4.multiArgsAggTest.orderBy = true

	multiArgsTests := []multiArgsAggMemTest{multiArgsTest1, multiArgsTest2, multiArgsTest3, multiArgsTest4}
	for _, test := range multiArgsTests {
		s.testMultiArgsAggMemFunc(c, test)
	}
}

func groupConcatMultiArgsUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
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
			buffer.WriteString(separator)
		}
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			buffer.WriteString(curVal)
		}
		memDelta := int64(buffer.Cap() - oldMemSize)
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatOrderMultiArgsUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
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
			buffer.WriteString(separator)
		}
		for j := 0; j < len(dataType); j++ {
			curVal := row.GetString(j)
			buffer.WriteString(curVal)
		}
		memDelta := int64(buffer.Len() - oldMemSize)
		for _, byItem := range byItems {
			fdt, _ := byItem.Expr.Eval(row)
			datumMem := aggfuncs.GetDatumMemSize(&fdt)
			memDelta += datumMem
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatDistinctMultiArgsUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
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
		oldMemSize := buffer.Cap()
		if i != 0 {
			buffer.WriteString(separator)
		}
		buffer.WriteString(valsBuf.String())
		memDelta := int64(len(joinedVal) + (buffer.Cap() - oldMemSize))
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func groupConcatDistinctOrderMultiArgsUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
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
		oldMemSize := buffer.Len()
		if i != 0 {
			buffer.WriteString(separator)
		}
		valSet.Insert(joinedVal)
		buffer.WriteString(valsBuf.String())
		memDelta := int64(len(joinedVal) + (buffer.Len() - oldMemSize))
		for _, byItem := range byItems {
			fdt, _ := byItem.Expr.Eval(row)
			datumMem := aggfuncs.GetDatumMemSize(&fdt)
			memDelta += datumMem
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}
