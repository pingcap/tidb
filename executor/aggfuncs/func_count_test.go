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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"encoding/binary"
	"testing"

	"github.com/dgryski/go-farm"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
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

func (s *testSuite) TestMergePartialResult4Count(c *C) {
	tester := buildAggTester(ast.AggFuncCount, mysql.TypeLonglong, 5, 5, 3, 8)
	s.testMergePartialResult(c, tester)

	tester = buildAggTester(ast.AggFuncApproxCountDistinct, mysql.TypeLonglong, 5, genApproxDistinctMergePartialResult(0, 5), genApproxDistinctMergePartialResult(2, 5), 5)
	s.testMergePartialResult(c, tester)
}

func (s *testSuite) TestCount(c *C) {
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
	for _, test := range tests {
		s.testAggFunc(c, test)
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
	for _, test := range tests2 {
		s.testMultiArgsAggFunc(c, test)
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
		s.testAggFunc(c, test)
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

	for _, test := range tests4 {
		s.testMultiArgsAggFunc(c, test)
	}
}

func BenchmarkCount(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

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
		s.benchmarkAggFunc(b, test)
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
		s.benchmarkMultiArgsAggFunc(b, test)
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
		s.benchmarkMultiArgsAggFunc(b, test)
	}
}
