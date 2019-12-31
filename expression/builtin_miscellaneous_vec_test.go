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

package expression

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinMiscellaneousCases = map[string][]vecExprBenchCase{
	ast.Inet6Aton: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6StrGener{}}},
	},
	ast.IsIPv6: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Sleep: {},
	ast.UUID:  {},
	ast.Inet6Ntoa: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{
			&selectStringGener{
				candidates: []string{
					"192.168.0.1",
					"2001:db8::68", //ipv6
				},
			}}},
	},
	ast.InetAton: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4StrGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{
			&selectStringGener{
				candidates: []string{
					"11.11.11.11.",    // last char is .
					"266.266.266.266", // int in string exceed 255
					"127",
					".122",
					".123.123",
					"127.255",
					"127.2.1",
				},
			}}},
	},
	ast.IsIPv4Mapped: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4MappedByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6ByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4ByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{1.0, types.ETString}}},
	},
	ast.IsIPv4Compat: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4CompatByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6ByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4ByteGener{}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{1.0, types.ETString}}},
	},
	ast.InetNtoa: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsIPv4: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.AnyValue: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
	},
	ast.NameConst: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDuration}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETString, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETString, types.ETReal}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETString, types.ETJson}},
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETString, types.ETTimestamp}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinMiscellaneousCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMiscellaneousCases)
}
