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
	"fmt"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
)

var _ = Suite(&testSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	TestingT(t)
}

type testSuite struct {
	*parser.Parser
	ctx     sessionctx.Context
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
}

func (s *testSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
	s.ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.store = store
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testSuite) TearDownSuite(c *C) {
}

func (s *testSuite) SetUpTest(c *C) {
	s.ctx.GetSessionVars().PlanColumnID = 0
}

func (s *testSuite) TearDownTest(c *C) {
	s.ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
}

type aggTest struct {
	dataType *types.FieldType
	numRows  int
	dataGen  func(i int) types.Datum
	funcName string
	results  []types.Datum
	orderBy  bool
}

func (p *aggTest) genSrcChk() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numRows)
	for i := 0; i < p.numRows; i++ {
		dt := p.dataGen(i)
		srcChk.AppendDatum(0, &dt)
	}
	srcChk.AppendDatum(0, &types.Datum{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *aggTest) messUpChunk(c *chunk.Chunk) {
	for i := 0; i < p.numRows; i++ {
		raw := c.Column(0).GetRaw(i)
		for i := range raw {
			raw[i] = 255
		}
	}
}

type multiArgsAggTest struct {
	dataTypes []*types.FieldType
	retType   *types.FieldType
	numRows   int
	dataGens  []func(i int) types.Datum
	funcName  string
	results   []types.Datum
	orderBy   bool
}

func (p *multiArgsAggTest) genSrcChk() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numRows)
	for i := 0; i < p.numRows; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			fdt := p.dataGens[j](i)
			srcChk.AppendDatum(j, &fdt)
		}
	}
	srcChk.AppendDatum(0, &types.Datum{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *multiArgsAggTest) messUpChunk(c *chunk.Chunk) {
	for i := 0; i < p.numRows; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			raw := c.Column(j).GetRaw(i)
			for i := range raw {
				raw[i] = 255
			}
		}
	}
}

type updateMemDeltaGens func(*chunk.Chunk, *types.FieldType) (memDeltas []int64, err error)

func defaultUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		memDeltas = append(memDeltas, int64(0))
	}
	return memDeltas, nil
}

func approxCountDistinctUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)

	buf := make([]byte, 8)
	p := aggfuncs.NewPartialResult4ApproxCountDistinct()
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemUsage := p.MemUsage()
		switch dataType.Tp {
		case mysql.TypeLonglong:
			val := row.GetInt64(0)
			*(*int64)(unsafe.Pointer(&buf[0])) = val
		case mysql.TypeString:
			val := row.GetString(0)
			buf = codec.EncodeCompactBytes(buf, hack.Slice(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.Tp)
		}

		x := farm.Hash64(buf)
		p.InsertHash64(x)
		newMemUsage := p.MemUsage()
		memDelta := newMemUsage - oldMemUsage
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func distinctUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		val := ""
		memDelta := int64(0)
		switch dataType.Tp {
		case mysql.TypeLonglong:
			val = strconv.FormatInt(row.GetInt64(0), 10)
			memDelta = aggfuncs.DefInt64Size
		case mysql.TypeFloat:
			val = strconv.FormatFloat(float64(row.GetFloat32(0)), 'f', 6, 64)
			memDelta = aggfuncs.DefFloat64Size
		case mysql.TypeDouble:
			val = strconv.FormatFloat(row.GetFloat64(0), 'f', 6, 64)
			memDelta = aggfuncs.DefFloat64Size
		case mysql.TypeNewDecimal:
			decimal := row.GetMyDecimal(0)
			hash, err := decimal.ToHashKey()
			if err != nil {
				memDeltas = append(memDeltas, int64(0))
				continue
			}
			val = string(hack.String(hash))
			memDelta = int64(len(val))
		case mysql.TypeString:
			val = row.GetString(0)
			memDelta = int64(len(val))
		case mysql.TypeDate:
			val = row.GetTime(0).String()
			memDelta = aggfuncs.DefTimeSize
		case mysql.TypeDuration:
			val = strconv.FormatInt(row.GetInt64(0), 10)
			memDelta = aggfuncs.DefInt64Size
		case mysql.TypeJSON:
			json := row.GetJSON(0)
			bytes := make([]byte, 0)
			bytes = append(bytes, json.TypeCode)
			bytes = append(bytes, json.Value...)
			val = string(bytes)
			memDelta = int64(len(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.Tp)
		}
		if valSet.Exist(val) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valSet.Insert(val)
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func rowMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		memDelta := aggfuncs.DefRowSize
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

type aggMemTest struct {
	aggTest            aggTest
	allocMemDelta      int64
	updateMemDeltaGens updateMemDeltaGens
	isDistinct         bool
}

func buildAggMemTester(funcName string, tp byte, numRows int, allocMemDelta int64, updateMemDeltaGens updateMemDeltaGens, isDistinct bool) aggMemTest {
	aggTest := buildAggTester(funcName, tp, numRows)
	pt := aggMemTest{
		aggTest:            aggTest,
		allocMemDelta:      allocMemDelta,
		updateMemDeltaGens: updateMemDeltaGens,
		isDistinct:         isDistinct,
	}
	return pt
}

func (s *testSuite) testMergePartialResult(c *C, p aggTest) {
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, 1)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 1)
	}

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt := resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeString))
	}
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeString))
	}
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))
	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)

	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	}
	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)

	dt = resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeLonglong))
	}
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[2])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[2]))
}

func buildAggTester(funcName string, tp byte, numRows int, results ...interface{}) aggTest {
	return buildAggTesterWithFieldType(funcName, types.NewFieldType(tp), numRows, results...)
}

func buildAggTesterWithFieldType(funcName string, ft *types.FieldType, numRows int, results ...interface{}) aggTest {
	pt := aggTest{
		dataType: ft,
		numRows:  numRows,
		funcName: funcName,
		dataGen:  getDataGenFunc(ft),
	}
	for _, result := range results {
		pt.results = append(pt.results, types.NewDatum(result))
	}
	return pt
}

func (s *testSuite) testMultiArgsMergePartialResult(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.retType}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt := resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)

	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)

	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[2])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

// for multiple args in aggfuncs such as json_objectagg(c1, c2)
func buildMultiArgsAggTester(funcName string, tps []byte, rt byte, numRows int, results ...interface{}) multiArgsAggTest {
	fts := make([]*types.FieldType, len(tps))
	for i := 0; i < len(tps); i++ {
		fts[i] = types.NewFieldType(tps[i])
	}
	return buildMultiArgsAggTesterWithFieldType(funcName, fts, types.NewFieldType(rt), numRows, results...)
}

func buildMultiArgsAggTesterWithFieldType(funcName string, fts []*types.FieldType, rt *types.FieldType, numRows int, results ...interface{}) multiArgsAggTest {
	dataGens := make([]func(i int) types.Datum, len(fts))
	for i := 0; i < len(fts); i++ {
		dataGens[i] = getDataGenFunc(fts[i])
	}
	mt := multiArgsAggTest{
		dataTypes: fts,
		retType:   rt,
		numRows:   numRows,
		funcName:  funcName,
		dataGens:  dataGens,
	}
	for _, result := range results {
		mt.results = append(mt.results, types.NewDatum(result))
	}
	return mt
}

func getDataGenFunc(ft *types.FieldType) func(i int) types.Datum {
	switch ft.Tp {
	case mysql.TypeLonglong:
		return func(i int) types.Datum { return types.NewIntDatum(int64(i)) }
	case mysql.TypeFloat:
		return func(i int) types.Datum { return types.NewFloat32Datum(float32(i)) }
	case mysql.TypeNewDecimal:
		return func(i int) types.Datum { return types.NewDecimalDatum(types.NewDecFromInt(int64(i))) }
	case mysql.TypeDouble:
		return func(i int) types.Datum { return types.NewFloat64Datum(float64(i)) }
	case mysql.TypeString:
		return func(i int) types.Datum { return types.NewStringDatum(fmt.Sprintf("%d", i)) }
	case mysql.TypeDate:
		return func(i int) types.Datum { return types.NewTimeDatum(types.TimeFromDays(int64(i + 365))) }
	case mysql.TypeDuration:
		return func(i int) types.Datum { return types.NewDurationDatum(types.Duration{Duration: time.Duration(i)}) }
	case mysql.TypeJSON:
		return func(i int) types.Datum { return types.NewDatum(json.CreateBinary(int64(i))) }
	case mysql.TypeEnum:
		elems := []string{"a", "b", "c", "d", "e"}
		return func(i int) types.Datum {
			e, _ := types.ParseEnumValue(elems, uint64(i+1))
			return types.NewCollateMysqlEnumDatum(e, ft.Collate)
		}
	case mysql.TypeSet:
		elems := []string{"a", "b", "c", "d", "e"}
		return func(i int) types.Datum {
			e, _ := types.ParseSetValue(elems, uint64(i+1))
			return types.NewMysqlSetDatum(e, ft.Collate)
		}
	}
	return nil
}

func (s *testSuite) testAggFunc(c *C, p aggTest) {
	srcChk := p.genSrcChk()

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))
}

func (s *testSuite) testAggMemFunc(c *C, p aggMemTest) {
	srcChk := p.aggTest.genSrcChk()

	args := []expression.Expression{&expression.Column{RetType: p.aggTest.dataType, Index: 0}}
	if p.aggTest.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.aggTest.funcName, args, p.isDistinct)
	c.Assert(err, IsNil)
	if p.aggTest.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	c.Assert(memDelta, Equals, p.allocMemDelta)

	updateMemDeltas, err := p.updateMemDeltaGens(srcChk, p.aggTest.dataType)
	c.Assert(err, IsNil)
	iter := chunk.NewIterator4Chunk(srcChk)
	i := 0
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		memDelta, err := finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
		c.Assert(err, IsNil)
		c.Assert(memDelta, Equals, updateMemDeltas[i])
		i++
	}
}

func (s *testSuite) testMultiArgsAggFunc(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk()

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

func (s *testSuite) benchmarkAggFunc(b *testing.B, p aggTest) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numRows)
	for i := 0; i < p.numRows; i++ {
		dt := p.dataGen(i)
		srcChk.AppendDatum(0, &dt)
	}
	srcChk.AppendDatum(0, &types.Datum{})

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Row, 0, iter.Len())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		input = append(input, row)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataType), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataType), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})
}

func (s *testSuite) benchmarkMultiArgsAggFunc(b *testing.B, p multiArgsAggTest) {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numRows)
	for i := 0; i < p.numRows; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			fdt := p.dataGens[j](i)
			srcChk.AppendDatum(j, &fdt)
		}
	}
	srcChk.AppendDatum(0, &types.Datum{})

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(" "), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Row, 0, iter.Len())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		input = append(input, row)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})
}

func (s *testSuite) baseBenchmarkAggFunc(b *testing.B,
	finalFunc aggfuncs.AggFunc, input []chunk.Row, output *chunk.Chunk) {
	finalPr, _ := finalFunc.AllocPartialResult()
	output.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		finalFunc.UpdatePartialResult(s.ctx, input, finalPr)
		b.StopTimer()
		output.Reset()
		b.StartTimer()
	}
}
