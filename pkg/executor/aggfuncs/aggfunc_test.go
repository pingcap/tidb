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
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

const (
	// separator argument for group_concat() test cases
	separator = " "

	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func generateRandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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
	for i := range p.numRows {
		dt := p.dataGen(i)
		srcChk.AppendDatum(0, &dt)
	}
	srcChk.AppendDatum(0, &types.Datum{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *aggTest) messUpChunk(c *chunk.Chunk) {
	for i := range p.numRows {
		raw := c.Column(0).GetRaw(i)
		for i := range raw {
			raw[i] = 255
		}
	}
}

type parallelDistinctAggTestCase struct {
	dataTypes []*types.FieldType
	funcName  string
	srcChks   []*chunk.Chunk
	result    types.Datum
}

func newParallelDistinctAggTestCase(funcName string, dataTypes []*types.FieldType, numRows, ndv int, needNull bool, allNull bool) *parallelDistinctAggTestCase {
	testCase := &parallelDistinctAggTestCase{
		dataTypes: dataTypes,
		funcName:  funcName,
	}

	var dataGenFunc func() types.Datum

	intDatums := make(map[int]struct{})
	float64Datums := make(map[float64]struct{})
	decimalDatums := make(map[string]*types.MyDecimal)
	stringDatums := make(map[string]struct{})
	durationDatums := make(map[int64]struct{})

	hasMultiArgs := len(dataTypes) > 1

	// In this ut, we ensure arg types are the same when there are multi args.
	// Just for convenience.
	if hasMultiArgs && dataTypes[0].GetType() != dataTypes[1].GetType() {
		panic("Need same types")
	}

	switch dataTypes[0].GetType() {
	case mysql.TypeLonglong:
		dataGenFunc = func() types.Datum {
			for {
				newVal := rand.Intn(1000000000) - 500000000
				_, ok := intDatums[newVal]
				if ok {
					continue
				}

				intDatums[newVal] = struct{}{}
				return types.NewIntDatum(int64(newVal))
			}
		}
	case mysql.TypeDouble:
		dataGenFunc = func() types.Datum {
			for {
				newVal := rand.Float64()*100 - 50
				_, ok := float64Datums[newVal]
				if ok {
					continue
				}

				float64Datums[newVal] = struct{}{}
				return types.NewFloat64Datum(newVal)
			}
		}
	case mysql.TypeNewDecimal:
		dataGenFunc = func() types.Datum {
			for {
				newVal := types.NewDecFromStringForTest(fmt.Sprintf("%.4f", rand.Float64()*100-50))
				hashKeyBytes, err := newVal.ToHashKey()
				if err != nil {
					panic(fmt.Sprintf("newVal: %s is invalid, err: %v", newVal, err))
				}

				_, ok := decimalDatums[string(hashKeyBytes)]
				if ok {
					continue
				}

				decimalDatums[string(hashKeyBytes)] = newVal
				return types.NewDecimalDatum(newVal)
			}
		}
	case mysql.TypeVarString:
		dataGenFunc = func() types.Datum {
			for {
				newVal := generateRandomString(rand.Intn(5)) // TODO revoked to 100
				_, ok := stringDatums[newVal]
				if ok {
					continue
				}

				stringDatums[newVal] = struct{}{}
				return types.NewStringDatum(newVal)
			}
		}
	case mysql.TypeDuration:
		dataGenFunc = func() types.Datum {
			for {
				newVal := types.NewDuration(rand.Intn(800), rand.Intn(60), rand.Intn(60), 0, 0)
				_, ok := durationDatums[int64(newVal.Duration)]
				if ok {
					continue
				}

				durationDatums[int64(newVal.Duration)] = struct{}{}
				return types.NewDurationDatum(newVal)
			}
		}
	}

	datumsForNDV := make([][]types.Datum, 0, ndv)
	for range ndv {
		if hasMultiArgs {
			datumsForNDV = append(datumsForNDV, []types.Datum{dataGenFunc(), dataGenFunc()})
		} else {
			datumsForNDV = append(datumsForNDV, []types.Datum{dataGenFunc()})
		}
	}

	srcChkNum := 10
	testCase.srcChks = make([]*chunk.Chunk, 0, srcChkNum)
	for range srcChkNum {
		testCase.srcChks = append(testCase.srcChks, chunk.NewChunkWithCapacity(dataTypes, numRows))
	}

	insertedIdxs := make(map[int]struct{})
	nullValProportion := rand.Intn(9) + 1
	for range numRows {
		if allNull || (needNull && rand.Intn(10) < nullValProportion) {
			nilDatum := types.NewDatum(nil)
			testCase.srcChks[rand.Intn(srcChkNum)].AppendDatum(0, &nilDatum)
			if hasMultiArgs {
				testCase.srcChks[rand.Intn(srcChkNum)].AppendDatum(1, &nilDatum)
			}
			continue
		}

		idx := rand.Intn(ndv)
		chkIdx := rand.Intn(srcChkNum)
		testCase.srcChks[chkIdx].AppendDatum(0, &datumsForNDV[idx][0])
		if hasMultiArgs {
			testCase.srcChks[chkIdx].AppendDatum(1, &datumsForNDV[idx][1])
		}

		log.Info(fmt.Sprintf("case insert: %s-%s", datumsForNDV[idx][0].GetString(), datumsForNDV[idx][1].GetString()))

		insertedIdxs[idx] = struct{}{}
	}

	insertedDistinctValNum := len(insertedIdxs)

	switch funcName {
	case ast.AggFuncCount:
		testCase.result = types.NewIntDatum(int64(insertedDistinctValNum))
	case ast.AggFuncAvg:
		if len(insertedIdxs) == 0 {
			testCase.result = types.NewDatum(nil)
			break
		}
		switch dataTypes[0].GetType() {
		case mysql.TypeDouble:
			s := float64(0)
			for idx := range insertedIdxs {
				s += datumsForNDV[idx][0].GetFloat64()
			}
			testCase.result = types.NewFloat64Datum(s / float64(insertedDistinctValNum))
		case mysql.TypeNewDecimal:
			s := types.NewDecFromStringForTest("0.0000")
			for idx := range insertedIdxs {
				dec := datumsForNDV[idx][0].GetMysqlDecimal()
				tmp := s
				s = types.NewDecFromStringForTest("0.0000")
				err := types.DecimalAdd(dec, tmp, s)
				if err != nil {
					panic(err)
				}
			}
			num := types.NewDecFromInt(int64(insertedDistinctValNum))
			res := types.NewDecFromInt(0)
			types.DecimalDiv(s, num, res, 0)
			testCase.result = types.NewDecimalDatum(res)
		default:
			// In actual execution, some data type will be converted before entering avg agg.
			// So it's needless to test them in the ut.
			panic("Not supported in test")
		}
	case ast.AggFuncSum:
		if len(insertedIdxs) == 0 {
			testCase.result = types.NewDatum(nil)
			break
		}
		switch dataTypes[0].GetType() {
		case mysql.TypeDouble:
			s := float64(0)
			for idx := range insertedIdxs {
				s += datumsForNDV[idx][0].GetFloat64()
			}
			testCase.result = types.NewFloat64Datum(s)
		case mysql.TypeNewDecimal:
			s := types.NewDecFromStringForTest("0.0000")
			for idx := range insertedIdxs {
				dec := datumsForNDV[idx][0].GetMysqlDecimal()
				tmp := s
				s = types.NewDecFromStringForTest("0.0000")
				err := types.DecimalAdd(dec, tmp, s)
				if err != nil {
					panic(err)
				}
			}
			testCase.result = types.NewDecimalDatum(s)
		default:
			// In actual execution, some data type will be converted before entering avg agg.
			// So it's needless to test them in the ut.
			panic("Not supported in test")
		}
	case ast.AggFuncGroupConcat:
		if dataTypes[0].GetType() != mysql.TypeVarString {
			panic("Data type is not string")
		}

		isFirst := true
		resultStr := ""
		for idx := range insertedIdxs {
			if isFirst {
				resultStr = fmt.Sprintf("%s%s", datumsForNDV[idx][0].GetString(), datumsForNDV[idx][1].GetString())
				isFirst = false
			} else {
				resultStr = fmt.Sprintf("%s%s%s%s", resultStr, separator, datumsForNDV[idx][0].GetString(), datumsForNDV[idx][1].GetString())
			}
		}
		testCase.result = types.NewStringDatum(resultStr)
	default:
		panic("Not supported")
	}
	return testCase
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
	for i := range p.numRows {
		for j := range p.dataGens {
			fdt := p.dataGens[j](i)
			srcChk.AppendDatum(j, &fdt)
		}
	}
	srcChk.AppendDatum(0, &types.Datum{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *multiArgsAggTest) messUpChunk(c *chunk.Chunk) {
	for i := range p.numRows {
		for j := range p.dataGens {
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
	for range srcChk.NumRows() {
		memDeltas = append(memDeltas, int64(0))
	}
	return memDeltas, nil
}

func approxCountDistinctUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)

	buf := make([]byte, 8)
	p := aggfuncs.NewPartialResult4ApproxCountDistinct()
	for i := range srcChk.NumRows() {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemUsage := p.MemUsage()
		switch dataType.GetType() {
		case mysql.TypeLonglong:
			val := row.GetInt64(0)
			*(*int64)(unsafe.Pointer(&buf[0])) = val
		case mysql.TypeString:
			val := row.GetString(0)
			buf = codec.EncodeCompactBytes(buf, hack.Slice(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.GetType())
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
	for i := range srcChk.NumRows() {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		val := ""
		memDelta := int64(0)
		switch dataType.GetType() {
		case mysql.TypeLonglong:
			val = strconv.FormatInt(row.GetInt64(0), 10)
		case mysql.TypeFloat:
			val = strconv.FormatFloat(float64(row.GetFloat32(0)), 'f', 6, 64)
		case mysql.TypeDouble:
			val = strconv.FormatFloat(row.GetFloat64(0), 'f', 6, 64)
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
			// the distinct count aggFunc need 16 bytes to encode the Datetime type.
			memDelta = 16
		case mysql.TypeDuration:
			val = strconv.FormatInt(row.GetInt64(0), 10)
		case mysql.TypeJSON:
			jsonVal := row.GetJSON(0)
			bytes := make([]byte, 0)
			bytes = jsonVal.HashValue(bytes)
			val = string(bytes)
			memDelta = int64(len(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.GetType())
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
	for range srcChk.NumRows() {
		memDelta := aggfuncs.DefRowSize
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

type multiArgsUpdateMemDeltaGens func(sessionctx.Context, *chunk.Chunk, []*types.FieldType, []*util.ByItems) (memDeltas []int64, err error)

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

type multiArgsAggMemTest struct {
	multiArgsAggTest            multiArgsAggTest
	allocMemDelta               int64
	multiArgsUpdateMemDeltaGens multiArgsUpdateMemDeltaGens
	isDistinct                  bool
}

func buildMultiArgsAggMemTester(funcName string, tps []byte, rt byte, numRows int, allocMemDelta int64, updateMemDeltaGens multiArgsUpdateMemDeltaGens, isDistinct bool) multiArgsAggMemTest {
	multiArgsAggTest := buildMultiArgsAggTester(funcName, tps, rt, numRows)
	pt := multiArgsAggMemTest{
		multiArgsAggTest:            multiArgsAggTest,
		allocMemDelta:               allocMemDelta,
		multiArgsUpdateMemDeltaGens: updateMemDeltaGens,
		isDistinct:                  isDistinct,
	}
	return pt
}

func testMergePartialResult(t *testing.T, p aggTest) {
	ctx := mock.NewContext()
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	ctor := collate.GetCollator(p.dataType.GetCollate())
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, 1)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 1)
	}
	if p.funcName == ast.AggFuncJsonArrayagg {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeJSON)}, 1)
	}

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, partialResult)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	err = partialFunc.AppendFinalResult2Chunk(ctx, partialResult, resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeString))
	}
	if p.funcName == ast.AggFuncJsonArrayagg {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeJSON))
	}
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[0])

	_, err = finalFunc.MergePartialResult(ctx, partialResult, finalPr)
	require.NoError(t, err)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		_, err = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, partialResult)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	err = partialFunc.AppendFinalResult2Chunk(ctx, partialResult, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeString))
	}
	if p.funcName == ast.AggFuncJsonArrayagg {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeJSON))
	}
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[1])
	_, err = finalFunc.MergePartialResult(ctx, partialResult, finalPr)
	require.NoError(t, err)

	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	}
	if p.funcName == ast.AggFuncJsonArrayagg {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeJSON)}, 1)
	}
	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)

	dt = resultChk.GetRow(0).GetDatum(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeLonglong))
	}
	if p.funcName == ast.AggFuncJsonArrayagg {
		dt = resultChk.GetRow(0).GetDatum(0, types.NewFieldType(mysql.TypeJSON))
	}
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[2], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[2])
}

func buildAggTester(funcName string, tp byte, numRows int, results ...any) aggTest {
	return buildAggTesterWithFieldType(funcName, types.NewFieldType(tp), numRows, results...)
}

func buildAggTesterWithFieldType(funcName string, ft *types.FieldType, numRows int, results ...any) aggTest {
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

func testMultiArgsMergePartialResult(t *testing.T, ctx *mock.Context, p multiArgsAggTest) {
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := make([]expression.Expression, len(p.dataTypes))
	for k := range p.dataTypes {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}

	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	ctor := collate.GetCollator(args[0].GetType(ctx).GetCollate())
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.retType}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot assert error since there are cases of error, e.g. JSON documents may not contain NULL member
		_, _ = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	err = partialFunc.AppendFinalResult2Chunk(ctx, partialResult, resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Zero(t, result)

	_, err = finalFunc.MergePartialResult(ctx, partialResult, finalPr)
	require.NoError(t, err)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot check error
		_, _ = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	err = partialFunc.AppendFinalResult2Chunk(ctx, partialResult, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Zero(t, result)
	_, err = finalFunc.MergePartialResult(ctx, partialResult, finalPr)
	require.NoError(t, err)

	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)

	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[2], ctor)
	require.NoError(t, err)
	require.Zero(t, result)
}

// for multiple args in aggfuncs such as json_objectagg(c1, c2)
func buildMultiArgsAggTester(funcName string, tps []byte, rt byte, numRows int, results ...any) multiArgsAggTest {
	fts := make([]*types.FieldType, len(tps))
	for i := range tps {
		fts[i] = types.NewFieldType(tps[i])
	}
	return buildMultiArgsAggTesterWithFieldType(funcName, fts, types.NewFieldType(rt), numRows, results...)
}

func buildMultiArgsAggTesterWithFieldType(funcName string, fts []*types.FieldType, rt *types.FieldType, numRows int, results ...any) multiArgsAggTest {
	dataGens := make([]func(i int) types.Datum, len(fts))
	for i := range fts {
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
	switch ft.GetType() {
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
		return func(i int) types.Datum { return types.NewDatum(types.CreateBinaryJSON(int64(i))) }
	case mysql.TypeEnum:
		elems := []string{"e", "d", "c", "b", "a"}
		return func(i int) types.Datum {
			e, _ := types.ParseEnumValue(elems, uint64(i+1))
			return types.NewCollateMysqlEnumDatum(e, ft.GetCollate())
		}
	case mysql.TypeSet:
		elems := []string{"e", "d", "c", "b", "a"}
		return func(i int) types.Datum {
			e, _ := types.ParseSetValue(elems, uint64(i+1))
			return types.NewMysqlSetDatum(e, ft.GetCollate())
		}
	}
	return nil
}

func testParallelDistinctAggFunc(t *testing.T, p parallelDistinctAggTestCase, multiArgs bool) {
	ctx := mock.NewContext()

	var args []expression.Expression
	if multiArgs {
		args = []expression.Expression{
			&expression.Column{RetType: p.dataTypes[0], Index: 0},
			&expression.Column{RetType: p.dataTypes[1], Index: 1},
		}
	} else {
		args = []expression.Expression{&expression.Column{RetType: p.dataTypes[0], Index: 0}}
	}

	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, true)
	require.NoError(t, err)

	partialDesc, finalDesc := desc.Split([]int{0, 1})
	partialFunc := aggfuncs.Build(ctx, partialDesc, 0)
	finalFunc := aggfuncs.Build(ctx, finalDesc, 0)

	ctor := collate.GetCollator(finalDesc.RetTp.GetCollate())

	srcChkNum := len(p.srcChks)
	partialPtrs := make([]aggfuncs.PartialResult, 0, srcChkNum)
	for range srcChkNum {
		ptr, _ := partialFunc.AllocPartialResult()
		partialPtrs = append(partialPtrs, ptr)
	}

	for i := range srcChkNum {
		iter := chunk.NewIterator4Chunk(p.srcChks[i])
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			_, err = partialFunc.UpdatePartialResult(ctx, []chunk.Row{row}, partialPtrs[i])
			require.NoError(t, err)
		}
	}

	for i := 1; i < srcChkNum; i++ {
		finalFunc.MergePartialResult(ctx, partialPtrs[i], partialPtrs[0])
	}

	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	err = finalFunc.AppendFinalResult2Chunk(ctx, partialPtrs[0], resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)

	if p.funcName == ast.AggFuncGroupConcat {
		exp := p.result.GetString()
		act := dt.GetString()
		expectRes := strings.Split(exp, separator)
		actualRes := strings.Split(act, separator)
		if len(expectRes) != len(actualRes) {
			panic(fmt.Sprintf("expect len: %d, actual len: %d", len(expectRes), len(actualRes)))
		}

		slices.Sort(expectRes)
		slices.Sort(actualRes)

		for i := range expectRes {
			if expectRes[i] == actualRes[i] {
				continue
			}
			panic(fmt.Sprintf("i: %d, expect: %s, actual: %s", i, expectRes[i], actualRes[i]))
		}
		return
	}

	if dt.Kind() == types.KindFloat64 {
		// Truncate the float, as float is imprecise and the tailing numbers may be different
		floatNum := dt.GetFloat64()
		floatStr := fmt.Sprintf("%.2f", floatNum)
		floatNum, err = strconv.ParseFloat(floatStr, 64)
		if err != nil {
			panic(err)
		}
		dt = types.NewFloat64Datum(floatNum)

		floatNum = p.result.GetFloat64()
		floatStr = fmt.Sprintf("%.2f", floatNum)
		floatNum, err = strconv.ParseFloat(floatStr, 64)
		if err != nil {
			panic(err)
		}
		p.result = types.NewFloat64Datum(floatNum)
	}
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.result, ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "expect: %v, actual: %v", dt.String(), p.result)
}

func testAggFunc(t *testing.T, p aggTest) {
	srcChk := p.genSrcChk()
	ctx := mock.NewContext()

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	ctor := collate.GetCollator(p.dataType.GetCollate())
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	if p.funcName == ast.AggFuncApproxPercentile {
		args = append(args, &expression.Constant{Value: types.NewIntDatum(50), RetType: types.NewFieldType(mysql.TypeLong)})
	}
	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[1])

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[0])

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(ctx, p.funcName, args, true)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Equalf(t, 0, result, "%v != %v", dt.String(), p.results[1])
}

func testAggFuncWithoutDistinct(t *testing.T, p aggTest) {
	srcChk := p.genSrcChk()

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	ctor := collate.GetCollator(p.dataType.GetCollate())
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	if p.funcName == ast.AggFuncApproxPercentile {
		args = append(args, &expression.Constant{Value: types.NewIntDatum(50), RetType: types.NewFieldType(mysql.TypeLong)})
	}
	ctx := mock.NewContext()
	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.NoError(t, err)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Zerof(t, result, "%v != %v", dt.String(), p.results[1])

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Zerof(t, result, "%v != %v", dt.String(), p.results[0])
}

func testAggMemFunc(t *testing.T, p aggMemTest) {
	srcChk := p.aggTest.genSrcChk()
	ctx := mock.NewContext()

	args := []expression.Expression{&expression.Column{RetType: p.aggTest.dataType, Index: 0}}
	if p.aggTest.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(ctx, p.aggTest.funcName, args, p.isDistinct)
	require.NoError(t, err)
	if p.aggTest.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	require.Equal(t, p.allocMemDelta, memDelta)

	updateMemDeltas, err := p.updateMemDeltaGens(srcChk, p.aggTest.dataType)
	require.NoError(t, err)
	iter := chunk.NewIterator4Chunk(srcChk)
	i := 0
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		memDelta, err := finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.NoError(t, err)
		require.Equal(t, updateMemDeltas[i], memDelta)
		i++
	}
}

func testMultiArgsAggFunc(t *testing.T, ctx *mock.Context, p multiArgsAggTest) {
	srcChk := p.genSrcChk()

	args := make([]expression.Expression, len(p.dataTypes))
	for k := range p.dataTypes {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	ctor := collate.GetCollator(args[0].GetType(ctx).GetCollate())
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot assert error since there are cases of error, e.g. rows were cut by GROUPCONCAT
		_, _ = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Zerof(t, result, "%v != %v", dt.String(), p.results[1])

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Zerof(t, result, "%v != %v", dt.String(), p.results[0])

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(ctx, p.funcName, args, true)
	require.NoError(t, err)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot check error
		_, _ = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot check error
		_, _ = finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[1], ctor)
	require.NoError(t, err)
	require.Zerof(t, result, "%v != %v", dt.String(), p.results[1])

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(ctx, finalPr, resultChk)
	require.NoError(t, err)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &p.results[0], ctor)
	require.NoError(t, err)
	require.Zero(t, result)
}

func testMultiArgsAggMemFunc(t *testing.T, p multiArgsAggMemTest) {
	srcChk := p.multiArgsAggTest.genSrcChk()
	ctx := mock.NewContext()

	args := make([]expression.Expression, len(p.multiArgsAggTest.dataTypes))
	for k := range p.multiArgsAggTest.dataTypes {
		args[k] = &expression.Column{RetType: p.multiArgsAggTest.dataTypes[k], Index: k}
	}
	if p.multiArgsAggTest.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(ctx, p.multiArgsAggTest.funcName, args, p.isDistinct)
	require.NoError(t, err)
	if p.multiArgsAggTest.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	require.Equal(t, p.allocMemDelta, memDelta)

	updateMemDeltas, err := p.multiArgsUpdateMemDeltaGens(ctx, srcChk, p.multiArgsAggTest.dataTypes, desc.OrderByItems)
	require.NoError(t, err)
	iter := chunk.NewIterator4Chunk(srcChk)
	i := 0
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		memDelta, _ := finalFunc.UpdatePartialResult(ctx, []chunk.Row{row}, finalPr)
		require.Equal(t, updateMemDeltas[i], memDelta)
		i++
	}
}

func benchmarkAggFunc(b *testing.B, ctx *mock.Context, p aggTest) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numRows)
	for i := range p.numRows {
		dt := p.dataGen(i)
		srcChk.AppendDatum(0, &dt)
	}
	srcChk.AppendDatum(0, &types.Datum{})

	args := []expression.Expression{&expression.Column{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Row, 0, iter.Len())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		input = append(input, row)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataType), func(b *testing.B) {
		baseBenchmarkAggFunc(b, ctx, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataType), func(b *testing.B) {
		baseBenchmarkAggFunc(b, ctx, finalFunc, input, resultChk)
	})
}

func benchmarkMultiArgsAggFunc(b *testing.B, ctx *mock.Context, p multiArgsAggTest) {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numRows)
	for i := range p.numRows {
		for j := range p.dataGens {
			fdt := p.dataGens[j](i)
			srcChk.AppendDatum(j, &fdt)
		}
	}
	srcChk.AppendDatum(0, &types.Datum{})

	args := make([]expression.Expression, len(p.dataTypes))
	for k := range p.dataTypes {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Row, 0, iter.Len())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		input = append(input, row)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		baseBenchmarkAggFunc(b, ctx, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		baseBenchmarkAggFunc(b, ctx, finalFunc, input, resultChk)
	})
}

func baseBenchmarkAggFunc(b *testing.B, ctx aggfuncs.AggFuncUpdateContext, finalFunc aggfuncs.AggFunc, input []chunk.Row, output *chunk.Chunk) {
	finalPr, _ := finalFunc.AllocPartialResult()
	output.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := finalFunc.UpdatePartialResult(ctx, input, finalPr)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		output.Reset()
		b.StartTimer()
	}
}

func TestAggApproxCountDistinctPushDown(t *testing.T) {
	ctx := mock.NewContext()

	args := make([]expression.Expression, 0)
	args = append(args, &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLonglong),
		ID:      1,
		Index:   int(1),
	})

	aggDesc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncApproxCountDistinct, args, false)

	require.NoError(t, err)

	// can only pushdown to TiFlash
	require.True(t, aggregation.CheckAggPushDown(ctx, aggDesc, kv.TiFlash))
	require.False(t, aggregation.CheckAggPushDown(ctx, aggDesc, kv.TiKV))
	require.False(t, aggregation.CheckAggPushDown(ctx, aggDesc, kv.TiDB))
	require.False(t, aggregation.CheckAggPushDown(ctx, aggDesc, kv.UnSpecified))
}
