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

package evaluators

import (
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type aggEvaluator4AvgDecimal struct {
	baseAggEvaluator
}

type partialResult4AvgDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *aggEvaluator4AvgDecimal) toPartialResult(partialBytes []byte) *partialResult4AvgDecimal {
	return (*partialResult4AvgDecimal)(unsafe.Pointer(&partialBytes[0]))
}

func (e *aggEvaluator4AvgDecimal) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4AvgDecimal{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *aggEvaluator4AvgDecimal) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4AvgDecimal{}
}

func (e *aggEvaluator4AvgDecimal) AppendPartialResult2Chunk(ctx context.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendMyDecimal(e.outputIdx[0], &partialResult.sum)
	chk.AppendInt64(e.outputIdx[1], partialResult.count)
	return nil
}

func (e *aggEvaluator4AvgDecimal) AppendFinalResult2Chunk(ctx context.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	if partialResult.count == 0 {
		chk.AppendNull(e.outputIdx[0])
		return nil
	}
	decimalCount := types.NewDecFromInt(partialResult.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&partialResult.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendMyDecimal(e.outputIdx[0], finalResult)
	return nil
}

type aggEvaluator4MapAvgDecimal struct {
	aggEvaluator4AvgDecimal
}

func (e *aggEvaluator4MapAvgDecimal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&partialResult.sum, input.GetMyDecimal(e.inputIdx[0]), newSum)
	if err != nil {
		return errors.Trace(err)
	}
	partialResult.sum = *newSum
	partialResult.count++
	return nil
}

type aggEvaluator4MapAvgInt struct {
	aggEvaluator4AvgDecimal
}

func (e *aggEvaluator4MapAvgInt) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	inputDecimal := types.NewDecFromInt(input.GetInt64(e.inputIdx[0]))
	err := types.DecimalAdd(&partialResult.sum, inputDecimal, newSum)
	if err != nil {
		return errors.Trace(err)
	}
	partialResult.sum = *newSum
	partialResult.count++
	return nil
}

type aggEvaluator4DistinctMapAvgDecimal struct {
	aggEvaluator4AvgDecimal
}

func (e *aggEvaluator4DistinctMapAvgDecimal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&partialResult.sum, input.GetMyDecimal(e.inputIdx[0]), newSum)
	if err != nil {
		return errors.Trace(err)
	}
	partialResult.sum = *newSum
	partialResult.count++
	return nil
}

type aggEvaluator4ReduceAvgDecimal struct {
	aggEvaluator4AvgDecimal
}

func (e *aggEvaluator4ReduceAvgDecimal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&partialResult.sum, input.GetMyDecimal(e.inputIdx[1]), newSum)
	if err != nil {
		return errors.Trace(err)
	}
	partialResult.sum = *newSum
	partialResult.count += input.GetInt64(e.inputIdx[0])
	return nil
}

type aggEvaluator4AvgReal struct {
	baseAggEvaluator
}

type partialResult4AvgReal struct {
	sum   float64
	count int64
}

func (e *aggEvaluator4AvgReal) toPartialResult(partialBytes []byte) *partialResult4AvgReal {
	return (*partialResult4AvgReal)(unsafe.Pointer(&partialBytes[0]))
}

func (e *aggEvaluator4AvgReal) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4AvgReal{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *aggEvaluator4AvgReal) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4AvgReal{}
}

func (e *aggEvaluator4AvgReal) AppendPartialResult2Chunk(ctx context.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat64(e.outputIdx[0], partialResult.sum)
	chk.AppendInt64(e.outputIdx[1], partialResult.count)
	return nil
}

func (e *aggEvaluator4AvgReal) AppendFinalResult2Chunk(ctx context.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendFloat64(e.outputIdx[0], partialResult.sum/float64(partialResult.count))
	return nil
}

type aggEvaluator4MapAvgReal struct {
	aggEvaluator4AvgReal
}

func (e *aggEvaluator4MapAvgReal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	partialResult.sum += input.GetFloat64(e.inputIdx[0])
	partialResult.count++
	return nil
}

type aggEvaluator4DistinctMapAvgReal struct {
	aggEvaluator4AvgReal
}

func (e *aggEvaluator4DistinctMapAvgReal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	partialResult.sum += input.GetFloat64(e.inputIdx[0])
	partialResult.count++
	return nil
}

type aggEvaluator4ReduceAvgReal struct {
	aggEvaluator4AvgReal
}

func (e *aggEvaluator4ReduceAvgReal) UpdatePartialResult(ctx context.Context, input types.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	partialResult.sum += input.GetFloat64(e.inputIdx[1])
	partialResult.count += input.GetInt64(e.inputIdx[0])
	return nil
}
