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

package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

// varianceKind stands for kind of population standard deviation/sample variance aggregate functions.
type varianceKind int

const (
	kindNone varianceKind = iota
	kindVariance
)

// All the following function implementations return the float64 result,
// which store the partial results in "partialResult4VarianceFloat64".
//
// "baseVarianceFloat64" is wrapped by:
// - "baseVarianceOriginal4Float64"
// - "baseVariancePartial4Float64"
type baseVarianceFloat64 struct {
	baseAggFunc
	varianceKind
}

type partialResult4VarianceFloat64 struct {
	sum      float64
	variance float64
	count    int64
}

func (e *baseVarianceFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4VarianceFloat64{})
}

func (e *baseVarianceFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceFloat64)(pr)
	p.sum, p.variance, p.count = 0, 0, 0
}

func (e *baseVarianceFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarianceFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		switch e.varianceKind {
		case kindVariance:
			chk.AppendFloat64(e.ordinal, p.variance/float64(p.count))
		default:
			panic("Not implemented")
		}
	}
	return nil
}

type baseVarianceOriginal4Float64 struct {
	baseVarianceFloat64
}

func (e *baseVarianceOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		if p.count == 0 {
			p.sum, p.variance, p.count = input, float64(0), int64(1)
		} else {
			variance := types.CalculateMerge(p.count, 1, p.sum, input, p.variance, 0)
			p.sum += input
			p.variance = variance
			p.count++
		}
	}
	return nil
}

type baseVariancePartial4Float64 struct {
	baseVarianceFloat64
}

func (e *baseVariancePartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceFloat64)(pr)
	for _, row := range rowsInGroup {
		inputVariance, isNull, err := e.args[2].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		inputSum, isNull, err := e.args[1].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		if p.count == 0 {
			p.sum, p.variance, p.count = inputSum, inputVariance, inputCount
		} else if inputCount != 0 {
			variance := types.CalculateMerge(p.count, inputCount, p.sum, inputSum, p.variance, inputVariance)
			p.sum += inputSum
			p.variance = variance
			p.count += inputCount
		}
	}
	return nil
}

func (e *baseVariancePartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4VarianceFloat64)(src), (*partialResult4VarianceFloat64)(dst)
	if p2.count == 0 {
		p2.sum, p2.variance, p2.count = p1.sum, p1.variance, p1.count
	} else if p1.count != 0 {
		variance := types.CalculateMerge(p1.count, p2.count, p1.sum, p2.sum, p1.variance, p2.variance)
		p2.sum += p1.sum
		p2.variance = variance
		p2.count += p1.count
	}
	return nil
}
