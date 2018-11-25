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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
)

// varianceKind stands for kind of population standard deviation/sample variance aggregate functions.
type varianceKind int

const (
	kindNone varianceKind = iota
	kindVarPop
	kindVarSamp
	kindStdPop
	kinkStdSamp
)

// All the following function implementations return the float64 result,
// which store the partial results in "partialResult4VarianceFloat64".
//
// "baseVarianceFloat64" is wrapped by:
// - "varianceOriginal4Float64"
// - "variancePartial4Float64"
type baseVarianceFloat64 struct {
	baseAggFunc
	varianceKind
}

type partialResult4VarianceFloat64 struct {
	count    int64
	sum      float64
	variance float64 // $$\sum_{k=i}^{j}{(a_k-avg_{ij})^2}$$ (this is actually n times the variance)
}

func (p *partialResult4VarianceFloat64) merge(count int64, sum float64, variance float64) {
	if p.count == 0 {
		p.count, p.sum, p.variance = count, sum, variance
	} else {
		variance = types.CalculateMerge(p.count, count, p.sum, sum, p.variance, variance)
		p.count += count
		p.sum += sum
		p.variance = variance
	}
}

func (e *baseVarianceFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4VarianceFloat64{})
}

func (e *baseVarianceFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceFloat64)(pr)
	p.count, p.sum, p.variance = 0, 0, 0
}

func (e *baseVarianceFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarianceFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	switch e.varianceKind {
	case kindVarPop:
		chk.AppendFloat64(e.ordinal, p.variance/float64(p.count))
	default:
		// TODO support more population standard deviation/sample variance
		panic("Not implemented")
	}
	return nil
}

type varianceOriginal4Float64 struct {
	baseVarianceFloat64
}

func (e *varianceOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		p.merge(1, input, 0)
	}
	return nil
}

type variancePartial4Float64 struct {
	baseVarianceFloat64
}

func (e *variancePartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
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
		p.merge(inputCount, inputSum, inputVariance)
	}
	return nil
}

func (e *variancePartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4VarianceFloat64)(src), (*partialResult4VarianceFloat64)(dst)
	p2.merge(p1.count, p1.sum, p1.variance)
	return nil
}

type partialResult4VarianceDistinctFloat64 struct {
	partialResult4VarianceFloat64
	valSet set.Float64Set
}

type varianceOriginal4DistinctFloat64 struct {
	baseVarianceFloat64
}

func (e *varianceOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	p := &partialResult4VarianceDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *varianceOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceDistinctFloat64)(pr)
	p.sum = float64(0)
	p.count = int64(0)
	p.variance = float64(0)
	p.valSet = set.NewFloat64Set()
}

func (e *varianceOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		p.merge(1, input, 0)
		p.valSet.Insert(input)
	}
	return nil
}
