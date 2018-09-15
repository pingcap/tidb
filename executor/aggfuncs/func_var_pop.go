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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

// All the following avg function implementations return the float64 result,
// which store the partial results in "partialResult4VarPopFloat64".
//
// "baseVarPopFloat64" is wrapped by:
// - "varPopOriginal4Float64"
// - "varPopPartial4Float64"
type baseVarPopFloat64 struct {
	baseAggFunc
}

type partialResult4VarPopFloat64 struct {
	sum       float64
	squareSum float64
	count     int64
}

func (e *baseVarPopFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4VarPopFloat64{})
}

func (e *baseVarPopFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarPopFloat64)(pr)
	p.sum, p.squareSum, p.count = 0, 0, 0
}

func (e *baseVarPopFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		chk.AppendFloat64(e.ordinal, (p.squareSum-p.sum*p.sum/float64(p.count))/float64(p.count))
	}
	return nil
}

type varPopOriginal4Float64 struct {
	baseVarPopFloat64
}

func (e *varPopOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarPopFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		p.sum += input
		p.squareSum += input * input
		p.count++
	}
	return nil
}

type varPopPartial4Float64 struct {
	baseVarPopFloat64
}

func (e *varPopPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarPopFloat64)(pr)
	for _, row := range rowsInGroup {
		inputSquareSum, isNull, err := e.args[2].EvalReal(sctx, row)
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
		p.sum += inputSum
		p.squareSum += inputSquareSum
		p.count += inputCount
	}
	return nil
}

func (e *varPopPartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4VarPopFloat64)(src), (*partialResult4VarPopFloat64)(dst)
	p2.sum += p1.sum
	p2.squareSum += p1.squareSum
	p2.count += p1.count
	return nil
}
