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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type partialResult4SumFloat64 struct {
	val    float64
	isNull bool
}

type partialResult4SumDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type partialResult4SumDistinctFloat64 struct {
	partialResult4SumFloat64
	valSet float64Set
}

type partialResult4SumDistinctDecimal struct {
	partialResult4SumDecimal
	valSet decimalSet
}

type baseSumAggFunc struct {
	baseAggFunc
}

type sum4Float64 struct {
	baseSumAggFunc
}

func (e *sum4Float64) AllocPartialResult() PartialResult {
	p := new(partialResult4SumFloat64)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *sum4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *sum4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		p.val += input
	}
	return nil
}

func (e *sum4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4SumFloat64)(src), (*partialResult4SumFloat64)(dst)
	if p1.isNull {
		return nil
	}
	p2.val += p1.val
	p2.isNull = false
	return nil
}

type sum4Decimal struct {
	baseSumAggFunc
}

func (e *sum4Decimal) AllocPartialResult() PartialResult {
	p := new(partialResult4SumDecimal)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDecimal)(pr)
	p.isNull = true
}

func (e *sum4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *sum4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.val = *newSum
	}
	return nil
}

func (e *sum4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4SumDecimal)(src), (*partialResult4SumDecimal)(dst)
	if p1.isNull {
		return nil
	}
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&p1.val, &p2.val, newSum)
	if err != nil {
		return errors.Trace(err)
	}
	p2.val = *newSum
	p2.isNull = false
	return nil
}

type sum4DistinctFloat64 struct {
	baseSumAggFunc
}

func (e *sum4DistinctFloat64) AllocPartialResult() PartialResult {
	p := new(partialResult4SumDistinctFloat64)
	p.isNull = true
	p.valSet = newFloat64Set()
	return PartialResult(p)
}

func (e *sum4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctFloat64)(pr)
	p.isNull = true
	p.valSet = newFloat64Set()
}

func (e *sum4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.exist(input) {
			continue
		}
		p.valSet.insert(input)
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		p.val += input
	}
	return nil
}

func (e *sum4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type sum4DistinctDecimal struct {
	baseSumAggFunc
}

func (e *sum4DistinctDecimal) AllocPartialResult() PartialResult {
	p := new(partialResult4SumDistinctDecimal)
	p.isNull = true
	p.valSet = newDecimalSet()
	return PartialResult(p)
}

func (e *sum4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctDecimal)(pr)
	p.isNull = true
	p.valSet = newDecimalSet()
}

func (e *sum4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.exist(input) {
			continue
		}
		p.valSet.insert(input)
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		newSum := new(types.MyDecimal)
		if err = types.DecimalAdd(&p.val, input, newSum); err != nil {
			return errors.Trace(err)
		}
		p.val = *newSum
	}
	return nil
}

func (e *sum4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}
