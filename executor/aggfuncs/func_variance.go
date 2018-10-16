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
	"math"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
	"github.com/pkg/errors"
)

type varianceType int

const (
	varianceNone varianceType = iota
	varianceVarPop
	varianceVarSamp
	varianceStdPop
	varianceStdSamp
)

// "baseVarianceDecimal" is wrapped by:
// - "varianceOriginal4Decimal"
// - "variancePartial4Decimal"
type baseVarianceDecimal struct {
	baseAggFunc
	varianceType
}

type partialResult4VarianceDecimal struct {
	count    int64
	sum      types.MyDecimal
	variance types.MyDecimal
}

func (p *partialResult4VarianceDecimal) merge(count int64, sum, variance types.MyDecimal) error {
	if p.count == 0 {
		p.count, p.sum, p.variance = count, sum, variance
	} else {
		variance, sum, err := types.CalculateMergeDecimal(p.count, count, &p.sum, &sum, &p.variance, &variance)
		if err != nil {
			return errors.Trace(err)
		}
		p.variance = *variance
		p.sum = *sum
		p.count += count
	}
	return nil
}

func (e *baseVarianceDecimal) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4VarianceDecimal{})
}

func (e *baseVarianceDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceDecimal)(pr)
	p.sum, p.variance = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseVarianceDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarianceDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	res := new(types.MyDecimal)
	switch e.varianceType {
	case varianceStdPop:
		decimalCount := types.NewDecFromInt(p.count)
		tmp := new(types.MyDecimal)
		err := types.DecimalDiv(&p.sum, decimalCount, tmp, types.DivFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
		err = types.DecimalSqrt(tmp, res, types.SqrtFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
	case varianceStdSamp:
		// stdSamp is sqrt(variance/(count-1)), so p.count must exceed 1.
		if p.count == 1 {
			chk.AppendNull(e.ordinal)
			return nil
		}
		decimalCount := types.NewDecFromInt(p.count - 1)
		tmp := new(types.MyDecimal)
		err := types.DecimalDiv(&p.sum, decimalCount, tmp, types.DivFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
		err = types.DecimalSqrt(tmp, res, types.SqrtFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
	case varianceVarPop:
		decimalCount := types.NewDecFromInt(p.count)
		err := types.DecimalDiv(&p.sum, decimalCount, res, types.DivFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
	case varianceVarSamp:
		// varSamp is variance/(count-1), so p.count must exceed 1.
		if p.count == 1 {
			chk.AppendNull(e.ordinal)
			return nil
		}
		decimalCount := types.NewDecFromInt(p.count - 1)
		err := types.DecimalDiv(&p.sum, decimalCount, res, types.DivFracIncr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	chk.AppendMyDecimal(e.ordinal, res)
	return nil
}

type varianceOriginal4Decimal struct {
	baseVarianceDecimal
}

func (e *varianceOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		err = p.merge(1, *input, *types.NewDecFromInt(0))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type variancePartial4Decimal struct {
	baseVarianceDecimal
}

func (e *variancePartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceDecimal)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, row)
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

		inputVariance, isNull, err := e.args[2].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		err = p.merge(inputCount, *inputSum, *inputVariance)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *variancePartial4Decimal) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4VarianceDecimal)(src), (*partialResult4VarianceDecimal)(dst)
	err := p2.merge(p1.count, p1.sum, p1.variance)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type varianceOriginal4DistinctDecimal struct {
	baseVarianceDecimal
}

type partialResult4VarianceDistinctDecimal struct {
	partialResult4VarianceDecimal
	valSet set.DecimalSet
}

func (e *varianceOriginal4DistinctDecimal) AllocPartialResult() PartialResult {
	p := &partialResult4VarianceDistinctDecimal{
		valSet: set.NewDecimalSet(),
	}
	return PartialResult(p)
}

func (e *varianceOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceDistinctDecimal)(pr)
	p.sum, p.variance = *types.NewDecFromInt(0), *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewDecimalSet()
}

func (e *varianceOriginal4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		err = p.merge(1, *input, *types.NewDecFromInt(0))
		if err != nil {
			return errors.Trace(err)
		}
		p.valSet.Insert(input)
	}
	return nil
}

// "baseVarianceFloat64" is wrapped by:
// - "varianceOriginal4Float64"
// - "variancePartial4DFloat64"
type baseVarianceFloat64 struct {
	baseAggFunc
	varianceType
}

type partialResult4VarianceFloat64 struct {
	count    int64
	sum      float64
	variance float64
}

func (p *partialResult4VarianceFloat64) merge(count int64, sum, variance float64) error {
	if p.count == 0 {
		p.count, p.sum, p.variance = count, sum, variance
	} else {
		variance, sum, err := types.CalculateMergeFloat64(p.count, count, p.sum, sum, p.variance, variance)
		if err != nil {
			return errors.Trace(err)
		}
		p.variance = variance
		p.sum = sum
	}
	return nil
}

func (e *baseVarianceFloat64) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4VarianceFloat64{})
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

	var res float64
	switch e.varianceType {
	case varianceStdPop:
		res = math.Sqrt(p.variance / float64(p.count))
	case varianceStdSamp:
		// stdSamp is sqrt(variance/(count-1)), so p.count must exceed 1.
		if p.count == 1 {
			chk.AppendNull(e.ordinal)
			return nil
		}
		res = math.Sqrt(p.variance / float64(p.count-1))
	case varianceVarPop:
		res = p.variance / float64(p.count)
	case varianceVarSamp:
		// varSamp is variance/(count-1), so p.count must exceed 1.
		if p.count == 1 {
			chk.AppendNull(e.ordinal)
			return nil
		}
		res = p.variance / float64(p.count-1)
	}

	chk.AppendFloat64(e.ordinal, res)
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
		err = p.merge(1, input, 0)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type variancePartial4Float64 struct {
	baseVarianceFloat64
}

func (e *variancePartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4VarianceFloat64)(pr)
	for _, row := range rowsInGroup {
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

		inputVariance, isNull, err := e.args[2].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		err = p.merge(inputCount, inputSum, inputVariance)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *variancePartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4VarianceFloat64)(src), (*partialResult4VarianceFloat64)(dst)
	err := p2.merge(p1.count, p1.sum, p1.variance)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type varianceOriginal4DistinctFloat64 struct {
	baseVarianceFloat64
}

type partialResult4VarianceDistinctFloat64 struct {
	partialResult4VarianceFloat64
	valSet set.Float64Set
}

func (e *varianceOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	p := &partialResult4VarianceDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *varianceOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarianceDistinctFloat64)(pr)
	p.count, p.sum, p.variance = 0, 0, 0
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

		err = p.merge(1, input, 0)
		if err != nil {
			return errors.Trace(err)
		}
		p.valSet.Insert(input)
	}
	return nil
}
