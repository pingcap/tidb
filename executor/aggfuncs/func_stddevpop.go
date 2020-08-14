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

package aggfuncs

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
)

type baseStdDevPopAggFunc struct {
	baseAggFunc
}

type stdDevPop4Float64 struct {
	baseStdDevPopAggFunc
}

type partialResult4StdDevPopFloat64 struct {
	count    int64
	sum      float64
	variance float64
}

func (e *stdDevPop4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4StdDevPopFloat64{}), 0
}

func (e *stdDevPop4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StdDevPopFloat64)(pr)
	p.count = 0
	p.sum = 0
	p.variance = 0
}

func (e *stdDevPop4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StdDevPopFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	varicance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, math.Sqrt(varicance))
	return nil
}

//func calculateIntermediate(count int64, sum float64, input float64, variance float64) float64 {
//	t := float64(count)*input - sum
//	variance += (t * t) / (float64(count * (count - 1)))
//	return variance
//}

func (e *stdDevPop4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4StdDevPopFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if isNull {
			continue
		}
		p.count++
		p.sum += input
		if p.count > 1 {
			p.variance = calculateIntermediate(p.count, p.sum, input, p.variance)
		}
	}
	return 0, nil
}

func (e *stdDevPop4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4StdDevPopFloat64)(src), (*partialResult4StdDevPopFloat64)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	if p2.count == 0 {
		p2.count = p1.count
		p2.sum = p1.sum
		p2.variance = p1.variance
		return 0, nil
	}
	if p2.count != 0 && p1.count != 0 {
		p2.variance = calculateMerge(p1.count, p2.count, p1.sum, p2.sum, p1.variance, p2.variance)
		p2.count += p1.count
		p2.sum += p1.sum
	}
	return 0, nil
}

type stdDevPop4DistinctFloat64 struct {
	baseStdDevPopAggFunc
}

type partialResult4StdDevPopDistinctFloat64 struct {
	count    int64
	sum      float64
	variance float64
	valSet   set.Float64Set
}

func (e *stdDevPop4DistinctFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4StdDevPopDistinctFloat64)
	p.count = 0
	p.sum = 0
	p.variance = 0
	p.valSet = set.NewFloat64Set()
	return PartialResult(p), 0
}

func (e *stdDevPop4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StdDevPopDistinctFloat64)(pr)
	p.count = 0
	p.sum = 0
	p.variance = 0
	p.valSet = set.NewFloat64Set()
}

func (e *stdDevPop4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StdDevPopDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	varicance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, math.Sqrt(varicance))
	return nil
}

func (e *stdDevPop4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4StdDevPopDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		p.count++
		p.sum += input
		if p.count > 1 {
			p.variance = calculateIntermediate(p.count, p.sum, input, p.variance)
		}
	}
	return 0, nil
}
