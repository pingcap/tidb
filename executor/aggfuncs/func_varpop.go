package aggfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type varPop4Float64 struct {
	baseAggFunc
}

type partialResult4Float64 struct {
	count        int64
	sum          float64
	variance	float64
}

func (e *varPop4Float64) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Float64{})
}

func (e *varPop4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Float64)(pr)
	p.sum = 0
	p.count = 0
}

func (e *varPop4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Float64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	varicance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, varicance)
	return nil
}

func calculateIntermediate(count int64, sum float64, input float64, variance float64) float64 {
	t := float64(count) * input - sum
	variance += (t * t) / (float64(count * (count - 1)))
	return variance
}

func (e *varPop4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Float64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
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
	return nil
}

func calculateMerge(srcCount, dstCount int64, srcSum, dstSum, srcVariance, dstVariance float64) float64 {
	srcCountFloat64 := float64(srcCount)
	dstCountFloat64 := float64(dstCount)

	t := (srcCountFloat64 / dstCountFloat64) * dstSum - srcSum
	dstVariance += srcVariance + ((dstCountFloat64 / srcCountFloat64) / (dstCountFloat64 + srcCountFloat64)) * t * t
	return dstVariance
}

func (e *varPop4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4Float64)(src), (*partialResult4Float64)(dst)
	if p1.count == 0 {
		return nil
	}
	if p2.count == 0 {
		p2.count = p1.count
		p2.sum = p1.sum
		p2.variance = p1.variance
		return nil
	}
	if p2.count != 0 && p1.count != 0 {
		p2.variance = calculateMerge(p1.count, p2.count, p1.sum, p2.sum, p1.variance, p2.variance)
		p2.count += p1.count
		p2.sum += p1.sum
	}
	return nil
}

