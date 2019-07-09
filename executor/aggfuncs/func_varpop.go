package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/errors"
)

type varPop4Float64 struct {
	baseAggFunc
}

type partialResult4Float64 struct {
	count int64
	sum float64
	quadratic_sum float64;
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
	varPop := p.quadratic_sum / float64(p.count) - p.sum * p.sum / float64(p.count * p.count)
	chk.AppendFloat64(e.ordinal, varPop)
	return nil
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
		p.sum += input
		p.quadratic_sum += input * input
		p.count++
	}
	return nil
}

func (e *varPop4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4Float64)(src), (*partialResult4Float64)(dst)
	if p1.count == 0 {
		return nil
	}
	p2.sum += p1.sum
	p2.quadratic_sum += p1.quadratic_sum
	p2.count += p1.count
	return nil
}
