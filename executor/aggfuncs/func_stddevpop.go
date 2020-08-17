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

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseStdDevPopAggFunc struct {
	varPop4Float64
}

type stdDevPop4Float64 struct {
	baseStdDevPopAggFunc
}

func (e *stdDevPop4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	varicance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, math.Sqrt(varicance))
	return nil
}

type stdDevPop4DistinctFloat64 struct {
	baseStdDevPopAggFunc
}

func (e *stdDevPop4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	varicance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, math.Sqrt(varicance))
	return nil
}
