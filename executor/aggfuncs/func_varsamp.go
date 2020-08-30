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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseVarSampAggFunc struct {
	varPop4Float64
}

type varSamp4Float64 struct {
	baseVarSampAggFunc
}

func (e *varSamp4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	//TODO: if p.count = 1 variance will devided by zero
	variance := p.variance / float64(p.count-1)
	chk.AppendFloat64(e.ordinal, variance)
	return nil
}

type varSamp4DistinctFloat64 struct {
	baseVarSampAggFunc
}

func (e *varSamp4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	variance := p.variance / float64(p.count-1)
	chk.AppendFloat64(e.ordinal, variance)
	return nil
}
