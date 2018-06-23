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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// All the AggFunc implementations are listed here for navigation.
var (
	// All the AggFunc implementations for "COUNT".
	// All the AggFunc implementations for "SUM".
	_ AggFunc = (*sum4Decimal)(nil)

	// All the AggFunc implementations for "AVG".
	_ AggFunc = (*avgDedup4Decimal)(nil)
	_ AggFunc = (*avgOriginal4Decimal)(nil)
	_ AggFunc = (*avgPartial4Decimal)(nil)

	_ AggFunc = (*avgDedup4Float64)(nil)
	_ AggFunc = (*avgOriginal4Float64)(nil)
	_ AggFunc = (*avgPartial4Float64)(nil)

	_ AggFunc = (*avgDedup4Float32)(nil)
	_ AggFunc = (*avgOriginal4Float32)(nil)
	_ AggFunc = (*avgPartial4Float32)(nil)

	// All the AggFunc implementations for "FIRSTROW".
	// All the AggFunc implementations for "MAX".
	// All the AggFunc implementations for "MIN".
	// All the AggFunc implementations for "GROUP_CONCAT".
	// All the AggFunc implementations for "BIT_OR".
	// All the AggFunc implementations for "BIT_XOR".
	// All the AggFunc implementations for "BIT_AND".
)

// AggFunc is the interface to evaluate aggregate functions.
type AggFunc interface {
	AllocPartialResult() []byte
	ResetPartialResult(partialBytes []byte)
	UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error

	AppendPartialResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error
	AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error
}

type baseAggFunc struct {
	input  []expression.Expression
	output []int
}
