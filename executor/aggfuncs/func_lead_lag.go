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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseLeadLag struct {
	baseAggFunc
	valueEvaluator // TODO: move it to partial result when parallel execution is supported.

	defaultExpr expression.Expression
	offset      uint64
}

type partialResult4LeadLag struct {
	rows   []chunk.Row
	curIdx uint64
}

func (v *baseLeadLag) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4LeadLag{})
}

func (v *baseLeadLag) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4LeadLag)(pr)
	p.rows = p.rows[:0]
	p.curIdx = 0
}

func (v *baseLeadLag) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (int, error) {
	p := (*partialResult4LeadLag)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	return 0, nil
}

type lead struct {
	baseLeadLag
}

func (v *lead) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx+v.offset < uint64(len(p.rows)) {
		err = v.evaluateRow(sctx, v.args[0], p.rows[p.curIdx+v.offset])
	} else {
		err = v.evaluateRow(sctx, v.defaultExpr, p.rows[p.curIdx])
	}
	if err != nil {
		return err
	}
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}

type lag struct {
	baseLeadLag
}

func (v *lag) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx >= v.offset {
		err = v.evaluateRow(sctx, v.args[0], p.rows[p.curIdx-v.offset])
	} else {
		err = v.evaluateRow(sctx, v.defaultExpr, p.rows[p.curIdx])
	}
	if err != nil {
		return err
	}
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}
