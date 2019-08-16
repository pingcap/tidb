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
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type baseLeadLag struct {
	baseAggFunc

	defaultExpr expression.Expression
	offset      uint64
	retTp       *types.FieldType
}

type circleBuf struct {
	buf        []valueExtractor
	head, tail int
	size       int
}

func (cb *circleBuf) reset() {
	cb.buf = cb.buf[:0]
	cb.head, cb.tail = 0, 0
}

func (cb *circleBuf) append(e valueExtractor) {
	if len(cb.buf) < cb.size {
		cb.buf = append(cb.buf, e)
		cb.tail++
	} else {
		if cb.tail >= cb.size {
			cb.tail = 0
		}
		cb.buf[cb.tail] = e
		cb.tail++
	}
}

func (cb *circleBuf) get() (e valueExtractor) {
	if len(cb.buf) < cb.size {
		e = cb.buf[cb.head]
		cb.head++
	} else {
		if cb.tail >= cb.size {
			cb.tail = 0
		}
		e = cb.buf[cb.tail]
		cb.tail++
	}
	return e
}

type partialResult4Lead struct {
	seenRows              uint64
	curIdx                int
	extractors            []valueExtractor
	defaultExtractors     circleBuf
	defaultConstExtractor valueExtractor
}

const maxDefaultExtractorBufferSize = 1000

type lead struct {
	baseLeadLag
}

func (v *lead) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Lead{
		defaultExtractors: circleBuf{
			// Do not use v.offset directly since v.offset is defined by user
			// and may larger than a table size.
			buf:  make([]valueExtractor, 0, mathutil.MinUint64(v.offset, maxDefaultExtractorBufferSize)),
			size: int(v.offset),
		},
	})
}

func (v *lead) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Lead)(pr)
	p.seenRows = 0
	p.curIdx = 0
	p.extractors = p.extractors[:0]
	p.defaultExtractors.reset()
	p.defaultConstExtractor = nil
}

func (v *lead) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4Lead)(pr)
	for _, row := range rowsInGroup {
		p.seenRows++
		if p.seenRows > v.offset {
			e := buildValueExtractor(v.retTp)
			err = e.extractRow(sctx, v.args[0], row)
			if err != nil {
				return err
			}
			p.extractors = append(p.extractors, e)
		}
		if v.offset > 0 {
			if !v.defaultExpr.ConstItem() {
				// We must cache the results of last v.offset lines.
				e := buildValueExtractor(v.retTp)
				err = e.extractRow(sctx, v.defaultExpr, row)
				if err != nil {
					return err
				}
				p.defaultExtractors.append(e)
			} else if p.defaultConstExtractor == nil {
				e := buildValueExtractor(v.retTp)
				err = e.extractRow(sctx, v.defaultExpr, chunk.Row{})
				if err != nil {
					return err
				}
				p.defaultConstExtractor = e
			}
		}
	}
	return nil
}

func (v *lead) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Lead)(pr)
	var e valueExtractor
	if p.curIdx < len(p.extractors) {
		e = p.extractors[p.curIdx]
	} else {
		if !v.defaultExpr.ConstItem() {
			e = p.defaultExtractors.get()
		} else {
			e = p.defaultConstExtractor
		}
	}
	e.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}

type partialResult4Lag struct {
	seenRows          uint64
	curIdx            uint64
	extractors        []valueExtractor
	defaultExtractors []valueExtractor
}

type lag struct {
	baseLeadLag
}

func (v *lag) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4Lag{
		defaultExtractors: make([]valueExtractor, 0, mathutil.MinUint64(v.offset, maxDefaultExtractorBufferSize)),
	})
}

func (v *lag) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Lag)(pr)
	p.seenRows = 0
	p.curIdx = 0
	p.extractors = p.extractors[:0]
	p.defaultExtractors = p.defaultExtractors[:0]
}

func (v *lag) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4Lag)(pr)
	for _, row := range rowsInGroup {
		p.seenRows++
		if p.seenRows <= v.offset {
			e := buildValueExtractor(v.retTp)
			err = e.extractRow(sctx, v.defaultExpr, row)
			if err != nil {
				return err
			}
			p.defaultExtractors = append(p.defaultExtractors, e)
		}
		e := buildValueExtractor(v.retTp)
		err = e.extractRow(sctx, v.args[0], row)
		if err != nil {
			return err
		}
		p.extractors = append(p.extractors, e)
	}
	return nil
}

func (v *lag) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Lag)(pr)
	var e valueExtractor
	if p.curIdx < v.offset {
		e = p.defaultExtractors[p.curIdx]
	} else {
		e = p.extractors[p.curIdx-v.offset]
	}
	e.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}
