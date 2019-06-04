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

package tables

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (t *tableCommon) AllocAffinityID(ctx sessionctx.Context, r []types.Datum) (int64, error) {
	rowID, err := t.Allocator(ctx).Alloc(t.tableID)
	if err != nil {
		return 0, err
	}

	f := newExpColumnBitFn(t.affinityExpr, t.meta.Affinity.BitWidth)
	bitLen := f.fnBits()
	if bitLen > 0 {
		if OverflowShardBits(rowID, f.fnBits()) {
			return 0, autoid.ErrAutoincReadFailed
		}
		v := f.fn(r)
		rowID |= v << (64 - bitLen - 1)
	}
	return rowID, nil
}

type affinityFn interface {
	fnBits() uint64
	fn(r []types.Datum) int64
}

type exprColumnBitFn struct {
	exp      expression.Expression
	bitWidth uint64
	mask     int64
}

func newExpColumnBitFn(exp expression.Expression, bitWidth uint64) affinityFn {
	return &exprColumnBitFn{
		exp:      exp,
		bitWidth: bitWidth,
		mask:     (1 << bitWidth) - 1,
	}
}

func (e *exprColumnBitFn) fnBits() uint64 {
	return e.bitWidth
}

func (e *exprColumnBitFn) fn(r []types.Datum) int64 {
	d, err := e.exp.Eval(chunk.MutRowFromDatums(r).ToRow()) // bad performance but just for test.
	if err != nil {
		return -1
	}
	return d.GetInt64() & e.mask
}
