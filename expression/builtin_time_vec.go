// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package expression

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinMonthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if buf.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				} else {
					result.SetNull(i, true)
					continue
				}
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Month())
	}
	return nil
}

func (b *builtinMonthSig) vectorized() bool {
	return true
}
