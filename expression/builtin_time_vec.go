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

package expression

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := 0; i < len(times); i++ {
		if result.IsNull(i) {
			continue
		}
		if times[i].IsZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			times[i].Time = types.FromDate(times[i].Time.Year(), times[i].Time.Month(), times[i].Time.Day(), 0, 0, 0, 0)
			times[i].Type = mysql.TypeDate
		}
	}
	return nil
}

func (b *builtinDateSig) vectorized() bool {
	return true
}
