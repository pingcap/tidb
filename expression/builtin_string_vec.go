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

package expression

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"unicode/utf8"
)

func (b *builtinLowerSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}
	if types.IsBinaryStr(b.args[0].GetType()) {
		return nil
	}
	for i := 0; i < input.NumRows(); i++ {
		str := result.GetBytes(i)
		isASCII := true
		for _, c := range str {
			if c >= utf8.RuneSelf {
				isASCII = false
			}
		}
		if !isASCII {
			continue
		}
		for i := range str {
			if str[i] >= 'A' && str[i] <= 'Z' {
				str[i] += 'a' - 'A'
			}
		}
	}
	return nil
}
