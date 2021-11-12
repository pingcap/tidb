// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var _ builtinFunc = &builtinCharsetConvSig{}

type builtinCharsetConvSig struct {
	baseBuiltinFunc
}

func (b *builtinCharsetConvSig) Clone() builtinFunc {
	newSig := &builtinCharsetConvSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCharsetConvSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tp := b.args[0].GetType()
	enc := charset.NewEncoding(tp.Charset)
	res, err = enc.EncodeString(val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, b.ctx.GetSessionVars().StmtCtx, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

func (b *builtinCharsetConvSig) vectorized() bool {
	return true
}

func (b *builtinCharsetConvSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		var str string
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str = buf.GetString(i)
		str, err = types.ProduceStrWithSpecifiedTp(str, b.tp, b.ctx.GetSessionVars().StmtCtx, false)
		if err != nil {
			return err
		}
		var d bool
		str, d, err = padZeroForBinaryType(str, b.tp, b.ctx)
		if err != nil {
			return err
		}
		if d {
			result.AppendNull()
		} else {
			result.AppendString(str)
		}
	}
	return nil
}
