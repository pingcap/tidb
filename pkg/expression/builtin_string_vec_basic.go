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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"encoding/hex"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

//revive:disable:defer

// runeByteIndex returns the byte index in str corresponding to the given rune index.
// If runeIdx exceeds the number of runes in str, len(str) is returned.
func runeByteIndex(str string, runeIdx int) int {
	byteIdx := 0
	for i := 0; i < runeIdx && byteIdx < len(str); i++ {
		_, size := utf8.DecodeRuneInString(str[byteIdx:])
		byteIdx += size
	}
	return byteIdx
}

func (b *builtinLowerSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	// if error is not nil return error, or builtinLowerSig is for binary strings (do nothing)
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinLowerSig) vectorized() bool {
	return true
}

func (b *builtinLowerUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(enc.ToLower(buf.GetString(i)))
		}
	}
	return nil
}

func (b *builtinLowerUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRepeatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		num := nums[i]
		if num < 1 {
			result.AppendString("")
			continue
		}
		if num > math.MaxInt32 {
			// to avoid overflow when calculating uint64(byteLength)*uint64(num) later
			num = math.MaxInt32
		}

		str := buf.GetString(i)
		byteLength := len(str)
		if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "repeat", b.maxAllowedPacket); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		if int64(byteLength) > int64(b.tp.GetFlen())/num {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.Repeat(str, int(num)))
	}
	return nil
}

func (b *builtinRepeatSig) vectorized() bool {
	return true
}

func (b *builtinStringIsNullSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinStringIsNullSig) vectorized() bool {
	return true
}

func (b *builtinUpperUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(enc.ToUpper(buf.GetString(i)))
		}
	}
	return nil
}

func (b *builtinUpperUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinUpperSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinUpperSig) vectorized() bool {
	return true
}

func (b *builtinLeftUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		leftLength := int(nums[i])
		if leftLength < 0 {
			leftLength = 0
		}

		byteIdx := 0
		for j := 0; j < leftLength && byteIdx < len(str); j++ {
			_, size := utf8.DecodeRuneInString(str[byteIdx:])
			byteIdx += size
		}
		result.AppendString(str[:byteIdx])
	}
	return nil
}

func (b *builtinLeftUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRightUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		strLength := utf8.RuneCountInString(str)
		rightLength := int(nums[i])
		if rightLength > strLength {
			rightLength = strLength
		} else if rightLength < 0 {
			rightLength = 0
		}

		// Skip (strLength - rightLength) runes from the start to find byte offset
		byteIdx := 0
		for j := 0; j < strLength-rightLength; j++ {
			_, size := utf8.DecodeRuneInString(str[byteIdx:])
			byteIdx += size
		}
		result.AppendString(str[byteIdx:])
	}
	return nil
}

func (b *builtinRightUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		num := max(nums[i], 0)
		if uint64(num) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "space", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		if num > mysql.MaxBlobWidth {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.Repeat(" ", int(num)))
	}
	return nil
}

func (b *builtinSpaceSig) vectorized() bool {
	return true
}

// vecEvalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		str := result.GetString(i)
		reversed := reverseRunes([]rune(str))
		result.SetRaw(i, []byte(string(reversed)))
	}
	return nil
}

func (b *builtinReverseUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinConcatSig) vectorized() bool {
	return true
}

// vecEvalString evals a CONCAT(str1,str2,...)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	strs := make([][]byte, n)
	isNulls := make([]bool, n)
	result.ReserveString(n)
	var byteBuf []byte
	for j := range b.args {
		if err := b.args[j].VecEvalString(ctx, input, buf); err != nil {
			return err
		}
		for i := range n {
			if isNulls[i] {
				continue
			}
			if buf.IsNull(i) {
				isNulls[i] = true
				continue
			}
			byteBuf = buf.GetBytes(i)
			if uint64(len(strs[i])+len(byteBuf)) > b.maxAllowedPacket {
				if err := handleAllowedPacketOverflowed(ctx, "concat", b.maxAllowedPacket); err != nil {
					return err
				}

				isNulls[i] = true
				continue
			}
			strs[i] = append(strs[i], byteBuf...)
		}
	}
	for i := range n {
		if isNulls[i] {
			result.AppendNull()
		} else {
			result.AppendBytes(strs[i])
		}
	}
	return nil
}

func (b *builtinLocate3ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str,pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	// store positions in result
	if err := b.args[2].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	ci := collate.IsCICollation(b.collation)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		pos := i64s[i]

		// Transfer the argument which starts from 1 to real index which starts from 0.
		pos--
		strLen := int64(utf8.RuneCountInString(str))
		subStrLen := int64(utf8.RuneCountInString(subStr))
		if pos < 0 || pos > strLen-subStrLen {
			i64s[i] = 0
			continue
		} else if subStrLen == 0 {
			i64s[i] = pos + 1
			continue
		}
		slice := str[runeByteIndex(str, int(pos)):]
		if ci {
			subStr = strings.ToLower(subStr)
			slice = strings.ToLower(slice)
		}
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = pos + int64(utf8.RuneCountInString(slice[:idx])) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinHexStrArgSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.ToUpper(hex.EncodeToString(buf0.GetBytes(i))))
	}
	return nil
}
