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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinLowerSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}
	if types.IsBinaryStr(b.args[0].GetType()) {
		return nil
	}

Loop:
	for i := 0; i < input.NumRows(); i++ {
		str := result.GetBytes(i)
		for _, c := range str {
			if c >= utf8.RuneSelf {
				continue Loop
			}
		}
		for i := range str {
			if str[i] >= 'A' && str[i] <= 'Z' {
				str[i] += 'a' - 'A'
			}
		}
	}
	return nil
}

func (b *builtinLowerSig) vectorized() bool {
	return true
}

func (b *builtinRepeatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := 0; i < n; i++ {
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
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("repeat", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}
		if int64(byteLength) > int64(b.tp.Flen)/num {
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

func (b *builtinStringIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
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

func (b *builtinUpperSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}
	if types.IsBinaryStr(b.args[0].GetType()) {
		return nil
	}

Loop:
	for i := 0; i < input.NumRows(); i++ {
		str := result.GetBytes(i)
		for _, c := range str {
			if c >= utf8.RuneSelf {
				continue Loop
			}
		}
		for i := range str {
			if str[i] >= 'a' && str[i] <= 'z' {
				str[i] -= 'a' - 'A'
			}
		}
	}
	return nil
}

func (b *builtinUpperSig) vectorized() bool {
	return true
}

func (b *builtinLeftSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		runes, leftLength := []rune(str), int(nums[i])
		if runeLength := len(runes); leftLength > runeLength {
			leftLength = runeLength
		} else if leftLength < 0 {
			leftLength = 0
		}

		result.AppendString(string(runes[:leftLength]))
	}
	return nil
}

func (b *builtinLeftSig) vectorized() bool {
	return true
}

func (b *builtinRightSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		runes := []rune(str)
		strLength, rightLength := len(runes), int(nums[i])
		if rightLength > strLength {
			rightLength = strLength
		} else if rightLength < 0 {
			rightLength = 0
		}

		result.AppendString(string(runes[strLength-rightLength:]))
	}
	return nil
}

func (b *builtinRightSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		num := nums[i]
		if num < 0 {
			num = 0
		}
		if uint64(num) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("space", b.maxAllowedPacket))
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
func (b *builtinReverseSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		str := result.GetString(i)
		reversed := reverseRunes([]rune(str))
		result.SetRaw(i, []byte(string(reversed)))
	}
	return nil
}

func (b *builtinReverseSig) vectorized() bool {
	return true
}

func (b *builtinConcatSig) vectorized() bool {
	return true
}

// vecEvalString evals a CONCAT(str1,str2,...)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	strs := make([][]byte, n)
	isNulls := make([]bool, n)
	result.ReserveString(n)
	var byteBuf []byte
	for j := 0; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, buf); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if isNulls[i] {
				continue
			}
			if buf.IsNull(i) {
				isNulls[i] = true
				continue
			}
			byteBuf = buf.GetBytes(i)
			if uint64(len(strs[i])+len(byteBuf)) > b.maxAllowedPacket {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("concat", b.maxAllowedPacket))
				isNulls[i] = true
				continue
			}
			strs[i] = append(strs[i], byteBuf...)
		}
	}
	for i := 0; i < n; i++ {
		if isNulls[i] {
			result.AppendNull()
		} else {
			result.AppendBytes(strs[i])
		}
	}
	return nil
}

func (b *builtinLocate3ArgsSig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str,pos), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}
	// store positions in result
	if err := b.args[2].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		pos := i64s[i]

		// Transfer the argument which starts from 1 to real index which starts from 0.
		pos--
		strLen := int64(len([]rune(str)))
		subStrLen := int64(len([]rune(subStr)))
		if pos < 0 || pos > strLen-subStrLen {
			i64s[i] = 0
			continue
		} else if subStrLen == 0 {
			i64s[i] = pos + 1
			continue
		}
		slice := string([]rune(str)[pos:])
		subStr = strings.ToLower(subStr)
		slice = strings.ToLower(slice)
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

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.ToUpper(hex.EncodeToString(buf0.GetBytes(i))))
	}
	return nil
}

func (b *builtinLTrimSig) vectorized() bool {
	return true
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		result.AppendString(strings.TrimLeft(str, spaceChars))
	}

	return nil
}

func (b *builtinQuoteSig) vectorized() bool {
	return true
}

func (b *builtinQuoteSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendString("NULL")
			continue
		}
		str := buf.GetString(i)
		result.AppendString(Quote(str))
	}
	return nil
}

func (b *builtinInsertBinarySig) vectorized() bool {
	return false
}

func (b *builtinInsertBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinConcatWSSig) vectorized() bool {
	return false
}

func (b *builtinConcatWSSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinConvertSig) vectorized() bool {
	return false
}

func (b *builtinConvertSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstringIndexSig) vectorized() bool {
	return true
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	counts := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		delim := buf1.GetString(i)
		count := counts[i]

		if len(delim) == 0 {
			result.AppendString("")
			continue
		}

		strs := strings.Split(str, delim)
		start, end := int64(0), int64(len(strs))
		if count > 0 {
			// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
			if count < end {
				end = count
			}
		} else {
			// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
			count = -count
			if count < 0 {
				// -count overflows max int64, returns an empty string.
				result.AppendString("")
				continue
			}

			if count < end {
				start = end - count
			}
		}
		substrs := strs[start:end]
		result.AppendString(strings.Join(substrs, delim))
	}

	return nil
}

func (b *builtinUnHexSig) vectorized() bool {
	return true
}

func (b *builtinUnHexSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		if len(str)%2 != 0 {
			str = "0" + str
		}
		bs, e := hex.DecodeString(str)
		if e != nil {
			result.AppendNull()
			continue
		}
		result.AppendString(string(bs))
	}
	return nil
}

func (b *builtinExportSet3ArgSig) vectorized() bool {
	return false
}

func (b *builtinExportSet3ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinASCIISig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		if len(str) == 0 {
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(str[0])
	}
	return nil
}

func (b *builtinLpadBinarySig) vectorized() bool {
	return false
}

func (b *builtinLpadBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLpadSig) vectorized() bool {
	return true
}

// vecEvalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf1.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("lpad", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}
		if buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		padStr := buf2.GetString(i)
		runeLength := len([]rune(str))
		padLength := len([]rune(padStr))

		if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
			result.AppendNull()
			continue
		}
		if tailLen := targetLength - runeLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = string([]rune(strings.Repeat(padStr, repeatCount))[:tailLen]) + str
		}
		result.AppendString(string([]rune(str)[:targetLength]))
	}
	return nil
}

func (b *builtinFindInSetSig) vectorized() bool {
	return false
}

func (b *builtinFindInSetSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLeftBinarySig) vectorized() bool {
	return true
}

func (b *builtinLeftBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}
	left := buf2.Int64s()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		leftLength, str := int(left[i]), buf.GetString(i)
		if strLength := len(str); leftLength > strLength {
			leftLength = strLength
		} else if leftLength < 0 {
			leftLength = 0
		}
		result.AppendString(str[:leftLength])
	}
	return nil
}

func (b *builtinReverseBinarySig) vectorized() bool {
	return true
}

func (b *builtinReverseBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		reversed := reverseBytes(result.GetBytes(i))
		result.SetRaw(i, reversed)
	}
	return nil
}

func (b *builtinRTrimSig) vectorized() bool {
	return true
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		result.AppendString(strings.TrimRight(str, spaceChars))
	}

	return nil
}

func (b *builtinStrcmpSig) vectorized() bool {
	return false
}

func (b *builtinStrcmpSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLocateBinary2ArgsSig) vectorized() bool {
	return false
}

func (b *builtinLocateBinary2ArgsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLocateBinary3ArgsSig) vectorized() bool {
	return false
}

func (b *builtinLocateBinary3ArgsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinExportSet4ArgSig) vectorized() bool {
	return false
}

func (b *builtinExportSet4ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRpadBinarySig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, strBuf); err != nil {
		return err
	}
	lenBuf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lenBuf)
	if err := b.args[1].VecEvalInt(b.ctx, input, lenBuf); err != nil {
		return err
	}
	padBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(padBuf)
	if err := b.args[2].VecEvalString(b.ctx, input, padBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := lenBuf.Int64s()
	lenBuf.MergeNulls(strBuf)
	for i := 0; i < n; i++ {
		if lenBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}

		if padBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := strBuf.GetString(i)
		strLength := len(str)
		padStr := padBuf.GetString(i)
		padLength := len(padStr)
		if targetLength < 0 || targetLength > b.tp.Flen || (strLength < targetLength && padLength == 0) {
			result.AppendNull()
			continue
		}
		if tailLen := targetLength - strLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = str + strings.Repeat(padStr, repeatCount)
		}
		result.AppendString(str[:targetLength])
	}
	return nil
}

func (b *builtinFormatWithLocaleSig) vectorized() bool {
	return false
}

func (b *builtinFormatWithLocaleSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstringBinary2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinSubstringBinary2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := nums[i]
		length := int64(len(str))
		if pos < 0 {
			pos += length
		} else {
			pos--
		}
		if pos > length || pos < 0 {
			pos = length
		}
		result.AppendString(str[pos:])
	}
	return nil
}

func (b *builtinSubstring2ArgsSig) vectorized() bool {
	return true
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := nums[i]

		runes := []rune(str)
		length := int64(len(runes))
		if pos < 0 {
			pos += length
		} else {
			pos--
		}
		if pos > length || pos < 0 {
			pos = length
		}
		result.AppendString(string(runes[pos:]))
	}

	return nil
}

func (b *builtinTrim2ArgsSig) vectorized() bool {
	return true
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		remstr := buf1.GetString(i)
		result.AppendString(trimRight(trimLeft(str, remstr), remstr))
	}

	return nil
}

func (b *builtinInstrSig) vectorized() bool {
	return false
}

func (b *builtinInstrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinOctStringSig) vectorized() bool {
	return true
}

func (b *builtinOctStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		negative, overflow := false, false
		str := buf.GetString(i)
		str = getValidPrefix(strings.TrimSpace(str), 10)
		if len(str) == 0 {
			result.AppendString("0")
			continue
		}
		if str[0] == '-' {
			negative, str = true, str[1:]
		}
		numVal, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			numError, ok := err.(*strconv.NumError)
			if !ok || numError.Err != strconv.ErrRange {
				return err
			}
			overflow = true
		}
		if negative && !overflow {
			numVal = -numVal
		}
		result.AppendString(strconv.FormatUint(numVal, 8))
	}
	return nil
}

func (b *builtinEltSig) vectorized() bool {
	return true
}

// vecEvalString evals a ELT(N,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf0.Int64s()
	argLen := len(b.args)
	bufs := make([]*chunk.Column, argLen)
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		j := i64s[i]
		if j < 1 || j >= int64(argLen) {
			result.AppendNull()
			continue
		}
		if bufs[j] == nil {
			bufs[j], err = b.bufAllocator.get(types.ETString, n)
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(bufs[j])
			if err := b.args[j].VecEvalString(b.ctx, input, bufs[j]); err != nil {
				return err
			}
		}
		if bufs[j].IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(bufs[j].GetString(i))
	}
	return nil
}

func (b *builtinInsertSig) vectorized() bool {
	return true
}

// vecEvalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_insert
func (b *builtinInsertSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}
	buf3, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf3)
	if err := b.args[3].VecEvalString(b.ctx, input, buf3); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s1 := buf1.Int64s()
	i64s2 := buf2.Int64s()
	buf1.MergeNulls(buf2)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf3.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		pos := i64s1[i]
		length := i64s2[i]
		newstr := buf3.GetString(i)

		runes := []rune(str)
		runeLength := int64(len(runes))
		if pos < 1 || pos > runeLength {
			result.AppendString(str)
			continue
		}
		if length > runeLength-pos+1 || length < 0 {
			length = runeLength - pos + 1
		}

		strHead := string(runes[0 : pos-1])
		strTail := string(runes[pos+length-1:])
		if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("insert", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}
		result.AppendString(strHead + newstr + strTail)
	}
	return nil
}

func (b *builtinExportSet5ArgSig) vectorized() bool {
	return false
}

func (b *builtinExportSet5ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstring3ArgsSig) vectorized() bool {
	return true
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	positions := buf1.Int64s()
	lengths := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := positions[i]
		length := lengths[i]
		runes := []rune(str)
		numRunes := int64(len(runes))
		if pos < 0 {
			pos += numRunes
		} else {
			pos--
		}
		if pos > numRunes || pos < 0 {
			pos = numRunes
		}
		end := pos + length
		if end < pos {
			result.AppendString("")
			continue
		} else if end < numRunes {
			result.AppendString(string(runes[pos:end]))
			continue
		}
		result.AppendString(string(runes[pos:]))
	}

	return nil
}

func (b *builtinTrim3ArgsSig) vectorized() bool {
	return false
}

func (b *builtinTrim3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinOrdSig) vectorized() bool {
	return true
}

func (b *builtinOrdSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		i64s[i] = ord(str)
	}
	return nil
}

func (b *builtinInstrBinarySig) vectorized() bool {
	return false
}

func (b *builtinInstrBinarySig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLengthSig) vectorized() bool {
	return true
}

// vecEvalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str))
	}
	return nil
}

func (b *builtinLocate2ArgsSig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		subStrLen := int64(len([]rune(subStr)))
		if subStrLen == 0 {
			i64s[i] = 1
			continue
		}
		slice := string([]rune(str))
		slice = strings.ToLower(slice)
		subStr = strings.ToLower(subStr)
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = int64(utf8.RuneCountInString(slice[:idx])) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinBitLengthSig) vectorized() bool {
	return true
}

func (b *builtinBitLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str) * 8)
	}
	return nil
}

func (b *builtinCharSig) vectorized() bool {
	return false
}

func (b *builtinCharSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinReplaceSig) vectorized() bool {
	return true
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		oldStr := buf1.GetString(i)
		newStr := buf2.GetString(i)
		if oldStr == "" {
			result.AppendString(str)
			continue
		}
		str = strings.Replace(str, oldStr, newStr, -1)
		result.AppendString(str)
	}
	return nil
}

func (b *builtinMakeSetSig) vectorized() bool {
	return true
}

// evalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	bitsBuf, err := b.bufAllocator.get(types.ETInt, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bitsBuf)
	if err := b.args[0].VecEvalInt(b.ctx, input, bitsBuf); err != nil {
		return err
	}

	strBuf := make([]*chunk.Column, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		strBuf[i-1], err = b.bufAllocator.get(types.ETString, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(strBuf[i-1])
		if err := b.args[i].VecEvalString(b.ctx, input, strBuf[i-1]); err != nil {
			return err
		}
	}

	bits := bitsBuf.Int64s()
	result.ReserveString(nr)
	sets := make([]string, 0, len(b.args)-1)
	for i := 0; i < nr; i++ {
		if bitsBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		sets = sets[:0]
		for j := 0; j < len(b.args)-1; j++ {
			if strBuf[j].IsNull(i) || (bits[i]&(1<<uint(j))) == 0 {
				continue
			}
			sets = append(sets, strBuf[j].GetString(i))
		}
		result.AppendString(strings.Join(sets, ","))
	}

	return nil
}

func (b *builtinOctIntSig) vectorized() bool {
	return true
}

func (b *builtinOctIntSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strconv.FormatUint(uint64(nums[i]), 8))
	}
	return nil
}

func (b *builtinToBase64Sig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		needEncodeLen := base64NeededEncodedLength(len(str))
		if needEncodeLen == -1 {
			result.AppendNull()
			continue
		} else if needEncodeLen > int(b.maxAllowedPacket) {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("to_base64", b.maxAllowedPacket))
			result.AppendNull()
			continue
		} else if b.tp.Flen == -1 || b.tp.Flen > mysql.MaxBlobWidth {
			result.AppendNull()
			continue
		}

		newStr := base64.StdEncoding.EncodeToString([]byte(str))
		//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
		count := len(newStr)
		if count > 76 {
			newStr = strings.Join(splitToSubN(newStr, 76), "\n")
		}
		result.AppendString(newStr)
	}
	return nil
}

func (b *builtinTrim1ArgSig) vectorized() bool {
	return true
}

func (b *builtinTrim1ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		result.AppendString(strings.Trim(str, spaceChars))
	}

	return nil
}

func (b *builtinRpadSig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf1.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}
		if buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		padStr := buf2.GetString(i)
		runeLength := len([]rune(str))
		padLength := len([]rune(padStr))

		if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
			result.AppendNull()
			continue
		}
		if tailLen := targetLength - runeLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = str + strings.Repeat(padStr, repeatCount)
		}
		result.AppendString(string([]rune(str)[:targetLength]))
	}
	return nil
}

func (b *builtinCharLengthBinarySig) vectorized() bool {
	return false
}

func (b *builtinCharLengthBinarySig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBinSig) vectorized() bool {
	return true
}

func (b *builtinBinSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(fmt.Sprintf("%b", uint64(nums[i])))
	}
	return nil
}

func (b *builtinFormatSig) vectorized() bool {
	return false
}

func (b *builtinFormatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRightBinarySig) vectorized() bool {
	return true
}

func (b *builtinRightBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}
	right := buf2.Int64s()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str, rightLength := buf.GetString(i), int(right[i])
		strLength := len(str)
		if rightLength > strLength {
			rightLength = strLength
		} else if rightLength < 0 {
			rightLength = 0
		}
		result.AppendString(str[strLength-rightLength:])
	}
	return nil
}

func (b *builtinSubstringBinary3ArgsSig) vectorized() bool {
	return true
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	positions := buf1.Int64s()
	lengths := buf2.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := positions[i]
		length := lengths[i]

		byteLen := int64(len(str))
		if pos < 0 {
			pos += byteLen
		} else {
			pos--
		}
		if pos > byteLen || pos < 0 {
			pos = byteLen
		}
		end := pos + length
		if end < pos {
			result.AppendString("")
			continue
		} else if end < byteLen {
			result.AppendString(str[pos:end])
			continue
		}
		result.AppendString(str[pos:])
	}

	return nil
}

func (b *builtinHexIntArgSig) vectorized() bool {
	return true
}

func (b *builtinHexIntArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	i64s := buf.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.ToUpper(fmt.Sprintf("%x", uint64(i64s[i]))))
	}
	return nil
}

func (b *builtinFromBase64Sig) vectorized() bool {
	return true
}

// vecEvalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		needDecodeLen := base64NeededDecodedLength(len(str))
		if needDecodeLen == -1 {
			result.AppendNull()
			continue
		} else if needDecodeLen > int(b.maxAllowedPacket) {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("from_base64", b.maxAllowedPacket))
			result.AppendNull()
			continue
		}

		str = strings.Replace(str, "\t", "", -1)
		str = strings.Replace(str, " ", "", -1)
		newStr, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			// When error happens, take `from_base64("asc")` as an example, we should return NULL.
			result.AppendNull()
			continue
		}
		result.AppendString(string(newStr))
	}
	return nil
}

func (b *builtinCharLengthSig) vectorized() bool {
	return true
}

func (b *builtinCharLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		i64s[i] = int64(len([]rune(str)))
	}
	return nil
}
