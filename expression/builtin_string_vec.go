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
		// TODO: introduce vectorized null-bitmap to speed it up.
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
	return false
}

func (b *builtinConcatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLocate3ArgsSig) vectorized() bool {
	return false
}

func (b *builtinLocate3ArgsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinHexStrArgSig) vectorized() bool {
	return false
}

func (b *builtinHexStrArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLTrimSig) vectorized() bool {
	return false
}

func (b *builtinLTrimSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFieldStringSig) vectorized() bool {
	return false
}

func (b *builtinFieldStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinQuoteSig) vectorized() bool {
	return false
}

func (b *builtinQuoteSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinSubstringIndexSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinASCIISig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLpadBinarySig) vectorized() bool {
	return false
}

func (b *builtinLpadBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLpadSig) vectorized() bool {
	return false
}

func (b *builtinLpadSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFindInSetSig) vectorized() bool {
	return false
}

func (b *builtinFindInSetSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLeftBinarySig) vectorized() bool {
	return false
}

func (b *builtinLeftBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinReverseBinarySig) vectorized() bool {
	return false
}

func (b *builtinReverseBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRTrimSig) vectorized() bool {
	return false
}

func (b *builtinRTrimSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinRpadBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFormatWithLocaleSig) vectorized() bool {
	return false
}

func (b *builtinFormatWithLocaleSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstringBinary2ArgsSig) vectorized() bool {
	return false
}

func (b *builtinSubstringBinary2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstring2ArgsSig) vectorized() bool {
	return false
}

func (b *builtinSubstring2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTrim2ArgsSig) vectorized() bool {
	return false
}

func (b *builtinTrim2ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInstrSig) vectorized() bool {
	return false
}

func (b *builtinInstrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFieldRealSig) vectorized() bool {
	return false
}

func (b *builtinFieldRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinOctStringSig) vectorized() bool {
	return false
}

func (b *builtinOctStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEltSig) vectorized() bool {
	return false
}

func (b *builtinEltSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) || buf3.IsNull(i) {
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
	return false
}

func (b *builtinSubstring3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTrim3ArgsSig) vectorized() bool {
	return false
}

func (b *builtinTrim3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinOrdSig) vectorized() bool {
	return false
}

func (b *builtinOrdSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInstrBinarySig) vectorized() bool {
	return false
}

func (b *builtinInstrBinarySig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLengthSig) vectorized() bool {
	return false
}

func (b *builtinLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
		subStr := strings.ToLower(buf.GetString(i))
		str := strings.ToLower(buf1.GetString(i))

		if int64(len([]rune(subStr))) == 0 {
			i64s[i] = 1
			continue
		}
		slice := string([]rune(str))
		ret, idx := 0, strings.Index(slice, subStr)
		if idx != -1 {
			ret = utf8.RuneCountInString(slice[:idx]) + 1
		}
		i64s[i] = int64(ret)
	}
	return nil
}

func (b *builtinBitLengthSig) vectorized() bool {
	return false
}

func (b *builtinBitLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinMakeSetSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinToBase64Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTrim1ArgSig) vectorized() bool {
	return false
}

func (b *builtinTrim1ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRpadSig) vectorized() bool {
	return false
}

func (b *builtinRpadSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinRightBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubstringBinary3ArgsSig) vectorized() bool {
	return false
}

func (b *builtinSubstringBinary3ArgsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinHexIntArgSig) vectorized() bool {
	return false
}

func (b *builtinHexIntArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFieldIntSig) vectorized() bool {
	return false
}

func (b *builtinFieldIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFromBase64Sig) vectorized() bool {
	return false
}

func (b *builtinFromBase64Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCharLengthSig) vectorized() bool {
	return false
}

func (b *builtinCharLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
