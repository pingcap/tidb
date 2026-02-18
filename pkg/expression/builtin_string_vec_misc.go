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
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

//revive:disable:defer

func (b *builtinLocate3ArgsSig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
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

	result.MergeNulls(buf0, buf1)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		pos := i64s[i]
		// Transfer the argument which starts from 1 to real index which starts from 0.
		pos--
		subStr := buf0.GetString(i)
		str := buf1.GetString(i)
		subStrLen := len(subStr)
		if pos < 0 || pos > int64(len(str)-subStrLen) {
			i64s[i] = 0
			continue
		} else if subStrLen == 0 {
			i64s[i] = pos + 1
			continue
		}
		slice := str[pos:]
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = pos + int64(idx) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinExportSet4ArgSig) vectorized() bool {
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bits)
	if err := b.args[0].VecEvalInt(ctx, input, bits); err != nil {
		return err
	}
	on, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(on)
	if err := b.args[1].VecEvalString(ctx, input, on); err != nil {
		return err
	}
	off, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(off)
	if err := b.args[2].VecEvalString(ctx, input, off); err != nil {
		return err
	}
	separator, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(separator)
	if err := b.args[3].VecEvalString(ctx, input, separator); err != nil {
		return err
	}
	result.ReserveString(n)
	i64s := bits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) || separator.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			separator.GetString(i), 64))
	}
	return nil
}

func (b *builtinRpadSig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(ctx, input, strBuf); err != nil {
		return err
	}
	lenBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lenBuf)
	if err := b.args[1].VecEvalInt(ctx, input, lenBuf); err != nil {
		return err
	}
	padBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(padBuf)
	if err := b.args[2].VecEvalString(ctx, input, padBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := lenBuf.Int64s()
	lenBuf.MergeNulls(strBuf)
	for i := range n {
		if lenBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket); err != nil {
				return err
			}

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
		if targetLength < 0 || targetLength > b.tp.GetFlen() {
			result.AppendNull()
			continue
		}
		if strLength < targetLength && padLength == 0 {
			result.AppendString("")
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
	return true
}

func (b *builtinFormatWithLocaleSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dBuf)
	if err := b.args[1].VecEvalInt(ctx, input, dBuf); err != nil {
		return err
	}
	dInt64s := dBuf.Int64s()

	localeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(localeBuf)
	if err := b.args[2].VecEvalString(ctx, input, localeBuf); err != nil {
		return err
	}

	// decimal x
	if b.args[0].GetType(ctx).EvalType() == types.ETDecimal {
		xBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(xBuf)
		if err := b.args[0].VecEvalDecimal(ctx, input, xBuf); err != nil {
			return err
		}

		result.ReserveString(n)
		xBuf.MergeNulls(dBuf)
		return formatDecimal(ctx, xBuf, dInt64s, result, localeBuf)
	}

	// real x
	xBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(xBuf)
	if err := b.args[0].VecEvalReal(ctx, input, xBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	xBuf.MergeNulls(dBuf)
	return formatReal(ctx, xBuf, dInt64s, result, localeBuf)
}

func (b *builtinSubstring2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinSubstring2ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

func (b *builtinSubstring2ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		pos := nums[i]

		length := int64(utf8.RuneCountInString(str))
		if pos < 0 {
			pos += length
		} else {
			pos--
		}
		if pos > length || pos < 0 {
			pos = length
		}
		result.AppendString(str[runeByteIndex(str, int(pos)):])
	}

	return nil
}

func (b *builtinTrim2ArgsSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	result.ReserveString(n)
	for i := range n {
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

func (b *builtinInstrUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinInstrUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	substr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(substr)
	if err := b.args[1].VecEvalString(ctx, input, substr); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(str, substr)
	res := result.Int64s()
	ci := collate.IsCICollation(b.collation)
	var strI string
	var substrI string
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ci {
			strI = strings.ToLower(str.GetString(i))
			substrI = strings.ToLower(substr.GetString(i))
		} else {
			strI = str.GetString(i)
			substrI = substr.GetString(i)
		}
		idx := strings.Index(strI, substrI)
		if idx == -1 {
			res[i] = 0
			continue
		}
		res[i] = int64(utf8.RuneCountInString(strI[:idx]) + 1)
	}
	return nil
}

func (b *builtinOctStringSig) vectorized() bool {
	return true
}

func (b *builtinOctStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		// for issue #59446 should return NULL for empty string
		str := buf.GetString(i)
		if len(str) == 0 {
			result.AppendNull()
			continue
		}

		negative, overflow := false, false
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
func (b *builtinEltSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf0.Int64s()
	argLen := len(b.args)
	bufs := make([]*chunk.Column, argLen)
	for i := range n {
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
			bufs[j], err = b.bufAllocator.get()
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(bufs[j])
			if err := b.args[j].VecEvalString(ctx, input, bufs[j]); err != nil {
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

func (b *builtinInsertUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_insert
func (b *builtinInsertUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	buf3, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf3)
	if err := b.args[3].VecEvalString(ctx, input, buf3); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s1 := buf1.Int64s()
	i64s2 := buf2.Int64s()
	buf1.MergeNulls(buf2)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf3.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		pos := i64s1[i]
		length := i64s2[i]
		newstr := buf3.GetString(i)

		runeLength := int64(utf8.RuneCountInString(str))
		if pos < 1 || pos > runeLength {
			result.AppendString(str)
			continue
		}
		if length > runeLength-pos+1 || length < 0 {
			length = runeLength - pos + 1
		}

		headEnd := runeByteIndex(str, int(pos-1))
		tailStart := runeByteIndex(str, int(pos+length-1))
		strHead := str[:headEnd]
		strTail := str[tailStart:]
		if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		result.AppendString(strHead + newstr + strTail)
	}
	return nil
}

func (b *builtinExportSet5ArgSig) vectorized() bool {
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bits)
	if err := b.args[0].VecEvalInt(ctx, input, bits); err != nil {
		return err
	}
	on, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(on)
	if err := b.args[1].VecEvalString(ctx, input, on); err != nil {
		return err
	}
	off, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(off)
	if err := b.args[2].VecEvalString(ctx, input, off); err != nil {
		return err
	}
	separator, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(separator)
	if err := b.args[3].VecEvalString(ctx, input, separator); err != nil {
		return err
	}
	numberOfBits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(numberOfBits)
	if err := b.args[4].VecEvalInt(ctx, input, numberOfBits); err != nil {
		return err
	}
	result.ReserveString(n)
	bits.MergeNulls(numberOfBits)
	i64s := bits.Int64s()
	i64s2 := numberOfBits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) || separator.IsNull(i) {
			result.AppendNull()
			continue
		}
		if i64s2[i] < 0 || i64s2[i] > 64 {
			i64s2[i] = 64
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			separator.GetString(i), i64s2[i]))
	}
	return nil
}

func (b *builtinSubstring3ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	positions := buf1.Int64s()
	lengths := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := positions[i]
		length := lengths[i]
		numRunes := int64(utf8.RuneCountInString(str))
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
		}
		bytePos := runeByteIndex(str, int(pos))
		if end < numRunes {
			byteEnd := bytePos + runeByteIndex(str[bytePos:], int(end-pos))
			result.AppendString(str[bytePos:byteEnd])
			continue
		}
		result.AppendString(str[bytePos:])
	}

	return nil
}

func (b *builtinTrim3ArgsSig) vectorized() bool {
	return true
}

func (b *builtinTrim3ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		direction := ast.TrimDirectionType(buf2.GetInt64(i))
		baseStr := buf0.GetString(i)
		remStr := buf1.GetString(i)
		switch direction {
		case ast.TrimLeading:
			result.AppendString(trimLeft(baseStr, remStr))
		case ast.TrimTrailing:
			result.AppendString(trimRight(baseStr, remStr))
		default:
			tmpStr := trimLeft(baseStr, remStr)
			result.AppendString(trimRight(tmpStr, remStr))
		}
	}
	return nil
}

func (b *builtinOrdSig) vectorized() bool {
	return true
}

func (b *builtinOrdSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	var x [4]byte
	encBuf := bytes.NewBuffer(x[:])
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strBytes := buf.GetBytes(i)
		w := len(charset.EncodingUTF8Impl.Peek(strBytes))
		val, err := enc.Transform(encBuf, strBytes[:w], charset.OpEncode)
		if err != nil {
			i64s[i] = calcOrd(strBytes[:1])
			continue
		}
		// Only the first character is considered.
		i64s[i] = calcOrd(val[:len(enc.Peek(val))])
	}
	return nil
}

func (b *builtinInstrSig) vectorized() bool {
	return true
}

func (b *builtinInstrSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	substr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(substr)
	if err := b.args[1].VecEvalString(ctx, input, substr); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(str, substr)
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strI := str.GetString(i)
		substrI := substr.GetString(i)
		idx := strings.Index(strI, substrI)
		res[i] = int64(idx + 1)
	}
	return nil
}

func (b *builtinLengthSig) vectorized() bool {
	return true
}

// vecEvalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str))
	}
	return nil
}

func (b *builtinLocate2ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	result.ResizeInt64(n, false)
	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	ci := collate.IsCICollation(b.collation)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		subStrLen := int64(utf8.RuneCountInString(subStr))
		if subStrLen == 0 {
			i64s[i] = 1
			continue
		}
		slice := str
		if ci {
			slice = strings.ToLower(slice)
			subStr = strings.ToLower(subStr)
		}
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

func (b *builtinBitLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str) * 8)
	}
	return nil
}

func (b *builtinCharSig) vectorized() bool {
	return true
}

func (b *builtinCharSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	l := len(b.args)
	buf := make([]*chunk.Column, l-1)
	for i := range len(b.args) - 1 {
		te, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		buf[i] = te
		defer b.bufAllocator.put(buf[i])
		if err := b.args[i].VecEvalInt(ctx, input, buf[i]); err != nil {
			return err
		}
	}
	bufstr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufstr)
	bigints := make([]int64, 0, l-1)
	result.ReserveString(n)
	bufint := make([]([]int64), l-1)
	for i := range l - 1 {
		bufint[i] = buf[i].Int64s()
	}
	encBuf := &bytes.Buffer{}
	enc := charset.FindEncoding(b.tp.GetCharset())
	hasStrictMode := sqlMode(ctx).HasStrictMode()
	for i := range n {
		bigints = bigints[0:0]
		for j := range l - 1 {
			if buf[j].IsNull(i) {
				continue
			}
			bigints = append(bigints, bufint[j][i])
		}
		dBytes := b.convertToBytes(bigints)
		resultBytes, err := enc.Transform(encBuf, dBytes, charset.OpDecode)
		if err != nil {
			tc := typeCtx(ctx)
			tc.AppendWarning(err)
			if hasStrictMode {
				result.AppendNull()
				continue
			}
		}
		result.AppendString(string(resultBytes))
	}
	return nil
}

func (b *builtinReplaceSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
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
		str = strings.ReplaceAll(str, oldStr, newStr)
		result.AppendString(str)
	}
	return nil
}

func (b *builtinMakeSetSig) vectorized() bool {
	return true
}

// vecEvalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	bitsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bitsBuf)
	if err := b.args[0].VecEvalInt(ctx, input, bitsBuf); err != nil {
		return err
	}

	strBuf := make([]*chunk.Column, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		strBuf[i-1], err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(strBuf[i-1])
		if err := b.args[i].VecEvalString(ctx, input, strBuf[i-1]); err != nil {
			return err
		}
	}

	bits := bitsBuf.Int64s()
	result.ReserveString(nr)
	sets := make([]string, 0, len(b.args)-1)
	for i := range nr {
		if bitsBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		sets = sets[:0]
		for j := range len(b.args) - 1 {
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

func (b *builtinOctIntSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(strconv.FormatUint(uint64(nums[i]), 8))
	}
	return nil
}

func (b *builtinToBase64Sig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range n {
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
			if err := handleAllowedPacketOverflowed(ctx, "to_base64", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		} else if b.tp.GetFlen() == -1 || b.tp.GetFlen() > mysql.MaxBlobWidth {
			b.tp.SetFlen(mysql.MaxBlobWidth)
		}

		newStr := base64.StdEncoding.EncodeToString([]byte(str))
		// A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
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

func (b *builtinTrim1ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		result.AppendString(strings.Trim(str, spaceChars))
	}

	return nil
}

func (b *builtinRpadUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf1.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		if buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		padStr := buf2.GetString(i)
		runeLength := utf8.RuneCountInString(str)
		padLength := utf8.RuneCountInString(padStr)

		if targetLength < 0 || targetLength*4 > b.tp.GetFlen() {
			result.AppendNull()
			continue
		}
		if runeLength < targetLength && padLength == 0 {
			result.AppendString("")
			continue
		}
		if tailLen := targetLength - runeLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = str + strings.Repeat(padStr, repeatCount)
		}
		result.AppendString(str[:runeByteIndex(str, targetLength)])
	}
	return nil
}

func (b *builtinCharLengthBinarySig) vectorized() bool {
	return true
}

func (b *builtinCharLengthBinarySig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		res[i] = int64(len(str))
	}
	return nil
}

func (b *builtinBinSig) vectorized() bool {
	return true
}

func (b *builtinBinSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(fmt.Sprintf("%b", uint64(nums[i])))
	}
	return nil
}

func (b *builtinFormatSig) vectorized() bool {
	return true
}

// vecEvalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dBuf)
	if err := b.args[1].VecEvalInt(ctx, input, dBuf); err != nil {
		return err
	}
	dInt64s := dBuf.Int64s()

	// decimal x
	if b.args[0].GetType(ctx).EvalType() == types.ETDecimal {
		xBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(xBuf)
		if err := b.args[0].VecEvalDecimal(ctx, input, xBuf); err != nil {
			return err
		}

		result.ReserveString(n)
		xBuf.MergeNulls(dBuf)
		return formatDecimal(ctx, xBuf, dInt64s, result, nil)
	}

	// real x
	xBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(xBuf)
	if err := b.args[0].VecEvalReal(ctx, input, xBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	xBuf.MergeNulls(dBuf)
	return formatReal(ctx, xBuf, dInt64s, result, nil)
}

func (b *builtinRightSig) vectorized() bool {
	return true
}

func (b *builtinRightSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	right := buf2.Int64s()
	result.ReserveString(n)
	for i := range n {
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

func (b *builtinSubstring3ArgsSig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	positions := buf1.Int64s()
	lengths := buf2.Int64s()
	for i := range n {
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

func (b *builtinHexIntArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	i64s := buf.Int64s()
	for i := range n {
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
func (b *builtinFromBase64Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range n {
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
			if err := handleAllowedPacketOverflowed(ctx, "from_base64", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}

		str = strings.ReplaceAll(str, "\t", "")
		str = strings.ReplaceAll(str, " ", "")
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

func (b *builtinCharLengthUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinCharLengthUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		i64s[i] = int64(utf8.RuneCountInString(str))
	}
	return nil
}

func formatDecimal(ctx EvalContext, xBuf *chunk.Column, dInt64s []int64, result *chunk.Column, localeBuf *chunk.Column) error {
	xDecimals := xBuf.Decimals()
	for i := range xDecimals {
		if xBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		x, d := xDecimals[i], dInt64s[i]

		if d < 0 {
			d = 0
		} else if d > formatMaxDecimals {
			d = formatMaxDecimals
		}

		locale := "en_US"
		isNull := false
		if localeBuf == nil {
			// FORMAT(x, d)
		} else if localeBuf.IsNull(i) {
			// FORMAT(x, d, NULL)
			isNull = true
			tc := typeCtx(ctx)
			tc.AppendWarning(errUnknownLocale.FastGenByArgs("NULL"))
		} else {
			// force copy of the string
			// https://github.com/pingcap/tidb/issues/56193
			locale = strings.Clone(localeBuf.GetString(i))
		}

		xStr := roundFormatArgs(x.String(), int(d))
		dStr := strconv.FormatInt(d, 10)
		formatString, found, err := mysql.FormatByLocale(xStr, dStr, locale)
		if err != nil {
			return err
		}
		// Check 'found' flag, only warn for unknown locales
		if !isNull && !found {
			tc := typeCtx(ctx)
			if localeBuf != nil {
				locale = strings.Clone(localeBuf.GetString(i))
			}
			tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale))
		}
		result.AppendString(formatString)
	}
	return nil
}

func formatReal(ctx EvalContext, xBuf *chunk.Column, dInt64s []int64, result *chunk.Column, localeBuf *chunk.Column) error {
	xFloat64s := xBuf.Float64s()
	for i := range xFloat64s {
		if xBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		x, d := xFloat64s[i], dInt64s[i]

		if d < 0 {
			d = 0
		} else if d > formatMaxDecimals {
			d = formatMaxDecimals
		}

		locale := "en_US"
		isNull := false
		if localeBuf == nil {
			// FORMAT(x, d)
		} else if localeBuf.IsNull(i) {
			// FORMAT(x, d, NULL)
			isNull = true
			tc := typeCtx(ctx)
			tc.AppendWarning(errUnknownLocale.FastGenByArgs("NULL"))
		} else {
			// force copy of the string
			// https://github.com/pingcap/tidb/issues/56193
			locale = strings.Clone(localeBuf.GetString(i))
		}

		xStr := roundFormatArgs(strconv.FormatFloat(x, 'f', -1, 64), int(d))
		dStr := strconv.FormatInt(d, 10)

		formatString, found, err := mysql.FormatByLocale(xStr, dStr, locale)
		if err != nil {
			return err
		}
		if !isNull && !found {
			tc := typeCtx(ctx)
			if localeBuf != nil {
				locale = strings.Clone(localeBuf.GetString(i))
			}
			tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale))
		}
		result.AppendString(formatString)
	}
	return nil
}

func (b *builtinTranslateBinarySig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	var (
		mp           map[byte]uint16
		useCommonMap = false
	)
	_, isFromConst := b.args[1].(*Constant)
	_, isToConst := b.args[2].(*Constant)
	if isFromConst && isToConst {
		if !(ExprNotNull(ctx, b.args[1]) && ExprNotNull(ctx, b.args[2])) {
			for range n {
				result.AppendNull()
			}
			return nil
		}
		useCommonMap = true
		fromBytes, toBytes := []byte(buf1.GetString(0)), []byte(buf2.GetString(0))
		mp = buildTranslateMap4Binary(fromBytes, toBytes)
	}
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		srcStr := buf0.GetString(i)
		var tgt []byte
		if !useCommonMap {
			fromBytes, toBytes := []byte(buf1.GetString(i)), []byte(buf2.GetString(i))
			mp = buildTranslateMap4Binary(fromBytes, toBytes)
		}
		for _, charSrc := range []byte(srcStr) {
			if charTo, ok := mp[charSrc]; ok {
				if charTo != invalidByte {
					tgt = append(tgt, byte(charTo))
				}
			} else {
				tgt = append(tgt, charSrc)
			}
		}
		result.AppendString(string(tgt))
	}
	return nil
}

func (b *builtinTranslateBinarySig) vectorized() bool {
	return true
}

func (b *builtinTranslateUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	var (
		mp           map[rune]rune
		useCommonMap = false
	)
	_, isFromConst := b.args[1].(*Constant)
	_, isToConst := b.args[2].(*Constant)
	if isFromConst && isToConst {
		if !(ExprNotNull(ctx, b.args[1]) && ExprNotNull(ctx, b.args[2])) {
			for range n {
				result.AppendNull()
			}
			return nil
		}
		useCommonMap = true
		fromRunes, toRunes := []rune(buf1.GetString(0)), []rune(buf2.GetString(0))
		mp = buildTranslateMap4UTF8(fromRunes, toRunes)
	}
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		srcStr := buf0.GetString(i)
		var tgt strings.Builder
		if !useCommonMap {
			fromRunes, toRunes := []rune(buf1.GetString(i)), []rune(buf2.GetString(i))
			mp = buildTranslateMap4UTF8(fromRunes, toRunes)
		}
		for _, charSrc := range srcStr {
			if charTo, ok := mp[charSrc]; ok {
				if charTo != invalidRune {
					tgt.WriteRune(charTo)
				}
			} else {
				tgt.WriteRune(charSrc)
			}
		}
		result.AppendString(tgt.String())
	}
	return nil
}

func (b *builtinTranslateUTF8Sig) vectorized() bool {
	return true
}
