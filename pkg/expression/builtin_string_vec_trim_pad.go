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
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

//revive:disable:defer

func (b *builtinLTrimSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(strings.TrimLeft(str, spaceChars))
	}

	return nil
}

func (b *builtinQuoteSig) vectorized() bool {
	return true
}

func (b *builtinQuoteSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			result.AppendString("NULL")
			continue
		}
		str := buf.GetString(i)
		result.AppendString(Quote(str))
	}
	return nil
}

func (b *builtinInsertSig) vectorized() bool {
	return true
}

func (b *builtinInsertSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	pos, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(pos)
	if err := b.args[1].VecEvalInt(ctx, input, pos); err != nil {
		return err
	}
	length, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(length)
	if err := b.args[2].VecEvalInt(ctx, input, length); err != nil {
		return err
	}
	newstr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(newstr)
	if err := b.args[3].VecEvalString(ctx, input, newstr); err != nil {
		return err
	}
	posIs := pos.Int64s()
	lengthIs := length.Int64s()
	result.ReserveString(n)
	for i := range n {
		if str.IsNull(i) || pos.IsNull(i) || length.IsNull(i) || newstr.IsNull(i) {
			result.AppendNull()
			continue
		}
		strI := str.GetString(i)
		strLength := int64(len(strI))
		posI := posIs[i]
		if posI < 1 || posI > strLength {
			result.AppendString(strI)
			continue
		}
		lengthI := lengthIs[i]
		if lengthI > strLength-posI+1 || lengthI < 0 {
			lengthI = strLength - posI + 1
		}
		newstrI := newstr.GetString(i)
		if uint64(strLength-lengthI+int64(len(newstrI))) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		result.AppendString(strI[0:posI-1] + newstrI + strI[posI+lengthI-1:])
	}
	return nil
}

func (b *builtinConcatWSSig) vectorized() bool {
	return true
}

// vecEvalString evals a CONCAT_WS(separator,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	argsLen := len(b.args)

	bufs := make([]*chunk.Column, argsLen)
	var err error
	for i := range argsLen {
		bufs[i], err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(bufs[i])
		if err := b.args[i].VecEvalString(ctx, input, bufs[i]); err != nil {
			return err
		}
	}

	isNulls := make([]bool, n)
	seps := make([]string, n)
	strs := make([][]string, n)
	for i := range n {
		if bufs[0].IsNull(i) {
			// If the separator is NULL, the result is NULL.
			isNulls[i] = true
			continue
		}
		isNulls[i] = false
		seps[i] = bufs[0].GetString(i)
		strs[i] = make([]string, 0, argsLen-1)
	}

	var strBuf string
	targetLengths := make([]int, n)
	for j := 1; j < argsLen; j++ {
		for i := range n {
			if isNulls[i] || bufs[j].IsNull(i) {
				// CONCAT_WS() does not skip empty strings. However,
				// it does skip any NULL values after the separator argument.
				continue
			}
			strBuf = bufs[j].GetString(i)
			targetLengths[i] += len(strBuf)
			if i > 1 {
				targetLengths[i] += len(seps[i])
			}
			if uint64(targetLengths[i]) > b.maxAllowedPacket {
				if err := handleAllowedPacketOverflowed(ctx, "concat_ws", b.maxAllowedPacket); err != nil {
					return err
				}

				isNulls[i] = true
				continue
			}
			strs[i] = append(strs[i], strBuf)
		}
	}
	result.ReserveString(n)
	for i := range n {
		if isNulls[i] {
			result.AppendNull()
			continue
		}
		str := strings.Join(strs[i], seps[i])
		// todo check whether the length of result is larger than flen
		// if b.tp.flen != types.UnspecifiedLength && len(str) > b.tp.flen {
		//	result.AppendNull()
		//	continue
		// }
		result.AppendString(str)
	}
	return nil
}

func (b *builtinConvertSig) vectorized() bool {
	return true
}

func (b *builtinConvertSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	expr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(expr)
	if err := b.args[0].VecEvalString(ctx, input, expr); err != nil {
		return err
	}
	argTp, resultTp := b.args[0].GetType(ctx), b.tp
	result.ReserveString(n)
	done := vecEvalStringConvertBinary(result, n, expr, argTp, resultTp)
	if done {
		return nil
	}
	enc := charset.FindEncoding(resultTp.GetCharset())
	encBuf := &bytes.Buffer{}
	for i := range n {
		if expr.IsNull(i) {
			result.AppendNull()
			continue
		}
		exprI := expr.GetBytes(i)
		if !enc.IsValid(exprI) {
			val, _ := enc.Transform(encBuf, exprI, charset.OpReplaceNoErr)
			result.AppendBytes(val)
		} else {
			result.AppendBytes(exprI)
		}
	}
	return nil
}

func vecEvalStringConvertBinary(result *chunk.Column, n int, expr *chunk.Column,
	argTp, resultTp *types.FieldType) (done bool) {
	var chs string
	var op charset.Op
	if types.IsBinaryStr(argTp) {
		chs = resultTp.GetCharset()
		op = charset.OpDecode
	} else if types.IsBinaryStr(resultTp) {
		chs = argTp.GetCharset()
		op = charset.OpEncode
	} else {
		return false
	}
	enc := charset.FindEncoding(chs)
	encBuf := &bytes.Buffer{}
	for i := range n {
		if expr.IsNull(i) {
			result.AppendNull()
			continue
		}
		val, err := enc.Transform(encBuf, expr.GetBytes(i), op)
		if err != nil {
			result.AppendNull()
		} else {
			result.AppendBytes(val)
		}
		continue
	}
	return true
}

func (b *builtinSubstringIndexSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	counts := buf2.Int64s()
	for i := range n {
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

		// when count > MaxInt64, returns whole string.
		if count < 0 && mysql.HasUnsignedFlag(b.args[2].GetType(ctx).GetFlag()) {
			result.AppendString(str)
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
				// -count overflows max int64, returns whole string.
				result.AppendString(str)
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

func (b *builtinUnHexSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.ReserveString(n)
	i64s := bits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			",", 64))
	}
	return nil
}

func (b *builtinASCIISig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(ctx, input, buf); err != nil {
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
		if len(str) == 0 {
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(str[0])
	}
	return nil
}

func (b *builtinLpadSig) vectorized() bool {
	return true
}

// vecEvalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			if err := handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket); err != nil {
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
			str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
		}
		result.AppendString(str[:targetLength])
	}
	return nil
}

func (b *builtinLpadUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			if err := handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket); err != nil {
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
			repeated := strings.Repeat(padStr, repeatCount)
			str = repeated[:runeByteIndex(repeated, tailLen)] + str
		}
		result.AppendString(str[:runeByteIndex(str, targetLength)])
	}
	return nil
}

func (b *builtinFindInSetSig) vectorized() bool {
	return true
}

func (b *builtinFindInSetSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	strlist, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strlist)
	if err := b.args[1].VecEvalString(ctx, input, strlist); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(str, strlist)
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strlistI := strlist.GetString(i)
		if len(strlistI) == 0 {
			res[i] = 0
			continue
		}
		for j, strInSet := range strings.Split(strlistI, ",") {
			if b.ctor.Compare(str.GetString(i), strInSet) == 0 {
				res[i] = int64(j + 1)
			}
		}
	}
	return nil
}

func (b *builtinLeftSig) vectorized() bool {
	return true
}

func (b *builtinLeftSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	left := buf2.Int64s()
	result.ReserveString(n)
	for i := range n {
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

func (b *builtinReverseSig) vectorized() bool {
	return true
}

func (b *builtinReverseSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}
	for i := range input.NumRows() {
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

// vecEvalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(strings.TrimRight(str, spaceChars))
	}

	return nil
}

func (b *builtinStrcmpSig) vectorized() bool {
	return true
}

func (b *builtinStrcmpSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	leftBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(leftBuf)
	if err := b.args[0].VecEvalString(ctx, input, leftBuf); err != nil {
		return err
	}
	rightBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rightBuf)
	if err := b.args[1].VecEvalString(ctx, input, rightBuf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(leftBuf, rightBuf)
	i64s := result.Int64s()
	for i := range n {
		// if left or right is null, then set to null and return 0(which is the default value)
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(types.CompareString(leftBuf.GetString(i), rightBuf.GetString(i), b.collation))
	}
	return nil
}

func (b *builtinLocate2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinLocate2ArgsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf0, buf1)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf0.GetString(i)
		if len(subStr) == 0 {
			i64s[i] = 1
			continue
		}
		i64s[i] = int64(strings.Index(buf1.GetString(i), subStr) + 1)
	}
	return nil
}
