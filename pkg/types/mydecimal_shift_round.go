// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"github.com/pingcap/errors"
)

// Shift shifts decimal digits in given number (with rounding if it need), shift > 0 means shift to left shift,
// shift < 0 means right shift. In fact it is multiplying on 10^shift.
//
// RETURN
//
//	eDecOK          OK
//	eDecOverflow    operation lead to overflow, number is untoched
//	eDecTruncated   number was rounded to fit into buffer
func (d *MyDecimal) Shift(shift int) error {
	var err error
	if shift == 0 {
		return nil
	}
	var (
		// digitBegin is index of first non zero digit (all indexes from 0).
		digitBegin int
		// digitEnd is index of position after last decimal digit.
		digitEnd int
		// point is index of digit position just after point.
		point = digitsToWords(int(d.digitsInt)) * digitsPerWord
		// new point position.
		newPoint = point + shift
		// number of digits in result.
		digitsInt, digitsFrac int
		newFront              int
	)
	digitBegin, digitEnd = d.digitBounds()
	if digitBegin == digitEnd {
		*d = zeroMyDecimal
		return nil
	}

	digitsInt = max(newPoint-digitBegin, 0)
	digitsFrac = max(digitEnd-newPoint, 0)
	wordsInt := digitsToWords(digitsInt)
	wordsFrac := digitsToWords(digitsFrac)
	newLen := wordsInt + wordsFrac
	if newLen > wordBufLen {
		lack := newLen - wordBufLen
		if wordsFrac < lack {
			return ErrOverflow
		}
		/* cut off fraction part to allow new number to fit in our buffer */
		err = ErrTruncated
		wordsFrac -= lack
		diff := digitsFrac - wordsFrac*digitsPerWord
		err1 := d.Round(d, digitEnd-point-diff, ModeHalfUp)
		if err1 != nil {
			return errors.Trace(err1)
		}
		digitEnd -= diff
		digitsFrac = wordsFrac * digitsPerWord
		if digitEnd <= digitBegin {
			/*
			   We lost all digits (they will be shifted out of buffer), so we can
			   just return 0.
			*/
			*d = zeroMyDecimal
			return ErrTruncated
		}
	}

	if shift%digitsPerWord != 0 {
		var lMiniShift, rMiniShift, miniShift int
		var doLeft bool
		/*
		   Calculate left/right shift to align decimal digits inside our bug
		   digits correctly.
		*/
		if shift > 0 {
			lMiniShift = shift % digitsPerWord
			rMiniShift = digitsPerWord - lMiniShift
			doLeft = lMiniShift <= digitBegin
		} else {
			rMiniShift = (-shift) % digitsPerWord
			lMiniShift = digitsPerWord - rMiniShift
			doLeft = (digitsPerWord*wordBufLen - digitEnd) < rMiniShift
		}
		if doLeft {
			d.doMiniLeftShift(lMiniShift, digitBegin, digitEnd)
			miniShift = -lMiniShift
		} else {
			d.doMiniRightShift(rMiniShift, digitBegin, digitEnd)
			miniShift = rMiniShift
		}
		newPoint += miniShift
		/*
		   If number is shifted and correctly aligned in buffer we can finish.
		*/
		if shift+miniShift == 0 && (newPoint-digitsInt) < digitsPerWord {
			d.digitsInt = int8(digitsInt)
			d.digitsFrac = int8(digitsFrac)
			return err /* already shifted as it should be */
		}
		digitBegin += miniShift
		digitEnd += miniShift
	}

	/* if new 'decimal front' is in first digit, we do not need move digits */
	newFront = newPoint - digitsInt
	if newFront >= digitsPerWord || newFront < 0 {
		/* need to move digits */
		var wordShift int
		if newFront > 0 {
			/* move left */
			wordShift = newFront / digitsPerWord
			to := digitBegin/digitsPerWord - wordShift
			barier := (digitEnd-1)/digitsPerWord - wordShift
			for ; to <= barier; to++ {
				d.wordBuf[to] = d.wordBuf[to+wordShift]
			}
			for barier += wordShift; to <= barier; to++ {
				d.wordBuf[to] = 0
			}
			wordShift = -wordShift
		} else {
			/* move right */
			wordShift = (1 - newFront) / digitsPerWord
			to := (digitEnd-1)/digitsPerWord + wordShift
			barier := digitBegin/digitsPerWord + wordShift
			for ; to >= barier; to-- {
				d.wordBuf[to] = d.wordBuf[to-wordShift]
			}
			for barier -= wordShift; to >= barier; to-- {
				d.wordBuf[to] = 0
			}
		}
		digitShift := wordShift * digitsPerWord
		digitBegin += digitShift
		digitEnd += digitShift
		newPoint += digitShift
	}
	/*
	   If there are gaps then fill them with 0.

	   Only one of following 'for' loops will work because wordIdxBegin <= wordIdxEnd.
	*/
	wordIdxBegin := digitBegin / digitsPerWord
	wordIdxEnd := (digitEnd - 1) / digitsPerWord
	wordIdxNewPoint := 0

	/* We don't want negative new_point below */
	if newPoint != 0 {
		wordIdxNewPoint = (newPoint - 1) / digitsPerWord
	}
	if wordIdxNewPoint > wordIdxEnd {
		for wordIdxNewPoint > wordIdxEnd {
			d.wordBuf[wordIdxNewPoint] = 0
			wordIdxNewPoint--
		}
	} else {
		for ; wordIdxNewPoint < wordIdxBegin; wordIdxNewPoint++ {
			d.wordBuf[wordIdxNewPoint] = 0
		}
	}
	d.digitsInt = int8(digitsInt)
	d.digitsFrac = int8(digitsFrac)
	return err
}

/*
digitBounds returns bounds of decimal digits in the number.

	start - index (from 0 ) of first decimal digits.
	end   - index of position just after last decimal digit.
*/
func (d *MyDecimal) digitBounds() (start, end int) {
	var i int
	bufBeg := 0
	bufLen := digitsToWords(int(d.digitsInt)) + digitsToWords(int(d.digitsFrac))
	bufEnd := bufLen - 1

	/* find non-zero digit from number beginning */
	for bufBeg < bufLen && d.wordBuf[bufBeg] == 0 {
		bufBeg++
	}
	if bufBeg >= bufLen {
		return 0, 0
	}

	/* find non-zero decimal digit from number beginning */
	if bufBeg == 0 && d.digitsInt > 0 {
		i = (int(d.digitsInt) - 1) % digitsPerWord
		start = digitsPerWord - i - 1
	} else {
		i = digitsPerWord - 1
		start = bufBeg * digitsPerWord
	}
	if bufBeg < bufLen {
		start += countLeadingZeroes(i, d.wordBuf[bufBeg])
	}

	/* find non-zero digit at the end */
	for bufEnd > bufBeg && d.wordBuf[bufEnd] == 0 {
		bufEnd--
	}
	/* find non-zero decimal digit from the end */
	if bufEnd == bufLen-1 && d.digitsFrac > 0 {
		i = (int(d.digitsFrac)-1)%digitsPerWord + 1
		end = bufEnd*digitsPerWord + i
		i = digitsPerWord - i + 1
	} else {
		end = (bufEnd + 1) * digitsPerWord
		i = 1
	}
	end -= countTrailingZeroes(i, d.wordBuf[bufEnd])
	return start, end
}

/*
doMiniLeftShift does left shift for alignment of data in buffer.

	shift   number of decimal digits on which it should be shifted
	beg/end bounds of decimal digits (see digitsBounds())

NOTE

	Result fitting in the buffer should be garanted.
	'shift' have to be from 1 to digitsPerWord-1 (inclusive)
*/
func (d *MyDecimal) doMiniLeftShift(shift, beg, end int) {
	bufFrom := beg / digitsPerWord
	bufEnd := (end - 1) / digitsPerWord
	cShift := digitsPerWord - shift
	if beg%digitsPerWord < shift {
		d.wordBuf[bufFrom-1] = d.wordBuf[bufFrom] / powers10[cShift]
	}
	for bufFrom < bufEnd {
		d.wordBuf[bufFrom] = (d.wordBuf[bufFrom]%powers10[cShift])*powers10[shift] + d.wordBuf[bufFrom+1]/powers10[cShift]
		bufFrom++
	}
	d.wordBuf[bufFrom] = (d.wordBuf[bufFrom] % powers10[cShift]) * powers10[shift]
}

/*
doMiniRightShift does right shift for alignment of data in buffer.

	shift   number of decimal digits on which it should be shifted
	beg/end bounds of decimal digits (see digitsBounds())

NOTE

	Result fitting in the buffer should be garanted.
	'shift' have to be from 1 to digitsPerWord-1 (inclusive)
*/
func (d *MyDecimal) doMiniRightShift(shift, beg, end int) {
	bufFrom := (end - 1) / digitsPerWord
	bufEnd := beg / digitsPerWord
	cShift := digitsPerWord - shift
	if digitsPerWord-((end-1)%digitsPerWord+1) < shift {
		d.wordBuf[bufFrom+1] = (d.wordBuf[bufFrom] % powers10[shift]) * powers10[cShift]
	}
	for bufFrom > bufEnd {
		d.wordBuf[bufFrom] = d.wordBuf[bufFrom]/powers10[shift] + (d.wordBuf[bufFrom-1]%powers10[shift])*powers10[cShift]
		bufFrom--
	}
	d.wordBuf[bufFrom] = d.wordBuf[bufFrom] / powers10[shift]
}

// Round rounds the decimal to "frac" digits.
//
//	   to			- result buffer. d == to is allowed
//	   frac			- to what position after fraction point to round. can be negative!
//	   roundMode		- round to nearest even or truncate
//				ModeHalfUp rounds normally.
//				ModeTruncate just truncates the decimal.
//
// NOTES
//
//	frac can be negative !
//	one TRUNCATED error (line XXX below) isn't treated very logical :(
//
// RETURN VALUE
//
//	nil/ErrTruncated/ErrOverflow
func (d *MyDecimal) Round(to *MyDecimal, frac int, roundMode RoundMode) (err error) {
	// wordsFracTo is the number of fraction words in buffer.
	wordsFracTo := (frac + 1) / digitsPerWord
	if frac > 0 {
		wordsFracTo = digitsToWords(frac)
	}
	wordsFrac := digitsToWords(int(d.digitsFrac))
	wordsInt := digitsToWords(int(d.digitsInt))

	roundDigit := int32(roundMode)
	/* TODO - fix this code as it won't work for CEILING mode */

	if wordsInt+wordsFracTo > wordBufLen {
		wordsFracTo = wordBufLen - wordsInt
		frac = wordsFracTo * digitsPerWord
		err = ErrTruncated
	}
	if int(d.digitsInt)+frac < 0 {
		*to = zeroMyDecimal
		return nil
	}
	if to != d {
		copy(to.wordBuf[:], d.wordBuf[:])
		to.negative = d.negative
		to.digitsInt = int8(min(wordsInt, wordBufLen) * digitsPerWord)
	}
	if wordsFracTo > wordsFrac {
		idx := wordsInt + wordsFrac
		for wordsFracTo > wordsFrac {
			wordsFracTo--
			to.wordBuf[idx] = 0
			idx++
		}
		to.digitsFrac = int8(frac)
		to.resultFrac = to.digitsFrac
		return
	}
	if frac >= int(d.digitsFrac) {
		to.digitsFrac = int8(frac)
		to.resultFrac = to.digitsFrac
		return
	}

	// Do increment.
	toIdx := wordsInt + wordsFracTo - 1
	if frac == wordsFracTo*digitsPerWord {
		doInc := false
		switch roundMode {
		// Notice: No support for ceiling mode now.
		case ModeCeiling:
			// If any word after scale is not zero, do increment.
			// e.g ceiling 3.0001 to scale 1, gets 3.1
			idx := toIdx + (wordsFrac - wordsFracTo)
			for idx > toIdx {
				if d.wordBuf[idx] != 0 {
					doInc = true
					break
				}
				idx--
			}
		case ModeHalfUp:
			digAfterScale := d.wordBuf[toIdx+1] / digMask // the first digit after scale.
			// If first digit after scale is equal to or greater than 5, do increment.
			doInc = digAfterScale >= 5
		case ModeTruncate:
			// Never round, just truncate.
			doInc = false
		}
		if doInc {
			if toIdx >= 0 {
				to.wordBuf[toIdx]++
			} else {
				toIdx++
				to.wordBuf[toIdx] = wordBase
			}
		} else if wordsInt+wordsFracTo == 0 {
			*to = zeroMyDecimal
			return nil
		}
	} else {
		/* TODO - fix this code as it won't work for CEILING mode */
		pos := wordsFracTo*digitsPerWord - frac - 1
		shiftedNumber := to.wordBuf[toIdx] / powers10[pos]
		digAfterScale := shiftedNumber % 10
		if digAfterScale > roundDigit || (roundDigit == 5 && digAfterScale == 5) {
			shiftedNumber += 10
		}
		to.wordBuf[toIdx] = powers10[pos] * (shiftedNumber - digAfterScale)
	}
	/*
	   In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal words inside
	   the buffer are as follows.

	   Before <1, 5e8>
	   After  <2, 5e8>

	   Hence we need to set the 2nd field to 0.
	   The same holds if we round 1.5e-9 to 2e-9.
	*/
	if wordsFracTo < wordsFrac {
		idx := wordsInt + wordsFracTo
		if frac == 0 && wordsInt == 0 {
			idx = 1
		}
		for idx < wordBufLen {
			to.wordBuf[idx] = 0
			idx++
		}
	}

	// Handle carry.
	var carry int32
	if to.wordBuf[toIdx] >= wordBase {
		carry = 1
		to.wordBuf[toIdx] -= wordBase
		for carry == 1 && toIdx > 0 {
			toIdx--
			to.wordBuf[toIdx], carry = add(to.wordBuf[toIdx], 0, carry)
		}
		if carry > 0 {
			if wordsInt+wordsFracTo >= wordBufLen {
				wordsFracTo--
				frac = wordsFracTo * digitsPerWord
				err = ErrTruncated
			}
			for toIdx = wordsInt + max(wordsFracTo, 0); toIdx > 0; toIdx-- {
				if toIdx < wordBufLen {
					to.wordBuf[toIdx] = to.wordBuf[toIdx-1]
				} else {
					err = ErrOverflow
				}
			}
			to.wordBuf[toIdx] = 1
			/* We cannot have more than 9 * 9 = 81 digits. */
			if int(to.digitsInt) < digitsPerWord*wordBufLen {
				to.digitsInt++
			} else {
				err = ErrOverflow
			}
		}
	} else {
		for {
			if to.wordBuf[toIdx] != 0 {
				break
			}
			if toIdx == 0 {
				/* making 'zero' with the proper scale */
				idx := wordsFracTo + 1
				to.digitsInt = 1
				to.digitsFrac = int8(max(frac, 0))
				to.negative = false
				for toIdx < idx {
					to.wordBuf[toIdx] = 0
					toIdx++
				}
				to.resultFrac = to.digitsFrac
				return nil
			}
			toIdx--
		}
	}
	/* Here we check 999.9 -> 1000 case when we need to increase intDigCnt */
	firstDig := mod9[to.digitsInt]
	if firstDig > 0 && to.wordBuf[toIdx] >= powers10[firstDig] {
		to.digitsInt++
	}
	if frac < 0 {
		frac = 0
	}
	to.digitsFrac = int8(frac)
	to.resultFrac = to.digitsFrac
	return
}

// FromParquetArray sets the decimal value from Parquet byte array representation.
// It assumes that the input buffer is disposable, which will be modified during
// the conversion.
// Note:
//  1. The input buffer will be modified in-place. Callers must pass a disposable
//     copy if they need to preserve the original data.
//     For the data layout stored in parquet, please refer to
//     https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
//  2. This function doesn't handle overflow/truncate, use it with caution.
func (d *MyDecimal) FromParquetArray(buf []byte, scale int) (err error) {
	// MyDecimal's wordBuf stores absolute value, so we need to get absolute
	// value from two's complement first.
	d.negative = (buf[0] & 0x80) != 0
	if d.negative {
		for i := range buf {
			buf[i] = ^buf[i]
		}
		for i := len(buf) - 1; i >= 0; i-- {
			buf[i]++
			if buf[i] != 0 {
				break
			}
		}
	}

	var (
		startIndex = 0
		endIndex   = len(buf)
	)

	for startIndex < endIndex && buf[startIndex] == 0 {
		startIndex++
	}

	// Apply long‑division algorithm to do radix conversion.
	wordIdx := 0
	for startIndex < endIndex {
		var rem uint64
		for i := startIndex; i < endIndex; i++ {
			v := (rem << 8) | uint64(buf[i])
			q := v / ten9
			rem = v % ten9
			buf[i] = byte(q)
			if q == 0 && i == startIndex {
				startIndex++
			}
		}

		if wordIdx >= wordBufLen {
			return ErrOverflow
		}

		d.wordBuf[wordIdx] = int32(rem)
		wordIdx++
	}

	for idx := range wordIdx / 2 {
		d.wordBuf[idx], d.wordBuf[wordIdx-idx-1] =
			d.wordBuf[wordIdx-idx-1], d.wordBuf[idx]
	}

	d.digitsFrac = 0
	d.resultFrac = 0
	d.digitsInt = int8(wordIdx * digitsPerWord)
	if err := d.Shift(-scale); err != nil {
		return err
	}

	return d.Round(d, scale, ModeTruncate)
}
