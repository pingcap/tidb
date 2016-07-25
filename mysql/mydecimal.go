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
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"fmt"
	"math"
)

type roundMode int

const (
	truncate roundMode = 0
	halfEven roundMode = 1
	halfUp   roundMode = 2
	ceiling  roundMode = 3
	floor    roundMode = 4
)

const (
	ten0 = 1
	ten1 = 10
	ten2 = 100
	ten3 = 1000
	ten4 = 10000
	ten5 = 100000
	ten6 = 1000000
	ten7 = 10000000
	ten8 = 100000000
	ten9 = 1000000000

	maxWordBufLen = 9 // A MyDecimal holds 9 words.
	digitsPerWord = 9 // A word holds 9 digits.
	wordSize      = 4 // A word is 4 bytes int32.
	digMask       = ten8
	wordBase      = ten9
	wordMax       = wordBase - 1

	eDecOK       = 0
	eDecTruncate = 1
	eDecOverflow = 2
	eDecDivZero  = 4
	eDecBadNum   = 8
)

var (
	wordBufLen    = 9
	powers10      = [10]int32{ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9}
	dig2bytes     = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	zeroMyDecimal = MyDecimal{digitsInt: 1}
)

// add adds a and b and carry, returns the sum and new carry.
func add(a, b, carry int32) (int32, int32) {
	sum := a + b + carry
	if sum >= wordBase {
		carry = 1
		sum -= wordBase
	} else {
		carry = 0
	}
	return sum, carry
}

// fixWordCntError limits word count in wordBufLen, and returns overflow or truncate error
func fixWordCntError(wordsInt, wordsFrac int) (newWordsInt, newWordsFrac, errcode int) {
	if wordsInt+wordsFrac > wordBufLen {
		if wordsInt > wordBufLen {
			return wordBufLen, 0, eDecOverflow
		}
		return wordsInt, wordBufLen - wordsInt, eDecTruncate
	}
	return wordsInt, wordsFrac, eDecOK
}

/*
  countLeadingZeroes returns the number of leading zeroes that can be removed from fraction.

  @param   i    start index
  @param   word value to compare against list of powers of 10
*/
func countLeadingZeroes(i int, word int32) int {
	leading := 0
	for word < powers10[i] {
		i--
		leading++
	}
	return leading
}

/*
  countTrailingZeros returns the number of trailing zeroes that can be removed from fraction.

  @param   i    start index
  @param   word  value to compare against list of powers of 10
*/
func countTrailingZeroes(i int, word int32) int {
	trailing := 0
	for word%powers10[i] == 0 {
		i++
		trailing++
	}
	return trailing
}

// MyDecimal represents a decimal value.
type MyDecimal struct {
	// The number of *decimal* digits before the point.
	digitsInt int

	// The number of decimal digits after the point.
	digitsFrac int

	negative bool

	// An array of int32 words.
	// A word is an int32 value can hold 9 digits.(0 <= word < wordBase)
	wordBuf [maxWordBufLen]int32
}

func (d *MyDecimal) String() string {
	return fmt.Sprintf("di:%d, df:%d, neg:%v, buf:%v", d.digitsInt, d.digitsFrac, d.negative, d.wordBuf)
}

// FromString parses decimal from string.
func (d *MyDecimal) FromString(str []byte) int {
	ec := eDecBadNum
	for i := 0; i < len(str); i++ {
		if !isSpace(str[i]) {
			str = str[i:]
			break
		}
	}
	if len(str) == 0 {
		*d = zeroMyDecimal
		return eDecBadNum
	}
	switch str[0] {
	case '-':
		d.negative = true
		fallthrough
	case '+':
		str = str[1:]
	}
	var strIdx int
	for strIdx < len(str) && isDigit(str[strIdx]) {
		strIdx++
	}
	digitsInt := strIdx
	var digitsFrac int
	var endIdx int
	if strIdx < len(str) && str[strIdx] == '.' {
		endIdx = strIdx + 1
		for endIdx < len(str) && isDigit(str[endIdx]) {
			endIdx++
		}
		digitsFrac = endIdx - strIdx - 1
	} else {
		digitsFrac = 0
		endIdx = strIdx
	}
	if digitsInt+digitsFrac == 0 {
		*d = zeroMyDecimal
		return eDecBadNum
	}
	wordsInt := (digitsInt + digitsPerWord - 1) / digitsPerWord
	wordsFrac := (digitsFrac + digitsPerWord - 1) / digitsPerWord
	wordsInt, wordsFrac, ec = fixWordCntError(wordsInt, wordsFrac)
	if ec != eDecOK {
		digitsFrac = wordsFrac * digitsPerWord
		if ec == eDecOverflow {
			digitsInt = wordsInt * digitsPerWord
		}
	}
	d.digitsInt = digitsInt
	d.digitsFrac = digitsFrac
	wordIdx := wordsInt
	strIdxTmp := strIdx
	var word int32
	var innerIdx int
	for digitsInt > 0 {
		digitsInt--
		strIdx--
		word += int32(str[strIdx]-'0') * powers10[innerIdx]
		innerIdx++
		if innerIdx == digitsPerWord {
			wordIdx--
			d.wordBuf[wordIdx] = word
			word = 0
			innerIdx = 0
		}
	}
	if innerIdx != 0 {
		wordIdx--
		d.wordBuf[wordIdx] = word
	}

	wordIdx = wordsInt
	strIdx = strIdxTmp
	word = 0
	innerIdx = 0
	for digitsFrac > 0 {
		digitsFrac--
		strIdx++
		word = int32(str[strIdx]-'0') + word*10
		innerIdx++
		if innerIdx == digitsPerWord {
			d.wordBuf[wordIdx] = word
			wordIdx++
			word = 0
			innerIdx = 0
		}
	}
	if innerIdx != 0 {
		d.wordBuf[wordIdx] = word * powers10[digitsPerWord-innerIdx]
	}
	if endIdx+1 < len(str) && (str[endIdx] == 'e' || str[endIdx] == 'E') {
		exponent, err := strToInt(string(str[endIdx+1:]))
		// TODO: need a way to check if there is at least one digit.
		if err != nil {
			*d = zeroMyDecimal
			return eDecBadNum
		}
		if exponent > math.MaxInt32/2 {
			*d = zeroMyDecimal
			return eDecOverflow
		}
		if exponent < math.MinInt32/2 && ec != eDecOverflow {
			*d = zeroMyDecimal
			return eDecTruncate
		}
		if ec != eDecOverflow {
			ec = d.Shift(int(exponent))
		}
	}
	return ec
}

// Shift shifts decimal digits in given number (with rounding if it need), shift > 0 means shift to left shift,
// shift < 0 means right shift. In fact it is multiplying on 10^shift.
//
// RETURN
//   eDecOK          OK
//   eDecOverflow    operation lead to overflow, number is untoched
//   eDecTruncated   number was rounded to fit into buffer
//
func (d *MyDecimal) Shift(shift int) int {
	if shift == 0 {
		return eDecOK
	}
	var (
		/* index of first non zero digit (all indexes from 0) */
		digitBegin int
		/* index of position after last decimal digit */
		digitEnd int
		/* index of digit position just after point */
		point = (d.digitsInt + digitsPerWord - 1) / digitsPerWord * digitsPerWord
		/* new point position */
		newPoint = point + shift
		/* number of digits in result */
		digitsInt, digitsFrac int
		/* return code */
		errCode  = eDecOK
		newFront int
	)
	digitBegin, digitEnd = d.digitBounds()
	if digitBegin == digitEnd {
		*d = zeroMyDecimal
		return eDecOK
	}

	digitsInt = newPoint - digitBegin
	if digitsInt < 0 {
		digitsInt = 0
	}
	digitsFrac = digitEnd - newPoint
	if digitsFrac < 0 {
		digitsFrac = 0
	}
	wordsInt := (digitsInt + digitsPerWord - 1) / digitsPerWord
	wordsFrac := (digitsFrac + digitsPerWord - 1) / digitsPerWord
	newLen := wordsInt + wordsFrac
	if newLen > wordBufLen {
		lack := newLen - wordBufLen
		if wordsFrac < lack {
			return eDecOverflow
		}
		/* cat off fraction part to allow new number to fit in our buffer */
		errCode = eDecTruncate
		wordsFrac -= lack
		diff := digitsFrac - wordsFrac*digitsPerWord
		d.Round(d, digitEnd-point-diff, halfUp)
		digitEnd -= diff
		digitsFrac = wordsFrac * digitsPerWord
		if digitEnd <= digitBegin {
			/*
			   We lost all digits (they will be shifted out of buffer), so we can
			   just return 0.
			*/
			*d = zeroMyDecimal
			return eDecTruncate
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
			d.digitsInt = digitsInt
			d.digitsFrac = digitsFrac
			return errCode /* already shifted as it should be */
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
	d.digitsInt = digitsInt
	d.digitsFrac = digitsFrac
	return errCode
}

/*
  digitBounds returns bounds of decimal digits in the number

  SYNOPSIS
    digits_bounds()
      from         - decimal number for processing
      start - index (from 0 ) of first decimal digits.
      end   - index of position just after last decimal digit.
*/
func (d *MyDecimal) digitBounds() (start, end int) {
	var i int
	bufBeg := 0
	bufLen := (d.digitsInt+digitsPerWord-1)/digitsPerWord + (d.digitsFrac+digitsPerWord-1)/digitsPerWord
	bufEnd := bufLen - 1

	/* find non-zero digit from number begining */
	for bufBeg < bufLen && d.wordBuf[bufBeg] == 0 {
		bufBeg++
	}
	if bufBeg >= bufLen {
		return 0, 0
	}

	/* find non-zero decimal digit from number begining */
	if bufBeg == 0 && d.digitsInt > 0 {
		i = (d.digitsInt - 1) % digitsPerWord
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
		i = (d.digitsFrac-1)%digitsPerWord + 1
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
  doMiniLeftShift does left shift for alignment of data in buffer

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
  doMiniRightShift does right shift for alignment of data in buffer

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

// Round rounds the decimal to "frac" digits
//
// SYNOPSIS
//  decimal_round()
//    to      - result buffer. d == to is allowed
//    frac   - to what position after fraction point to round. can be negative!
//    mode    - round to nearest even or truncate
//
// NOTES
//  scale can be negative !
//  one TRUNCATED error (line XXX below) isn't treated very logical :(
//
// RETURN VALUE
//  eDecOK/eDecTruncated
func (d *MyDecimal) Round(to *MyDecimal, frac int, mode roundMode) (errcode int) {
	// wordsFracTo is the number of fraction words in buffer.
	wordsFracTo := (frac + 1) / digitsPerWord
	if frac > 0 {
		wordsFracTo = (frac + digitsPerWord - 1) / digitsPerWord
	}
	wordsFrac := (d.digitsFrac + digitsPerWord - 1) / digitsPerWord
	wordsInt := (d.digitsInt + digitsPerWord - 1) / digitsPerWord

	var roundDigit int32
	switch mode {
	case halfUp, halfEven:
		roundDigit = 5
	case ceiling:
		if d.negative {
			roundDigit = 10
		}
	case floor:
		if !d.negative {
			roundDigit = 10
		}
	case truncate:
		roundDigit = 10
	}

	if wordsInt+wordsFracTo > wordBufLen {
		wordsFracTo = wordBufLen - wordsInt
		frac = wordsFracTo * digitsPerWord
		errcode = eDecTruncate
	}
	if d.digitsInt+frac < 0 {
		*to = zeroMyDecimal
		return eDecOK
	}
	if to != d {
		copy(to.wordBuf[:], d.wordBuf[:])
		to.negative = d.negative
		to.digitsInt = myMin(wordsInt, wordBufLen) * digitsPerWord
	}
	if wordsFracTo > wordsFrac {
		idx := wordsInt + wordsFrac
		for wordsFracTo > wordsFrac {
			wordsFracTo--
			to.wordBuf[idx] = 0
			idx++
		}
		to.digitsFrac = frac
		return
	}
	if frac >= d.digitsFrac {
		to.digitsFrac = frac
		return
	}

	// Do increment.
	toIdx := wordsInt + wordsFracTo - 1
	if frac == wordsFracTo*digitsPerWord {
		doInc := false
		switch roundDigit {
		case 0:
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
		case 5:
			digAfterScale := d.wordBuf[toIdx+1] / digMask // the first digit after scale.
			// If first digit after scale is 5 and round even, do incre if digit at scale is odd.
			doInc = (digAfterScale > 5) || ((digAfterScale == 5) && (mode == halfUp ||
				(toIdx >= 0 && (d.wordBuf[toIdx]&1 == 1))))
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
			return eDecOK
		}
	} else {
		/* TODO - fix this code as it won't work for CEILING mode */
		pos := wordsFracTo*digitsPerWord - frac - 1
		shiftedNumber := to.wordBuf[toIdx] / powers10[pos]
		digAfterScale := shiftedNumber % 10
		if digAfterScale > roundDigit || (roundDigit == 5 && digAfterScale == 5 && (mode == halfUp || (shiftedNumber/10)&1 == 1)) {
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
				errcode = eDecTruncate
			}
			for toIdx = wordsInt + myMax(wordsFracTo, 0); toIdx > 0; toIdx-- {
				if toIdx < wordBufLen {
					to.wordBuf[toIdx] = to.wordBuf[toIdx-1]
				} else {
					errcode = eDecOverflow
				}
			}
			to.wordBuf[toIdx] = 1
			/* We cannot have more than 9 * 9 = 81 digits. */
			if to.digitsInt < digitsPerWord*wordBufLen {
				to.digitsInt++
			} else {
				errcode = eDecOverflow
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
				to.digitsFrac = myMax(frac, 0)
				to.negative = false
				for toIdx < idx {
					to.wordBuf[toIdx] = 0
					toIdx++
				}
				return eDecOK
			}
			toIdx--
		}
	}
	/* Here we check 999.9 -> 1000 case when we need to increase intDigCnt */
	firstDig := to.digitsInt % digitsPerWord
	if firstDig > 0 && to.wordBuf[toIdx] >= powers10[firstDig] {
		to.digitsInt++
	}
	if frac < 0 {
		frac = 0
	}
	to.digitsFrac = frac
	return
}

// ToInt returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToInt() (int64, int) {
	var x int64
	wordIdx := 0
	for i := d.digitsInt; i > 0; i -= digitsPerWord {
		y := x
		/*
		   Attention: trick!
		   we're calculating -|from| instead of |from| here
		   because |LONGLONG_MIN| > LONGLONG_MAX
		   so we can convert -9223372036854775808 correctly
		*/
		x = x*wordBase - int64(d.wordBuf[wordIdx])
		wordIdx++
		if y < math.MinInt64/wordBase || x > y {
			/*
			   the decimal is bigger than any possible integer
			   return border integer depending on the sign
			*/
			if d.negative {
				return math.MinInt64, eDecOverflow
			}
			return math.MaxInt64, eDecOverflow
		}
	}
	/* boundary case: 9223372036854775808 */
	if !d.negative && x == math.MinInt64 {
		return math.MaxInt64, eDecOverflow
	}
	if !d.negative {
		x = -x
	}
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		if d.wordBuf[wordIdx] != 0 {
			return x, eDecTruncate
		}
		wordIdx++
	}
	return x, eDecOK
}

// ToUint returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToUint() (uint64, int) {
	if d.negative {
		return 0, eDecOverflow
	}
	var x uint64
	wordIdx := 0
	for i := d.digitsInt; i > 0; i -= digitsPerWord {
		y := x
		x = x*wordBase + uint64(d.wordBuf[wordIdx])
		wordIdx++
		if y > math.MaxUint64/wordBase || x < y {
			return math.MaxUint64, eDecOverflow
		}
	}
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		if d.wordBuf[wordIdx] != 0 {
			return x, eDecTruncate
		}
		wordIdx++
	}
	return x, eDecOK
}
