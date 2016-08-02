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
	"strconv"
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
	wordBufLen = 9
	powers10   = [10]int32{ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9}
	dig2bytes  = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	fracMax    = [8]int32{
		900000000,
		990000000,
		999000000,
		999900000,
		999990000,
		999999000,
		999999900,
		999999990,
	}
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

// sub substracts b and carry from a, returns the diff and new carry.
func sub(a, b, carry int32) (int32, int32) {
	diff := a - b - carry
	if diff < 0 {
		carry = 1
		diff += wordBase
	} else {
		carry = 0
	}
	return diff, carry
}

// fixWordCntError limits word count in wordBufLen, and returns overflow or truncate error.
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

func (d *MyDecimal) stringSize() int {
	// sign, zero integer and dot.
	return d.digitsInt + d.digitsFrac + 3
}

func (d *MyDecimal) removeLeadingZeros() (wordIdx int, digitsInt int) {
	digitsInt = d.digitsInt
	i := ((digitsInt - 1) % digitsPerWord) + 1
	for digitsInt > 0 && d.wordBuf[wordIdx] == 0 {
		digitsInt -= i
		i = digitsPerWord
		wordIdx++
	}
	if digitsInt > 0 {
		digitsInt -= countLeadingZeroes((digitsInt-1)%digitsPerWord, d.wordBuf[wordIdx])
	} else {
		digitsInt = 0
	}
	return
}

// ToString converts decimal to its printable string representation.
//
//      fixedPrec - 0 if representation can be variable length and
//                  fixed_decimals will not be checked in this case.
//                  Put number as with fixed point position with this
//                  number of digits (sign counted and decimal point is
//                  counted)
//      fixedDec  - number digits after point.
//      filler    - character to fill gaps in case of fixed_precision > 0
//
//  RETURN VALUE
//
//      str       - result string
//      errCode   - eDecOK/eDecTruncate/eDecOverflow
//
func (d *MyDecimal) ToString(fixedPrec, fixedDec int, filler byte) (str []byte, errCode int) {
	fixedDigitsInt := 0
	if fixedPrec > 0 {
		fixedDigitsInt = fixedPrec - fixedDec
	}
	str = make([]byte, d.stringSize())
	digitsFrac := d.digitsFrac
	wordStartIdx, digitsInt := d.removeLeadingZeros()
	if digitsInt+digitsFrac == 0 {
		digitsInt = 1
		wordStartIdx = 0
	}

	digitsIntLen := digitsInt
	if fixedPrec > 0 {
		digitsIntLen = fixedDigitsInt
	}
	if digitsIntLen == 0 {
		digitsIntLen = 1
	}
	digitsFracLen := digitsFrac
	if fixedPrec > 0 {
		digitsFracLen = fixedDec
	}
	length := digitsIntLen + digitsFracLen
	if d.negative {
		length++
	}
	if digitsFrac > 0 {
		length++
	}

	if fixedPrec > 0 {
		if digitsFrac > fixedDec {
			errCode = eDecTruncate
			digitsFrac = fixedDec
		}
		if digitsInt > fixedDigitsInt {
			errCode = eDecOverflow
			digitsInt = fixedDigitsInt
		}
	}
	str = str[:length]
	strIdx := 0
	if d.negative {
		str[strIdx] = '-'
		strIdx++
	}
	var fill int
	if digitsFrac > 0 {
		fracIdx := strIdx + digitsIntLen
		fill = digitsFracLen - digitsFrac
		wordIdx := wordStartIdx + (digitsInt+digitsPerWord-1)/digitsPerWord
		str[fracIdx] = '.'
		fracIdx++
		for ; digitsFrac > 0; digitsFrac -= digitsPerWord {
			x := d.wordBuf[wordIdx]
			wordIdx++
			for i := myMin(digitsFrac, digitsPerWord); i > 0; i-- {
				y := x / digMask
				str[fracIdx] = byte(y) + '0'
				fracIdx++
				x -= y * digMask
				x *= 10
			}
		}
		for ; fill > 0; fill-- {
			str[fracIdx] = filler
			fracIdx++
		}
	}
	fill = digitsIntLen - digitsInt
	if digitsInt == 0 {
		fill-- /* symbol 0 before digital point */
	}
	for ; fill > 0; fill-- {
		str[strIdx] = filler
		strIdx++
	}
	if digitsInt > 0 {
		strIdx += digitsInt
		wordIdx := wordStartIdx + (digitsInt+digitsPerWord-1)/digitsPerWord
		for ; digitsInt > 0; digitsInt -= digitsPerWord {
			wordIdx--
			x := d.wordBuf[wordIdx]
			for i := myMin(digitsInt, digitsPerWord); i > 0; i-- {
				y := x / 10
				strIdx--
				str[strIdx] = '0' + byte(x-y*10)
				x = y
			}
		}
	} else {
		str[strIdx] = '0'
	}
	return
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
  digitBounds returns bounds of decimal digits in the number.

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
//    to     - result buffer. d == to is allowed
//    frac   - to what position after fraction point to round. can be negative!
//    mode   - round to nearest even or truncate
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

// FromInt sets the decimal value from int64.
func (d *MyDecimal) FromInt(val int64) int {
	var uVal uint64
	if val < 0 {
		d.negative = true
		uVal = uint64(-val)
	} else {
		uVal = uint64(val)
	}
	return d.FromUint(uVal)
}

// FromUint sets the decimal value from uint64.
func (d *MyDecimal) FromUint(val uint64) int {
	x := val
	wordIdx := 1
	for x >= wordBase {
		wordIdx++
		x /= wordBase
	}
	if wordIdx > wordBufLen {
		wordIdx = wordBufLen
		return eDecOverflow
	}
	d.digitsFrac = 0
	d.digitsInt = wordIdx * digitsPerWord
	x = val
	for wordIdx > 0 {
		wordIdx--
		y := x / wordBase
		d.wordBuf[wordIdx] = int32(x - y*wordBase)
		x = y
	}
	return eDecOK
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

// FromFloat64 creates a decimal from float64 value.
func (d *MyDecimal) FromFloat64(f float64) int {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	return d.FromString([]byte(s))
}

// ToFloat64 converts decimal to float64 value.
func (d *MyDecimal) ToFloat64() (float64, int) {
	str, ec := d.ToString(0, 0, 0)
	if ec != eDecOK {
		return 0, ec
	}
	f, err := strconv.ParseFloat(string(str), 64)
	if err != nil {
		ec = eDecOverflow
	}
	return f, ec
}

/*
ToBin converts decimal to its binary fixed-length representation
two representations of the same length can be compared with memcmp
with the correct -1/0/+1 result

  PARAMS
		precision/frac - see decimalBinSize() below

  NOTE
    the buffer is assumed to be of the size decimalBinSize(precision, frac)

  RETURN VALUE
  	bin     - binary value
    errCode - eDecOK/eDecTruncate/eDecOverflow

  DESCRIPTION
    for storage decimal numbers are converted to the "binary" format.

    This format has the following properties:
      1. length of the binary representation depends on the {precision, frac}
      as provided by the caller and NOT on the digitsInt/digitsFrac of the decimal to
      convert.
      2. binary representations of the same {precision, frac} can be compared
      with memcmp - with the same result as DecimalCompare() of the original
      decimals (not taking into account possible precision loss during
      conversion).

    This binary format is as follows:
      1. First the number is converted to have a requested precision and frac.
      2. Every full digitsPerWord digits of digitsInt part are stored in 4 bytes
         as is
      3. The first digitsInt % digitesPerWord digits are stored in the reduced
         number of bytes (enough bytes to store this number of digits -
         see dig2bytes)
      4. same for frac - full word are stored as is,
         the last frac % digitsPerWord digits - in the reduced number of bytes.
      5. If the number is negative - every byte is inversed.
      5. The very first bit of the resulting byte array is inverted (because
         memcmp compares unsigned bytes, see property 2 above)

    Example:

      1234567890.1234

    internally is represented as 3 words

      1 234567890 123400000

    (assuming we want a binary representation with precision=14, frac=4)
    in hex it's

      00-00-00-01  0D-FB-38-D2  07-5A-EF-40

    now, middle word is full - it stores 9 decimal digits. It goes
    into binary representation as is:


      ...........  0D-FB-38-D2 ............

    First word has only one decimal digit. We can store one digit in
    one byte, no need to waste four:

                01 0D-FB-38-D2 ............

    now, last word. It's 123400000. We can store 1234 in two bytes:

                01 0D-FB-38-D2 04-D2

    So, we've packed 12 bytes number in 7 bytes.
    And now we invert the highest bit to get the final result:

                81 0D FB 38 D2 04 D2

    And for -1234567890.1234 it would be

                7E F2 04 C7 2D FB 2D
*/
func (d *MyDecimal) ToBin(precision, frac int) ([]byte, int) {
	var errCode int
	var mask int32
	if d.negative {
		mask = -1
	}
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	leadingDigits := digitsInt - wordsInt*digitsPerWord
	wordsFrac := frac / digitsPerWord
	trailingDigits := frac - wordsFrac*digitsPerWord

	wordsFracFrom := d.digitsFrac / digitsPerWord
	trailingDigitsFrom := d.digitsFrac - wordsFracFrom*digitsPerWord
	intSize := wordsInt*wordSize + dig2bytes[leadingDigits]
	fracSize := wordsFrac*wordSize + dig2bytes[trailingDigits]
	fracSizeFrom := wordsFracFrom*wordSize + dig2bytes[trailingDigitsFrom]
	originIntSize := intSize
	originFracSize := fracSize

	bin := make([]byte, intSize+fracSize)
	binIdx := 0
	wordIdxFrom, digitsIntFrom := d.removeLeadingZeros()
	if digitsIntFrom+fracSizeFrom == 0 {
		mask = 0
		digitsInt = 1
	}

	wordsIntFrom := digitsIntFrom / digitsPerWord
	leadingDigitsFrom := digitsIntFrom - wordsIntFrom*digitsPerWord
	iSizeFrom := wordsIntFrom*wordSize + dig2bytes[leadingDigitsFrom]

	if wordsInt < wordsIntFrom {
		wordIdxFrom += wordsIntFrom - wordsInt
		if leadingDigitsFrom > 0 {
			wordIdxFrom++
		}
		if leadingDigits > 0 {
			wordIdxFrom--
		}
		wordsIntFrom = wordsInt
		leadingDigitsFrom = leadingDigits
		errCode = eDecOverflow
	} else if intSize > iSizeFrom {
		for intSize > iSizeFrom {
			intSize--
			bin[binIdx] = byte(mask)
			binIdx++
		}
	}

	if fracSize < fracSizeFrom {
		wordsFracFrom = wordsFrac
		trailingDigitsFrom = trailingDigits
		errCode = eDecTruncate
	} else if fracSize > fracSizeFrom && trailingDigitsFrom > 0 {
		if wordsFrac == wordsFracFrom {
			trailingDigitsFrom = trailingDigits
			fracSize = fracSizeFrom
		} else {
			wordsFracFrom++
			trailingDigitsFrom = 0
		}
	}
	// xIntFrom part
	if leadingDigitsFrom > 0 {
		i := dig2bytes[leadingDigitsFrom]
		x := (d.wordBuf[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask
		wordIdxFrom++
		writeWord(bin[binIdx:], x, i)
		binIdx += i
	}

	// wordsInt + wordsFrac part.
	for stop := wordIdxFrom + wordsIntFrom + wordsFracFrom; wordIdxFrom < stop; binIdx += wordSize {
		x := d.wordBuf[wordIdxFrom] ^ mask
		wordIdxFrom++
		writeWord(bin[binIdx:], x, 4)
	}

	// xFracFrom part
	if trailingDigitsFrom > 0 {
		var x int32
		i := dig2bytes[trailingDigitsFrom]
		lim := trailingDigits
		if wordsFracFrom < wordsFrac {
			lim = digitsPerWord
		}

		for trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i {
			trailingDigitsFrom++
		}
		x = (d.wordBuf[wordIdxFrom] / powers10[digitsPerWord-trailingDigitsFrom]) ^ mask
		writeWord(bin[binIdx:], x, i)
		binIdx += i
	}
	if fracSize > fracSizeFrom {
		binIdxEnd := originIntSize + originFracSize
		for fracSize > fracSizeFrom && binIdx < binIdxEnd {
			fracSize--
			bin[binIdx] = byte(mask)
			binIdx++
		}
	}
	bin[0] ^= 0x80
	return bin, errCode
}

// FromBin Restores decimal from its binary fixed-length representation.
func (d *MyDecimal) FromBin(bin []byte, precision, frac int) int {
	var errcode = eDecOK
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	leadingDigits := digitsInt - wordsInt*digitsPerWord
	wordsFrac := frac / digitsPerWord
	trailingDigits := frac - wordsFrac*digitsPerWord
	wordsIntTo := wordsInt
	if leadingDigits > 0 {
		wordsIntTo++
	}
	wordsFracTo := wordsFrac
	if trailingDigits > 0 {
		wordsFracTo++
	}

	binIdx := 0
	mask := int32(-1)
	if bin[binIdx]&0x80 > 0 {
		mask = 0
	}
	binSize := decimalBinSize(precision, frac)
	dCopy := make([]byte, binSize)
	copy(dCopy, bin)
	dCopy[0] ^= 0x80
	bin = dCopy
	oldWordsIntTo := wordsIntTo
	wordsIntTo, wordsFracTo, errcode = fixWordCntError(wordsIntTo, wordsFracTo)
	if errcode != 0 {
		if wordsIntTo < oldWordsIntTo {
			binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize
		} else {
			trailingDigits = 0
			wordsFrac = wordsFracTo
		}
	}
	d.negative = mask != 0
	d.digitsInt = wordsInt*digitsPerWord + leadingDigits
	d.digitsFrac = wordsFrac*digitsPerWord + trailingDigits

	wordIdx := 0
	if leadingDigits > 0 {
		i := dig2bytes[leadingDigits]
		x := readWord(bin[binIdx:], i)
		binIdx += i
		d.wordBuf[wordIdx] = x ^ mask
		if uint64(d.wordBuf[wordIdx]) >= uint64(powers10[leadingDigits+1]) {
			*d = zeroMyDecimal
			return eDecBadNum
		}
		if wordIdx > 0 || d.wordBuf[wordIdx] != 0 {
			wordIdx++
		} else {
			d.digitsInt -= leadingDigits
		}
	}
	for stop := binIdx + wordsInt*wordSize; binIdx < stop; binIdx += wordSize {
		d.wordBuf[wordIdx] = readWord(bin[binIdx:], 4) ^ mask
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			*d = zeroMyDecimal
			return eDecBadNum
		}
		if wordIdx > 0 || d.wordBuf[wordIdx] != 0 {
			wordIdx++
		} else {
			d.digitsInt -= digitsPerWord
		}
	}

	for stop := binIdx + wordsFrac*wordSize; binIdx < stop; binIdx += wordSize {
		d.wordBuf[wordIdx] = readWord(bin[binIdx:], 4) ^ mask
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			*d = zeroMyDecimal
			return eDecBadNum
		}
		wordIdx++
	}

	if trailingDigits > 0 {
		i := dig2bytes[trailingDigits]
		x := readWord(bin[binIdx:], i)
		d.wordBuf[wordIdx] = (x ^ mask) * powers10[digitsPerWord-trailingDigits]
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			*d = zeroMyDecimal
			return eDecBadNum
		}
		wordIdx++
	}

	if d.digitsInt == 0 && d.digitsFrac == 0 {
		*d = zeroMyDecimal
	}
	return errcode
}

// decimalBinSize returns the size of array to hold a binary representation of a decimal.
func decimalBinSize(precision, frac int) int {
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	wordsFrac := frac / digitsPerWord
	xInt := digitsInt - wordsInt*digitsPerWord
	xFrac := frac - wordsFrac*digitsPerWord
	return wordsInt*wordSize + dig2bytes[xInt] + wordsFrac*wordSize + dig2bytes[xFrac]
}

func readWord(b []byte, size int) int32 {
	var x int32
	switch size {
	case 1:
		x = int32(int8(b[0]))
	case 2:
		x = int32(int8(b[0]))<<8 + int32(b[1])
	case 3:
		if b[0]&128 > 0 {
			x = int32(uint32(255)<<24 | uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
		} else {
			x = int32(uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
		}
	case 4:
		x = int32(b[3]) + int32(b[2])<<8 + int32(b[1])<<16 + int32(int8(b[0]))<<24
	}
	return x
}

func writeWord(b []byte, word int32, size int) {
	v := uint32(word)
	switch size {
	case 1:
		b[0] = byte(word)
	case 2:
		b[0] = byte(v >> 8)
		b[1] = byte(v)
	case 3:
		b[0] = byte(v >> 16)
		b[1] = byte(v >> 8)
		b[2] = byte(v)
	case 4:
		b[0] = byte(v >> 24)
		b[1] = byte(v >> 16)
		b[2] = byte(v >> 8)
		b[3] = byte(v)
	}
}

// Compare compares one decimal to another, returns -1/0/1.
func (d *MyDecimal) Compare(to *MyDecimal) int {
	if d.negative == to.negative {
		return doSub(d, to, nil)
	}
	if d.negative {
		return -1
	}
	return 1
}

// DecimalAdd adds two decimals, sets the result to 'to'.
func DecimalAdd(from1, from2, to *MyDecimal) int {
	if from1.negative == from2.negative {
		return doAdd(from1, from2, to)
	}
	return doSub(from1, from2, to)
}

// DecimalSub subs one decimal from another, sets the result to 'to'.
func DecimalSub(from1, from2, to *MyDecimal) int {
	if from1.negative == from2.negative {
		return doSub(from1, from2, to)
	}
	return doAdd(from1, from2, to)
}

func doSub(from1, from2, to *MyDecimal) int {
	var (
		errcode     int
		wordsInt1   = (from1.digitsInt + digitsPerWord - 1) / digitsPerWord
		wordsFrac1  = (from1.digitsFrac + digitsPerWord - 1) / digitsPerWord
		wordsInt2   = (from2.digitsInt + digitsPerWord - 1) / digitsPerWord
		wordsFrac2  = (from2.digitsFrac + digitsPerWord - 1) / digitsPerWord
		wordsFracTo = myMax(wordsFrac1, wordsFrac2)

		start1 = 0
		stop1  = wordsInt1
		idx1   = 0
		start2 = 0
		stop2  = wordsInt2
		idx2   = 0
	)
	if from1.wordBuf[idx1] == 0 {
		for idx1 < stop1 && from1.wordBuf[idx1] == 0 {
			idx1++
		}
		start1 = idx1
		wordsInt1 = stop1 - idx1
	}
	if from2.wordBuf[idx2] == 0 {
		for idx2 < stop2 && from2.wordBuf[idx2] == 0 {
			idx2++
		}
		start2 = idx2
		wordsInt2 = stop2 - idx2
	}

	var carry int32
	if wordsInt2 > wordsInt1 {
		carry = 1
	} else if wordsInt2 == wordsInt1 {
		end1 := stop1 + wordsFrac1 - 1
		end2 := stop2 + wordsFrac2 - 1
		for idx1 <= end1 && from1.wordBuf[end1] == 0 {
			end1--
		}
		for idx2 <= end2 && from2.wordBuf[end2] == 0 {
			end2--
		}
		wordsFrac1 = end1 - stop1 + 1
		wordsFrac2 = end2 - stop2 + 1
		for idx1 <= end1 && idx2 <= end2 && from1.wordBuf[idx1] == from2.wordBuf[idx2] {
			idx1++
			idx2++
		}
		if idx1 <= end1 {
			if idx2 <= end2 && from2.wordBuf[idx2] > from1.wordBuf[idx1] {
				carry = 1
			} else {
				carry = 0
			}
		} else {
			if idx2 <= end2 {
				carry = 1
			} else {
				if to == nil {
					return 0
				}
				*to = zeroMyDecimal
				return eDecOK
			}
		}
	}

	if to == nil {
		if carry > 0 == from1.negative { // from2 is negative too.
			return 1
		}
		return -1
	}

	to.negative = from1.negative

	/* ensure that always idx1 > idx2 (and wordsInt1 >= wordsInt2) */
	if carry > 0 {
		from1, from2 = from2, from1
		start1, start2 = start2, start1
		wordsInt1, wordsInt2 = wordsInt2, wordsInt1
		wordsFrac1, wordsFrac2 = wordsFrac2, wordsFrac1
		to.negative = !to.negative
	}

	wordsInt1, wordsFracTo, errcode = fixWordCntError(wordsInt1, wordsFracTo)
	idxTo := wordsInt1 + wordsFracTo
	to.digitsFrac = from1.digitsFrac
	if to.digitsFrac < from2.digitsFrac {
		to.digitsFrac = from2.digitsFrac
	}
	to.digitsInt = wordsInt1 * digitsPerWord
	if errcode != eDecOK {
		if to.digitsFrac > wordsFracTo*digitsPerWord {
			to.digitsFrac = wordsFracTo * digitsPerWord
		}
		if wordsFrac1 > wordsFracTo {
			wordsFrac1 = wordsFracTo
		}
		if wordsFrac2 > wordsFracTo {
			wordsFrac2 = wordsFracTo
		}
		if wordsInt2 > wordsInt1 {
			wordsInt2 = wordsInt1
		}
	}
	carry = 0

	/* part 1 - max(frac) ... min (frac) */
	if wordsFrac1 > wordsFrac2 {
		idx1 = start1 + wordsInt1 + wordsFrac1
		stop1 = start1 + wordsInt1 + wordsFrac2
		idx2 = start2 + wordsInt2 + wordsFrac2
		for wordsFracTo > wordsFrac1 {
			wordsFracTo--
			idxTo--
			to.wordBuf[idxTo] = 0
		}
		for idx1 > stop1 {
			idxTo--
			idx1--
			to.wordBuf[idxTo] = from1.wordBuf[idx1]
		}
	} else {
		idx1 = start1 + wordsInt1 + wordsFrac1
		idx2 = start2 + wordsInt2 + wordsFrac2
		stop2 = start2 + wordsInt2 + wordsFrac1
		for wordsFracTo > wordsFrac2 {
			wordsFracTo--
			idxTo--
			to.wordBuf[idxTo] = 0
		}
		for idx2 > stop2 {
			idxTo--
			idx2--
			to.wordBuf[idxTo], carry = sub(0, from2.wordBuf[idx2], carry)
		}
	}

	/* part 2 - min(frac) ... wordsInt2 */
	for idx2 > start2 {
		idxTo--
		idx1--
		idx2--
		to.wordBuf[idxTo], carry = sub(from1.wordBuf[idx1], from2.wordBuf[idx2], carry)
	}

	/* part 3 - wordsInt2 ... wordsInt1 */
	for carry > 0 && idx1 > start1 {
		idxTo--
		idx1--
		to.wordBuf[idxTo], carry = sub(from1.wordBuf[idx1], 0, carry)
	}
	for idx1 > start1 {
		idxTo--
		idx1--
		to.wordBuf[idxTo] = from1.wordBuf[idx1]
	}
	for idxTo > 0 {
		idxTo--
		to.wordBuf[idxTo] = 0
	}
	return errcode
}

func doAdd(from1, from2, to *MyDecimal) int {
	var (
		errCode     int
		wordsInt1   = (from1.digitsInt + digitsPerWord - 1) / digitsPerWord
		wordsFrac1  = (from1.digitsFrac + digitsPerWord - 1) / digitsPerWord
		wordsInt2   = (from2.digitsInt + digitsPerWord - 1) / digitsPerWord
		wordsFrac2  = (from2.digitsFrac + digitsPerWord - 1) / digitsPerWord
		wordsIntTo  = myMax(wordsInt1, wordsInt2)
		wordsFracTo = myMax(wordsFrac1, wordsFrac2)
	)

	var x int32
	if wordsInt1 > wordsInt2 {
		x = from1.wordBuf[0]
	} else if wordsInt2 < wordsInt1 {
		x = from2.wordBuf[0]
	} else {
		x = from1.wordBuf[0] + from2.wordBuf[0]
	}
	if x > wordMax-1 { /* yes, there is */
		wordsIntTo++
		to.wordBuf[0] = 0 /* safety */
	}

	wordsIntTo, wordsFracTo, errCode = fixWordCntError(wordsIntTo, wordsFracTo)
	if errCode == eDecOverflow {
		maxDecimal(wordBufLen*digitsPerWord, 0, to)
		return errCode
	}
	idxTo := wordsIntTo + wordsFracTo
	to.negative = from1.negative
	to.digitsInt = wordsIntTo * digitsPerWord
	to.digitsFrac = myMax(from1.digitsFrac, from2.digitsFrac)

	if errCode != eDecOK {
		if to.digitsFrac > wordsFracTo*digitsPerWord {
			to.digitsFrac = wordsFracTo * digitsPerWord
		}
		if wordsFrac1 > wordsFracTo {
			wordsFrac1 = wordsFracTo
		}
		if wordsFrac2 > wordsFracTo {
			wordsFrac2 = wordsFracTo
		}
		if wordsInt1 > wordsIntTo {
			wordsInt1 = wordsIntTo
		}
		if wordsInt2 > wordsIntTo {
			wordsInt2 = wordsIntTo
		}
	}
	var dec1, dec2 = from1, from2
	var idx1, idx2, stop, stop2 int
	/* part 1 - max(frac) ... min (frac) */
	if wordsFrac1 > wordsFrac2 {
		idx1 = wordsInt1 + wordsFrac1
		stop = wordsInt1 + wordsFrac2
		idx2 = wordsInt2 + wordsFrac2
		if wordsInt1 > wordsInt2 {
			stop2 = wordsInt1 - wordsInt2
		}
	} else {
		idx1 = wordsInt2 + wordsFrac2
		stop = wordsInt2 + wordsFrac1
		idx2 = wordsInt1 + wordsFrac1
		if wordsInt2 > wordsInt1 {
			stop2 = wordsInt2 - wordsInt1
		}
		dec1, dec2 = from2, from1
	}
	for idx1 > stop {
		idxTo--
		idx1--
		to.wordBuf[idxTo] = dec1.wordBuf[idx1]
	}

	/* part 2 - min(frac) ... min(digitsInt) */
	carry := int32(0)
	for idx1 > stop2 {
		idx1--
		idx2--
		idxTo--
		to.wordBuf[idxTo], carry = add(dec1.wordBuf[idx1], dec2.wordBuf[idx2], carry)
	}

	/* part 3 - min(digitsInt) ... max(digitsInt) */
	stop = 0
	if wordsInt1 > wordsInt2 {
		idx1 = wordsInt1 - wordsInt2
		dec1, dec2 = from1, from2
	} else {
		idx1 = wordsInt2 - wordsInt1
		dec1, dec2 = from2, from1
	}
	for idx1 > stop {
		idxTo--
		idx1--
		to.wordBuf[idxTo], carry = add(dec1.wordBuf[idx1], 0, carry)
	}
	if carry > 0 {
		idxTo--
		to.wordBuf[idxTo] = 1
	}
	return errCode
}

func maxDecimal(precision, frac int, to *MyDecimal) {
	digitsInt := precision - frac
	to.negative = false
	to.digitsInt = digitsInt
	idx := 0
	if digitsInt > 0 {
		firstWordDigits := digitsInt % digitsPerWord
		if firstWordDigits > 0 {
			to.wordBuf[idx] = powers10[firstWordDigits] - 1 /* get 9 99 999 ... */
			idx++
		}
		for digitsInt /= digitsPerWord; digitsInt > 0; digitsInt-- {
			to.wordBuf[idx] = wordMax
			idx++
		}
	}
	to.digitsFrac = frac
	if frac > 0 {
		lastDigits := frac % digitsPerWord
		for frac /= digitsPerWord; frac > 0; frac-- {
			to.wordBuf[idx] = wordMax
			idx++
		}
		if lastDigits > 0 {
			to.wordBuf[idx] = fracMax[lastDigits-1]
		}
	}
}
