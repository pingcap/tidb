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
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// DecimalNeg reverses decimal's sign.
func DecimalNeg(from *MyDecimal) *MyDecimal {
	to := *from
	if from.IsZero() {
		return &to
	}
	to.negative = !from.negative
	return &to
}

// DecimalAdd adds two decimals, sets the result to 'to'.
// Note: DO NOT use `from1` or `from2` as `to` since the metadata
// of `to` may be changed during evaluating.
func DecimalAdd(from1, from2, to *MyDecimal) error {
	from1, from2, to = validateArgs(from1, from2, to)
	to.resultFrac = max(from1.resultFrac, from2.resultFrac)
	if from1.negative == from2.negative {
		return doAdd(from1, from2, to)
	}
	_, err := doSub(from1, from2, to)
	return err
}

// DecimalSub subs one decimal from another, sets the result to 'to'.
func DecimalSub(from1, from2, to *MyDecimal) error {
	from1, from2, to = validateArgs(from1, from2, to)
	to.resultFrac = max(from1.resultFrac, from2.resultFrac)
	if from1.negative == from2.negative {
		_, err := doSub(from1, from2, to)
		return err
	}
	return doAdd(from1, from2, to)
}

func validateArgs(f1, f2, to *MyDecimal) (*MyDecimal, *MyDecimal, *MyDecimal) {
	if to == nil {
		return f1, f2, to
	}
	if f1 == to {
		tmp := *f1
		f1 = &tmp
	}
	if f2 == to {
		tmp := *f2
		f2 = &tmp
	}
	to.digitsFrac = 0
	to.digitsInt = 0
	to.resultFrac = 0
	to.negative = false
	for i := range to.wordBuf {
		to.wordBuf[i] = 0
	}
	return f1, f2, to
}

func doSub(from1, from2, to *MyDecimal) (cmp int, err error) {
	var (
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsFracTo = max(wordsFrac1, wordsFrac2)

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
			if idx2 > end2 {
				if to == nil {
					return 0, nil
				}
				*to = zeroMyDecimalWithFrac(to.resultFrac)
				return 0, nil
			}
			carry = 1
		}
	}

	if to == nil {
		if carry > 0 == from1.negative { // from2 is negative too.
			return 1, nil
		}
		return -1, nil
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

	wordsInt1, wordsFracTo, err = fixWordCntError(wordsInt1, wordsFracTo)
	idxTo := wordsInt1 + wordsFracTo
	to.digitsFrac = max(from1.digitsFrac, from2.digitsFrac)
	to.digitsInt = int8(wordsInt1 * digitsPerWord)
	if err != nil {
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
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
	return 0, err
}

func doAdd(from1, from2, to *MyDecimal) error {
	var (
		err         error
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsIntTo  = max(wordsInt1, wordsInt2)
		wordsFracTo = max(wordsFrac1, wordsFrac2)
	)

	var x int32
	if wordsInt1 > wordsInt2 {
		x = from1.wordBuf[0]
	} else if wordsInt2 > wordsInt1 {
		x = from2.wordBuf[0]
	} else {
		x = from1.wordBuf[0] + from2.wordBuf[0]
	}
	if x > wordMax-1 { /* yes, there is */
		wordsIntTo++
		to.wordBuf[0] = 0 /* safety */
	}

	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	if err == ErrOverflow {
		maxDecimal(wordBufLen*digitsPerWord, 0, to)
		return err
	}
	idxTo := wordsIntTo + wordsFracTo
	to.negative = from1.negative
	to.digitsInt = int8(wordsIntTo * digitsPerWord)
	to.digitsFrac = max(from1.digitsFrac, from2.digitsFrac)

	if err != nil {
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
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
		dec1 = from1
	} else {
		idx1 = wordsInt2 - wordsInt1
		dec1 = from2
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
	return err
}

func maxDecimal(precision, frac int, to *MyDecimal) {
	digitsInt := precision - frac
	to.negative = false
	to.digitsInt = int8(digitsInt)
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
	to.digitsFrac = int8(frac)
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

/*
DecimalMul multiplies two decimals.

	    from1, from2 - factors
	    to      - product

	RETURN VALUE
	  E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW;

	NOTES
	  in this implementation, with wordSize=4 we have digitsPerWord=9,
	  and 63-digit number will take only 7 words (basically a 7-digit
	  "base 999999999" number).  Thus there's no need in fast multiplication
	  algorithms, 7-digit numbers can be multiplied with a naive O(n*n)
	  method.

	  XXX if this library is to be used with huge numbers of thousands of
	  digits, fast multiplication must be implemented.
*/
func DecimalMul(from1, from2, to *MyDecimal) error {
	from1, from2, to = validateArgs(from1, from2, to)
	var (
		err         error
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsIntTo  = digitsToWords(int(from1.digitsInt) + int(from2.digitsInt))
		wordsFracTo = wordsFrac1 + wordsFrac2
		idx1        = wordsInt1
		idx2        = wordsInt2
		idxTo       int
		tmp1        = wordsIntTo
		tmp2        = wordsFracTo
	)
	to.resultFrac = min(from1.resultFrac+from2.resultFrac, mysql.MaxDecimalScale)
	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	to.negative = from1.negative != from2.negative
	to.digitsFrac = min(from1.digitsFrac+from2.digitsFrac, notFixedDec)
	to.digitsInt = int8(wordsIntTo * digitsPerWord)
	if err == ErrOverflow {
		return err
	}
	if err != nil {
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
		}
		if to.digitsInt > int8(wordsIntTo*digitsPerWord) {
			to.digitsInt = int8(wordsIntTo * digitsPerWord)
		}
		if tmp1 > wordsIntTo {
			tmp1 -= wordsIntTo
			tmp2 = tmp1 >> 1
			wordsInt2 -= tmp1 - tmp2
			wordsFrac1 = 0
			wordsFrac2 = 0
		} else {
			tmp2 -= wordsFracTo
			tmp1 = tmp2 >> 1
			if wordsFrac1 <= wordsFrac2 {
				wordsFrac1 -= tmp1
				wordsFrac2 -= tmp2 - tmp1
			} else {
				wordsFrac2 -= tmp1
				wordsFrac1 -= tmp2 - tmp1
			}
		}
	}
	startTo := wordsIntTo + wordsFracTo - 1
	start2 := idx2 + wordsFrac2 - 1
	stop1 := idx1 - wordsInt1
	stop2 := idx2 - wordsInt2
	to.wordBuf = zeroMyDecimal.wordBuf

	for idx1 += wordsFrac1 - 1; idx1 >= stop1; idx1-- {
		carry := int32(0)
		idxTo = startTo
		idx2 = start2
		for idx2 >= stop2 {
			var hi, lo int32
			p := int64(from1.wordBuf[idx1]) * int64(from2.wordBuf[idx2])
			hi = int32(p / wordBase)
			lo = int32(p - int64(hi)*wordBase)
			to.wordBuf[idxTo], carry = add2(to.wordBuf[idxTo], lo, carry)
			carry += hi
			idx2--
			idxTo--
		}
		if carry > 0 {
			if idxTo < 0 {
				return ErrOverflow
			}
			to.wordBuf[idxTo], carry = add2(to.wordBuf[idxTo], 0, carry)
		}
		for idxTo--; carry > 0; idxTo-- {
			if idxTo < 0 {
				return ErrOverflow
			}
			to.wordBuf[idxTo], carry = add(to.wordBuf[idxTo], 0, carry)
		}
		startTo--
	}

	/* Now we have to check for -0.000 case */
	if to.negative {
		idx := 0
		end := wordsIntTo + wordsFracTo
		for {
			if to.wordBuf[idx] != 0 {
				break
			}
			idx++
			/* We got decimal zero */
			if idx == end {
				*to = zeroMyDecimalWithFrac(to.resultFrac)
				break
			}
		}
	}

	idxTo = 0
	dToMove := wordsIntTo + digitsToWords(int(to.digitsFrac))
	for to.wordBuf[idxTo] == 0 && to.digitsInt > digitsPerWord {
		idxTo++
		to.digitsInt -= digitsPerWord
		dToMove--
	}
	if idxTo > 0 {
		curIdx := 0
		for dToMove > 0 {
			to.wordBuf[curIdx] = to.wordBuf[idxTo]
			curIdx++
			idxTo++
			dToMove--
		}
	}
	return err
}

// DecimalDiv does division of two decimals.
//
// from1    - dividend
// from2    - divisor
// to       - quotient
// fracIncr - increment of fraction
func DecimalDiv(from1, from2, to *MyDecimal, fracIncr int) error {
	from1, from2, to = validateArgs(from1, from2, to)
	to.resultFrac = min(from1.resultFrac+int8(fracIncr), mysql.MaxDecimalScale)
	return doDivMod(from1, from2, to, nil, fracIncr)
}

/*
DecimalMod does modulus of two decimals.

	    from1   - dividend
	    from2   - divisor
	    to      - modulus

	RETURN VALUE
	  E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW/E_DEC_DIV_ZERO;

	NOTES
	  see do_div_mod()

	DESCRIPTION
	  the modulus R in    R = M mod N

	 is defined as

	   0 <= |R| < |M|
	   sign R == sign M
	   R = M - k*N, where k is integer

	 thus, there's no requirement for M or N to be integers
*/
func DecimalMod(from1, from2, to *MyDecimal) error {
	from1, from2, to = validateArgs(from1, from2, to)
	to.resultFrac = max(from1.resultFrac, from2.resultFrac)
	return doDivMod(from1, from2, nil, to, 0)
}

func doDivMod(from1, from2, to, mod *MyDecimal, fracIncr int) error {
	var (
		frac1 = digitsToWords(int(from1.digitsFrac)) * digitsPerWord
		prec1 = int(from1.digitsInt) + frac1
		frac2 = digitsToWords(int(from2.digitsFrac)) * digitsPerWord
		prec2 = int(from2.digitsInt) + frac2
	)
	if mod != nil {
		to = mod
	}

	/* removing all the leading zeros */
	i := ((prec2 - 1) % digitsPerWord) + 1
	idx2 := 0
	for prec2 > 0 && from2.wordBuf[idx2] == 0 {
		prec2 -= i
		i = digitsPerWord
		idx2++
	}
	if prec2 <= 0 {
		/* short-circuit everything: from2 == 0 */
		return ErrDivByZero
	}

	prec2 -= countLeadingZeroes((prec2-1)%digitsPerWord, from2.wordBuf[idx2])
	i = ((prec1 - 1) % digitsPerWord) + 1
	idx1 := 0
	for prec1 > 0 && from1.wordBuf[idx1] == 0 {
		prec1 -= i
		i = digitsPerWord
		idx1++
	}
	if prec1 <= 0 {
		/* short-circuit everything: from1 == 0 */
		*to = zeroMyDecimalWithFrac(to.resultFrac)
		return nil
	}
	prec1 -= countLeadingZeroes((prec1-1)%digitsPerWord, from1.wordBuf[idx1])

	/* let's fix fracIncr, taking into account frac1,frac2 increase */
	fracIncr -= frac1 - int(from1.digitsFrac) + frac2 - int(from2.digitsFrac)
	if fracIncr < 0 {
		fracIncr = 0
	}

	digitsIntTo := (prec1 - frac1) - (prec2 - frac2)
	if from1.wordBuf[idx1] >= from2.wordBuf[idx2] {
		digitsIntTo++
	}
	var wordsIntTo int
	if digitsIntTo < 0 {
		digitsIntTo /= digitsPerWord
		wordsIntTo = 0
	} else {
		wordsIntTo = digitsToWords(digitsIntTo)
	}
	var wordsFracTo int
	var err error
	if mod != nil {
		// we're calculating N1 % N2.
		// The result will have
		// digitsFrac=max(frac1, frac2), as for subtraction
		// digitsInt=from2.digitsInt
		to.negative = from1.negative
		to.digitsFrac = max(from1.digitsFrac, from2.digitsFrac)
	} else {
		wordsFracTo = digitsToWords(frac1 + frac2 + fracIncr)
		wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
		to.negative = from1.negative != from2.negative
		to.digitsInt = int8(wordsIntTo * digitsPerWord)
		to.digitsFrac = int8(wordsFracTo * digitsPerWord)
	}
	idxTo := 0
	stopTo := wordsIntTo + wordsFracTo
	if mod == nil {
		for digitsIntTo < 0 && idxTo < wordBufLen {
			to.wordBuf[idxTo] = 0
			idxTo++
			digitsIntTo++
		}
	}
	i = digitsToWords(prec1)
	len1 := max(i+digitsToWords(2*frac2+fracIncr+1)+1, 3)

	tmp1 := make([]int32, len1)
	copy(tmp1, from1.wordBuf[idx1:idx1+i])

	start1 := 0
	var stop1 int
	start2 := idx2
	stop2 := idx2 + digitsToWords(prec2) - 1

	/* removing end zeroes */
	for from2.wordBuf[stop2] == 0 && stop2 >= start2 {
		stop2--
	}
	len2 := stop2 - start2
	stop2++

	/*
	   calculating norm2 (normalized from2.wordBuf[start2]) - we need from2.wordBuf[start2] to be large
	   (at least > DIG_BASE/2), but unlike Knuth's Alg. D we don't want to
	   normalize input numbers (as we don't make a copy of the divisor).
	   Thus we normalize first dec1 of buf2 only, and we'll normalize tmp1[start1]
	   on the fly for the purpose of guesstimation only.
	   It's also faster, as we're saving on normalization of from2.
	*/
	normFactor := wordBase / int64(from2.wordBuf[start2]+1)
	norm2 := int32(normFactor * int64(from2.wordBuf[start2]))
	if len2 > 0 {
		norm2 += int32(normFactor * int64(from2.wordBuf[start2+1]) / wordBase)
	}
	dcarry := int32(0)
	if tmp1[start1] < from2.wordBuf[start2] {
		dcarry = tmp1[start1]
		start1++
	}

	// main loop
	var guess int64
	for ; idxTo < stopTo; idxTo++ {
		/* short-circuit, if possible */
		if dcarry == 0 && tmp1[start1] < from2.wordBuf[start2] {
			guess = 0
		} else {
			/* D3: make a guess */
			x := int64(tmp1[start1]) + int64(dcarry)*wordBase
			y := int64(tmp1[start1+1])
			guess = (normFactor*x + normFactor*y/wordBase) / int64(norm2)
			if guess >= wordBase {
				guess = wordBase - 1
			}

			if len2 > 0 {
				/* remove normalization */
				if int64(from2.wordBuf[start2+1])*guess > (x-guess*int64(from2.wordBuf[start2]))*wordBase+y {
					guess--
				}
				if int64(from2.wordBuf[start2+1])*guess > (x-guess*int64(from2.wordBuf[start2]))*wordBase+y {
					guess--
				}
			}

			/* D4: multiply and subtract */
			idx2 = stop2
			idx1 = start1 + len2
			var carry int32
			for carry = 0; idx2 > start2; idx1-- {
				var hi, lo int32
				idx2--
				x = guess * int64(from2.wordBuf[idx2])
				hi = int32(x / wordBase)
				lo = int32(x - int64(hi)*wordBase)
				tmp1[idx1], carry = sub2(tmp1[idx1], lo, carry)
				carry += hi
			}
			if dcarry < carry {
				carry = 1
			} else {
				carry = 0
			}

			/* D5: check the remainder */
			if carry > 0 {
				/* D6: correct the guess */
				guess--
				idx2 = stop2
				idx1 = start1 + len2
				for carry = 0; idx2 > start2; idx1-- {
					idx2--
					tmp1[idx1], carry = add(tmp1[idx1], from2.wordBuf[idx2], carry)
				}
			}
		}
		if mod == nil {
			to.wordBuf[idxTo] = int32(guess)
		}
		dcarry = tmp1[start1]
		start1++
	}
	if mod != nil {
		/*
		   now the result is in tmp1, it has
		   digitsInt=prec1-frac1
		   digitsFrac=max(frac1, frac2)
		*/
		if dcarry != 0 {
			start1--
			tmp1[start1] = dcarry
		}
		idxTo = 0

		digitsIntTo = prec1 - frac1 - start1*digitsPerWord
		if digitsIntTo < 0 {
			/* If leading zeroes in the fractional part were earlier stripped */
			wordsIntTo = digitsIntTo / digitsPerWord
		} else {
			wordsIntTo = digitsToWords(digitsIntTo)
		}

		wordsFracTo = digitsToWords(int(to.digitsFrac))
		err = nil
		if wordsIntTo == 0 && wordsFracTo == 0 {
			*to = zeroMyDecimal
			return err
		}
		if wordsIntTo <= 0 {
			if -wordsIntTo >= wordBufLen {
				*to = zeroMyDecimal
				return ErrTruncated
			}
			stop1 = start1 + wordsIntTo + wordsFracTo
			wordsFracTo += wordsIntTo
			to.digitsInt = 0
			for wordsIntTo < 0 {
				to.wordBuf[idxTo] = 0
				idxTo++
				wordsIntTo++
			}
		} else {
			if wordsIntTo > wordBufLen {
				to.digitsInt = int8(digitsPerWord * wordBufLen)
				to.digitsFrac = 0
				return ErrOverflow
			}
			stop1 = start1 + wordsIntTo + wordsFracTo
			to.digitsInt = int8(min(wordsIntTo*digitsPerWord, int(from2.digitsInt)))
		}
		if wordsIntTo+wordsFracTo > wordBufLen {
			stop1 -= wordsIntTo + wordsFracTo - wordBufLen
			wordsFracTo = wordBufLen - wordsIntTo
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
			err = ErrTruncated
		}
		for start1 < stop1 {
			to.wordBuf[idxTo] = tmp1[start1]
			idxTo++
			start1++
		}
	}
	idxTo, digitsIntTo = to.removeLeadingZeros()
	to.digitsInt = int8(digitsIntTo)
	if idxTo != 0 {
		copy(to.wordBuf[:], to.wordBuf[idxTo:])
	}

	if to.IsZero() {
		to.negative = false
	}
	return err
}
