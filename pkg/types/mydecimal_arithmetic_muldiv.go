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
