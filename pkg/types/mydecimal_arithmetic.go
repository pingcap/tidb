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
