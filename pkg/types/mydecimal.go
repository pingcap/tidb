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
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"go.uber.org/zap"
)

// RoundMode is the type for round mode.
type RoundMode int32

// constant values.
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
	notFixedDec   = 31

	// Round up to the next integer if positive or down to the next integer if negative.
	ModeHalfUp RoundMode = 5
	// Truncate just truncates the decimal.
	ModeTruncate RoundMode = 10
	// Ceiling is not supported now.
	ModeCeiling RoundMode = 0

	pow10off int = 81
)

var (
	wordBufLen = 9
	mod9       = [128]int8{
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1,
	}
	div9 = [128]int{
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1, 1,
		2, 2, 2, 2, 2, 2, 2, 2, 2,
		3, 3, 3, 3, 3, 3, 3, 3, 3,
		4, 4, 4, 4, 4, 4, 4, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5, 5,
		6, 6, 6, 6, 6, 6, 6, 6, 6,
		7, 7, 7, 7, 7, 7, 7, 7, 7,
		8, 8, 8, 8, 8, 8, 8, 8, 8,
		9, 9, 9, 9, 9, 9, 9, 9, 9,
		10, 10, 10, 10, 10, 10, 10, 10, 10,
		11, 11, 11, 11, 11, 11, 11, 11, 11,
		12, 12, 12, 12, 12, 12, 12, 12, 12,
		13, 13, 13, 13, 13, 13, 13, 13, 13,
		14, 14,
	}
	powers10  = [10]int32{ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9}
	dig2bytes = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	fracMax   = [8]int32{
		900000000,
		990000000,
		999000000,
		999900000,
		999990000,
		999999000,
		999999900,
		999999990,
	}
	zeroMyDecimal = MyDecimal{}
	pow10off81    = [...]float64{1e-81, 1e-80, 1e-79, 1e-78, 1e-77, 1e-76, 1e-75, 1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66, 1e-65, 1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57, 1e-56, 1e-55, 1e-54, 1e-53, 1e-52, 1e-51, 1e-50, 1e-49, 1e-48, 1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41, 1e-40, 1e-39, 1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29, 1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42, 1e43, 1e44, 1e45, 1e46, 1e47, 1e48, 1e49, 1e50, 1e51, 1e52, 1e53, 1e54, 1e55, 1e56, 1e57, 1e58, 1e59, 1e60, 1e61, 1e62, 1e63, 1e64, 1e65, 1e66, 1e67, 1e68, 1e69, 1e70, 1e71, 1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78, 1e79, 1e80, 1e81}
)

// get the zero of MyDecimal with the specified result fraction digits
func zeroMyDecimalWithFrac(frac int8) MyDecimal {
	zero := MyDecimal{}
	zero.digitsFrac = frac
	zero.resultFrac = frac
	return zero
}

// add adds a and b and carry, returns the sum and new carry.
func add(a, b, carry int32) (sum int32, newCarry int32) {
	sum = a + b + carry
	if sum >= wordBase {
		newCarry = 1
		sum -= wordBase
	} else {
		newCarry = 0
	}
	return sum, newCarry
}

// add2 adds a and b and carry, returns the sum and new carry.
// It is only used in DecimalMul.
// nolint: revive
func add2(a, b, carry int32) (int32, int32) {
	sum := int64(a) + int64(b) + int64(carry)
	if sum >= wordBase {
		carry = 1
		sum -= wordBase
	} else {
		carry = 0
	}

	if sum >= wordBase {
		sum -= wordBase
		carry++
	}
	return int32(sum), carry
}

// sub subtracts b and carry from a, returns the diff and new carry.
func sub(a, b, carry int32) (diff int32, newCarry int32) {
	diff = a - b - carry
	if diff < 0 {
		newCarry = 1
		diff += wordBase
	} else {
		newCarry = 0
	}
	return diff, newCarry
}

// sub2 subtracts b and carry from a, returns the diff and new carry.
// the new carry may be 2.
func sub2(a, b, carry int32) (diff int32, newCarray int32) {
	diff = a - b - carry
	if diff < 0 {
		newCarray = 1
		diff += wordBase
	} else {
		newCarray = 0
	}
	if diff < 0 {
		diff += wordBase
		newCarray++
	}
	return diff, newCarray
}

// fixWordCntError limits word count in wordBufLen, and returns overflow or truncate error.
func fixWordCntError(wordsInt, wordsFrac int) (newWordsInt int, newWordsFrac int, err error) {
	if wordsInt+wordsFrac > wordBufLen {
		if wordsInt > wordBufLen {
			return wordBufLen, 0, ErrOverflow
		}
		return wordsInt, wordBufLen - wordsInt, ErrTruncated
	}
	return wordsInt, wordsFrac, nil
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

func digitsToWords(digits int) int {
	if digits+digitsPerWord-1 >= 0 && digits+digitsPerWord-1 < 128 {
		return div9[digits+digitsPerWord-1]
	}
	return (digits + digitsPerWord - 1) / digitsPerWord
}

// MyDecimalStructSize is the struct size of MyDecimal.
const MyDecimalStructSize = 40

// MyDecimal represents a decimal value.
type MyDecimal struct {
	digitsInt int8 // the number of *decimal* digits before the point.

	digitsFrac int8 // the number of decimal digits after the point.

	resultFrac int8 // result fraction digits.

	negative bool

	//  wordBuf is an array of int32 words.
	// A word is an int32 value can hold 9 digits.(0 <= word < wordBase)
	wordBuf [maxWordBufLen]int32
}

// IsNegative returns whether a decimal is negative.
func (d *MyDecimal) IsNegative() bool {
	return d.negative
}

// GetDigitsFrac returns the digitsFrac.
func (d *MyDecimal) GetDigitsFrac() int8 {
	return d.digitsFrac
}

// GetDigitsInt returns the digitsInt.
func (d *MyDecimal) GetDigitsInt() int8 {
	return d.digitsInt
}

// String returns the decimal string representation rounded to resultFrac.
func (d *MyDecimal) String() string {
	tmp := *d
	err := tmp.Round(&tmp, int(tmp.resultFrac), ModeHalfUp)
	terror.Log(errors.Trace(err))
	return string(tmp.ToString())
}

func (d *MyDecimal) stringSize() int {
	// sign, zero integer and dot.
	return int(d.digitsInt + d.digitsFrac + 3)
}

func (d *MyDecimal) removeLeadingZeros() (wordIdx int, digitsInt int) {
	digitsInt = int(d.digitsInt)
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

func (d *MyDecimal) removeTrailingZeros() (lastWordIdx int, digitsFrac int) {
	digitsFrac = int(d.digitsFrac)
	i := ((digitsFrac - 1) % digitsPerWord) + 1
	lastWordIdx = digitsToWords(int(d.digitsInt)) + digitsToWords(int(d.digitsFrac))
	for digitsFrac > 0 && d.wordBuf[lastWordIdx-1] == 0 {
		digitsFrac -= i
		i = digitsPerWord
		lastWordIdx--
	}
	if digitsFrac > 0 {
		digitsFrac -= countTrailingZeroes(9-((digitsFrac-1)%digitsPerWord), d.wordBuf[lastWordIdx-1])
	} else {
		digitsFrac = 0
	}
	return
}

// ToString converts decimal to its printable string representation without rounding.
//
//	RETURN VALUE
//
//	    str       - result string
//	    errCode   - eDecOK/eDecTruncate/eDecOverflow
func (d *MyDecimal) ToString() (str []byte) {
	str = make([]byte, d.stringSize())
	digitsFrac := int(d.digitsFrac)
	wordStartIdx, digitsInt := d.removeLeadingZeros()
	if digitsInt+digitsFrac == 0 {
		digitsInt = 1
		wordStartIdx = 0
	}

	digitsIntLen := digitsInt
	if digitsIntLen == 0 {
		digitsIntLen = 1
	}
	digitsFracLen := digitsFrac
	length := digitsIntLen + digitsFracLen
	if d.negative {
		length++
	}
	if digitsFrac > 0 {
		length++
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
		wordIdx := wordStartIdx + digitsToWords(digitsInt)
		str[fracIdx] = '.'
		fracIdx++
		for ; digitsFrac > 0; digitsFrac -= digitsPerWord {
			x := d.wordBuf[wordIdx]
			wordIdx++
			for i := min(digitsFrac, digitsPerWord); i > 0; i-- {
				y := x / digMask
				str[fracIdx] = byte(y) + '0'
				fracIdx++
				x -= y * digMask
				x *= 10
			}
		}
		for ; fill > 0; fill-- {
			str[fracIdx] = '0'
			fracIdx++
		}
	}
	fill = digitsIntLen - digitsInt
	if digitsInt == 0 {
		fill-- /* symbol 0 before digital point */
	}
	for ; fill > 0; fill-- {
		str[strIdx] = '0'
		strIdx++
	}
	if digitsInt > 0 {
		strIdx += digitsInt
		wordIdx := wordStartIdx + digitsToWords(digitsInt)
		for ; digitsInt > 0; digitsInt -= digitsPerWord {
			wordIdx--
			x := d.wordBuf[wordIdx]
			for i := min(digitsInt, digitsPerWord); i > 0; i-- {
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
func (d *MyDecimal) FromString(str []byte) error {
	for i := range str {
		if !isSpace(str[i]) {
			str = str[i:]
			break
		}
	}
	if len(str) == 0 {
		*d = zeroMyDecimal
		return ErrTruncatedWrongVal.FastGenByArgs("DECIMAL", str)
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
		return ErrTruncatedWrongVal.FastGenByArgs("DECIMAL", str)
	}
	wordsInt := digitsToWords(digitsInt)
	wordsFrac := digitsToWords(digitsFrac)
	wordsInt, wordsFrac, err := fixWordCntError(wordsInt, wordsFrac)
	if err != nil {
		digitsFrac = wordsFrac * digitsPerWord
		if err == ErrOverflow {
			digitsInt = wordsInt * digitsPerWord
		}
	}
	d.digitsInt = int8(digitsInt)
	d.digitsFrac = int8(digitsFrac)
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
	if endIdx+1 <= len(str) {
		if str[endIdx] == 'e' || str[endIdx] == 'E' {
			exponent, err1 := strToInt(string(str[endIdx+1:]))
			if err1 != nil {
				err = errors.Cause(err1)
				if err != ErrTruncated {
					*d = zeroMyDecimal
				}
			}
			if exponent > math.MaxInt32/2 {
				negative := d.negative
				maxDecimal(wordBufLen*digitsPerWord, 0, d)
				d.negative = negative
				err = ErrOverflow
			}
			if exponent < math.MinInt32/2 && err != ErrOverflow {
				*d = zeroMyDecimal
				err = ErrTruncated
			}
			if err != ErrOverflow {
				shiftErr := d.Shift(int(exponent))
				if shiftErr != nil {
					if shiftErr == ErrOverflow {
						negative := d.negative
						maxDecimal(wordBufLen*digitsPerWord, 0, d)
						d.negative = negative
					}
					err = shiftErr
				}
			}
		} else {
			trimstr := strings.TrimSpace(string(str[endIdx:]))
			if len(trimstr) != 0 {
				err = ErrTruncated
			}
		}
	}
	allZero := true
	for i := range wordBufLen {
		if d.wordBuf[i] != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		d.negative = false
	}
	d.resultFrac = d.digitsFrac
	return err
}



// Compare compares one decimal to another, returns -1/0/1.
func (d *MyDecimal) Compare(to *MyDecimal) int {
	if d.negative == to.negative {
		cmp, err := doSub(d, to, nil)
		terror.Log(errors.Trace(err))
		return cmp
	}
	if d.negative {
		return -1
	}
	return 1
}

// None of ToBin, ToFloat64, or ToString can encode MyDecimal without loss.
// So we still need a MarshalJSON/UnmarshalJSON function.
type jsonMyDecimal struct {
	DigitsInt  int8
	DigitsFrac int8
	ResultFrac int8
	Negative   bool
	WordBuf    [maxWordBufLen]int32
}

// MarshalJSON implements Marshaler.MarshalJSON interface.
func (d *MyDecimal) MarshalJSON() ([]byte, error) {
	var r jsonMyDecimal
	r.DigitsInt = d.digitsInt
	r.DigitsFrac = d.digitsFrac
	r.ResultFrac = d.resultFrac
	r.Negative = d.negative
	r.WordBuf = d.wordBuf
	return json.Marshal(r)
}

// UnmarshalJSON implements Unmarshaler.UnmarshalJSON interface.
func (d *MyDecimal) UnmarshalJSON(data []byte) error {
	var r jsonMyDecimal
	err := json.Unmarshal(data, &r)
	if err == nil {
		d.digitsInt = r.DigitsInt
		d.digitsFrac = r.DigitsFrac
		d.resultFrac = r.ResultFrac
		d.negative = r.Negative
		d.wordBuf = r.WordBuf
	}
	return err
}


// DecimalPeak returns the length of the encoded decimal.
func DecimalPeak(b []byte) (int, error) {
	if len(b) < 3 {
		return 0, ErrBadNumber
	}
	precision := int(b[0])
	frac := int(b[1])
	binSize, err := DecimalBinSize(precision, frac)
	if err != nil {
		return 0, err
	}
	return binSize + 2, nil
}

// NewDecFromInt creates a MyDecimal from int.
func NewDecFromInt(i int64) *MyDecimal {
	return new(MyDecimal).FromInt(i)
}

// NewDecFromUint creates a MyDecimal from uint.
func NewDecFromUint(i uint64) *MyDecimal {
	return new(MyDecimal).FromUint(i)
}

// NewDecFromFloatForTest creates a MyDecimal from float, as it returns no error, it should only be used in test.
func NewDecFromFloatForTest(f float64) *MyDecimal {
	dec := new(MyDecimal)
	err := dec.FromFloat64(f)
	if err != nil {
		log.Panic("encountered error", zap.Error(err), zap.String("DecimalStr", strconv.FormatFloat(f, 'g', -1, 64)))
	}
	return dec
}

// NewDecFromStringForTest creates a MyDecimal from string, as it returns no error, it should only be used in test.
func NewDecFromStringForTest(s string) *MyDecimal {
	dec := new(MyDecimal)
	err := dec.FromString([]byte(s))
	if err != nil {
		log.Panic("encountered error", zap.Error(err), zap.String("DecimalStr", s))
	}
	return dec
}

// NewMaxOrMinDec returns the max or min value decimal for given precision and fraction.
func NewMaxOrMinDec(negative bool, prec, frac int) *MyDecimal {
	str := make([]byte, prec+2)
	for i := range str {
		str[i] = '9'
	}
	if negative {
		str[0] = '-'
	} else {
		str[0] = '+'
	}
	str[1+prec-frac] = '.'
	dec := new(MyDecimal)
	err := dec.FromString(str)
	terror.Log(errors.Trace(err))
	return dec
}
