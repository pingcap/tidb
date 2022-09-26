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

//go:build !codes
// +build !codes

package test_driver

const panicInfo = "This branch is not implemented. " +
	"This is because you are trying to test something specific to TiDB's MyDecimal implementation. " +
	"It is recommended to do this in TiDB repository."

// constant values.
const (
	maxWordBufLen = 9 // A MyDecimal holds 9 words.
	digitsPerWord = 9 // A word holds 9 digits.
	digMask       = 100000000
)

var (
	wordBufLen = 9
)

// fixWordCntError limits word count in wordBufLen, and returns overflow or truncate error.
func fixWordCntError(wordsInt, wordsFrac int) (newWordsInt int, newWordsFrac int, err error) {
	if wordsInt+wordsFrac > wordBufLen {
		panic(panicInfo)
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
	for word < pow10(i) {
		i--
		leading++
	}
	return leading
}

func digitsToWords(digits int) int {
	return (digits + digitsPerWord - 1) / digitsPerWord
}

// MyDecimal represents a decimal value.
type MyDecimal struct {
	digitsInt int8 // the number of *decimal* digits before the point.

	digitsFrac int8 // the number of decimal digits after the point.

	resultFrac int8 // result fraction digits.

	negative bool

	// wordBuf is an array of int32 words.
	// A word is an int32 value can hold 9 digits.(0 <= word < wordBase)
	wordBuf [maxWordBufLen]int32
}

// String returns the decimal string representation rounded to resultFrac.
func (d *MyDecimal) String() string {
	tmp := *d
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
			for i := myMin(digitsFrac, digitsPerWord); i > 0; i-- {
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
func (d *MyDecimal) FromString(str []byte) error {
	for i := 0; i < len(str); i++ {
		if !isSpace(str[i]) {
			str = str[i:]
			break
		}
	}
	if len(str) == 0 {
		panic(panicInfo)
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
		panic(panicInfo)
	}
	wordsInt := digitsToWords(digitsInt)
	wordsFrac := digitsToWords(digitsFrac)
	wordsInt, _, err := fixWordCntError(wordsInt, wordsFrac)
	if err != nil {
		panic(panicInfo)
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
		word += int32(str[strIdx]-'0') * pow10(innerIdx)
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
		d.wordBuf[wordIdx] = word * pow10(digitsPerWord-innerIdx)
	}
	if endIdx+1 <= len(str) && (str[endIdx] == 'e' || str[endIdx] == 'E') {
		panic(panicInfo)
	}
	allZero := true
	for i := 0; i < wordBufLen; i++ {
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
