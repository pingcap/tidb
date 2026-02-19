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
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// FromInt sets the decimal value from int64.
func (d *MyDecimal) FromInt(val int64) *MyDecimal {
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
func (d *MyDecimal) FromUint(val uint64) *MyDecimal {
	x := val
	wordIdx := 1
	for x >= wordBase {
		wordIdx++
		x /= wordBase
	}
	d.digitsFrac = 0
	d.digitsInt = int8(wordIdx * digitsPerWord)
	x = val
	for wordIdx > 0 {
		wordIdx--
		y := x / wordBase
		d.wordBuf[wordIdx] = int32(x - y*wordBase)
		x = y
	}
	return d
}

// ToInt returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToInt() (int64, error) {
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
				return math.MinInt64, ErrOverflow
			}
			return math.MaxInt64, ErrOverflow
		}
	}
	/* boundary case: 9223372036854775808 */
	if !d.negative && x == math.MinInt64 {
		return math.MaxInt64, ErrOverflow
	}
	if !d.negative {
		x = -x
	}
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		if d.wordBuf[wordIdx] != 0 {
			return x, ErrTruncated
		}
		wordIdx++
	}
	return x, nil
}

// ToUint returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToUint() (uint64, error) {
	if d.negative {
		return 0, ErrOverflow
	}
	var x uint64
	wordIdx := 0
	for i := d.digitsInt; i > 0; i -= digitsPerWord {
		y := x
		x = x*wordBase + uint64(d.wordBuf[wordIdx])
		wordIdx++
		if y > math.MaxUint64/wordBase || x < y {
			return math.MaxUint64, ErrOverflow
		}
	}
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		if d.wordBuf[wordIdx] != 0 {
			return x, ErrTruncated
		}
		wordIdx++
	}
	return x, nil
}

// FromFloat64 creates a decimal from float64 value.
func (d *MyDecimal) FromFloat64(f float64) error {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	return d.FromString([]byte(s))
}

// ToFloat64 converts decimal to float64 value.
func (d *MyDecimal) ToFloat64() (f float64, err error) {
	digitsInt := int(d.digitsInt)
	digitsFrac := int(d.digitsFrac)
	// https://en.wikipedia.org/wiki/Double-precision_floating-point_format#IEEE_754_double-precision_binary_floating-point_format:_binary64
	// "The 53-bit significand precision gives from 15 to 17 significant decimal digits precision (2−53 ≈ 1.11 × 10−16).
	// If a decimal string with at most 15 significant digits is converted to IEEE 754 double-precision representation,
	// and then converted back to a decimal string with the same number of digits, the final result should match the original string."
	// The new method is about 10.5X faster than the old one according to the benchmark in types/mydecimal_benchmark_test.go.
	// The initial threshold here is 15, we adjusted it to 12 for compatibility with previous.
	// We did a full test of 12 significant digits to make sure it's correct and behaves as before.
	if digitsInt+digitsFrac > 12 {
		f, err = strconv.ParseFloat(d.String(), 64)
		if err != nil {
			err = ErrOverflow
		}
		return
	}
	wordsInt := (digitsInt-1)/digitsPerWord + 1
	wordIdx := 0
	for i := 0; i < digitsInt; i += digitsPerWord {
		x := d.wordBuf[wordIdx]
		wordIdx++
		// Equivalent to f += float64(x) * math.Pow10((wordsInt-wordIdx)*digitsPerWord)
		f += float64(x) * pow10off81[(wordsInt-wordIdx)*digitsPerWord+pow10off]
	}
	fracStart := wordIdx
	for i := 0; i < digitsFrac; i += digitsPerWord {
		x := d.wordBuf[wordIdx]
		wordIdx++
		// Equivalent to f += float64(x) * math.Pow10(-digitsPerWord*(wordIdx-fracStart))
		f += float64(x) * pow10off81[-digitsPerWord*(wordIdx-fracStart)+pow10off]
	}
	// Equivalent to unit := math.Pow10(int(d.resultFrac))
	unit := pow10off81[int(d.resultFrac)+pow10off]
	f = math.Round(f*unit) / unit
	if d.negative {
		f = -f
	}
	return
}

/*
ToBin converts decimal to its binary fixed-length representation
two representations of the same length can be compared with memcmp
with the correct -1/0/+1 result

	  PARAMS
			precision/frac - if precision is 0, internal value of the decimal will be used,
			then the encoded value is not memory comparable.

	  NOTE
	    the buffer is assumed to be of the size DecimalBinSize(precision, frac)

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
func (d *MyDecimal) ToBin(precision, frac int) ([]byte, error) {
	return d.WriteBin(precision, frac, []byte{})
}

// WriteBin encode and write the binary encoded to target buffer
func (d *MyDecimal) WriteBin(precision, frac int, buf []byte) ([]byte, error) {
	if precision > digitsPerWord*maxWordBufLen || precision < 0 || frac > mysql.MaxDecimalScale || frac < 0 {
		return buf, ErrBadNumber
	}
	var err error
	var mask int32
	if d.negative {
		mask = -1
	}
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	leadingDigits := digitsInt - wordsInt*digitsPerWord
	wordsFrac := frac / digitsPerWord
	trailingDigits := frac - wordsFrac*digitsPerWord

	wordsFracFrom := int(d.digitsFrac) / digitsPerWord
	trailingDigitsFrom := int(d.digitsFrac) - wordsFracFrom*digitsPerWord
	intSize := wordsInt*wordSize + dig2bytes[leadingDigits]
	fracSize := wordsFrac*wordSize + dig2bytes[trailingDigits]
	fracSizeFrom := wordsFracFrom*wordSize + dig2bytes[trailingDigitsFrom]
	originIntSize := intSize
	originFracSize := fracSize
	bufLen := len(buf)
	if bufLen+intSize+fracSize <= cap(buf) {
		buf = buf[:bufLen+intSize+fracSize]
	} else {
		buf = append(buf, make([]byte, intSize+fracSize)...)
	}
	bin := buf[bufLen:]
	binIdx := 0
	wordIdxFrom, digitsIntFrom := d.removeLeadingZeros()
	if digitsIntFrom+fracSizeFrom == 0 {
		mask = 0
		digitsInt = 1
	}

	wordsIntFrom := digitsIntFrom / digitsPerWord
	leadingDigitsFrom := digitsIntFrom - wordsIntFrom*digitsPerWord
	iSizeFrom := wordsIntFrom*wordSize + dig2bytes[leadingDigitsFrom]

	if digitsInt < digitsIntFrom {
		wordIdxFrom += wordsIntFrom - wordsInt
		if leadingDigitsFrom > 0 {
			wordIdxFrom++
		}
		if leadingDigits > 0 {
			wordIdxFrom--
		}
		wordsIntFrom = wordsInt
		leadingDigitsFrom = leadingDigits
		err = ErrOverflow
	} else if intSize > iSizeFrom {
		for intSize > iSizeFrom {
			intSize--
			bin[binIdx] = byte(mask)
			binIdx++
		}
	}

	if fracSize < fracSizeFrom ||
		(fracSize == fracSizeFrom && (trailingDigits <= trailingDigitsFrom || wordsFrac <= wordsFracFrom)) {
		if fracSize < fracSizeFrom || (fracSize == fracSizeFrom && trailingDigits < trailingDigitsFrom) || (fracSize == fracSizeFrom && wordsFrac < wordsFracFrom) {
			err = ErrTruncated
		}
		wordsFracFrom = wordsFrac
		trailingDigitsFrom = trailingDigits
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
	return buf, err
}

// ToHashKey removes the leading and trailing zeros and generates a hash key.
// Two Decimals dec0 and dec1 with different fraction will generate the same hash keys if dec0.Compare(dec1) == 0.
func (d *MyDecimal) ToHashKey() ([]byte, error) {
	_, digitsInt := d.removeLeadingZeros()
	_, digitsFrac := d.removeTrailingZeros()
	prec := digitsInt + digitsFrac
	if prec == 0 { // zeroDecimal
		prec = 1
	}
	buf, err := d.ToBin(prec, digitsFrac)
	if err == ErrTruncated {
		// This err is caused by shorter digitsFrac;
		// After removing the trailing zeros from a Decimal,
		// so digitsFrac may be less than the real digitsFrac of the Decimal,
		// thus ErrTruncated may be raised, we can ignore it here.
		err = nil
	}
	buf = append(buf, byte(digitsFrac))
	return buf, err
}

// HashKeySize returns the size of hash key
func (d *MyDecimal) HashKeySize() (int, error) {
	_, digitsInt := d.removeLeadingZeros()
	_, digitsFrac := d.removeTrailingZeros()
	prec := digitsInt + digitsFrac
	if prec == 0 { // zeroDecimal
		prec = 1
	}

	size, err := DecimalBinSize(prec, digitsFrac)
	if err != nil {
		return 0, err
	}

	return size + 1, nil
}

// PrecisionAndFrac returns the internal precision and frac number.
func (d *MyDecimal) PrecisionAndFrac() (precision, frac int) {
	frac = int(d.digitsFrac)
	_, digitsInt := d.removeLeadingZeros()
	precision = digitsInt + frac
	if precision == 0 {
		precision = 1
	}
	return
}

// IsZero checks whether it's a zero decimal.
func (d *MyDecimal) IsZero() bool {
	isZero := true
	for _, val := range d.wordBuf {
		if val != 0 {
			isZero = false
			break
		}
	}
	return isZero
}

// FromBin Restores decimal from its binary fixed-length representation.
func (d *MyDecimal) FromBin(bin []byte, precision, frac int) (binSize int, err error) {
	if len(bin) == 0 {
		*d = zeroMyDecimal
		return 0, ErrBadNumber
	}
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
	binSize, err = DecimalBinSize(precision, frac)
	if err != nil {
		return 0, err
	}
	if binSize < 0 || binSize > 40 {
		return 0, ErrBadNumber
	}
	dCopy := make([]byte, 40)
	dCopy = dCopy[:binSize]
	copy(dCopy, bin)
	dCopy[0] ^= 0x80
	bin = dCopy
	oldWordsIntTo := wordsIntTo
	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	if err != nil {
		if wordsIntTo < oldWordsIntTo {
			binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize
		} else {
			trailingDigits = 0
			wordsFrac = wordsFracTo
		}
	}
	d.negative = mask != 0
	d.digitsInt = int8(wordsInt*digitsPerWord + leadingDigits)
	d.digitsFrac = int8(wordsFrac*digitsPerWord + trailingDigits)

	wordIdx := 0
	if leadingDigits > 0 {
		i := dig2bytes[leadingDigits]
		x := readWord(bin[binIdx:], i)
		binIdx += i
		d.wordBuf[wordIdx] = x ^ mask
		if uint64(d.wordBuf[wordIdx]) >= uint64(powers10[leadingDigits+1]) {
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
		if wordIdx > 0 || d.wordBuf[wordIdx] != 0 {
			wordIdx++
		} else {
			d.digitsInt -= int8(leadingDigits)
		}
	}
	for stop := binIdx + wordsInt*wordSize; binIdx < stop; binIdx += wordSize {
		d.wordBuf[wordIdx] = readWord(bin[binIdx:], 4) ^ mask
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
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
			return binSize, ErrBadNumber
		}
		wordIdx++
	}

	if trailingDigits > 0 {
		i := dig2bytes[trailingDigits]
		x := readWord(bin[binIdx:], i)
		d.wordBuf[wordIdx] = (x ^ mask) * powers10[digitsPerWord-trailingDigits]
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
	}

	if d.digitsInt == 0 && d.digitsFrac == 0 {
		*d = zeroMyDecimal
	}
	d.resultFrac = int8(frac)
	return binSize, err
}

// DecimalBinSize returns the size of array to hold a binary representation of a decimal.
func DecimalBinSize(precision, frac int) (int, error) {
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	wordsFrac := frac / digitsPerWord
	xInt := digitsInt - wordsInt*digitsPerWord
	xFrac := frac - wordsFrac*digitsPerWord
	if xInt < 0 || xInt >= len(dig2bytes) || xFrac < 0 || xFrac >= len(dig2bytes) {
		return 0, ErrBadNumber
	}
	return wordsInt*wordSize + dig2bytes[xInt] + wordsFrac*wordSize + dig2bytes[xFrac], nil
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
