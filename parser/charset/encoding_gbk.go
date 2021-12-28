// Copyright 2021 PingCAP, Inc.
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

package charset

import (
	"strings"
	"unicode"

	"golang.org/x/text/encoding/simplifiedchinese"
)

// EncodingGBKImpl is the instance of encodingGBK
var EncodingGBKImpl = &encodingGBK{encodingBase{enc: simplifiedchinese.GBK}}

func init() {
	EncodingGBKImpl.self = EncodingGBKImpl
}

// encodingGBK is GBK encoding.
type encodingGBK struct {
	encodingBase
}

// Name implements Encoding interface.
func (e *encodingGBK) Name() string {
	return CharsetGBK
}

// Tp implements Encoding interface.
func (e *encodingGBK) Tp() EncodingTp {
	return EncodingTpGBK
}

// Peek implements Encoding interface.
func (e *encodingGBK) Peek(src []byte) []byte {
	charLen := 2
	if len(src) == 0 || src[0] < 0x80 {
		// A byte in the range 00–7F is a single byte that means the same thing as it does in ASCII.
		charLen = 1
	}
	if charLen < len(src) {
		return src[:charLen]
	}
	return src
}

func (e *encodingGBK) MbLen(bs string) int {
	if len(bs) < 2 {
		return 0
	}

	if 0x81 <= bs[0] && bs[0] <= 0xfe {
		if (0x40 <= bs[1] && bs[1] <= 0x7e) || (0x80 <= bs[1] && bs[1] <= 0xfe) {
			return 2
		}
	}

	return 0
}

// ToUpper implements Encoding interface.
func (e *encodingGBK) ToUpper(d string) string {
	return strings.ToUpperSpecial(GBKCase, d)
}

// ToLower implements Encoding interface.
func (e *encodingGBK) ToLower(d string) string {
	return strings.ToLowerSpecial(GBKCase, d)
}

// GBKCase follows https://dev.mysql.com/worklog/task/?id=4583.
var GBKCase = unicode.SpecialCase{
	unicode.CaseRange{Lo: 0x00E0, Hi: 0x00E1, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x00E8, Hi: 0x00EA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x00EC, Hi: 0x00ED, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x00F2, Hi: 0x00F3, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x00F9, Hi: 0x00FA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x00FC, Hi: 0x00FC, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x0101, Hi: 0x0101, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x0113, Hi: 0x0113, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x011B, Hi: 0x011B, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x012B, Hi: 0x012B, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x0144, Hi: 0x0144, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x0148, Hi: 0x0148, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x014D, Hi: 0x014D, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x016B, Hi: 0x016B, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01CE, Hi: 0x01CE, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01D0, Hi: 0x01D0, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01D2, Hi: 0x01D2, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01D4, Hi: 0x01D4, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01D6, Hi: 0x01D6, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01D8, Hi: 0x01D8, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01DA, Hi: 0x01DA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x01DC, Hi: 0x01DC, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x216A, Hi: 0x216B, Delta: [unicode.MaxCase]rune{0, 0, 0}},
}
