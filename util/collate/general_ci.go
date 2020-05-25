// Copyright 2020 PingCAP, Inc.
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

package collate

import (
	"github.com/pingcap/tidb/util/stringutil"
)

const (
	// first byte of a 2-byte encoding starts 110 and carries 5 bits of data
	b2Lead = 0xC0 // 1100 0000
	b2Mask = 0x1F // 0001 1111

	// first byte of a 3-byte encoding starts 1110 and carries 4 bits of data
	b3Lead = 0xE0 // 1110 0000
	b3Mask = 0x0F // 0000 1111

	// first byte of a 4-byte encoding starts 11110 and carries 3 bits of data
	b4Lead = 0xF0 // 1111 0000
	b4Mask = 0x07 // 0000 0111

	// non-first bytes start 10 and carry 6 bits of data
	mbLead = 0x80 // 1000 0000
	mbMask = 0x3F // 0011 1111
)

type generalCICollator struct {
}

func sign(i int) int {
	if i < 0 {
		return -1
	} else if i > 0 {
		return 1
	}
	return 0
}

// compilePatternGeneralCI handles escapes and wild cards, generate pattern weights and types.
// This function is modified from stringutil.CompilePattern.
func compilePatternGeneralCI(pattern string, escape byte) (patWeights []uint16, patTypes []byte) {
	var lastAny bool
	runes := []rune(pattern)
	escapeRune := rune(escape)
	lenRunes := len(runes)
	patWeights = make([]uint16, lenRunes)
	patTypes = make([]byte, lenRunes)
	patLen := 0
	for i := 0; i < lenRunes; i++ {
		var tp byte
		var r = runes[i]
		switch r {
		case escapeRune:
			lastAny = false
			tp = stringutil.PatMatch
			if i < lenRunes-1 {
				i++
				r = runes[i]
				if r == escapeRune || r == '_' || r == '%' {
					// Valid escape.
				} else {
					// Invalid escape, fall back to escape byte.
					// mysql will treat escape character as the origin value even
					// the escape sequence is invalid in Go or C.
					// e.g., \m is invalid in Go, but in MySQL we will get "m" for select '\m'.
					// Following case is correct just for escape \, not for others like +.
					// TODO: Add more checks for other escapes.
					i--
					r = escapeRune
				}
			}
		case '_':
			if lastAny {
				continue
			}
			tp = stringutil.PatOne
		case '%':
			if lastAny {
				continue
			}
			lastAny = true
			tp = stringutil.PatAny
		default:
			lastAny = false
			tp = stringutil.PatMatch
		}
		patWeights[patLen] = convertRune(r)
		patTypes[patLen] = tp
		patLen++
	}
	patWeights = patWeights[:patLen]
	patTypes = patTypes[:patLen]
	return
}

// doMatchGeneralCI matches the string with patWeights and patTypes.
// The algorithm has linear time complexity.
// https://research.swtch.com/glob
// This function is modified from stringutil.DoMatch.
func doMatchGeneralCI(str string, patWeights []uint16, patTypes []byte) bool {
	// TODO(bb7133): it is possible to get the rune one by one to avoid the cost of get them as a whole.
	runes := []rune(str)
	lenRunes := len(runes)
	var rIdx, pIdx, nextRIdx, nextPIdx int
	for pIdx < len(patWeights) || rIdx < lenRunes {
		if pIdx < len(patWeights) {
			switch patTypes[pIdx] {
			case stringutil.PatMatch:
				if rIdx < lenRunes && convertRune(runes[rIdx]) == patWeights[pIdx] {
					pIdx++
					rIdx++
					continue
				}
			case stringutil.PatOne:
				if rIdx < len(str) {
					pIdx++
					rIdx++
					continue
				}
			case stringutil.PatAny:
				// Try to match at sIdx.
				// If that doesn't work out,
				// restart at sIdx+1 next.
				nextPIdx = pIdx
				nextRIdx = rIdx + 1
				pIdx++
				continue
			}
		}
		// Mismatch. Maybe restart.
		if 0 < nextRIdx && nextRIdx <= len(str) {
			pIdx = nextPIdx
			rIdx = nextRIdx
			continue
		}
		return false
	}
	// Matched all of pattern to all of name. Success.
	return true
}

// Compare implements Collator interface.
func (gc *generalCICollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)
	ai := 0
	bi := 0
	var r rune
	var au16 uint16
	var bu16 uint16
	for ai < len(a) && bi < len(b) {
		switch b0 := a[ai]; {
		case b0 < 0x80:
			r = rune(b0)
			ai++
		case b0 < 0xE0:
			r = rune(b0&b2Mask)<<6 |
				rune(a[1+ai]&mbMask)
			ai += 2
		case b0 < 0xF0:
			r = rune(b0&b3Mask)<<12 |
				rune(a[ai+1]&mbMask)<<6 |
				rune(a[ai+2]&mbMask)
			ai += 3
		default:
			r = rune(b0&b4Mask)<<18 |
				rune(a[ai+1]&mbMask)<<12 |
				rune(a[ai+2]&mbMask)<<6 |
				rune(a[ai+3]&mbMask)
			ai += 4
		}
		if r > 0xFFFF {
			au16 = 0xFFFD
		} else {
			au16 = mapTable[r]
		}

		switch b0 := b[bi]; {
		case b0 < 0x80:
			r = rune(b0)
			bi++
		case b0 < 0xE0:
			r = rune(b0&b2Mask)<<6 |
				rune(b[1+bi]&mbMask)
			bi += 2
		case b0 < 0xF0:
			r = rune(b0&b3Mask)<<12 |
				rune(b[bi+1]&mbMask)<<6 |
				rune(b[bi+2]&mbMask)
			bi += 3
		default:
			r = rune(b0&b4Mask)<<18 |
				rune(b[bi+1]&mbMask)<<12 |
				rune(b[bi+2]&mbMask)<<6 |
				rune(b[bi+3]&mbMask)
			bi += 4
		}
		if r > 0xFFFF {
			bu16 = 0xFFFD
		} else {
			bu16 = mapTable[r]
		}

		cmp := int(au16) - int(bu16)
		if cmp != 0 {
			return sign(cmp)
		}
	}
	return sign((len(a) - ai) - (len(b) - bi))
}

// Key implements Collator interface.
func (gc *generalCICollator) Key(str string) []byte {
	str = truncateTailingSpace(str)
	buf := make([]byte, 0, len(str))
	i := 0
	for _, r := range []rune(str) {
		u16 := convertRune(r)
		buf = append(buf, byte(u16>>8), byte(u16))
		i++
	}
	return buf
}

// KeyByBytes implements Collator interface.
func (gc *generalCICollator) KeyByBytes(buf *Buffer, str []byte) []byte {
	buf.init()
	str = truncateTailingSpaceByBytes(str)
	var u16 uint16
	var r rune
	for i := 0; i < len(str); {
		switch b0 := str[i]; {
		case b0 < 0x80:
			r = rune(b0)
			i++
		case b0 < 0xE0:
			r = rune(b0&b2Mask)<<6 |
				rune(str[1+i]&mbMask)
			i += 2
		case b0 < 0xF0:
			r = rune(b0&b3Mask)<<12 |
				rune(str[i+1]&mbMask)<<6 |
				rune(str[i+2]&mbMask)
			i += 3
		default:
			r = rune(b0&b4Mask)<<18 |
				rune(str[i+1]&mbMask)<<12 |
				rune(str[i+2]&mbMask)<<6 |
				rune(str[i+3]&mbMask)
			i += 4
		}
		if r > 0xFFFF {
			u16 = 0xFFFD
		} else {
			u16 = mapTable[r]
		}
		buf.key = append(buf.key, byte(u16>>8), byte(u16))
	}
	return buf.key
}

// Pattern implements Collator interface.
func (gc *generalCICollator) Pattern() WildcardPattern {
	return &ciPattern{}
}

type ciPattern struct {
	patChars []uint16
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *ciPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = compilePatternGeneralCI(patternStr, escape)
}

// Compile implements WildcardPattern interface.
func (p *ciPattern) DoMatch(str string) bool {
	return doMatchGeneralCI(str, p.patChars, p.patTypes)
}

func convertRune(r rune) uint16 {
	if r > 0xFFFF {
		return 0xFFFD
	}
	return mapTable[r]
}
