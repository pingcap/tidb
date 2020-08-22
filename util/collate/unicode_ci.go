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
	// magic number indicate weight has 2 uint64, should get from `longRuneMap`
	longRune uint64 = 0xFFFD
	// first byte of a 2-byte encoding starts 110 and carries 5 bits of data
	b2Mask = 0x1F // 0001 1111

	// first byte of a 3-byte encoding starts 1110 and carries 4 bits of data
	b3Mask = 0x0F // 0000 1111

	// first byte of a 4-byte encoding starts 11110 and carries 3 bits of data
	b4Mask = 0x07 // 0000 0111

	// non-first bytes start 10 and carry 6 bits of data
	mbMask = 0x3F // 0011 1111
)

// decode rune by hand
func decodeRune(s string, si int) (r rune, newIndex int) {
	switch b := s[si]; {
	case b < 0x80:
		r = rune(b)
		newIndex = si + 1
	case b < 0xE0:
		r = rune(b&b2Mask)<<6 |
			rune(s[1+si]&mbMask)
		newIndex = si + 2
	case b < 0xF0:
		r = rune(b&b3Mask)<<12 |
			rune(s[si+1]&mbMask)<<6 |
			rune(s[si+2]&mbMask)
		newIndex = si + 3
	default:
		r = rune(b&b4Mask)<<18 |
			rune(s[si+1]&mbMask)<<12 |
			rune(s[si+2]&mbMask)<<6 |
			rune(s[si+3]&mbMask)
		newIndex = si + 4
	}
	return
}

// unicodeCICollator implements UCA. see http://unicode.org/reports/tr10/
type unicodeCICollator struct {
}

// Compare implements Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)
	an, bn := uint64(0), uint64(0)
	ar, br := rune(0), rune(0)
	as, bs := uint64(0), uint64(0)
	ai, bi := 0, 0
	for {
		if an == 0 {
			if as == 0 {
				for an == 0 && ai < len(a) {
					ar, ai = decodeRune(a, ai)
					an, as = convertUnicode(ar)
				}
			} else {
				an = as
				as = 0
			}
		}

		if bn == 0 {
			if bs == 0 {
				for bn == 0 && bi < len(b) {
					br, bi = decodeRune(b, bi)
					bn, bs = convertUnicode(br)
				}
			} else {
				bn = bs
				bs = 0
			}
		}

		if an == 0 || bn == 0 {
			return sign(int(an) - int(bn))
		}

		if an == bn {
			an, bn = 0, 0
			continue
		}

		for an != 0 && bn != 0 {
			if (an^bn)&0xFFFF == 0 {
				an >>= 16
				bn >>= 16
			} else {
				return sign(int(an&0xFFFF) - int(bn&0xFFFF))
			}
		}
	}
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	str = truncateTailingSpace(str)
	buf := make([]byte, 0, len(str)*2)
	r, si := rune(0), 0
	sn, ss := uint64(0), uint64(0)

	for si < len(str) {
		r, si = decodeRune(str, si)
		sn, ss = convertUnicode(r)
		for sn != 0 {
			buf = append(buf, byte((sn&0xFF00)>>8), byte(sn))
			sn >>= 16
		}
		for ss != 0 {
			buf = append(buf, byte((ss&0xFF00)>>8), byte(ss))
			ss >>= 16
		}
	}
	return buf
}

func convertUnicode(r rune) (uint64, uint64) {
	if r > 0xFFFF {
		return 0xFFFD, 0
	}

	sn := mapTable[r]
	if sn != 0 {
		if sn == longRune {
			return longRuneMap[r][0], longRuneMap[r][1]
		}
		return sn, 0
	}
	return 0, 0
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return &unicodePattern{}
}

type unicodePattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = compilePatternUnicodeCI(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *unicodePattern) DoMatch(str string) bool {
	return doMatchUnicodeCI(str, p.patChars, p.patTypes)
}

// compilePatternUnicodeCI handles escapes and wild cards, generate pattern weights and types.
// This function is modified from stringutil.CompilePattern.
func compilePatternUnicodeCI(pattern string, escape byte) (patWeights []rune, patTypes []byte) {
	runes := []rune(pattern)
	escapeRune := rune(escape)
	lenRunes := len(runes)
	patWeights = make([]rune, lenRunes)
	patTypes = make([]byte, lenRunes)
	patLen := 0
	for i := 0; i < lenRunes; i++ {
		var tp byte
		var r = runes[i]
		switch r {
		case escapeRune:
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
			// %_ => _%
			if patLen > 0 && patTypes[patLen-1] == stringutil.PatAny {
				tp = stringutil.PatAny
				r = '%'
				patWeights[patLen-1], patTypes[patLen-1] = '_', stringutil.PatOne
			} else {
				tp = stringutil.PatOne
			}
		case '%':
			// %% => %
			if patLen > 0 && patTypes[patLen-1] == stringutil.PatAny {
				continue
			}
			tp = stringutil.PatAny
		default:
			tp = stringutil.PatMatch
		}
		patWeights[patLen] = r
		patTypes[patLen] = tp
		patLen++
	}
	patWeights = patWeights[:patLen]
	patTypes = patTypes[:patLen]
	return
}

// doMatchUnicodeCI matches the string with patWeights and patTypes.
// The algorithm has linear time complexity.
// https://research.swtch.com/glob
// This function is modified from stringutil.DoMatch.
func doMatchUnicodeCI(str string, patWeights []rune, patTypes []byte) bool {
	runes := []rune(str)
	lenRunes := len(runes)
	var rIdx, pIdx, nextRIdx, nextPIdx int
	for pIdx < len(patWeights) || rIdx < lenRunes {
		if pIdx < len(patWeights) {
			switch patTypes[pIdx] {
			case stringutil.PatMatch:
				if rIdx < lenRunes && runeEqual(runes[rIdx], patWeights[pIdx]) {
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

// runeEqual compare rune is equal with unicode_ci collation
func runeEqual(a, b rune) bool {
	if a > 0xFFFF || b > 0xFFFF {
		return a == b
	}

	ar, br := mapTable[a], mapTable[b]
	if ar != br {
		return false
	}

	if ar == longRune {
		return a == b
	}

	return true
}
