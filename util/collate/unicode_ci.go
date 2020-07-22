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
	"unicode/utf8"
)

const terminal uint16 = 0x0000

// unicodeScanner used to scan unicode string
type unicodeScanner struct {
	s      string
	expand []uint16
}

func newUnicodeScanner(s string) *unicodeScanner {
	return &unicodeScanner{s: s}
}

// return next weight of string. return `terminal` when string is done
func (us *unicodeScanner) next() uint16 {
	if len(us.expand) != 0 && us.expand[0] != terminal {
		r := us.expand[0]
		us.expand = us.expand[1:]
		return r
	}
	for len(us.s) > 0 {
		r, rsize := utf8.DecodeRuneInString(us.s)
		us.s = us.s[rsize:]

		unicodeWeight := convertRuneToWeight(r)
		if unicodeWeight != nil {
			us.expand = unicodeWeight[1:]
			return unicodeWeight[0]
		}
	}

	return terminal
}

type unicodeCICollator struct {
}

// Compare implement Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	as := newUnicodeScanner(truncateTailingSpace(a))
	bs := newUnicodeScanner(truncateTailingSpace(b))

	for {
		an, bn := as.next(), bs.next()
		if an != bn || an == terminal {
			return sign(int(an) - int(bn))
		}
	}
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	buf := make([]byte, 0, len(str)*2)
	scanner := newUnicodeScanner(truncateTailingSpace(str))

	for w := scanner.next(); w != terminal; w = scanner.next() {
		buf = append(buf, byte(w>>8), byte(w))
	}

	return buf
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return &unicodeCIPattern{}
}

type unicodeCIPattern struct {
	patChars []rune
	patTypes []byte
}

// compilePatternUnicodeCI handles escapes and wild cards, generate pattern weights and types.
// This function is modified from stringutil.CompilePattern.
func compilePatternUnicodeCI(pattern string, escape byte) (patWeights []rune, patTypes []byte) {
	runes := []rune(pattern)
	escapeRune := rune(escape)
	lenRunes := len(runes)
	patWeights = make([]rune, 0, lenRunes)
	patTypes = make([]byte, 0, lenRunes)
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
			tp = stringutil.PatOne
		case '%':
			tp = stringutil.PatAny
		default:
			tp = stringutil.PatMatch
		}
		patWeights = append(patWeights, r)
		patTypes = append(patTypes, tp)
	}
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

// Compile implements WildcardPattern interface.
func (p *unicodeCIPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = compilePatternUnicodeCI(patternStr, escape)
}

// Compile implements WildcardPattern interface.
func (p *unicodeCIPattern) DoMatch(str string) bool {
	return doMatchUnicodeCI(str, p.patChars, p.patTypes)
}

// runeEqual compare rune is equal with unicode_ci collation
func runeEqual(a, b rune) bool {
	if a > 0xFFFF || b > 0xFFFF {
		return a == b
	}

	getWeight := func(r rune) []uint16 {
		if runePlaneTable[r>>8] == nil {
			return nil
		}

		offset := uint16(r&0xff) * uint16(runeWeightLength[r>>8])
		return runePlaneTable[r>>8][offset : offset+uint16(runeWeightLength[r>>8])]
	}

	weightA, weightB := getWeight(a), getWeight(b)

	if weightA == nil || weightB == nil {
		return a == b
	}

	for weightA[0] == weightB[0] && weightA[0] != terminal {
		weightA, weightB = weightA[1:], weightB[1:]
	}

	return weightA[0] == weightB[0]
}

// convertRuneToWeight convert rune to weight, return nil if rune is ignorable
func convertRuneToWeight(r rune) []uint16 {
	if r > 0xFFFF {
		return []uint16{0xFFFD}
	}

	// a unicode character is not in the table
	// see https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html#charset-unicode-sets-collating-weights
	if runePlaneTable[r>>8] == nil {
		base := 0
		if r >= 0x3400 && r <= 0x4DB5 {
			base = 0xFB80
		} else if r >= 0x4E00 && r <= 0x9FA5 {
			base = 0xFB40
		} else {
			base = 0xFBC0
		}
		return []uint16{uint16(base + int(r>>15)), uint16((r & 0x7FFF) | 0x8000)}
	}

	plane := r >> 8
	offset := uint16(r&0xFF) * uint16(runeWeightLength[plane])
	if runePlaneTable[plane][offset] != terminal {
		return runePlaneTable[plane][offset : offset+uint16(runeWeightLength[plane])]
	}

	return nil
}

var (
	runeWeightLength = [4352]uint8{}
	runePlaneTable = [4352][]uint16{}
)
