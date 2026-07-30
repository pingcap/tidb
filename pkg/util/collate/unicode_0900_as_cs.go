// Copyright 2026 PingCAP, Inc.
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

package collate

import (
	"bytes"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/util/collate/ucadata"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// unicode0900ASCSCollator implements the accent-sensitive, case-sensitive UCA collation
// utf8mb4_0900_as_cs. Unlike the ai_ci collators it compares all three weight levels
// (primary, secondary, tertiary), so 'e' < 'é' and 'a' < 'A'. See http://unicode.org/reports/tr10/.
//
// Sort keys follow the standard UCA form: the primary weights of every collation element,
// a level separator, the secondary weights, a separator, then the tertiary weights. Because
// no real weight is zero at any level (zero-weight levels are skipped), the 0x0000 separator
// is always smaller than any weight, which gives the correct cross-level ordering.
// caseFirst selects the ICU `kf` ordering between letters that differ only by case.
type caseFirst uint8

const (
	caseFirstOff   caseFirst = iota // case ordered by the tertiary level (lowercase first)
	caseFirstUpper                  // uppercase sorts before lowercase
	caseFirstLower                  // lowercase sorts before uppercase
)

// strengthLevel selects the ICU `ks` comparison strength: how many weight levels are significant.
type strengthLevel uint8

const (
	// strengthTertiary (the zero value) compares primary, secondary, and tertiary levels:
	// accent- and case-sensitive. This is plain utf8mb4_0900_as_cs.
	strengthTertiary strengthLevel = iota
	// strengthPrimary compares the primary level only: accent- and case-insensitive.
	strengthPrimary
	// strengthSecondary compares primary and secondary: accent-sensitive, case-insensitive
	// (the customer's ks-level2). The case level and tertiary level are dropped, so kf is moot here.
	strengthSecondary
)

type unicode0900ASCSCollator struct {
	// numeric, when true, enables natural numeric ordering (the ICU `kn` option): a maximal run
	// of decimal digits is compared by its numeric value rather than digit by digit, so "id-9"
	// sorts before "id-10". MySQL has no collation name for this; it is a TiDB extension used to
	// build the locale-aware tailored family. It does not affect the as_cs collation itself.
	numeric bool

	// caseFirst selects the ICU `kf` option. When not caseFirstOff, a dedicated case level is
	// inserted between the secondary and tertiary levels so that uppercase (or lowercase) sorts
	// first regardless of the default tertiary case ordering. caseFirstOff is the zero value, so
	// the plain utf8mb4_0900_as_cs collation is unaffected.
	caseFirst caseFirst

	// strength selects the ICU `ks` comparison strength. The zero value (strengthTertiary) keeps
	// full accent- and case-sensitive behavior, so the plain utf8mb4_0900_as_cs collation is
	// unaffected.
	strength strengthLevel
}

// caseWeightOf maps a tertiary weight to a 1-byte case-level weight. In the DUCET, lowercase
// letters carry a tertiary weight around 0x0002 and uppercase around 0x0008; uncased elements
// use the low range and are treated as lowercase here (they never differ by case anyway).
func caseWeightOf(tertiary uint16, mode caseFirst) byte {
	upper := tertiary >= 0x0008
	if mode == caseFirstUpper {
		if upper {
			return 0
		}
		return 1
	}
	// caseFirstLower
	if upper {
		return 1
	}
	return 0
}

// collationElements returns the packed collation elements for the whole string.
// Each element is (primary<<32 | secondary<<16 | tertiary).
func (uc *unicode0900ASCSCollator) collationElements(str string) []uint64 {
	ces := make([]uint64, 0, len(str))
	for si := 0; si < len(str); {
		r, rLen := utf8.DecodeRuneInString(str[si:])
		// Invalid UTF-8 decodes to (RuneError, 1); stop, matching the ai_ci collators.
		if r == utf8.RuneError && rLen == 1 {
			break
		}
		if uc.numeric && r >= '0' && r <= '9' {
			// Consume the whole digit run and emit numeric collation elements for it.
			start := si
			for si < len(str) {
				d, dLen := utf8.DecodeRuneInString(str[si:])
				if d < '0' || d > '9' {
					break
				}
				si += dLen
			}
			ces = appendNumericCEs(ces, str[start:si])
			continue
		}
		si += rLen
		ces = append(ces, asCSWeights(r)...)
	}
	return ces
}

// appendNumericCEs appends collation elements that sort a run of decimal digits by numeric
// value. Leading zeros are ignored. The run is encoded as one element for the count of
// significant digits, followed by one element per digit; all primaries are below any real
// character weight (count < 0x0100, digits in 0x0100..0x0109), so two runs compare
// count-first then digit-by-digit, i.e. by numeric value. Correct for up to 255 significant
// digits, which is far beyond any practical number.
func appendNumericCEs(ces []uint64, digits string) []uint64 {
	i := 0
	for i < len(digits)-1 && digits[i] == '0' {
		i++
	}
	sig := digits[i:]
	ces = append(ces, packCE(uint16(len(sig)), 0x0020, 0x0002))
	for k := range len(sig) {
		ces = append(ces, packCE(0x0100+uint16(sig[k]-'0'), 0x0020, 0x0002))
	}
	return ces
}

// asCSWeights returns the collation elements of a single rune, falling back to the
// UCA implicit weights for runes not explicitly listed in the DUCET.
func asCSWeights(r rune) []uint64 {
	if r >= 0 && int(r) < len(ucadata.DUCET0900MultiTable.Offset)-1 {
		lo := ucadata.DUCET0900MultiTable.Offset[r]
		hi := ucadata.DUCET0900MultiTable.Offset[r+1]
		if hi > lo {
			return ucadata.DUCET0900MultiTable.CEData[lo:hi]
		}
	}
	return asCSImplicit(r)
}

func packCE(primary, secondary, tertiary uint16) uint64 {
	return uint64(primary)<<32 | uint64(secondary)<<16 | uint64(tertiary)
}

// asCSImplicit computes the implicit collation elements for runes absent from the DUCET,
// following UTS #10. The logic mirrors the weight-table generator's getImplicitWeight0900.
func asCSImplicit(r rune) []uint64 {
	// Invalid characters (UTF-16 surrogates) and the replacement character.
	if (r >= 0xD800 && r <= 0xDFFF) || r == 0xFFFD {
		return []uint64{packCE(0xFFFD, 0x0020, 0x0002)}
	}

	// Hangul syllables decompose into jamo, each of which is explicitly listed.
	if r >= 0xAC00 && r <= 0xD7AF {
		var ces []uint64
		for _, j := range decomposeHangulSyllable(r) {
			ces = append(ces, asCSWeights(j)...)
		}
		return ces
	}

	// The implicit weight is [.AAAA.0020.0002][.BBBB.0000.0000]; AAAA and BBBB per UCA.
	var aaaa, bbbb uint16
	if r >= 0x17000 && r <= 0x18AFF {
		// Tangut characters.
		aaaa = 0xFB00
		bbbb = uint16((r - 0x17000) | 0x8000)
	} else {
		base := uint16(r >> 15)
		if (r >= 0x3400 && r <= 0x4DB5) || (r >= 0x20000 && r <= 0x2A6D6) ||
			(r >= 0x2A700 && r <= 0x2B734) || (r >= 0x2B740 && r <= 0x2B81D) ||
			(r >= 0x2B820 && r <= 0x2CEA1) {
			aaaa = base + 0xFB80
		} else if (r >= 0x4E00 && r <= 0x9FD5) || (r >= 0xFA0E && r <= 0xFA29) {
			aaaa = base + 0xFB40
		} else {
			aaaa = base + 0xFBC0
		}
		bbbb = uint16((r & 0x7FFF) | 0x8000)
	}
	return []uint64{packCE(aaaa, 0x0020, 0x0002), packCE(bbbb, 0, 0)}
}

func decomposeHangulSyllable(r rune) []rune {
	const (
		syllableBase     rune = 0xAC00
		leadingJamoBase  rune = 0x1100
		vowelJamoBase    rune = 0x1161
		trailingJamoBase rune = 0x11A7
		vowelJamoCnt     rune = 21
		trailingJamoCnt  rune = 28
	)

	syllableIndex := r - syllableBase
	vtCombination := vowelJamoCnt * trailingJamoCnt
	leadingJamoIndex := syllableIndex / vtCombination
	vowelJamoIndex := (syllableIndex % vtCombination) / trailingJamoCnt
	trailingJamoIndex := syllableIndex % trailingJamoCnt

	result := []rune{leadingJamoBase + leadingJamoIndex, vowelJamoBase + vowelJamoIndex}
	if trailingJamoIndex > 0 {
		result = append(result, trailingJamoBase+trailingJamoIndex)
	}
	return result
}

// Clone implements Collator interface.
func (uc *unicode0900ASCSCollator) Clone() Collator {
	return &unicode0900ASCSCollator{numeric: uc.numeric, caseFirst: uc.caseFirst, strength: uc.strength}
}

// Compare implements Collator interface.
func (uc *unicode0900ASCSCollator) Compare(a, b string) int {
	return bytes.Compare(uc.Key(a), uc.Key(b))
}

// Key implements Collator interface.
func (uc *unicode0900ASCSCollator) Key(str string) []byte {
	return uc.KeyWithoutTrimRightSpace(str)
}

// ImmutableKey implements Collator interface.
func (uc *unicode0900ASCSCollator) ImmutableKey(str string) []byte {
	return uc.KeyWithoutTrimRightSpace(str)
}

// KeyWithoutTrimRightSpace implements Collator interface. utf8mb4_0900 collations are NO PAD,
// so there is no trailing-space trimming.
func (uc *unicode0900ASCSCollator) KeyWithoutTrimRightSpace(str string) []byte {
	return buildMultiLevelKey(uc.collationElements(str), uc.caseFirst, uc.strength)
}

// buildMultiLevelKey turns a sequence of packed collation elements (primary<<32 | secondary<<16 |
// tertiary) into a byte-comparable UCA sort key, honoring strength and case-first. It is shared by
// every multi-level collator regardless of where the elements come from (DUCET or ICU-extracted),
// because the weights are only ever compared within a single collation, never across collations.
// The 0x0000 level separators are always smaller than any real weight (zero weights are skipped),
// giving correct cross-level ordering.
func buildMultiLevelKey(ces []uint64, cf caseFirst, st strengthLevel) []byte {
	buf := make([]byte, 0, len(ces)*4+6)
	appendWeight := func(w uint16) { buf = append(buf, byte(w>>8), byte(w)) }

	for _, c := range ces { // level 1: primary
		if p := uint16(c >> 32); p != 0 {
			appendWeight(p)
		}
	}
	if st == strengthPrimary {
		return buf
	}
	buf = append(buf, 0x00, 0x00) // level separator
	for _, c := range ces {       // level 2: secondary
		if s := uint16(c >> 16); s != 0 {
			appendWeight(s)
		}
	}
	if st == strengthSecondary {
		return buf
	}
	if cf != caseFirstOff { // optional case level, between secondary and tertiary
		buf = append(buf, 0x00, 0x00)
		for _, c := range ces {
			if uint16(c>>32) != 0 { // only cased (non-combining) elements carry a case weight
				buf = append(buf, caseWeightOf(uint16(c), cf))
			}
		}
	}
	buf = append(buf, 0x00, 0x00) // level separator
	for _, c := range ces {       // level 3: tertiary
		if t := uint16(c); t != 0 {
			appendWeight(t)
		}
	}
	return buf
}

// Pattern implements Collator interface.
func (*unicode0900ASCSCollator) Pattern() WildcardPattern {
	return &unicode0900ASCSPattern{}
}

// MaxKeyLen implements Collator interface. A rune expands to at most 8 collation elements,
// each contributing at most one weight (2 bytes) on each of the three levels plus one byte on
// the optional case level, plus up to three level separators.
func (*unicode0900ASCSCollator) MaxKeyLen(s string) int {
	return utf8.RuneCountInString(s)*8*7 + 6
}

type unicode0900ASCSPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicode0900ASCSPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface. Two runes match only when their full
// multi-level weights are identical, so matching is accent- and case-sensitive.
func (p *unicode0900ASCSPattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		if a == b {
			return true
		}
		wa, wb := asCSWeights(a), asCSWeights(b)
		if len(wa) != len(wb) {
			return false
		}
		for i := range wa {
			if wa[i] != wb[i] {
				return false
			}
		}
		return true
	})
}
