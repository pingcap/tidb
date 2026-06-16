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

	"github.com/pingcap/tidb/pkg/util/collate/icudata"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// This file implements the runtime side of the locale-aware ICU-derived collation family.
// The collation elements come from per-locale tables generated offline from ICU (so the shipped
// binary needs no ICU); see the generator under pkg/util/collate. Each element is packed as
// (primary<<32 | secondary<<16 | tertiary), the same format the multi-level key builder consumes.
//
// Unlike the ai_ci/as_cs collators, this family handles contractions (e.g. Danish "aa" sorting as
// a single unit after "z"), which locale tailorings require. Contraction matching is longest-first.

// icuRoot is the shared, locale-neutral (root) collation element lookup. Production uses the
// generated CSR table (csrRoot); tests use an in-memory map (mapRoot).
type icuRoot interface {
	ces(r rune) ([]uint64, bool)
}

// csrRoot adapts the generated compressed-sparse-row root table.
type csrRoot struct{ t *icudata.RootTable }

func (r csrRoot) ces(rn rune) ([]uint64, bool) {
	if rn >= 0 && int(rn) < len(r.t.Offset)-1 {
		lo, hi := r.t.Offset[rn], r.t.Offset[rn+1]
		if hi > lo {
			return r.t.CEData[lo:hi], true
		}
	}
	return nil, false
}

// mapRoot is a small map-backed root for tests and fixtures.
type mapRoot map[rune][]uint64

func (m mapRoot) ces(rn rune) ([]uint64, bool) { c, ok := m[rn]; return c, ok }

// icuTailoring is a per-locale table: the shared root plus locale-specific rune overrides and
// contractions. Storing only overrides (not a full copy of root per locale) keeps the data compact.
type icuTailoring struct {
	root                icuRoot
	override            map[rune][]uint64
	contraction         map[string][]uint64
	maxContractionRunes int
}

func (t *icuTailoring) runeCEs(r rune) []uint64 {
	if c, ok := t.override[r]; ok {
		return c
	}
	if t.root != nil {
		if c, ok := t.root.ces(r); ok {
			return c
		}
	}
	// The generated root table covers every assigned BMP code point, so this is only a defensive
	// fallback (and what unassigned/supplementary code points map to): a single replacement element.
	return []uint64{packCE(0xFFFD, 0x0020, 0x0002)}
}

// unicodeICUCollator is a locale-aware collator over an icuTailoring, with the same kn/kf/ks
// options as the multi-level engine.
type unicodeICUCollator struct {
	t         *icuTailoring
	numeric   bool
	caseFirst caseFirst
	strength  strengthLevel
}

func (uc *unicodeICUCollator) collationElements(str string) []uint64 {
	rs := []rune(str)
	out := make([]uint64, 0, len(rs))
	for i := 0; i < len(rs); {
		// numeric: consume a digit run and emit value-ordered elements.
		if uc.numeric && rs[i] >= '0' && rs[i] <= '9' {
			j := i
			for j < len(rs) && rs[j] >= '0' && rs[j] <= '9' {
				j++
			}
			out = appendNumericCEs(out, string(rs[i:j]))
			i = j
			continue
		}
		// contraction: longest match first.
		if matched := uc.matchContraction(rs, i); matched > 0 {
			out = append(out, uc.t.contraction[string(rs[i:i+matched])]...)
			i += matched
			continue
		}
		out = append(out, uc.t.runeCEs(rs[i])...)
		i++
	}
	return out
}

// matchContraction returns the rune length of the longest contraction in the table starting at
// rs[i], or 0 if none. Contractions are at least two runes long.
func (uc *unicodeICUCollator) matchContraction(rs []rune, i int) int {
	maxL := uc.t.maxContractionRunes
	if maxL > len(rs)-i {
		maxL = len(rs) - i
	}
	for l := maxL; l >= 2; l-- {
		if _, ok := uc.t.contraction[string(rs[i:i+l])]; ok {
			return l
		}
	}
	return 0
}

// Clone implements Collator interface.
func (uc *unicodeICUCollator) Clone() Collator {
	c := *uc
	return &c
}

// Compare implements Collator interface.
func (uc *unicodeICUCollator) Compare(a, b string) int {
	return bytes.Compare(uc.Key(a), uc.Key(b))
}

// Key implements Collator interface.
func (uc *unicodeICUCollator) Key(str string) []byte {
	return buildMultiLevelKey(uc.collationElements(str), uc.caseFirst, uc.strength)
}

// ImmutableKey implements Collator interface.
func (uc *unicodeICUCollator) ImmutableKey(str string) []byte {
	return uc.Key(str)
}

// KeyWithoutTrimRightSpace implements Collator interface. This family is NO PAD (no trailing-space trimming).
func (uc *unicodeICUCollator) KeyWithoutTrimRightSpace(str string) []byte {
	return uc.Key(str)
}

// MaxKeyLen implements Collator interface; see unicode0900ASCSCollator.MaxKeyLen for the bound.
func (*unicodeICUCollator) MaxKeyLen(s string) int {
	return utf8.RuneCountInString(s)*8*7 + 6
}

// Pattern implements Collator interface. Matching is per-rune on full collation elements, so it is
// accent- and case-sensitive; contractions are not folded in LIKE (matching the simple approach of
// the other UCA collators).
func (uc *unicodeICUCollator) Pattern() WildcardPattern {
	return &icuPattern{t: uc.t}
}

type icuPattern struct {
	t        *icuTailoring
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *icuPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *icuPattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		if a == b {
			return true
		}
		wa, wb := p.t.runeCEs(a), p.t.runeCEs(b)
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
