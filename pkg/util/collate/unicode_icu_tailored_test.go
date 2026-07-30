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
	"testing"

	"github.com/stretchr/testify/require"
)

// icuFixture builds a small, internally-consistent tailoring that stands in for a generated
// per-locale table. Weights are ICU-scale-like and only need to be self-consistent: primaries
// order a < b < e < z, 'A' shares a's primary/secondary but a higher tertiary (case), 'é' shares
// e's primary but adds a secondary (accent), and the contraction "aa" sorts after "z". This
// exercises the runtime engine mechanics (contraction longest-match, overrides, kn/kf/ks); the
// fidelity of real ICU weights is covered separately by the extraction proof.
func icuFixture() *icuTailoring {
	root := mapRoot{
		'a': {packCE(0x2900, 0x20, 0x05)},
		'A': {packCE(0x2900, 0x20, 0x08)},
		'b': {packCE(0x2A00, 0x20, 0x05)},
		'e': {packCE(0x2D00, 0x20, 0x05)},
		'é': {packCE(0x2D00, 0x20, 0x05), packCE(0x0000, 0x35, 0x05)},
		'z': {packCE(0x4200, 0x20, 0x05)},
	}
	return &icuTailoring{
		root:                root,
		override:            map[rune][]uint64{},
		contraction:         map[string][]uint64{"aa": {packCE(0x4300, 0x20, 0x05)}},
		maxContractionRunes: 2,
	}
}

func TestUnicodeICUCollator(t *testing.T) {
	tbl := icuFixture()
	c := &unicodeICUCollator{t: tbl}

	// Contraction "aa" is matched as a unit and sorts after "z" (0x4300 > z's 0x4200). If it were
	// treated as a+a it would start with 0x2900 and sort before "z" — so this proves the match.
	require.Equal(t, 1, sign(c.Compare("aa", "z")))
	require.Equal(t, 1, sign(c.Compare("aa", "ab")))
	require.Equal(t, -1, sign(c.Compare("ab", "aa")))
	require.Equal(t, -1, sign(c.Compare("a", "b"))) // single rune still uses the root table

	// Full strength: accent (secondary) and case (tertiary) are significant.
	require.Equal(t, -1, sign(c.Compare("e", "é"))) // accent
	require.Equal(t, -1, sign(c.Compare("a", "A"))) // case: lowercase first

	// Strength level 1: accent- and case-insensitive.
	l1 := &unicodeICUCollator{t: tbl, strength: strengthPrimary}
	require.Equal(t, 0, l1.Compare("a", "A"))
	require.Equal(t, 0, l1.Compare("e", "é"))

	// Strength level 2: accent-sensitive, case-insensitive.
	l2 := &unicodeICUCollator{t: tbl, strength: strengthSecondary}
	require.Equal(t, 0, l2.Compare("a", "A"))
	require.Equal(t, -1, sign(l2.Compare("e", "é")))

	// Case-first upper flips the case order.
	up := &unicodeICUCollator{t: tbl, caseFirst: caseFirstUpper}
	require.Equal(t, 1, sign(up.Compare("a", "A")))

	// Numeric: digit runs compare by value, independent of the table.
	num := &unicodeICUCollator{t: tbl, numeric: true}
	require.Equal(t, -1, sign(num.Compare("a2", "a10")))
	require.Equal(t, 1, sign(num.Compare("a10", "a2")))

	// Clone preserves options.
	cl := up.Clone().(*unicodeICUCollator)
	require.Equal(t, caseFirstUpper, cl.caseFirst)
	require.Equal(t, 1, sign(cl.Compare("a", "A")))

	// Equal strings produce identical keys.
	require.Equal(t, c.Key("aab"), c.Key("aab"))
}
