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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnicode0900ASCSCollator verifies the accent-sensitive, case-sensitive UCA collation.
// The expected orderings are the Unicode Collation Algorithm orderings (cross-checked against
// ICU via Intl.Collator during prototyping): accents differ at the secondary level and case
// differs at the tertiary level, with lowercase sorting before uppercase.
func TestUnicode0900ASCSCollator(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	c := GetCollator("utf8mb4_0900_as_cs")

	// Pairwise comparisons. Negative means Left < Right.
	pairs := []struct {
		left, right string
		expect      int
	}{
		{"e", "e", 0},
		{"e", "é", -1}, // accent-sensitive: base letter before accented
		{"é", "e", 1},
		{"é", "f", -1}, // accent difference is secondary, below the primary 'e' vs 'f'
		{"a", "A", -1}, // case-sensitive: lowercase before uppercase
		{"A", "a", 1},
		{"a", "á", -1},
		{"apple", "Apple", -1},
		{"e", "E", -1}, // contrast with ai_ci, which treats these as equal (asserted below)
	}
	for _, p := range pairs {
		require.Equal(t, p.expect, sign(c.Compare(p.left, p.right)),
			"Compare(%q, %q)", p.left, p.right)
		// Key ordering must agree with Compare.
		require.Equal(t, p.expect, sign(bytes.Compare(c.Key(p.left), c.Key(p.right))),
			"Key ordering for (%q, %q)", p.left, p.right)
	}

	// as_cs must distinguish what ai_ci folds together.
	aici := GetCollator("utf8mb4_0900_ai_ci")
	require.Equal(t, 0, aici.Compare("e", "É"), "ai_ci should fold accent and case")
	require.NotEqual(t, 0, c.Compare("e", "É"), "as_cs must not fold accent and case")

	// Full sort: primary groups a < b < e < f; within a group lowercase precedes uppercase,
	// and the accented form follows its base letter.
	input := []string{"f", "é", "e", "A", "a", "B", "b"}
	want := []string{"a", "A", "b", "B", "e", "é", "f"}
	got := append([]string(nil), input...)
	sort.SliceStable(got, func(i, j int) bool { return c.Compare(got[i], got[j]) < 0 })
	require.Equal(t, want, got)

	// Equal strings must produce identical keys.
	require.Equal(t, c.Key("café"), c.Key("café"))
}

// TestUnicode0900NumericCollation verifies the numeric (kn) option on the multi-level engine.
// Digit runs compare by numeric value. Expected orderings match ICU (Intl.Collator with
// numeric:true), cross-checked during prototyping. The numeric variant is not yet exposed as
// a named SQL collation; that happens with the tailored locale family.
func TestUnicode0900NumericCollation(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	num := &unicode0900ASCSCollator{numeric: true}
	plain := &unicode0900ASCSCollator{}

	// Without numeric, "id-10" < "id-9" lexicographically; with numeric, the order flips.
	require.Equal(t, -1, sign(plain.Compare("id-10", "id-9")))
	require.Equal(t, 1, sign(num.Compare("id-10", "id-9")))

	sortWith := func(c Collator, in []string) []string {
		out := append([]string(nil), in...)
		sort.SliceStable(out, func(i, j int) bool { return c.Compare(out[i], out[j]) < 0 })
		return out
	}

	require.Equal(t,
		[]string{"id-2", "id-9", "id-10", "id-100"},
		sortWith(num, []string{"id-100", "id-9", "id-2", "id-10"}))

	require.Equal(t,
		[]string{"x1", "x2", "x12", "x100"},
		sortWith(num, []string{"x12", "x2", "x1", "x100"}))

	// Leading zeros are ignored: equal numeric value -> equal key.
	require.Equal(t, 0, num.Compare("v007", "v7"))

	// Large multi-digit values compare by magnitude, not first digit.
	require.Equal(t, -1, sign(num.Compare("item-2", "item-19")))
	require.Equal(t, -1, sign(num.Compare("item-99", "item-100")))

	// Non-digit context still uses full accent/case-sensitive ordering.
	require.Equal(t, -1, sign(num.Compare("a1", "A1"))) // lowercase before uppercase
	require.Equal(t, -1, sign(num.Compare("e1", "é1"))) // accent-sensitive
}

// TestUnicode0900CaseFirstCollation verifies the case-first (kf) option. Expected orderings
// match ICU (Intl.Collator with caseFirst:'upper'/'lower'), cross-checked during prototyping.
func TestUnicode0900CaseFirstCollation(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	upper := &unicode0900ASCSCollator{caseFirst: caseFirstUpper}
	lower := &unicode0900ASCSCollator{caseFirst: caseFirstLower}
	plain := &unicode0900ASCSCollator{}

	// Default as_cs orders lowercase first (via the tertiary level); kf=lower agrees, kf=upper flips.
	require.Equal(t, -1, sign(plain.Compare("a", "A")))
	require.Equal(t, -1, sign(lower.Compare("a", "A")))
	require.Equal(t, 1, sign(upper.Compare("a", "A")))

	sortWith := func(c Collator, in []string) []string {
		out := append([]string(nil), in...)
		sort.SliceStable(out, func(i, j int) bool { return c.Compare(out[i], out[j]) < 0 })
		return out
	}

	require.Equal(t,
		[]string{"A", "a", "B", "b"},
		sortWith(upper, []string{"b", "B", "a", "A"}))

	require.Equal(t,
		[]string{"a", "A", "b", "B"},
		sortWith(lower, []string{"B", "b", "A", "a"}))

	// Case-first must not disturb primary or secondary ordering.
	require.Equal(t, -1, sign(upper.Compare("a", "b"))) // different base letters
	require.Equal(t, -1, sign(upper.Compare("e", "é"))) // accent (secondary) still ranks first
	require.Equal(t, 0, upper.Compare("café", "café"))  // equality preserved
}

// TestUnicode0900StrengthLevels verifies the strength (ks) option, including the customer's
// full ks-level2 (accent-sensitive, case-insensitive) combined with numeric and case-first.
func TestUnicode0900StrengthLevels(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	level2 := &unicode0900ASCSCollator{strength: strengthSecondary}
	level1 := &unicode0900ASCSCollator{strength: strengthPrimary}

	// Level 2: accent-sensitive but case-insensitive.
	require.Equal(t, 0, level2.Compare("a", "A"))        // case folded
	require.Equal(t, 0, level2.Compare("E", "e"))        // case folded
	require.Equal(t, -1, sign(level2.Compare("e", "é"))) // accent kept
	require.Equal(t, 0, level2.Compare("Café", "café"))  // case folded, accent kept -> equal

	// Level 1: both accent- and case-insensitive.
	require.Equal(t, 0, level1.Compare("a", "A"))
	require.Equal(t, 0, level1.Compare("e", "é"))        // accent folded
	require.Equal(t, -1, sign(level1.Compare("e", "f"))) // primary still distinct

	// The customer's tailoring: numeric + case-first(upper) + ks-level2. Case-first is moot at
	// level 2 (case is not significant), accents are kept, and digit runs sort by value.
	combo := &unicode0900ASCSCollator{numeric: true, caseFirst: caseFirstUpper, strength: strengthSecondary}
	require.Equal(t, 0, combo.Compare("Item-2", "item-2"))         // case-insensitive
	require.Equal(t, -1, sign(combo.Compare("item-2", "Item-10"))) // numeric, case-insensitive
	require.Equal(t, -1, sign(combo.Compare("cafe-1", "café-1")))  // accent-sensitive
}
