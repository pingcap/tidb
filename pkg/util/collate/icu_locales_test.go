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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestICULocaleCollations exercises the registered customer-facing locale collations
// (utf8mb4_<locale>_0900_as_ci_kn) end-to-end through GetCollator: per-locale ordering,
// accent-sensitive, case-insensitive, numeric. Expectations are the ICU orderings validated
// against Intl.Collator during the extraction proof.
func TestICULocaleCollations(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	sortWith := func(c Collator, in []string) []string {
		out := append([]string(nil), in...)
		sort.SliceStable(out, func(i, j int) bool { return c.Compare(out[i], out[j]) < 0 })
		return out
	}

	t.Run("German", func(t *testing.T) {
		c := GetCollator("utf8mb4_de_0900_as_ci_kn")
		require.Equal(t, 0, c.Compare("a", "A")) // case-insensitive (level 2)
		require.Equal(t, 0, c.Compare("Müller", "müller"))
		require.Equal(t, -1, sign(c.Compare("a", "ä")))              // accent-sensitive: a before ä
		require.Equal(t, -1, sign(c.Compare("ä", "b")))              // German standard: ä sorts near a
		require.Equal(t, -1, sign(c.Compare("Datei-2", "datei-10"))) // numeric + case-insensitive
		require.Equal(t, []string{"a", "ä", "b", "B", "z"},
			sortWith(c, []string{"z", "ä", "a", "b", "B"}))
	})

	t.Run("Danish", func(t *testing.T) {
		c := GetCollator("utf8mb4_da_0900_as_ci_kn")
		require.Equal(t, 1, sign(c.Compare("Aarhus", "Zealand"))) // aa contraction sorts after z
		require.Equal(t, -1, sign(c.Compare("øl", "år")))         // ø before å
		require.Equal(t, []string{"Bornholm", "Zealand", "Aarhus"},
			sortWith(c, []string{"Aarhus", "Bornholm", "Zealand"}))
	})

	t.Run("Spanish", func(t *testing.T) {
		c := GetCollator("utf8mb4_es_0900_as_ci_kn")
		require.Equal(t, -1, sign(c.Compare("n", "ñ"))) // ñ after n
		require.Equal(t, -1, sign(c.Compare("ñ", "o"))) // ñ before o
	})

	t.Run("Swedish", func(t *testing.T) {
		c := GetCollator("utf8mb4_sv_0900_as_ci_kn")
		require.Equal(t, -1, sign(c.Compare("z", "å"))) // å after z
		require.Equal(t, 1, sign(c.Compare("ö", "a")))  // ö late
	})

	t.Run("numeric and case-insensitive shared", func(t *testing.T) {
		c := GetCollator("utf8mb4_fr_0900_as_ci_kn")
		require.Equal(t, -1, sign(c.Compare("v9", "v10")))
		require.Equal(t, 0, c.Compare("CAFE", "cafe"))
	})
}

func TestIsTiDBOnlyCollation(t *testing.T) {
	// ICU locale collations and the multi-level as_cs are implemented only in TiDB.
	require.True(t, IsTiDBOnlyCollation("utf8mb4_de_0900_as_ci_kn"))
	require.True(t, IsTiDBOnlyCollation("utf8mb4_da_0900_as_ci_kn"))
	require.True(t, IsTiDBOnlyCollation("utf8mb4_0900_as_cs"))
	// Collations the coprocessor implements are pushable.
	require.False(t, IsTiDBOnlyCollation("utf8mb4_0900_ai_ci"))
	require.False(t, IsTiDBOnlyCollation("utf8mb4_general_ci"))
	require.False(t, IsTiDBOnlyCollation("utf8mb4_bin"))
	require.False(t, IsTiDBOnlyCollation(""))
}
