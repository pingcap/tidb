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

package matchagainst

import (
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

func TestLocalMatchTokenRune(t *testing.T) {
	// The character classes still use Unicode 15.0.0. Unlike a private table, the standard
	// library follows the toolchain, so an upgrade must fail, never skip this
	// check. Deploy TiFlash support before TiDB adopts a new semantic version.
	require.Equal(t, "15.0.0", unicode.Version, "Local MATCH requires Unicode 15.0.0; do not silently upgrade the token semantics")
	for _, r := range []rune{'a', 'Z', '0', '_', '中', 'é', '𝟙', '𞤀', '\U0002EBF0'} {
		require.Equal(t, r <= 0xFFFF, isNgramWordChar(r), "U+%04X", r)
	}
	for _, r := range []rune{-1, ' ', '.', '🙃', '👁', '\u0301', '\uFFFD', unicode.MaxRune + 1} {
		require.False(t, isNgramWordChar(r), "U+%04X", r)
	}
	// The NGRAM Boolean lexer and document scanner intentionally differ.
	// STANDARD's shared Go/C++ Unicode fingerprint is checked in fulltext.
	for r := rune(0); r <= unicode.MaxRune; r++ {
		want := r <= 0xFFFF && (unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_')
		got := isNgramWordChar(r)
		if got != want {
			t.Fatalf("classification mismatch at U+%04X: got %v want %v", r, got, want)
		}
	}
}
