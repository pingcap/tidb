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
	// Protocol v1 fixes Unicode 15.0.0. Unlike a private table, the standard
	// library follows the toolchain, so an upgrade must fail, never skip this
	// check. Deploy TiFlash support before TiDB adopts a new semantic version.
	require.Equal(t, "15.0.0", unicode.Version, "Local MATCH protocol v1 requires Unicode 15.0.0; do not silently upgrade the token semantics")
	for _, r := range []rune{'a', 'Z', '0', '_', '中', 'é', '𝟙', '𞤀', '\U0002EBF0'} {
		// U+2EBF0 was added in Unicode 15.1, outside protocol version 1.
		require.Equal(t, r != '\U0002EBF0', isNgramWordChar(r), "U+%04X", r)
	}
	for _, r := range []rune{-1, ' ', '.', '🙃', '👁', '\u0301', '\uFFFD', unicode.MaxRune + 1} {
		require.False(t, isNgramWordChar(r), "U+%04X", r)
	}
	// Hash one 0/1 byte for every Unicode code point, in ascending order.
	// TiFlash checks the same fixed fingerprint against its generated table;
	// this is a regression checksum, not a security primitive.
	fingerprint := uint64(14695981039346656037)
	for r := rune(0); r <= unicode.MaxRune; r++ {
		want := unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
		got := isNgramWordChar(r)
		if got != want {
			t.Fatalf("classification mismatch at U+%04X: got %v want %v", r, got, want)
		}
		if got {
			fingerprint ^= 1
		}
		fingerprint *= 1099511628211
	}
	// This constant must match the protocol-v1 gtest, not be regenerated when
	// either runtime's classification changes.
	require.Equal(t, uint64(0x71f51f3810b3b529), fingerprint, "protocol-v1 classification fingerprint: %016x", fingerprint)
}
