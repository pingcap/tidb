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

package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// splitmix64 is a high-quality bijective 64-bit mixer. HLL needs a uniformly
// distributed hash; a plain multiplier skews the leading-zero rank. This matches
// the helper used in the TiKV HLL tests.
func splitmix64(x uint64) uint64 {
	z := x + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func buildHLL(precision uint8, n uint64) *HLL {
	h := NewHLL(precision)
	for i := range n {
		h.InsertHash(splitmix64(i))
	}
	return h
}

func TestHLLCountWithinError(t *testing.T) {
	// p=14 has ~0.8% standard error; allow a generous 5% band.
	for _, n := range []uint64{0, 1, 100, 10000, 200000} {
		est := float64(buildHLL(DefaultHLLPrecision, n).Count())
		if n == 0 {
			require.Equal(t, 0.0, est)
			continue
		}
		require.Less(t, abs(est-float64(n))/float64(n), 0.05, "n=%d est=%f", n, est)
	}
}

func TestHLLMergeIsUnion(t *testing.T) {
	// Disjoint halves merged must estimate the union (~2n).
	a := NewHLL(DefaultHLLPrecision)
	b := NewHLL(DefaultHLLPrecision)
	for i := range uint64(100000) {
		a.InsertHash(splitmix64(i))
		b.InsertHash(splitmix64(i + 100000))
	}
	a.Merge(b)
	est := float64(a.Count())
	require.Less(t, abs(est-200000)/200000, 0.05, "est=%f", est)
}

func TestHLLProtoRoundTrip(t *testing.T) {
	h := buildHLL(DefaultHLLPrecision, 5000)
	before := h.Count()
	// Serialize and decode: the count must be unchanged (raw register transfer).
	restored := HLLFromProto(h.ToProto())
	require.NotNil(t, restored)
	require.Equal(t, before, restored.Count())
	require.Equal(t, uint32(DefaultHLLPrecision), h.ToProto().GetPrecision())
	require.Len(t, h.ToProto().GetRegisters(), 1<<DefaultHLLPrecision)

	// Merging a sketch with itself changes nothing (max of equal registers).
	same := h.Clone()
	same.Merge(h)
	require.Equal(t, before, same.Count())

	// A malformed proto decodes to nil rather than panicking.
	require.Nil(t, HLLFromProto(nil))
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
