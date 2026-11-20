// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func allFragments(traverse func(func(Valued) bool)) []string {
	var out []string
	traverse(func(v Valued) bool {
		out = append(out, fmt.Sprintf("%x|%x|%d|%d", v.Key.StartKey, v.Key.EndKey, v.Value.Size, v.Value.Number))
		return true
	})
	return out
}

// TestSplitHelperMatchesLegacy checks that SplitHelper yields exactly the
// fragments (keys and values, empty ones included) of the previous btree-based
// implementation for well-formed spans, including point spans.
func TestSplitHelperMatchesLegacy(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		legacy, h := newLegacySplitHelper(), NewSplitHelper()
		keySpace := 2 + rng.Intn(300)
		n := 1 + rng.Intn(600)
		for i := 0; i < n; i++ {
			a, b := rng.Intn(keySpace), rng.Intn(keySpace)
			if a > b {
				a, b = b, a
			}
			if rng.Intn(10) == 0 {
				b = a
			}
			v := Valued{
				Key:   Span{StartKey: []byte(fmt.Sprintf("k%05d", a)), EndKey: []byte(fmt.Sprintf("k%05d", b))},
				Value: Value{Size: uint64(rng.Intn(1 << 30)), Number: int64(rng.Intn(1 << 20))},
			}
			legacy.Merge(v)
			h.Merge(v)
		}
		require.Equal(t, allFragments(legacy.Traverse), allFragments(h.Traverse), "seed %d", seed)
	}
}

// TestSplitHelperMatchesLegacyStepwise compares after every merge on a tiny key
// space, so an intermediate error cannot be masked by later merges.
func TestSplitHelperMatchesLegacyStepwise(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		legacy, h := newLegacySplitHelper(), NewSplitHelper()
		for i := 0; i < 120; i++ {
			a, b := rng.Intn(12), rng.Intn(12)
			if a > b {
				a, b = b, a
			}
			v := Valued{
				Key:   Span{StartKey: []byte{'k', byte(a)}, EndKey: []byte{'k', byte(b)}},
				Value: Value{Size: uint64(rng.Intn(1000)), Number: int64(rng.Intn(100))},
			}
			legacy.Merge(v)
			h.Merge(v)
			require.Equal(t, allFragments(legacy.Traverse), allFragments(h.Traverse), "seed %d step %d", seed, i)
		}
	}
}

func TestSplitHelperIgnoresEmptyKeys(t *testing.T) {
	h := NewSplitHelper()
	h.Merge(Valued{Key: Span{StartKey: []byte(""), EndKey: []byte("a")}, Value: Value{Size: 10, Number: 1}})
	h.Merge(Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("")}, Value: Value{Size: 10, Number: 1}})
	require.Equal(t, []string{"||0|0"}, allFragments(h.Traverse))
}

// TestSplitHelperFallsBackOnInvertedSpan checks the switch to the btree
// implementation keeps the fragments and the legacy behavior afterwards.
func TestSplitHelperFallsBackOnInvertedSpan(t *testing.T) {
	legacy, h := newLegacySplitHelper(), NewSplitHelper()
	merge := func(s, e string, size uint64) {
		v := Valued{Key: Span{StartKey: []byte(s), EndKey: []byte(e)}, Value: Value{Size: size, Number: int64(size)}}
		legacy.Merge(v)
		h.Merge(v)
		require.Equal(t, allFragments(legacy.Traverse), allFragments(h.Traverse), "after [%s, %s)", s, e)
	}
	merge("b", "f", 100)
	merge("c", "d", 40)
	merge("e", "b", 60) // inverted, overlaps nothing: a no-op, no switch
	require.Nil(t, h.btree)
	merge("e", "dd", 60) // inverted and overlapping [d, f)
	require.NotNil(t, h.btree)
	merge("a", "g", 30)
	merge("d", "d", 9)
	require.Equal(t, legacy.inner.Len(), h.Len())
}

// TestSplitHelperOwnsKeys checks retained keys do not alias the caller's buffers.
func TestSplitHelperOwnsKeys(t *testing.T) {
	h := NewSplitHelper()
	start, end := []byte("k1"), []byte("k5")
	h.Merge(Valued{Key: Span{StartKey: start, EndKey: end}, Value: Value{Size: 10, Number: 2}})
	want := allFragments(h.Traverse)
	start[1], end[1] = '9', '9'
	require.Equal(t, want, allFragments(h.Traverse))
}
