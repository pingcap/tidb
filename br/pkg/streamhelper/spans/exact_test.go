// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package spans

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExactUpdates(t *testing.T) {
	f := NewFullWith([]Span{{StartKey: []byte("a"), EndKey: []byte("b")}}, 10)
	item, ok := f.inner.Get(makeValuedItem(&Valued{Key: Span{StartKey: []byte("a")}}))
	require.True(t, ok)
	record := item.record
	update := Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: 11}
	require.Zero(t, testing.AllocsPerRun(100, func() { update.Value++; f.Merge(update) }))
	f.Merge(Valued{Key: update.Key, Value: 0})
	require.Equal(t, update.Value, record.Value, "old notifications must not lower the checkpoint")
	require.Equal(t, 1, f.inner.Len())
	update.Key.StartKey[0], update.Key.EndKey[0] = 'x', 'y'
	f.Traverse(func(v Valued) bool {
		require.Equal(t, "a", string(v.Key.StartKey))
		require.Equal(t, "b", string(v.Key.EndKey))
		require.Equal(t, record.Value, v.Value, "traversal must see the in-place update")
		return true
	})
}

func TestStaleOverlapDoesNotRewrite(t *testing.T) {
	f := NewFullWith([]Span{{StartKey: []byte("a"), EndKey: []byte("d")}}, 10)
	f.Merge(Valued{Key: Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 20})
	before := make(map[string]*Valued)
	f.inner.Ascend(func(item valuedItem) bool { before[string(item.record.Key.StartKey)] = item.record; return true })
	checkScratch := func() {
		require.Equal(t, Valued{}, f.lookup, "query pivots must not retain keys")
		require.Empty(t, f.overlapScratch)
		for _, v := range f.overlapScratch[:cap(f.overlapScratch)] {
			require.Equal(t, Valued{}, v, "scratch must release old key references")
		}
	}
	for _, update := range []Valued{
		{Key: Span{StartKey: []byte("a"), EndKey: []byte("c")}, Value: 0},
		{Key: Span{StartKey: []byte("a"), EndKey: []byte("c")}, Value: 10},
		{Key: Span{StartKey: []byte("ab"), EndKey: []byte("cc")}, Value: 10},
		{Key: Span{}, Value: 10},
		{Key: Span{StartKey: []byte("x"), EndKey: []byte("z")}, Value: 30},
	} {
		require.Zero(t, testing.AllocsPerRun(100, func() { f.Merge(update) }), "warmed-up no-op merges must not allocate")
		checkScratch()
		require.Equal(t, len(before), f.inner.Len())
		f.inner.Ascend(func(item valuedItem) bool {
			v := item.record
			require.Same(t, before[string(v.Key.StartKey)], v, "no-op must retain tree records")
			require.Same(t, v, f.exact[maphash.Bytes(f.seed, v.Key.StartKey)])
			return true
		})
	}
	// Only some overlaps advance: the lower checkpoints must still be updated.
	f.Merge(Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("d")}, Value: 15})
	checkScratch()
	f.inner.Ascend(func(item valuedItem) bool { require.NotSame(t, &f.lookup, item.record); return true })
	var actual []Valued
	f.Traverse(func(v Valued) bool { actual = append(actual, v); return true })
	require.Equal(t, []Valued{
		{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: 15},
		{Key: Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 20},
		{Key: Span{StartKey: []byte("c"), EndKey: []byte("d")}, Value: 15},
	}, actual)
}

func TestExactFingerprintMismatch(t *testing.T) {
	f := NewFullWith([]Span{
		{StartKey: []byte("a"), EndKey: []byte("c")},
		{StartKey: []byte("d"), EndKey: []byte("f")},
	}, 10)
	item, ok := f.inner.Get(makeValuedItem(&Valued{Key: Span{StartKey: []byte("d")}}))
	require.True(t, ok)
	other := item.record
	// Simulate a fingerprint collision: a hash hit must not update another key.
	f.exact[maphash.Bytes(f.seed, []byte("a"))] = other
	f.Merge(Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("c")}, Value: 20})
	require.EqualValues(t, 10, other.Value)
	// Matching the start alone is also insufficient when a region splits.
	f.Merge(Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: 30})
	var actual []Valued
	f.Traverse(func(v Valued) bool { actual = append(actual, v); return true })
	require.Equal(t, []Valued{
		{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: 30},
		{Key: Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 20},
		{Key: Span{StartKey: []byte("d"), EndKey: []byte("f")}, Value: 10},
	}, actual)
}

func TestExactRangeChanges(t *testing.T) {
	key := func(i int) []byte { return []byte(fmt.Sprintf("%03d", i)) }
	f := NewFullWith([]Span{{StartKey: key(0), EndKey: key(16)}, {StartKey: key(32), EndKey: key(64)}}, 0)
	var expected [64]int64
	for i := 16; i < 32; i++ {
		expected[i] = -1
	}
	rng := rand.New(rand.NewSource(37))
	for step := 0; step < 512; step++ {
		lo := rng.Intn(64)
		hi := lo + 1 + rng.Intn(64-lo)
		update := Valued{Key: Span{StartKey: key(lo), EndKey: key(hi)}, Value: uint64(rng.Intn(step + 2))}
		if step%127 == 0 {
			lo, hi, update.Key = 0, 64, Span{}
		}
		f.Merge(update)
		for i := lo; i < hi; i++ {
			if expected[i] >= 0 {
				expected[i] = max(expected[i], int64(update.Value))
			}
		}
		var actual [64]int64
		for i := range actual {
			actual[i] = -1
		}
		f.Traverse(func(v Valued) bool {
			for i := range actual {
				point := key(i)
				if bytes.Compare(point, v.Key.StartKey) >= 0 && (len(v.Key.EndKey) == 0 || bytes.Compare(point, v.Key.EndKey) < 0) {
					require.EqualValues(t, -1, actual[i], "overlap at step %d", step)
					actual[i] = int64(v.Value)
				}
			}
			return true
		})
		require.Equal(t, expected, actual, "pointwise checkpoint at step %d", step)
		for hash, record := range f.exact {
			stored, ok := f.inner.Get(makeValuedItem(record))
			require.True(t, ok)
			require.Same(t, record, stored.record, "cache retained a deleted record at step %d", step)
			require.Equal(t, maphash.Bytes(f.seed, record.Key.StartKey), hash)
		}
	}
}

func TestOverlapScratchRetentionBound(t *testing.T) {
	f := NewFullWith(nil, 0)
	key := func(i int) []byte { var b [8]byte; binary.BigEndian.PutUint64(b[:], uint64(i)); return b[:] }
	for i := 0; i <= maxOverlapScratch; i++ {
		f.insert(Valued{Key: Span{StartKey: key(i), EndKey: key(i + 1)}, Value: uint64(i + 1)})
	}
	f.Merge(Valued{Value: 0})
	require.Nil(t, f.overlapScratch, "a wide update must not retain an oversized buffer")
	require.Equal(t, maxOverlapScratch+1, f.inner.Len())
}

func TestValuedItemOrdering(t *testing.T) {
	keys := [][]byte{nil}
	for n := 1; n <= 24; n++ {
		keys = append(keys, make([]byte, n))
		a := make([]byte, n)
		a[0] = 'a'
		keys = append(keys, a)
	}
	check := func(a, b []byte) {
		av, bv := Valued{Key: Span{StartKey: a}}, Valued{Key: Span{StartKey: b}}
		require.Equal(t, bytes.Compare(a, b) < 0, lessValuedItem(makeValuedItem(&av), makeValuedItem(&bv)), "key order: %x %x", a, b)
	}
	for _, a := range keys {
		for _, b := range keys {
			check(a, b)
		}
	}
	rng := rand.New(rand.NewSource(92))
	for i := 0; i < 20000; i++ {
		a, b := make([]byte, rng.Intn(192)), make([]byte, rng.Intn(192))
		rng.Read(a)
		rng.Read(b)
		if i%2 == 0 && len(a) >= 16 && len(b) >= 16 {
			copy(b[:16], a[:16])
		}
		check(a, b)
	}
}
