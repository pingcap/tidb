// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
)

// legacySplitHelper is the btree.Item based SplitHelper that deleted and
// re-inserted every overlapped range on each merge. It is the reference the
// current implementation must match exactly.
type legacySplitHelper struct {
	inner *btree.BTree
}

func newLegacySplitHelper() *legacySplitHelper {
	t := btree.New(16)
	t.ReplaceOrInsert(Valued{Value: Value{Size: 0, Number: 0}, Key: Span{StartKey: []byte(""), EndKey: []byte("")}})
	return &legacySplitHelper{inner: t}
}

func (f *legacySplitHelper) Merge(val Valued) {
	if len(val.Key.StartKey) == 0 || len(val.Key.EndKey) == 0 {
		return
	}
	overlaps := make([]Valued, 0, 8)
	f.overlapped(val.Key, &overlaps)
	f.mergeWithOverlap(val, overlaps)
}

func (f *legacySplitHelper) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item btree.Item) bool {
		return m(item.(Valued))
	})
}

func (f *legacySplitHelper) mergeWithOverlap(val Valued, overlapped []Valued) {
	if len(overlapped) == 0 {
		return
	}

	for _, r := range overlapped {
		f.inner.Delete(r)
	}

	appendValue := Value{
		Size:   val.Value.Size / uint64(len(overlapped)),
		Number: val.Value.Number / int64(len(overlapped)),
	}
	var (
		rightTrail      *Valued
		leftTrail       *Valued
		emitToCollected = func(rng Valued, standalone bool, split bool) {
			merged := rng.Value
			if split {
				merged.Size /= 2
				merged.Number /= 2
			}
			if !standalone {
				merged = join(appendValue, merged)
			}
			rng.Value = merged
			f.inner.ReplaceOrInsert(rng)
		}
	)

	leftmost := overlapped[0]
	if bytes.Compare(leftmost.Key.StartKey, val.Key.StartKey) < 0 {
		leftTrail = &Valued{
			Key:   Span{StartKey: leftmost.Key.StartKey, EndKey: val.Key.StartKey},
			Value: leftmost.Value,
		}
		overlapped[0].Key.StartKey = val.Key.StartKey
	}

	rightmost := overlapped[len(overlapped)-1]
	if utils.CompareBytesExt(rightmost.Key.EndKey, true, val.Key.EndKey, true) > 0 {
		rightTrail = &Valued{
			Key:   Span{StartKey: val.Key.EndKey, EndKey: rightmost.Key.EndKey},
			Value: rightmost.Value,
		}
		overlapped[len(overlapped)-1].Key.EndKey = val.Key.EndKey
		if len(overlapped) == 1 && leftTrail != nil {
			val := Value{
				Size:   rightTrail.Value.Size * 2 / 3,
				Number: rightTrail.Value.Number * 2 / 3,
			}
			leftTrail.Value = val
			overlapped[0].Value = val
			rightTrail.Value = val
		}
	}

	if leftTrail != nil {
		emitToCollected(*leftTrail, true, true)
	}

	for i, rng := range overlapped {
		split := (i == 0 && leftTrail != nil) || (i == len(overlapped)-1 && rightTrail != nil)
		emitToCollected(rng, false, split)
	}

	if rightTrail != nil {
		emitToCollected(*rightTrail, true, true)
	}
}

func (f *legacySplitHelper) overlapped(k Span, result *[]Valued) {
	var first Span
	f.inner.DescendLessOrEqual(Valued{Key: k}, func(item btree.Item) bool {
		first = item.(Valued).Key
		return false
	})

	f.inner.AscendGreaterOrEqual(Valued{Key: first}, func(item btree.Item) bool {
		r := item.(Valued)
		if !checkOverlaps(r.Key, k) {
			return false
		}
		*result = append(*result, r)
		return true
	})
}

type splitMerger interface {
	Merge(Valued)
	Traverse(func(Valued) bool)
}

func collectValueds(m splitMerger) []Valued {
	var out []Valued
	m.Traverse(func(v Valued) bool {
		out = append(out, v)
		return true
	})
	return out
}

func requireSameAsLegacy(t *testing.T, input []Valued, checkEveryStep bool) *SplitHelper {
	legacy := newLegacySplitHelper()
	current := NewSplitHelper()
	for i, v := range input {
		legacy.Merge(v)
		current.Merge(v)
		if checkEveryStep || i == len(input)-1 {
			require.Equal(t, collectValueds(legacy), collectValueds(current), "after merging #%d %v", i, v)
		}
	}
	return current
}

func randomValue(r *rand.Rand) Value {
	switch r.Intn(6) {
	case 0:
		return Value{}
	case 1:
		return Value{Size: math.MaxUint64 - uint64(r.Intn(1000)), Number: math.MaxInt64 - int64(r.Intn(1000))}
	case 2:
		return Value{Size: uint64(r.Intn(7)), Number: -int64(r.Intn(7))}
	default:
		return Value{Size: uint64(r.Int63n(1 << 40)), Number: r.Int63n(1 << 20)}
	}
}

func randomKey(r *rand.Rand, alphabet, maxLen int) []byte {
	n := r.Intn(maxLen + 1)
	if n == 0 {
		if r.Intn(2) == 0 {
			return nil
		}
		return []byte{}
	}
	k := make([]byte, n)
	for i := range k {
		k[i] = byte('a' + r.Intn(alphabet))
	}
	return k
}

func randomRanges(r *rand.Rand, n, alphabet, maxLen int, inverted bool) []Valued {
	out := make([]Valued, 0, n)
	for len(out) < n {
		start, end := randomKey(r, alphabet, maxLen), randomKey(r, alphabet, maxLen)
		switch r.Intn(10) {
		case 0:
			end = start
		case 1:
			if len(out) > 0 {
				prev := out[r.Intn(len(out))]
				start, end = prev.Key.StartKey, prev.Key.EndKey
			}
		case 2:
			if len(out) > 0 {
				start = out[len(out)-1].Key.EndKey
			}
		case 3:
			if inverted {
				start, end = end, start
			}
		}
		if bytes.Compare(start, end) > 0 && !(inverted && r.Intn(4) == 0) {
			start, end = end, start
		}
		out = append(out, NewValued(start, end, randomValue(r)))
	}
	return out
}

func TestSplitHelperMatchesLegacyOnRandomRanges(t *testing.T) {
	seed := rand.Int63()
	t.Logf("seed %d", seed)
	r := rand.New(rand.NewSource(seed))
	for range 500 {
		h := requireSameAsLegacy(t, randomRanges(r, 1+r.Intn(60), 2+r.Intn(4), 1+r.Intn(3), false), true)
		require.False(t, h.fragmented)
	}
	for range 10 {
		h := requireSameAsLegacy(t, randomRanges(r, 3000, 26, 4, false), false)
		require.False(t, h.fragmented)
	}
}

func TestSplitHelperMatchesLegacyWithInvertedRanges(t *testing.T) {
	seed := rand.Int63()
	t.Logf("seed %d", seed)
	r := rand.New(rand.NewSource(seed))
	for range 500 {
		requireSameAsLegacy(t, randomRanges(r, 1+r.Intn(60), 2+r.Intn(4), 1+r.Intn(3), true), true)
	}
	for range 10 {
		requireSameAsLegacy(t, randomRanges(r, 2000, 26, 4, true), false)
	}
}

func TestSplitHelperMatchesLegacyOnAdversarialRanges(t *testing.T) {
	k := func(s string) []byte { return []byte(s) }
	mb := func(n uint64) Value { return Value{Size: n << 20, Number: int64(n)} }

	cases := map[string][]Valued{
		"empty keys are ignored": {
			NewValued(nil, k("b"), mb(1)),
			NewValued(k("a"), nil, mb(1)),
			NewValued(k(""), k(""), mb(1)),
			NewValued(k("a"), k("b"), mb(1)),
		},
		"single key ranges": {
			NewValued(k("b"), k("b"), mb(9)),
			NewValued(k("a"), k("c"), mb(9)),
			NewValued(k("b"), k("b"), mb(9)),
			NewValued(k("a"), k("a"), mb(9)),
			NewValued(k("c"), k("c"), mb(9)),
			NewValued(k("bb"), k("bb"), mb(9)),
		},
		"inverted ranges": {
			NewValued(k("c"), k("a"), mb(9)),
			NewValued(k("a"), k("d"), mb(9)),
			NewValued(k("e"), k("b"), mb(9)),
			NewValued(k("b"), k("bb"), mb(9)),
			NewValued(k("dd"), k("d"), mb(9)),
		},
		"nested": {
			NewValued(k("a"), k("z"), mb(300)),
			NewValued(k("b"), k("y"), mb(300)),
			NewValued(k("c"), k("x"), mb(300)),
			NewValued(k("d"), k("w"), mb(300)),
			NewValued(k("e"), k("v"), mb(300)),
			NewValued(k("a"), k("z"), mb(300)),
		},
		"touching": {
			NewValued(k("a"), k("b"), mb(5)),
			NewValued(k("b"), k("c"), mb(5)),
			NewValued(k("c"), k("d"), mb(5)),
			NewValued(k("a"), k("d"), mb(5)),
			NewValued(k("d"), k("e"), mb(5)),
			NewValued(k("aa"), k("b"), mb(5)),
		},
		"three way split": {
			NewValued(k("a"), k("z"), mb(100)),
			NewValued(k("m"), k("n"), mb(100)),
			NewValued(k("m"), k("mm"), mb(7)),
		},
	}
	dups := make([]Valued, 0, 500)
	for i := range 500 {
		dups = append(dups, NewValued(k("t1"), k("t9"), Value{Size: uint64(i), Number: int64(i)}))
	}
	cases["many duplicates"] = dups

	growing := make([]Valued, 0, 300)
	for i := range 300 {
		growing = append(growing, NewValued(k("m"), fmt.Appendf(nil, "m%03d", i), mb(uint64(i))))
	}
	cases["same start growing end"] = growing

	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			requireSameAsLegacy(t, input, true)
		})
	}
}

func TestSplitHelperMatchesLegacyOnLogBackupWorkload(t *testing.T) {
	for name, files := range map[string][]tableFile{
		"overlapping": logBackupLikeFiles(rand.New(rand.NewSource(7)), 8, 25, 40),
		"disjoint":    disjointFiles(rand.New(rand.NewSource(7)), 8, 2000),
	} {
		t.Run(name, func(t *testing.T) {
			legacy := map[int64]*legacySplitHelper{}
			current := map[int64]*SplitHelper{}
			for _, f := range files {
				if legacy[f.tableID] == nil {
					legacy[f.tableID] = newLegacySplitHelper()
					current[f.tableID] = NewSplitHelper()
				}
				legacy[f.tableID].Merge(f.Valued)
				current[f.tableID].Merge(f.Valued)
			}
			for id := range legacy {
				require.Equal(t, collectValueds(legacy[id]), collectValueds(current[id]), "table %d", id)
				require.False(t, current[id].fragmented)
			}
		})
	}
}

type tableFile struct {
	tableID int64
	Valued
}

func rowKey(tableID int64, handle uint64) []byte {
	k := make([]byte, 0, 19)
	k = append(k, 't')
	k = binary.BigEndian.AppendUint64(k, uint64(tableID))
	k = append(k, '_', 'r')
	return binary.BigEndian.AppendUint64(k, handle)
}

// logBackupLikeFiles mimics the DML files of a log backup: every flush, every
// store writes one file per table it touched, spanning the smallest and the
// largest key written to that table. Hot tables written all over their key
// space give files that cover nearly the whole table, tables with an
// increasing handle give narrow files moving along the tail, and the rest
// touch a few random key spans.
func logBackupLikeFiles(r *rand.Rand, tables, stores, flushes int) []tableFile {
	const handleSpace = 1 << 40
	files := make([]tableFile, 0, tables*stores*flushes)
	for flush := range flushes {
		for table := range tables {
			tableID := int64(100 + table)
			kind := table % 4
			for range stores {
				var lo, hi uint64
				switch kind {
				case 0, 1:
					lo = uint64(r.Int63n(handleSpace / 50))
					hi = handleSpace - uint64(r.Int63n(handleSpace/50))
				case 2:
					tail := uint64(flush+1) * (handleSpace / uint64(flushes+1))
					lo = tail - uint64(r.Int63n(int64(handleSpace/(4*flushes))))
					hi = tail + uint64(r.Int63n(int64(handleSpace/(4*flushes))))
				default:
					lo = uint64(r.Int63n(handleSpace))
					hi = lo + uint64(r.Int63n(handleSpace/20))
				}
				if r.Intn(50) == 0 {
					hi = lo
				}
				files = append(files, tableFile{
					tableID: tableID,
					Valued: NewValued(rowKey(tableID, lo), rowKey(tableID, hi), Value{
						Size:   uint64(1<<10 + r.Intn(8<<20)),
						Number: int64(1 + r.Intn(50000)),
					}),
				})
			}
		}
	}
	return files
}

// disjointFiles gives every table files that each cover their own key span,
// in random order, so every merge overlaps a single range.
func disjointFiles(r *rand.Rand, tables, perTable int) []tableFile {
	files := make([]tableFile, 0, tables*perTable)
	for table := range tables {
		tableID := int64(100 + table)
		for i := range perTable {
			lo := uint64(i) << 20
			files = append(files, tableFile{
				tableID: tableID,
				Valued: NewValued(rowKey(tableID, lo), rowKey(tableID, lo+uint64(r.Intn(1<<19))), Value{
					Size:   uint64(1<<10 + r.Intn(8<<20)),
					Number: int64(1 + r.Intn(50000)),
				}),
			})
		}
	}
	r.Shuffle(len(files), func(i, j int) { files[i], files[j] = files[j], files[i] })
	return files
}

func BenchmarkSplitHelperMerge(b *testing.B) {
	workloads := []struct {
		name   string
		tables int
		files  func(*rand.Rand) []tableFile
	}{
		{"overlapping/files=300k", 100, func(r *rand.Rand) []tableFile { return logBackupLikeFiles(r, 100, 25, 120) }},
		{"overlapping/files=1.2M", 400, func(r *rand.Rand) []tableFile { return logBackupLikeFiles(r, 400, 25, 120) }},
		{"overlapping/files=3M", 1000, func(r *rand.Rand) []tableFile { return logBackupLikeFiles(r, 1000, 25, 120) }},
		{"disjoint/files=1M", 100, func(r *rand.Rand) []tableFile { return disjointFiles(r, 100, 10000) }},
	}
	impls := []struct {
		name string
		new  func() splitMerger
	}{
		{"legacy", func() splitMerger { return newLegacySplitHelper() }},
		{"current", func() splitMerger { return NewSplitHelper() }},
	}
	for _, w := range workloads {
		files := w.files(rand.New(rand.NewSource(1)))
		for _, impl := range impls {
			b.Run(w.name+"/"+impl.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					helpers := make(map[int64]splitMerger, w.tables)
					for _, f := range files {
						h, ok := helpers[f.tableID]
						if !ok {
							h = impl.new()
							helpers[f.tableID] = h
						}
						h.Merge(f.Valued)
					}
				}
			})
		}
	}
}
