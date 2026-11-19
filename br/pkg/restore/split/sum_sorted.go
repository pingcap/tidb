// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/kv"
)

// Value is the value type of stored in the span tree.
type Value struct {
	Size   uint64
	Number int64
}

// join finds the upper bound of two values.
func join(a, b Value) Value {
	return Value{
		Size:   a.Size + b.Size,
		Number: a.Number + b.Number,
	}
}

// Span is the type of an adjacent sub key space.
type Span = kv.KeyRange

// Valued is span binding to a value, which is the entry type of span tree.
type Valued struct {
	Key   Span
	Value Value
}

func NewValued(startKey, endKey []byte, value Value) Valued {
	return Valued{
		Key: Span{
			StartKey: startKey,
			EndKey:   endKey,
		},
		Value: value,
	}
}

func (v Valued) MemSize() int {
	return len(v.Key.StartKey) + len(v.Key.EndKey) +
		// slice header:
		int(reflect.TypeFor[[]string]().Size())*2 +
		// value: uint64 * 2
		8 + 8
}

func (v Valued) String() string {
	return fmt.Sprintf("(%s, %.2f MB, %d)", logutil.StringifyRange(v.Key), float64(v.Value.Size)/1024/1024, v.Value.Number)
}

func (v Valued) Less(other btree.Item) bool {
	return bytes.Compare(v.Key.StartKey, other.(Valued).Key.StartKey) < 0
}

// implement for `AppliedFile`
func (v Valued) GetStartKey() []byte {
	return v.Key.StartKey
}

// implement for `AppliedFile`
func (v Valued) GetEndKey() []byte {
	return v.Key.EndKey
}

// fragNode is a fragment of a size-augmented treap ordered by startKey. The end of a
// fragment is implicit: the start of the next fragment (+inf for the last one).
type fragNode struct {
	startKey    []byte
	val         Value // exact once ancestors' lazy values are pushed
	lazy        Value // pending addition for both children
	size        int
	prio        uint32
	left, right *fragNode
}

func fragSize(n *fragNode) int {
	if n == nil {
		return 0
	}
	return n.size
}

func (n *fragNode) apply(v Value) {
	if n != nil {
		n.val = join(n.val, v)
		n.lazy = join(n.lazy, v)
	}
}

func (n *fragNode) push() {
	if n.lazy != (Value{}) {
		n.left.apply(n.lazy)
		n.right.apply(n.lazy)
		n.lazy = Value{}
	}
}

func (n *fragNode) pull() { n.size = 1 + fragSize(n.left) + fragSize(n.right) }

// fragSplitKey splits t into nodes with startKey < key (or <= key when orEqual) and the rest.
func fragSplitKey(t *fragNode, key []byte, orEqual bool) (*fragNode, *fragNode) {
	if t == nil {
		return nil, nil
	}
	t.push()
	c := bytes.Compare(t.startKey, key)
	if c < 0 || (orEqual && c == 0) {
		l, r := fragSplitKey(t.right, key, orEqual)
		t.right = l
		t.pull()
		return t, r
	}
	l, r := fragSplitKey(t.left, key, orEqual)
	t.left = r
	t.pull()
	return l, t
}

// fragPopLast detaches the last node of t.
func fragPopLast(t *fragNode) (*fragNode, *fragNode) {
	if t == nil {
		return nil, nil
	}
	t.push()
	if t.right == nil {
		l := t.left
		t.left = nil
		t.pull()
		return l, t
	}
	rest, last := fragPopLast(t.right)
	t.right = rest
	t.pull()
	return t, last
}

func fragFirst(t *fragNode) *fragNode {
	if t == nil {
		return nil
	}
	for t.left != nil {
		t = t.left
	}
	return t
}

func fragMerge(a, b *fragNode) *fragNode {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.prio > b.prio {
		a.push()
		a.right = fragMerge(a.right, b)
		a.pull()
		return a
	}
	b.push()
	b.left = fragMerge(a, b.left)
	b.pull()
	return b
}

// Slabs and arena chunks start small and double, so helpers of small tables stay small.
const (
	fragArenaChunkMin = 512
	fragArenaChunkMax = 256 << 10
	fragSlabMin       = 8
	fragSlabMax       = 1024
)

// warnedInvertedSpan makes the first fallback to the btree implementation log at
// WARN level and later ones at DEBUG.
var warnedInvertedSpan atomic.Bool

// SplitHelper represents a set of valued ranges, which doesn't overlap and union of
// them all is the full key space. It accumulates the estimated data volume of
// backup files per key range for choosing region split points.
//
// Merge semantics: a new span's value is divided equally among the fragments it
// overlaps; partially overlapped edge fragments are split and their value halved
// (into thirds when one fragment is split three ways).
//
// Implementation: fragments live in a size-augmented treap ordered by start key
// (a fragment ends where the next one starts). The fragments overlapped by a span
// form one contiguous run, so the equal share is applied with a single lazy
// range-add and only the two edge fragments are rewritten: O(log n) per Merge.
// The previous btree implementation deleted and re-inserted every overlapped
// fragment, O(k log n) with k growing over time for hot regions, which made PiTR
// pre-split over long log windows take hours. Keys retained by the helper are
// copied into pointer-free arena chunks, so callers may release the buffers
// backing the merged spans.
//
// An inverted span (StartKey > EndKey) makes the previous implementation produce
// overlapping fragments, which the treap cannot represent. On the first one the
// helper moves its fragments into that implementation (btreeSplitHelper) and
// keeps using it, so the result stays the same for any input.
type SplitHelper struct {
	root  *fragNode
	seed  uint32
	arena []byte
	slab  []fragNode
	// btree is set once an inverted span has been merged.
	btree *btreeSplitHelper
}

// NewSplitHelper creates a set of spans covering the full key space with zero value.
func NewSplitHelper() *SplitHelper {
	// A random seed per helper keeps the expected O(log n) depth for any input
	// (the result does not depend on the tree shape).
	h := &SplitHelper{seed: rand.Uint32() | 1}
	h.root = h.newNode([]byte{}, Value{})
	return h
}

func (h *SplitHelper) newNode(start []byte, v Value) *fragNode {
	h.seed ^= h.seed << 13
	h.seed ^= h.seed >> 17
	h.seed ^= h.seed << 5
	if len(h.slab) == cap(h.slab) {
		h.slab = make([]fragNode, 0, min(fragSlabMax, max(fragSlabMin, 2*cap(h.slab))))
	}
	h.slab = append(h.slab, fragNode{startKey: start, val: v, size: 1, prio: h.seed})
	return &h.slab[len(h.slab)-1]
}

func (h *SplitHelper) own(k []byte) []byte {
	if cap(h.arena)-len(h.arena) < len(k) {
		h.arena = make([]byte, 0, max(len(k), min(fragArenaChunkMax, max(fragArenaChunkMin, 2*cap(h.arena)))))
	}
	off := len(h.arena)
	h.arena = append(h.arena, k...)
	return h.arena[off:len(h.arena):len(h.arena)]
}

// Len returns the number of fragments.
func (h *SplitHelper) Len() int {
	if h.btree != nil {
		return h.btree.inner.Len()
	}
	return fragSize(h.root)
}

// Merge adds a valued span. Spans with an empty start or end key are ignored.
func (h *SplitHelper) Merge(val Valued) {
	k := val.Key
	if len(k.StartKey) == 0 || len(k.EndKey) == 0 {
		return
	}
	if h.btree == nil && bytes.Compare(k.StartKey, k.EndKey) > 0 {
		if bytes.Compare(h.floorStart(k.StartKey), k.EndKey) >= 0 {
			// As in the btree implementation, an inverted span overlapping no
			// fragment changes nothing.
			return
		}
		logf := log.Debug
		if !warnedInvertedSpan.Swap(true) {
			logf = log.Warn // once per process; there may be many tables
		}
		logf("split helper: merging an inverted span, falling back to the btree implementation",
			logutil.Key("start-key", k.StartKey), logutil.Key("end-key", k.EndKey))
		h.switchToBTree()
	}
	if h.btree != nil {
		// Clone rather than copy into the arena: the btree keeps only some of
		// the keys, and the others must stay collectable.
		h.btree.Merge(Valued{Key: Span{StartKey: bytes.Clone(k.StartKey), EndKey: bytes.Clone(k.EndKey)}, Value: val.Value})
		return
	}
	le, gt := fragSplitKey(h.root, k.StartKey, true) // le is never empty: the head fragment starts at ""
	a, first := fragPopLast(le)
	if bytes.Compare(first.startKey, k.EndKey) >= 0 {
		// point span exactly at a fragment start: no overlap
		h.root = fragMerge(fragMerge(a, first), gt)
		return
	}
	b, c := fragSplitKey(gt, k.EndKey, false)
	cnt := 1 + fragSize(b)
	mid, last := fragPopLast(b)
	rightmost := first
	if last != nil {
		rightmost = last
	}
	var origEnd []byte // +inf when nil
	if n := fragFirst(c); n != nil {
		origEnd = n.startKey
	}

	appendValue := Value{Size: val.Value.Size / uint64(cnt), Number: val.Value.Number / int64(cnt)}
	half := func(v Value) Value { return Value{Size: v.Size / 2, Number: v.Number / 2} }
	hasLeft := bytes.Compare(first.startKey, k.StartKey) < 0
	hasRight := origEnd == nil || bytes.Compare(origEnd, k.EndKey) > 0
	origLeft, origRight := first.val, rightmost.val
	leftTrailV, rightTrailV, firstV := origLeft, origRight, origLeft
	if hasRight && cnt == 1 && hasLeft {
		v23 := Value{Size: origRight.Size * 2 / 3, Number: origRight.Number * 2 / 3}
		leftTrailV, firstV, rightTrailV = v23, v23, v23
	}

	var newMid, newRight *fragNode
	var endOwned []byte
	if hasRight {
		endOwned = h.own(k.EndKey)
		newRight = h.newNode(endOwned, half(rightTrailV))
	}
	fv := firstV
	if hasLeft || (cnt == 1 && hasRight) {
		fv = half(fv)
	}
	fv = join(appendValue, fv)
	if hasLeft {
		first.val = half(leftTrailV)
		// a point span leaves an empty middle part that SplitHelper replaces by the right trail
		if !(newRight != nil && bytes.Equal(k.StartKey, k.EndKey)) {
			newMid = h.newNode(h.own(k.StartKey), fv)
		}
	} else {
		first.val = fv
	}
	mid.apply(appendValue)
	if last != nil {
		lv := last.val
		if hasRight {
			lv = half(lv)
		}
		last.val = join(appendValue, lv)
	}

	t := fragMerge(a, first)
	t = fragMerge(t, newMid)
	t = fragMerge(t, mid)
	t = fragMerge(t, last)
	t = fragMerge(t, newRight)
	h.root = fragMerge(t, c)
}

// floorStart returns the start key of the fragment containing key.
func (h *SplitHelper) floorStart(key []byte) []byte {
	var start []byte
	for n := h.root; n != nil; {
		if bytes.Compare(n.startKey, key) <= 0 {
			start = n.startKey
			n = n.right
		} else {
			n = n.left
		}
	}
	return start
}

// switchToBTree moves the fragments into a btreeSplitHelper, which is used for
// every later merge.
func (h *SplitHelper) switchToBTree() {
	t := &btreeSplitHelper{inner: btree.New(16)}
	h.Traverse(func(v Valued) bool {
		t.inner.ReplaceOrInsert(v)
		return true
	})
	h.btree, h.root, h.slab = t, nil, nil
}

// Traverse visits fragments in ascending key order.
func (h *SplitHelper) Traverse(fn func(Valued) bool) {
	if h.btree != nil {
		h.btree.Traverse(fn)
		return
	}
	type frame struct {
		n   *fragNode
		add Value
	}
	var (
		stack   []frame
		prev    *Valued
		stopped bool
	)
	emit := func(v Valued) {
		if prev != nil && !stopped {
			p := *prev
			p.Key.EndKey = v.Key.StartKey
			if !fn(p) {
				stopped = true
			}
		}
		prev = &v
	}
	n, add := h.root, Value{}
	for (n != nil || len(stack) > 0) && !stopped {
		for n != nil {
			stack = append(stack, frame{n, add})
			add = join(add, n.lazy)
			n = n.left
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		emit(Valued{Key: Span{StartKey: f.n.startKey}, Value: join(f.n.val, f.add)})
		add = join(f.add, f.n.lazy)
		n = f.n.right
	}
	if prev != nil && !stopped {
		last := *prev
		last.Key.EndKey = []byte{}
		fn(last)
	}
}

// checkOverlaps checks whether two spans have overlapped part.
// `ap` should be a finite range
func checkOverlaps(a, ap Span) bool {
	if len(a.EndKey) == 0 {
		return bytes.Compare(ap.EndKey, a.StartKey) > 0
	}
	return bytes.Compare(a.StartKey, ap.EndKey) < 0 && bytes.Compare(ap.StartKey, a.EndKey) < 0
}
