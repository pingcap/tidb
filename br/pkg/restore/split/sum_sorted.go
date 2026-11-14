// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
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

// SplitHelper represents a set of valued ranges, which doesn't overlap and union of them all is the full key space.
//
// The ranges are kept in a treap ordered by start key. A merged range adds an
// equal share of its value to every range it overlaps, and that share is
// added lazily to the ranges strictly inside it, so a merge costs O(log n)
// however many ranges it covers.
type SplitHelper struct {
	root *spanNode
	seed uint64
	// fragmented is set once an inverted range (start key after end key) has
	// been merged. Such a range breaks the full key space partition, so from
	// then on the overlapped ranges are visited one by one.
	fragmented bool
	overlaps   []*spanNode
}

type spanNode struct {
	Valued
	// pending is added to both subtrees but not yet to the nodes in them.
	pending     Value
	size        int
	priority    uint64
	left, right *spanNode
}

// NewSplitHelper creates a set of a subset of spans, with the full key space as initial status
func NewSplitHelper() *SplitHelper {
	f := &SplitHelper{}
	f.root = f.newNode(Valued{Value: Value{Size: 0, Number: 0}, Key: Span{StartKey: []byte(""), EndKey: []byte("")}})
	return f
}

func (f *SplitHelper) newNode(v Valued) *spanNode {
	// splitmix64
	f.seed += 0x9e3779b97f4a7c15
	z := f.seed
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return &spanNode{Valued: v, size: 1, priority: z ^ (z >> 31)}
}

func (f *SplitHelper) Merge(val Valued) {
	if len(val.Key.StartKey) == 0 || len(val.Key.EndKey) == 0 {
		return
	}
	if f.fragmented || bytes.Compare(val.Key.StartKey, val.Key.EndKey) > 0 {
		f.mergeOneByOne(val)
		return
	}
	f.mergeRange(val)
}

// traverse the items in ascend order
func (f *SplitHelper) Traverse(m func(Valued) bool) {
	traverse(f.root, Value{}, m)
}

func traverse(n *spanNode, pending Value, m func(Valued) bool) bool {
	if n == nil {
		return true
	}
	below := join(pending, n.pending)
	if !traverse(n.left, below, m) {
		return false
	}
	v := n.Valued
	v.Value = join(pending, v.Value)
	return m(v) && traverse(n.right, below, m)
}

// mergeRange relies on the ranges being a partition of the key space, so the
// ranges val overlaps are the one holding val's start key and all those
// starting before val's end key.
func (f *SplitHelper) mergeRange(val Valued) {
	before, rest := splitBefore(f.root, val.Key.StartKey)
	inside, after := splitBefore(rest, val.Key.EndKey)
	var first *spanNode
	switch {
	case inside != nil && bytes.Equal(minNode(inside).Key.StartKey, val.Key.StartKey):
		inside, first = popMin(inside)
	case before != nil:
		before, first = popMax(before)
	}
	if first == nil || !checkOverlaps(first.Key, val.Key) {
		f.root = mergeNodes(mergeNodes(before, first), mergeNodes(inside, after))
		return
	}

	leftmost, rightmost := first, first
	count := 1 + size(inside)
	if inside != nil {
		inside, rightmost = popMax(inside)
	}
	share, newFirst, rightTrail := f.spread(val, count, leftmost, rightmost)
	addTo(inside, share)

	t := mergeNodes(before, leftmost)
	// A single key range splits one range in two: both new pieces start at that
	// key and the right one wins, as it is inserted last.
	if newFirst != nil && (rightTrail == nil || !bytes.Equal(newFirst.Key.StartKey, rightTrail.Key.StartKey)) {
		t = mergeNodes(t, newFirst)
	}
	t = mergeNodes(t, inside)
	if rightmost != leftmost {
		t = mergeNodes(t, rightmost)
	}
	t = mergeNodes(t, rightTrail)
	f.root = mergeNodes(t, after)
}

// mergeOneByOne collects the overlapped ranges the way a sorted walk from the
// last range starting at or before val's start key would, and does not
// assume the ranges are a partition.
func (f *SplitHelper) mergeOneByOne(val Valued) {
	before, rest := splitBefore(f.root, val.Key.StartKey)
	if rest == nil || !bytes.Equal(minNode(rest).Key.StartKey, val.Key.StartKey) {
		if before != nil {
			var first *spanNode
			before, first = popMax(before)
			rest = mergeNodes(first, rest)
		}
	}
	overlapped := f.overlaps[:0]
	for rest != nil && checkOverlaps(minNode(rest).Key, val.Key) {
		var n *spanNode
		rest, n = popMin(rest)
		overlapped = append(overlapped, n)
	}
	f.root = mergeNodes(before, rest)
	defer func() {
		clear(overlapped)
		f.overlaps = overlapped[:0]
	}()
	if len(overlapped) == 0 {
		return
	}
	f.fragmented = true

	last := len(overlapped) - 1
	share, newFirst, rightTrail := f.spread(val, len(overlapped), overlapped[0], overlapped[last])
	for i := 1; i < last; i++ {
		overlapped[i].Value = join(share, overlapped[i].Value)
	}
	f.root = insertOrReplace(f.root, overlapped[0])
	if newFirst != nil {
		f.root = insertOrReplace(f.root, newFirst)
	}
	for _, n := range overlapped[1:] {
		f.root = insertOrReplace(f.root, n)
	}
	if rightTrail != nil {
		f.root = insertOrReplace(f.root, rightTrail)
	}
}

func halve(v Value) Value {
	return Value{Size: v.Size / 2, Number: v.Number / 2}
}

// spread divides val's value equally into `count` shares, one for each range it
// overlaps. It updates the outer two of those ranges, leftmost and rightmost
// (the same node when count is 1), and returns the share the ranges between
// them get, plus the new ranges val's start and end keys cut off.
func (f *SplitHelper) spread(val Valued, count int, leftmost, rightmost *spanNode) (share Value, newFirst, rightTrail *spanNode) {
	share = Value{
		Size:   val.Value.Size / uint64(count),
		Number: val.Value.Number / int64(count),
	}

	// overlapped ranges   +-------------+----------+
	// new valued item             +-------------+
	//                     a       b     c       d  e
	// the part [a,b] is a left trail and [d,e] a right trail: they are not
	// overlapped with the new valued item. A range cut at b or d is `split`
	// and its value is halved before the share of the new item is added.
	hasLeftTrail := bytes.Compare(leftmost.Key.StartKey, val.Key.StartKey) < 0
	hasRightTrail := utils.CompareBytesExt(rightmost.Key.EndKey, true, val.Key.EndKey, true) > 0

	leftValue := leftmost.Value
	rightValue := rightmost.Value
	if count == 1 && hasLeftTrail && hasRightTrail {
		//                      (split)   (split)     (split)
		// overlapped ranges   +-----------------------------+
		// new valued item             +-------------+
		//                     a       b             c       d
		// now the overlapped range should be divided into 3 equal parts,
		// each of them gets half of 2/3x as the other split ranges do.
		val := Value{
			Size:   rightValue.Size * 2 / 3,
			Number: rightValue.Number * 2 / 3,
		}
		leftValue = val
		rightValue = val
	}

	if hasRightTrail {
		rightTrail = f.newNode(Valued{
			Key:   Span{StartKey: val.Key.EndKey, EndKey: rightmost.Key.EndKey},
			Value: halve(rightValue),
		})
		rightmost.Key.EndKey = val.Key.EndKey
	}
	if rightmost != leftmost {
		merged := rightmost.Value
		if hasRightTrail {
			merged = halve(merged)
		}
		rightmost.Value = join(share, merged)
	}
	if hasLeftTrail {
		newFirst = f.newNode(Valued{
			Key:   Span{StartKey: val.Key.StartKey, EndKey: leftmost.Key.EndKey},
			Value: join(share, halve(leftValue)),
		})
		leftmost.Key.EndKey = val.Key.StartKey
		leftmost.Value = halve(leftValue)
	} else {
		merged := leftmost.Value
		if count == 1 && hasRightTrail {
			merged = halve(merged)
		}
		leftmost.Value = join(share, merged)
	}
	return share, newFirst, rightTrail
}

func size(n *spanNode) int {
	if n == nil {
		return 0
	}
	return n.size
}

func addTo(n *spanNode, v Value) {
	if n == nil {
		return
	}
	n.Value = join(n.Value, v)
	n.pending = join(n.pending, v)
}

func (n *spanNode) push() {
	if n.pending != (Value{}) {
		addTo(n.left, n.pending)
		addTo(n.right, n.pending)
		n.pending = Value{}
	}
}

func (n *spanNode) update() {
	n.size = 1 + size(n.left) + size(n.right)
}

// splitBefore splits t into the nodes starting before key and the rest.
func splitBefore(t *spanNode, key []byte) (lt, ge *spanNode) {
	if t == nil {
		return nil, nil
	}
	t.push()
	if bytes.Compare(t.Key.StartKey, key) < 0 {
		t.right, ge = splitBefore(t.right, key)
		t.update()
		return t, ge
	}
	lt, t.left = splitBefore(t.left, key)
	t.update()
	return lt, t
}

// mergeNodes joins two treaps, every key of a being less than every key of b.
func mergeNodes(a, b *spanNode) *spanNode {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.priority > b.priority {
		a.push()
		a.right = mergeNodes(a.right, b)
		a.update()
		return a
	}
	b.push()
	b.left = mergeNodes(a, b.left)
	b.update()
	return b
}

func minNode(t *spanNode) *spanNode {
	for t.left != nil {
		t = t.left
	}
	return t
}

func popMin(t *spanNode) (rest, minimum *spanNode) {
	t.push()
	if t.left == nil {
		rest = t.right
		t.right = nil
		t.update()
		return rest, t
	}
	t.left, minimum = popMin(t.left)
	t.update()
	return t, minimum
}

func popMax(t *spanNode) (rest, maximum *spanNode) {
	t.push()
	if t.right == nil {
		rest = t.left
		t.left = nil
		t.update()
		return rest, t
	}
	t.right, maximum = popMax(t.right)
	t.update()
	return t, maximum
}

func insertOrReplace(t, n *spanNode) *spanNode {
	lt, ge := splitBefore(t, n.Key.StartKey)
	if ge != nil && bytes.Equal(minNode(ge).Key.StartKey, n.Key.StartKey) {
		ge, _ = popMin(ge)
	}
	return mergeNodes(mergeNodes(lt, n), ge)
}

// checkOverlaps checks whether two spans have overlapped part.
// `ap` should be a finite range
func checkOverlaps(a, ap Span) bool {
	if len(a.EndKey) == 0 {
		return bytes.Compare(ap.EndKey, a.StartKey) > 0
	}
	return bytes.Compare(a.StartKey, ap.EndKey) < 0 && bytes.Compare(ap.StartKey, a.EndKey) < 0
}
