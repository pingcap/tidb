package kv

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/tidwall/btree"
)

const degree = 50

type btreeKey Key

func (k btreeKey) Less(item btree.Item) bool {
	switch x := item.(type) {
	case btreeKey:
		return bytes.Compare(k, x) < 0
	case *btreePair:
		return bytes.Compare(k, x.key) < 0
	}
	return true
}

type btreePair struct {
	key   Key
	value []byte
}

func (pair *btreePair) Less(item btree.Item) bool {
	switch x := item.(type) {
	case btreeKey:
		return bytes.Compare(pair.key, x) < 0
	case *btreePair:
		return bytes.Compare(pair.key, x.key) < 0
	}
	return true
}

type bTreeBuffer struct {
	tree *btree.BTree
}

type bTreeIter struct {
	tree    *btree.BTree
	seek    Key
	pair    *btreePair
	reverse bool
}

// NewBTreeBuffer creates a new bTreeBuffer.
func NewBTreeBuffer() MemBuffer {
	return &bTreeBuffer{tree: btree.New(degree)}
}

// Seek creates an Iterator.
func (m *bTreeBuffer) Seek(k Key) (Iterator, error) {
	it := &bTreeIter{tree: m.tree, seek: k}
	it.Next()
	return it, nil
}

func (m *bTreeBuffer) SeekReverse(k Key) (Iterator, error) {
	if k != nil {
		k = k.Prev()
	}
	it := &bTreeIter{tree: m.tree, seek: k, reverse: true}
	it.Next()
	return it, nil
}

// Get returns the value associated with key.
func (m *bTreeBuffer) Get(k Key) ([]byte, error) {
	pair := m.tree.Get(btreeKey(k))
	if pair == nil {
		return nil, ErrNotExist
	}
	return pair.(*btreePair).value, nil
}

// Set associates key with value.
func (m *bTreeBuffer) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	m.tree.ReplaceOrInsert(&btreePair{key: k, value: v})
	return nil
}

// Delete removes the entry from buffer with provided key.
func (m *bTreeBuffer) Delete(k Key) error {
	m.tree.ReplaceOrInsert(&btreePair{key: k, value: nil})
	return nil
}

// Release reset the buffer.
func (m *bTreeBuffer) Release() {
	m.tree = btree.New(degree)
}

// Next implements the Iterator Next.
func (i *bTreeIter) Next() error {
	i.pair = nil
	if i.reverse {
		iterFunc := func(item btree.Item) bool {
			i.pair = item.(*btreePair)
			i.seek = i.pair.key.Prev()
			return false
		}
		if i.seek == nil {
			i.tree.Descend(iterFunc)
		} else {
			i.tree.DescendLessOrEqual(btreeKey(i.seek), iterFunc)
		}
	} else {
		i.tree.AscendGreaterOrEqual(btreeKey(i.seek), func(item btree.Item) bool {
			i.pair = item.(*btreePair)
			i.seek = i.pair.key.Next()
			return false
		})
	}
	return nil
}

// Valid implements the Iterator Valid.
func (i *bTreeIter) Valid() bool {
	return i.pair != nil
}

// Key implements the Iterator Key.
func (i *bTreeIter) Key() Key {
	return i.pair.key
}

// Value implements the Iterator Value.
func (i *bTreeIter) Value() []byte {
	return i.pair.value
}

// Close Implements the Iterator Close.
func (i *bTreeIter) Close() {
}
