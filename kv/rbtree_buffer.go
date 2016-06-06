package kv

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/petar/GoLLRB/llrb"
)

type keyItem Key

func (k keyItem) Less(item llrb.Item) bool {
	switch x := item.(type) {
	case keyItem:
		return bytes.Compare(k, x) < 0
	case *pairItem:
		return bytes.Compare(k, x.key) < 0
	}
	return true
}

type pairItem struct {
	key   Key
	value []byte
}

func (pair *pairItem) Less(item llrb.Item) bool {
	switch x := item.(type) {
	case keyItem:
		return bytes.Compare(pair.key, x) < 0
	case *pairItem:
		return bytes.Compare(pair.key, x.key) < 0
	}
	return true
}

type rbTreeBuffer struct {
	tree *llrb.LLRB
}

type rbTreeIter struct {
	tree *llrb.LLRB
	seek Key
	pair *pairItem
}

// NewRBTreeBuffer creates a new rbTreeBuffer.
func NewRBTreeBuffer() MemBuffer {
	return &rbTreeBuffer{tree: llrb.New()}
}

// Seek creates an Iterator.
func (m *rbTreeBuffer) Seek(k Key) (Iterator, error) {
	it := &rbTreeIter{tree: m.tree, seek: k}
	it.Next()
	return it, nil
}

func (m *rbTreeBuffer) SeekReverse(k Key) (Iterator, error) {
	// TODO: implement Prev.
	return nil, ErrNotImplemented
}

// Get returns the value associated with key.
func (m *rbTreeBuffer) Get(k Key) ([]byte, error) {
	pair := m.tree.Get(keyItem(k))
	if pair == nil {
		return nil, ErrNotExist
	}
	return pair.(*pairItem).value, nil
}

// Set associates key with value.
func (m *rbTreeBuffer) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	m.tree.ReplaceOrInsert(&pairItem{key: k, value: v})
	return nil
}

// Delete removes the entry from buffer with provided key.
func (m *rbTreeBuffer) Delete(k Key) error {
	m.tree.ReplaceOrInsert(&pairItem{key: k, value: nil})
	return nil
}

// Release reset the buffer.
func (m *rbTreeBuffer) Release() {
	m.tree = llrb.New()
}

// Next implements the Iterator Next.
func (i *rbTreeIter) Next() error {
	i.pair = nil
	i.tree.AscendGreaterOrEqual(keyItem(i.seek), func(item llrb.Item) bool {
		i.pair = item.(*pairItem)
		i.seek = i.pair.key.Next()
		return false
	})
	return nil
}

// Valid implements the Iterator Valid.
func (i *rbTreeIter) Valid() bool {
	return i.pair != nil
}

// Key implements the Iterator Key.
func (i *rbTreeIter) Key() Key {
	return i.pair.key
}

// Value implements the Iterator Value.
func (i *rbTreeIter) Value() []byte {
	return i.pair.value
}

// Close Implements the Iterator Close.
func (i *rbTreeIter) Close() {
}
