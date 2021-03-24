package unionstore

import (
	"context"

	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/kv"
)

// KVUnionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type KVUnionStore struct {
	memBuffer *memdb
	snapshot  tidbkv.Snapshot
	opts      options
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(snapshot tidbkv.Snapshot) *KVUnionStore {
	return &KVUnionStore{
		snapshot:  snapshot,
		memBuffer: newMemDB(),
		opts:      make(map[int]interface{}),
	}
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *KVUnionStore) GetMemBuffer() MemBuffer {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *KVUnionStore) Get(ctx context.Context, k tidbkv.Key) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if tidbkv.IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, tidbkv.ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *KVUnionStore) Iter(k tidbkv.Key, upperBound tidbkv.Key) (tidbkv.Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *KVUnionStore) IterReverse(k tidbkv.Key) (tidbkv.Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *KVUnionStore) HasPresumeKeyNotExists(k tidbkv.Key) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
func (us *KVUnionStore) UnmarkPresumeKeyNotExists(k tidbkv.Key) {
	us.memBuffer.UpdateFlags(k, kv.DelPresumeKeyNotExists)
}

// SetOption implements the unionStore SetOption interface.
func (us *KVUnionStore) SetOption(opt int, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the unionStore DelOption interface.
func (us *KVUnionStore) DelOption(opt int) {
	delete(us.opts, opt)
}

// GetOption implements the unionStore GetOption interface.
func (us *KVUnionStore) GetOption(opt int) interface{} {
	return us.opts[opt]
}

type options map[int]interface{}

func (opts options) Get(opt int) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
