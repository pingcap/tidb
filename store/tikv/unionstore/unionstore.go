package unionstore

import (
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
)

// type StagingHandle kv.StagingHandle
// type Iterator kv.Iterator
type Snapshot kv.Snapshot
type Getter kv.Getter

type UnionStore interface {
	kv.UnionStore
}

// MemBufferIterator is an Iterator with KeyFlags related functions.
type MemBufferIterator interface {
	kv.MemBufferIterator
	Flags() KeyFlags
	UpdateFlags(...FlagsOp)
	Handle() MemKeyHandle
}

// MemBuffer is an in-memory kv collection, can be used to buffer write operations.
type MemBuffer interface {
	kv.MemBuffer
	// GetFlags returns the latest flags associated with key.
	GetFlags(kv.Key) (KeyFlags, error)
	// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
	SetWithFlags(kv.Key, []byte, ...FlagsOp) error
	// UpdateFlags update the flags associated with key.
	UpdateFlags(kv.Key, ...FlagsOp)
	// DeleteWithFlags delete key with the given KeyFlags
	DeleteWithFlags(kv.Key, ...FlagsOp) error

	GetKeyByHandle(MemKeyHandle) []byte
	GetValueByHandle(MemKeyHandle) ([]byte, bool)
	// InspectStage used to inspect the value updates in the given stage.
	InspectStage(kv.StagingHandle, func(kv.Key, KeyFlags, []byte))
}

// unionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
	memBuffer    *memdb
	snapshot     Snapshot
	idxNameCache map[int64]*model.TableInfo
	opts         options
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	return &unionStore{
		snapshot:     snapshot,
		memBuffer:    newMemDB(),
		idxNameCache: make(map[int64]*model.TableInfo),
		opts:         make(map[kv.Option]interface{}),
	}
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *unionStore) GetMemBuffer() kv.MemBuffer {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *unionStore) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, kv.ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *unionStore) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *unionStore) IterReverse(k kv.Key) (kv.Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *unionStore) HasPresumeKeyNotExists(k kv.Key) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
func (us *unionStore) UnmarkPresumeKeyNotExists(k kv.Key) {
	us.memBuffer.UpdateFlags(k, DelPresumeKeyNotExists)
}

func (us *unionStore) GetTableInfo(id int64) *model.TableInfo {
	return us.idxNameCache[id]
}

func (us *unionStore) CacheTableInfo(id int64, info *model.TableInfo) {
	us.idxNameCache[id] = info
}

// SetOption implements the unionStore SetOption interface.
func (us *unionStore) SetOption(opt kv.Option, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the unionStore DelOption interface.
func (us *unionStore) DelOption(opt kv.Option) {
	delete(us.opts, opt)
}

// GetOption implements the unionStore GetOption interface.
func (us *unionStore) GetOption(opt kv.Option) interface{} {
	return us.opts[opt]
}

type options map[kv.Option]interface{}

func (opts options) Get(opt kv.Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
