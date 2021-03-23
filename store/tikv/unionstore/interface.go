package unionstore

import (
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/kv"
)

// MemBuffer is an in-memory kv collection, can be used to buffer write operations.
type MemBuffer interface {
	tidbkv.MemBuffer
	// IterWithFlags returns a MemBufferIterator.
	IterWithFlags(k tidbkv.Key, upperBound tidbkv.Key) MemBufferIterator
	// IterReverseWithFlags returns a reversed MemBufferIterator.
	IterReverseWithFlags(k tidbkv.Key) MemBufferIterator
	GetKeyByHandle(MemKeyHandle) []byte
	GetValueByHandle(MemKeyHandle) ([]byte, bool)
}

// MemBufferIterator is an Iterator with KeyFlags related functions.
type MemBufferIterator interface {
	tidbkv.Iterator
	HasValue() bool
	Flags() kv.KeyFlags
	UpdateFlags(...kv.FlagsOp)
	Handle() MemKeyHandle
}
