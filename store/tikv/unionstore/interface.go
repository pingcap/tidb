package unionstore

import (
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/kv"
)

// MemBufferIterator is an Iterator with KeyFlags related functions.
type MemBufferIterator interface {
	tidbkv.Iterator
	HasValue() bool
	Flags() kv.KeyFlags
	UpdateFlags(...kv.FlagsOp)
	Handle() MemKeyHandle
}
