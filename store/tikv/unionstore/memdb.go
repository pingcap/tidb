// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package unionstore

import (
	"bytes"
	"math"
	"reflect"
	"sync"
	"unsafe"

	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
)

var tombstone = []byte{}

// IsTombstone returns whether the value is a tombstone.
func IsTombstone(val []byte) bool { return len(val) == 0 }

// MemKeyHandle represents a pointer for key in MemBuffer.
type MemKeyHandle struct {
	// Opaque user data
	UserData uint16
	idx      uint16
	off      uint32
}

func (h MemKeyHandle) toAddr() memdbArenaAddr {
	return memdbArenaAddr{idx: uint32(h.idx), off: h.off}
}

// MemDB is rollbackable Red-Black Tree optimized for TiDB's transaction states buffer use scenario.
// You can think MemDB is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type MemDB struct {
	// This RWMutex only used to ensure memdbSnapGetter.Get will not race with
	// concurrent memdb.Set, memdb.SetWithFlags, memdb.Delete and memdb.UpdateFlags.
	sync.RWMutex
	root      memdbArenaAddr
	allocator nodeAllocator
	vlog      memdbVlog

	entrySizeLimit  uint64
	bufferSizeLimit uint64
	count           int
	size            int

	vlogInvalid bool
	dirty       bool
	stages      []memdbCheckpoint
}

func newMemDB() *MemDB {
	db := new(MemDB)
	db.allocator.init()
	db.root = nullAddr
	db.stages = make([]memdbCheckpoint, 0, 2)
	db.entrySizeLimit = math.MaxUint64
	db.bufferSizeLimit = math.MaxUint64
	return db
}

// Staging create a new staging buffer inside the MemBuffer.
// Subsequent writes will be temporarily stored in this new staging buffer.
// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
func (db *MemDB) Staging() int {
	db.Lock()
	defer db.Unlock()

	db.stages = append(db.stages, db.vlog.checkpoint())
	return len(db.stages)
}

// Release publish all modifications in the latest staging buffer to upper level.
func (db *MemDB) Release(h int) {
	if h != len(db.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic("cannot release staging buffer")
	}

	db.Lock()
	defer db.Unlock()
	if h == 1 {
		tail := db.vlog.checkpoint()
		if !db.stages[0].isSamePosition(&tail) {
			db.dirty = true
		}
	}
	db.stages = db.stages[:h-1]
}

// Cleanup cleanup the resources referenced by the StagingHandle.
// If the changes are not published by `Release`, they will be discarded.
func (db *MemDB) Cleanup(h int) {
	if h > len(db.stages) {
		return
	}
	if h < len(db.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic("cannot cleanup staging buffer")
	}

	db.Lock()
	defer db.Unlock()
	cp := &db.stages[h-1]
	if !db.vlogInvalid {
		curr := db.vlog.checkpoint()
		if !curr.isSamePosition(cp) {
			db.vlog.revertToCheckpoint(db, cp)
			db.vlog.truncate(cp)
		}
	}
	db.stages = db.stages[:h-1]
}

// Reset resets the MemBuffer to initial states.
func (db *MemDB) Reset() {
	db.root = nullAddr
	db.stages = db.stages[:0]
	db.dirty = false
	db.vlogInvalid = false
	db.size = 0
	db.count = 0
	db.vlog.reset()
	db.allocator.reset()
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (db *MemDB) DiscardValues() {
	db.vlogInvalid = true
	db.vlog.reset()
}

// InspectStage used to inspect the value updates in the given stage.
func (db *MemDB) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	idx := handle - 1
	tail := db.vlog.checkpoint()
	head := db.stages[idx]
	db.vlog.inspectKVInLog(db, &head, &tail, f)
}

// Get gets the value for key k from kv store.
// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
func (db *MemDB) Get(key []byte) ([]byte, error) {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	x := db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	return db.vlog.getValue(x.vptr), nil
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (db *MemDB) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	x := db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	result := db.vlog.selectValueHistory(x.vptr, func(addr memdbArenaAddr) bool {
		return predicate(db.vlog.getValue(addr))
	})
	if result.isNull() {
		return nil, nil
	}
	return db.vlog.getValue(result), nil
}

// GetFlags returns the latest flags associated with key.
func (db *MemDB) GetFlags(key []byte) (kv.KeyFlags, error) {
	x := db.traverse(key, false)
	if x.isNull() {
		return 0, tikverr.ErrNotExist
	}
	return x.getKeyFlags(), nil
}

// UpdateFlags update the flags associated with key.
func (db *MemDB) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	err := db.set(key, nil, ops...)
	_ = err // set without value will never fail
}

// Set sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (db *MemDB) Set(key []byte, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (db *MemDB) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value, ops...)
}

// Delete removes the entry for key k from kv store.
func (db *MemDB) Delete(key []byte) error {
	return db.set(key, tombstone)
}

// DeleteWithFlags delete key with the given KeyFlags
func (db *MemDB) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return db.set(key, tombstone, ops...)
}

// GetKeyByHandle returns key by handle.
func (db *MemDB) GetKeyByHandle(handle MemKeyHandle) []byte {
	x := db.getNode(handle.toAddr())
	return x.getKey()
}

// GetValueByHandle returns value by handle.
func (db *MemDB) GetValueByHandle(handle MemKeyHandle) ([]byte, bool) {
	if db.vlogInvalid {
		return nil, false
	}
	x := db.getNode(handle.toAddr())
	if x.vptr.isNull() {
		return nil, false
	}
	return db.vlog.getValue(x.vptr), true
}

// Len returns the number of entries in the DB.
func (db *MemDB) Len() int {
	return db.count
}

// Size returns sum of keys and values length.
func (db *MemDB) Size() int {
	return db.size
}

// Dirty returns whether the root staging buffer is updated.
func (db *MemDB) Dirty() bool {
	return db.dirty
}

func (db *MemDB) set(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	if value != nil {
		if size := uint64(len(key) + len(value)); size > db.entrySizeLimit {
			return &tikverr.ErrEntryTooLarge{
				Limit: db.entrySizeLimit,
				Size:  size,
			}
		}
	}

	db.Lock()
	defer db.Unlock()

	if len(db.stages) == 0 {
		db.dirty = true
	}
	x := db.traverse(key, true)

	if len(ops) != 0 {
		flags := kv.ApplyFlagsOps(x.getKeyFlags(), ops...)
		if flags.AndPersistent() != 0 {
			db.dirty = true
		}
		x.setKeyFlags(flags)
	}

	if value == nil {
		return nil
	}

	db.setValue(x, value)
	if uint64(db.Size()) > db.bufferSizeLimit {
		return &tikverr.ErrTxnTooLarge{Size: db.Size()}
	}
	return nil
}

func (db *MemDB) setValue(x memdbNodeAddr, value []byte) {
	var activeCp *memdbCheckpoint
	if len(db.stages) > 0 {
		activeCp = &db.stages[len(db.stages)-1]
	}

	var oldVal []byte
	if !x.vptr.isNull() {
		oldVal = db.vlog.getValue(x.vptr)
	}

	if len(oldVal) > 0 && db.vlog.canModify(activeCp, x.vptr) {
		// For easier to implement, we only consider this case.
		// It is the most common usage in TiDB's transaction buffers.
		if len(oldVal) == len(value) {
			copy(oldVal, value)
			return
		}
	}
	x.vptr = db.vlog.appendValue(x.addr, x.vptr, value)
	db.size = db.size - len(oldVal) + len(value)
}

// traverse search for and if not found and insert is true, will add a new node in.
// Returns a pointer to the new node, or the node found.
func (db *MemDB) traverse(key []byte, insert bool) memdbNodeAddr {
	x := db.getRoot()
	y := memdbNodeAddr{nil, nullAddr}
	found := false

	// walk x down the tree
	for !x.isNull() && !found {
		y = x
		cmp := bytes.Compare(key, x.getKey())
		if cmp < 0 {
			x = x.getLeft(db)
		} else if cmp > 0 {
			x = x.getRight(db)
		} else {
			found = true
		}
	}

	if found || !insert {
		return x
	}

	z := db.allocNode(key)
	z.up = y.addr

	if y.isNull() {
		db.root = z.addr
	} else {
		cmp := bytes.Compare(z.getKey(), y.getKey())
		if cmp < 0 {
			y.left = z.addr
		} else {
			y.right = z.addr
		}
	}

	z.left = nullAddr
	z.right = nullAddr

	// colour this new node red
	z.setRed()

	// Having added a red node, we must now walk back up the tree balancing it,
	// by a series of rotations and changing of colours
	x = z

	// While we are not at the top and our parent node is red
	// NOTE: Since the root node is guaranteed black, then we
	// are also going to stop if we are the child of the root

	for x.addr != db.root {
		xUp := x.getUp(db)
		if xUp.isBlack() {
			break
		}

		xUpUp := xUp.getUp(db)
		// if our parent is on the left side of our grandparent
		if x.up == xUpUp.left {
			// get the right side of our grandparent (uncle?)
			y = xUpUp.getRight(db)
			if y.isRed() {
				// make our parent black
				xUp.setBlack()
				// make our uncle black
				y.setBlack()
				// make our grandparent red
				xUpUp.setRed()
				// now consider our grandparent
				x = xUp.getUp(db)
			} else {
				// if we are on the right side of our parent
				if x.addr == xUp.right {
					// Move up to our parent
					x = x.getUp(db)
					db.leftRotate(x)
					xUp = x.getUp(db)
					xUpUp = xUp.getUp(db)
				}

				xUp.setBlack()
				xUpUp.setRed()
				db.rightRotate(xUpUp)
			}
		} else {
			// everything here is the same as above, but exchanging left for right
			y = xUpUp.getLeft(db)
			if y.isRed() {
				xUp.setBlack()
				y.setBlack()
				xUpUp.setRed()

				x = xUp.getUp(db)
			} else {
				if x.addr == xUp.left {
					x = x.getUp(db)
					db.rightRotate(x)
					xUp = x.getUp(db)
					xUpUp = xUp.getUp(db)
				}

				xUp.setBlack()
				xUpUp.setRed()
				db.leftRotate(xUpUp)
			}
		}
	}

	// Set the root node black
	db.getRoot().setBlack()

	return z
}

//
// Rotate our tree thus:-
//
//             X        leftRotate(X)--->           Y
//           /   \                                /   \
//          A     Y     <---rightRotate(Y)       X     C
//              /   \                          /   \
//             B     C                        A     B
//
// NOTE: This does not change the ordering.
//
// We assume that neither X nor Y is NULL
//

func (db *MemDB) leftRotate(x memdbNodeAddr) {
	y := x.getRight(db)

	// Turn Y's left subtree into X's right subtree (move B)
	x.right = y.left

	// If B is not null, set it's parent to be X
	if !y.left.isNull() {
		left := y.getLeft(db)
		left.up = x.addr
	}

	// Set Y's parent to be what X's parent was
	y.up = x.up

	// if X was the root
	if x.up.isNull() {
		db.root = y.addr
	} else {
		xUp := x.getUp(db)
		// Set X's parent's left or right pointer to be Y
		if x.addr == xUp.left {
			xUp.left = y.addr
		} else {
			xUp.right = y.addr
		}
	}

	// Put X on Y's left
	y.left = x.addr
	// Set X's parent to be Y
	x.up = y.addr
}

func (db *MemDB) rightRotate(y memdbNodeAddr) {
	x := y.getLeft(db)

	// Turn X's right subtree into Y's left subtree (move B)
	y.left = x.right

	// If B is not null, set it's parent to be Y
	if !x.right.isNull() {
		right := x.getRight(db)
		right.up = y.addr
	}

	// Set X's parent to be what Y's parent was
	x.up = y.up

	// if Y was the root
	if y.up.isNull() {
		db.root = x.addr
	} else {
		yUp := y.getUp(db)
		// Set Y's parent's left or right pointer to be X
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	// Put Y on X's right
	x.right = y.addr
	// Set Y's parent to be X
	y.up = x.addr
}

func (db *MemDB) deleteNode(z memdbNodeAddr) {
	var x, y memdbNodeAddr

	db.count--
	db.size -= int(z.klen)

	if z.left.isNull() || z.right.isNull() {
		y = z
	} else {
		y = db.successor(z)
	}

	if !y.left.isNull() {
		x = y.getLeft(db)
	} else {
		x = y.getRight(db)
	}
	x.up = y.up

	if y.up.isNull() {
		db.root = x.addr
	} else {
		yUp := y.getUp(db)
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	needFix := y.isBlack()

	// NOTE: traditional red-black tree will copy key from Y to Z and free Y.
	// We cannot do the same thing here, due to Y's pointer is stored in vlog and the space in Z may not suitable for Y.
	// So we need to copy states from Z to Y, and relink all nodes formerly connected to Z.
	if y != z {
		db.replaceNode(z, y)
	}

	if needFix {
		db.deleteNodeFix(x)
	}

	db.allocator.freeNode(z.addr)
}

func (db *MemDB) replaceNode(old memdbNodeAddr, new memdbNodeAddr) {
	if !old.up.isNull() {
		oldUp := old.getUp(db)
		if old.addr == oldUp.left {
			oldUp.left = new.addr
		} else {
			oldUp.right = new.addr
		}
	} else {
		db.root = new.addr
	}
	new.up = old.up

	left := old.getLeft(db)
	left.up = new.addr
	new.left = old.left

	right := old.getRight(db)
	right.up = new.addr
	new.right = old.right

	if old.isBlack() {
		new.setBlack()
	} else {
		new.setRed()
	}
}

func (db *MemDB) deleteNodeFix(x memdbNodeAddr) {
	for x.addr != db.root && x.isBlack() {
		xUp := x.getUp(db)
		if x.addr == xUp.left {
			w := xUp.getRight(db)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				db.leftRotate(xUp)
				w = x.getUp(db).getRight(db)
			}

			if w.getLeft(db).isBlack() && w.getRight(db).isBlack() {
				w.setRed()
				x = x.getUp(db)
			} else {
				if w.getRight(db).isBlack() {
					w.getLeft(db).setBlack()
					w.setRed()
					db.rightRotate(w)
					w = x.getUp(db).getRight(db)
				}

				xUp := x.getUp(db)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getRight(db).setBlack()
				db.leftRotate(xUp)
				x = db.getRoot()
			}
		} else {
			w := xUp.getLeft(db)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				db.rightRotate(xUp)
				w = x.getUp(db).getLeft(db)
			}

			if w.getRight(db).isBlack() && w.getLeft(db).isBlack() {
				w.setRed()
				x = x.getUp(db)
			} else {
				if w.getLeft(db).isBlack() {
					w.getRight(db).setBlack()
					w.setRed()
					db.leftRotate(w)
					w = x.getUp(db).getLeft(db)
				}

				xUp := x.getUp(db)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getLeft(db).setBlack()
				db.rightRotate(xUp)
				x = db.getRoot()
			}
		}
	}
	x.setBlack()
}

func (db *MemDB) successor(x memdbNodeAddr) (y memdbNodeAddr) {
	if !x.right.isNull() {
		// If right is not NULL then go right one and
		// then keep going left until we find a node with
		// no left pointer.

		y = x.getRight(db)
		for !y.left.isNull() {
			y = y.getLeft(db)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// left of its parent (or the root) and then return the
	// parent.

	y = x.getUp(db)
	for !y.isNull() && x.addr == y.right {
		x = y
		y = y.getUp(db)
	}
	return y
}

func (db *MemDB) predecessor(x memdbNodeAddr) (y memdbNodeAddr) {
	if !x.left.isNull() {
		// If left is not NULL then go left one and
		// then keep going right until we find a node with
		// no right pointer.

		y = x.getLeft(db)
		for !y.right.isNull() {
			y = y.getRight(db)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// right of its parent (or the root) and then return the
	// parent.

	y = x.getUp(db)
	for !y.isNull() && x.addr == y.left {
		x = y
		y = y.getUp(db)
	}
	return y
}

func (db *MemDB) getNode(x memdbArenaAddr) memdbNodeAddr {
	return memdbNodeAddr{db.allocator.getNode(x), x}
}

func (db *MemDB) getRoot() memdbNodeAddr {
	return db.getNode(db.root)
}

func (db *MemDB) allocNode(key []byte) memdbNodeAddr {
	db.size += len(key)
	db.count++
	x, xn := db.allocator.allocNode(key)
	return memdbNodeAddr{xn, x}
}

type memdbNodeAddr struct {
	*memdbNode
	addr memdbArenaAddr
}

func (a *memdbNodeAddr) isNull() bool {
	return a.addr.isNull()
}

func (a memdbNodeAddr) getUp(db *MemDB) memdbNodeAddr {
	return db.getNode(a.up)
}

func (a memdbNodeAddr) getLeft(db *MemDB) memdbNodeAddr {
	return db.getNode(a.left)
}

func (a memdbNodeAddr) getRight(db *MemDB) memdbNodeAddr {
	return db.getNode(a.right)
}

type memdbNode struct {
	up    memdbArenaAddr
	left  memdbArenaAddr
	right memdbArenaAddr
	vptr  memdbArenaAddr
	klen  uint16
	flags uint8
}

func (n *memdbNode) isRed() bool {
	return n.flags&nodeColorBit != 0
}

func (n *memdbNode) isBlack() bool {
	return !n.isRed()
}

func (n *memdbNode) setRed() {
	n.flags |= nodeColorBit
}

func (n *memdbNode) setBlack() {
	n.flags &= ^nodeColorBit
}

func (n *memdbNode) getKey() []byte {
	var ret []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	hdr.Data = uintptr(unsafe.Pointer(&n.flags)) + 1
	hdr.Len = int(n.klen)
	hdr.Cap = int(n.klen)
	return ret
}

const (
	// bit 1 => red, bit 0 => black
	nodeColorBit  uint8 = 0x80
	nodeFlagsMask       = ^nodeColorBit
)

func (n *memdbNode) getKeyFlags() kv.KeyFlags {
	return kv.KeyFlags(n.flags & nodeFlagsMask)
}

func (n *memdbNode) setKeyFlags(f kv.KeyFlags) {
	n.flags = (^nodeFlagsMask & n.flags) | uint8(f)
}
