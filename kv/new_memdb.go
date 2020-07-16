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

package kv

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"unsafe"
)

const (
	flagPresumeKNE NewKeyFlags = 1 << iota
	flagPessimisticLock

	persistentFlags = flagPessimisticLock
	// bit 1 => red, bit 0 => black
	nodeColorBit  uint8 = 0x80
	nodeFlagsMask       = ^nodeColorBit
)

// NewKeyFlags are metadata associated with key
type NewKeyFlags uint8

// HasPresumeKeyNotExists retruns whether the associated key use lazy check.
func (f NewKeyFlags) HasPresumeKeyNotExists() bool {
	return f&flagPresumeKNE != 0
}

// HasPessimisticLock retruns whether the associated key has acquired pessimistic lock.
func (f NewKeyFlags) HasPessimisticLock() bool {
	return f&flagPessimisticLock != 0
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint8

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	SetPresumeKeyNotExists FlagsOp = 1 << iota
	// DelPresumeKeyNotExists reverts SetPresumeKeyNotExists.
	DelPresumeKeyNotExists
	// SetPessimisticLock marks the associated key has acquired pessimistic lock.
	SetPessimisticLock
	// DelPessimisticLock reverts SetPessimisticLock.
	DelPessimisticLock
)

func applyFlagsOps(origin NewKeyFlags, ops ...FlagsOp) NewKeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE
		case DelPresumeKeyNotExists:
			origin &= ^flagPresumeKNE
		case SetPessimisticLock:
			origin |= flagPessimisticLock
		case DelPessimisticLock:
			origin &= ^flagPessimisticLock
		}
	}
	return origin
}

var tombstone = []byte{}

// memdb is rollbackable Red-Black Tree optimized for TiDB's transaction states buffer use scenario.
// You can think memdb is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discading a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type memdb struct {
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

func newNewMemDB() *memdb {
	db := new(memdb)
	db.allocator.init()
	db.root = nullAddr
	db.stages = make([]memdbCheckpoint, 0, 2)
	db.entrySizeLimit = atomic.LoadUint64(&TxnEntrySizeLimit)
	db.bufferSizeLimit = atomic.LoadUint64(&TxnTotalSizeLimit)
	return db
}

func (db *memdb) Staging() StagingHandle {
	db.stages = append(db.stages, db.vlog.checkpoint())
	return StagingHandle(len(db.stages))
}

func (db *memdb) Release(h StagingHandle) {
	if int(h) != len(db.stages) {
		// This should never happens in production environmen.
		// Use panic to make debug easier.
		panic("cannot release staging buffer")
	}
	if int(h) == 1 {
		tail := db.vlog.checkpoint()
		if !db.stages[0].isSamePosition(&tail) {
			db.dirty = true
		}
	}
	db.stages = db.stages[:int(h)-1]
}

func (db *memdb) Cleanup(h StagingHandle) {
	if int(h) > len(db.stages) {
		return
	}
	if int(h) < len(db.stages) {
		// This should never happens in production environmen.
		// Use panic to make debug easier.
		panic("cannot cleanup staging buffer")
	}

	cp := &db.stages[int(h)-1]
	if !db.vlogInvalid {
		db.vlog.revertToCheckpoint(db, cp)
		db.vlog.truncate(cp)
	}
	db.stages = db.stages[:int(h)-1]
}

func (db *memdb) Reset() {
	db.root = nullAddr
	db.stages = db.stages[:0]
	db.dirty = false
	db.vlogInvalid = false
	db.size = 0
	db.count = 0
	db.vlog.reset()
	db.allocator.reset()
}

func (db *memdb) DiscardValues() {
	db.vlogInvalid = true
	db.vlog.reset()
}

func (db *memdb) InspectStage(handle StagingHandle, f func(Key, NewKeyFlags, []byte)) {
	idx := int(handle) - 1
	tail := db.vlog.checkpoint()
	head := db.stages[idx]
	db.vlog.inspectKVInLog(db, &head, &tail, f)
}

func (db *memdb) Get(_ context.Context, key Key) ([]byte, error) {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	_, xn := db.tranverse(key, false)
	if xn == nil {
		return nil, ErrNotExist
	}
	if xn.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, ErrNotExist
	}
	return db.vlog.getValue(xn.vptr), nil
}

func (db *memdb) GetFlags(key Key) (NewKeyFlags, error) {
	_, xn := db.tranverse(key, false)
	if xn == nil {
		return 0, ErrNotExist
	}
	return xn.getKeyFlags(), nil
}

func (db *memdb) UpdateFlags(key Key, ops ...FlagsOp) {
	err := db.set(key, nil, ops...)
	_ = err // set without value will never fail
}

func (db *memdb) Set(key Key, value []byte) error {
	if len(value) == 0 {
		return ErrCannotSetNilValue
	}
	return db.set(key, value)
}

func (db *memdb) SetWithFlags(key Key, value []byte, ops ...FlagsOp) error {
	if len(value) == 0 {
		return ErrCannotSetNilValue
	}
	return db.set(key, value, ops...)
}

func (db *memdb) Delete(key Key) error {
	return db.set(key, tombstone)
}

func (db *memdb) Len() int {
	return db.count
}

func (db *memdb) Size() int {
	return db.size
}

func (db *memdb) Dirty() bool {
	return db.dirty
}

func (db *memdb) set(key Key, value []byte, ops ...FlagsOp) error {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	if value != nil {
		if size := uint64(len(key) + len(value)); size > db.entrySizeLimit {
			return ErrEntryTooLarge.GenWithStackByArgs(db.entrySizeLimit, size)
		}
	}
	if len(db.stages) == 0 {
		db.dirty = true
	}
	x, xn := db.tranverse(key, true)
	if xn.vptr.isNull() && value != nil {
		db.size += len(key)
		db.count++
	}

	if len(ops) != 0 {
		flags := applyFlagsOps(xn.getKeyFlags(), ops...)
		xn.setKeyFlags(flags)
	}

	if value == nil {
		return nil
	}

	db.setValue(x, xn, value)
	if uint64(db.Size()) > db.bufferSizeLimit {
		return ErrTxnTooLarge.GenWithStackByArgs(db.Size())
	}
	return nil
}

func (db *memdb) setValue(x memdbArenaAddr, xn *memdbNode, value []byte) {
	var activeCp *memdbCheckpoint
	if len(db.stages) > 0 {
		activeCp = &db.stages[len(db.stages)-1]
	}

	var oldVal []byte
	if !xn.vptr.isNull() {
		oldVal = db.vlog.getValue(xn.vptr)
	}

	if len(oldVal) > 0 && db.vlog.canModify(activeCp, xn.vptr) {
		// For easier to implement, we only consider this case.
		// It is the most common usage in TiDB's transaction buffers.
		if len(oldVal) == len(value) {
			copy(oldVal, value)
			return
		}
	}
	xn.vptr = db.vlog.appendValue(x, xn.vptr, value)
	db.size = db.size - len(oldVal) + len(value)
}

// tranverse search for and if not found and insert is true, will add a new node in.
// Returns a pointer to the new node, or the node found.
func (db *memdb) tranverse(key Key, insert bool) (memdbArenaAddr, *memdbNode) {
	var (
		x      = db.root
		y      = nullAddr
		xn, yn *memdbNode
		found  = false
	)

	// walk x down the tree
	for !x.isNull() && !found {
		y = x
		xn = db.allocator.getNode(x)
		cmp := bytes.Compare(key, xn.getKey())
		if cmp < 0 {
			x = xn.left
		} else if cmp > 0 {
			x = xn.right
		} else {
			found = true
		}
	}

	if found || !insert {
		if x.isNull() {
			xn = nil
		}
		return x, xn
	}

	z, zn := db.allocator.allocNode(key)
	yn = db.allocator.getNode(y)
	zn.up = y

	if y.isNull() {
		db.root = z
	} else {
		cmp := bytes.Compare(zn.getKey(), yn.getKey())
		if cmp < 0 {
			yn.left = z
		} else {
			yn.right = z
		}
	}

	zn.left = nullAddr
	zn.right = nullAddr

	// colour this new node red
	zn.setRed()

	// Having added a red node, we must now walk back up the tree balancing it,
	// by a series of rotations and changing of colours
	x = z
	xn = zn

	a := &db.allocator

	// While we are not at the top and our parent node is red
	// NOTE: Since the root node is guaranteed black, then we
	// are also going to stop if we are the child of the root

	for x != db.root {
		xupn := a.getNode(xn.up)
		if xupn.isBlack() {
			break
		}

		xgrandupn := a.getNode(xupn.up)
		// if our parent is on the left side of our grandparent
		if xn.up == xgrandupn.left {
			// get the right side of our grandparent (uncle?)
			y = xgrandupn.right
			yn = a.getNode(y)
			if yn.isRed() {
				// make our parent black
				xupn.setBlack()
				// make our uncle black
				yn.setBlack()
				// make our grandparent red
				xgrandupn.setRed()
				// now consider our grandparent
				x = xupn.up
				xn = xgrandupn
			} else {
				// if we are on the right side of our parent
				if x == xupn.right {
					// Move up to our parent
					x = xn.up
					xn = xupn
					db.leftRotate(x, xn)
					xupn = a.getNode(xn.up)
					xgrandupn = a.getNode(xupn.up)
				}

				xupn.setBlack()
				xgrandupn.setRed()
				db.rightRotate(xupn.up, xgrandupn)
			}
		} else {
			// everything here is the same as above, but exchanging left for right
			y = xgrandupn.left
			yn = a.getNode(y)
			if yn.isRed() {
				xupn.setBlack()
				yn.setBlack()
				xgrandupn.setRed()

				x = xupn.up
				xn = xgrandupn
			} else {
				if x == xupn.left {
					x = xn.up
					xn = xupn
					db.rightRotate(x, xn)
					xupn = a.getNode(xn.up)
					xgrandupn = a.getNode(xupn.up)
				}

				xupn.setBlack()
				xgrandupn.setRed()
				db.leftRotate(xupn.up, xgrandupn)
			}
		}
	}

	// Set the root node black
	a.getNode(db.root).setBlack()

	return z, zn
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
// We assume that neither X or Y is NULL
//

func (db *memdb) leftRotate(x memdbArenaAddr, xn *memdbNode) {
	y := xn.right
	yn := db.allocator.getNode(y)

	// Turn Y's left subtree into X's right subtree (move B)
	xn.right = yn.left

	// If B is not null, set it's parent to be X
	if !yn.left.isNull() {
		db.allocator.getNode(yn.left).up = x
	}

	// Set Y's parent to be what X's parent was
	yn.up = xn.up

	// if X was the root
	if xn.up.isNull() {
		db.root = y
	} else {
		xupn := db.allocator.getNode(xn.up)
		// Set X's parent's left or right pointer to be Y
		if x == xupn.left {
			xupn.left = y
		} else {
			xupn.right = y
		}
	}

	// Put X on Y's left
	yn.left = x
	// Set X's parent to be Y
	xn.up = y
}

func (db *memdb) rightRotate(y memdbArenaAddr, yn *memdbNode) {
	x := yn.left
	xn := db.allocator.getNode(x)

	// Turn X's right subtree into Y's left subtree (move B)
	yn.left = xn.right

	// If B is not null, set it's parent to be Y
	if !xn.right.isNull() {
		db.allocator.getNode(xn.right).up = y
	}

	// Set X's parent to be what Y's parent was
	xn.up = yn.up

	// if Y was the root
	if yn.up.isNull() {
		db.root = x
	} else {
		yupn := db.allocator.getNode(yn.up)
		// Set Y's parent's left or right pointer to be X
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	}

	// Put Y on X's right
	xn.right = y
	// Set Y's parent to be X
	yn.up = x
}

func (db *memdb) deleteNode(z memdbArenaAddr, zn *memdbNode) {
	var (
		x, y   memdbArenaAddr
		xn, yn *memdbNode
	)

	if zn.left.isNull() || zn.right.isNull() {
		y = z
		yn = zn
	} else {
		y, yn = db.successor(z, zn)
	}

	if !yn.left.isNull() {
		x = yn.left
		xn = db.allocator.getNode(x)
	} else {
		x = yn.right
		xn = db.allocator.getNode(x)
	}

	// NOTE: traditional red-black tree will copy key from Y to Z and free Y.
	// We cannot do the same thing here, due to Y's pointer is stored in vlog and the space in Z may not suitable for Y.
	// So we need to copy states from Z to Y, and relink all nodes formerly connected to Z.
	//
	// There is a special case, consider we choose Z's right child as Y, and Y has no children, X will point to the dummy node.
	// We need to set X's up to Y instead of Z, otherwise, the X will hold a dangling pointer to Z which will be freed later.
	// We cannot handle this special case in `replaceNode`, due to the address of the dummy node is nullAddr
	//
	//       Z
	//     /   \
	//    A     Y
	//
	if yn.up == z {
		xn.up = y
	} else {
		xn.up = yn.up
	}

	if yn.up.isNull() {
		db.root = x
	} else {
		yupn := db.allocator.getNode(yn.up)
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	}

	needFix := yn.isBlack()

	if y != z {
		db.replaceNode(y, yn, z, zn)
	}

	if needFix {
		db.deleteNodeFix(x, xn)
	}

	db.allocator.freeNode(z)
}

func (db *memdb) replaceNode(x memdbArenaAddr, xn *memdbNode, y memdbArenaAddr, yn *memdbNode) {
	if !yn.up.isNull() {
		yupn := db.allocator.getNode(yn.up)
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	} else {
		db.root = x
	}
	xn.up = yn.up

	if !yn.left.isNull() {
		db.allocator.getNode(yn.left).up = x
	}
	xn.left = yn.left

	if !yn.right.isNull() {
		db.allocator.getNode(yn.right).up = x
	}
	xn.right = yn.right

	if yn.isBlack() {
		xn.setBlack()
	} else {
		xn.setRed()
	}
}

func (db *memdb) deleteNodeFix(x memdbArenaAddr, xn *memdbNode) {
	a := &db.allocator
	for x != db.root && xn.isBlack() {
		xupn := a.getNode(xn.up)
		if x == xupn.left {
			w := xupn.right
			wn := a.getNode(w)
			if wn.isRed() {
				wn.setBlack()
				xupn.setRed()
				db.leftRotate(xn.up, xupn)
				w = a.getNode(xn.up).right
				wn = a.getNode(w)
			}

			if a.getNode(wn.left).isBlack() && a.getNode(wn.right).isBlack() {
				wn.setRed()
				x = xn.up
				xn = a.getNode(x)
			} else {
				if a.getNode(wn.right).isBlack() {
					a.getNode(wn.left).setBlack()
					wn.setRed()
					db.rightRotate(w, wn)
					w = a.getNode(xn.up).right
					wn = a.getNode(w)
				}

				xupn := a.getNode(xn.up)
				if xupn.isBlack() {
					wn.setBlack()
				} else {
					wn.setRed()
				}
				xupn.setBlack()
				a.getNode(wn.right).setBlack()
				db.leftRotate(xn.up, xupn)
				x = db.root
				xn = a.getNode(x)
			}
		} else {
			w := xupn.left
			wn := a.getNode(w)
			if wn.isRed() {
				wn.setBlack()
				xupn.setRed()
				db.rightRotate(xn.up, xupn)
				w = a.getNode(xn.up).left
				wn = a.getNode(w)
			}

			if a.getNode(wn.right).isBlack() && a.getNode(wn.left).isBlack() {
				wn.setRed()
				x = xn.up
				xn = a.getNode(x)
			} else {
				if a.getNode(wn.left).isBlack() {
					a.getNode(wn.right).setBlack()
					wn.setRed()
					db.leftRotate(w, wn)
					w = a.getNode(xn.up).left
					wn = a.getNode(w)
				}

				xupn := a.getNode(xn.up)
				if xupn.isBlack() {
					wn.setBlack()
				} else {
					wn.setRed()
				}
				xupn.setBlack()
				a.getNode(wn.left).setBlack()
				db.rightRotate(xn.up, xupn)
				x = db.root
				xn = a.getNode(x)
			}
		}
	}
	xn.setBlack()
}

func (db *memdb) successor(x memdbArenaAddr, xn *memdbNode) (y memdbArenaAddr, yn *memdbNode) {
	if !xn.right.isNull() {
		// If right is not NULL then go right one and
		// then keep going left until we find a node with
		// no left pointer.

		y = xn.right
		yn = db.allocator.getNode(y)
		for !yn.left.isNull() {
			y = yn.left
			yn = db.allocator.getNode(y)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// left of its parent (or the root) and then return the
	// parent.

	y = xn.up
	for !y.isNull() {
		yn = db.allocator.getNode(y)
		if x != yn.right {
			return y, yn
		}
		x = y
		y = yn.up
	}
	return nullAddr, nil
}

func (db *memdb) predecessor(x memdbArenaAddr, xn *memdbNode) (y memdbArenaAddr, yn *memdbNode) {
	if !xn.left.isNull() {
		// If left is not NULL then go left one and
		// then keep going right until we find a node with
		// no right pointer.

		y = xn.left
		yn = db.allocator.getNode(y)
		for !yn.right.isNull() {
			y = yn.right
			yn = db.allocator.getNode(y)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// right of its parent (or the root) and then return the
	// parent.

	y = xn.up
	for !y.isNull() {
		yn = db.allocator.getNode(y)
		if x != yn.left {
			return y, yn
		}
		x = y
		y = yn.up
	}
	return nullAddr, nil
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

func (n *memdbNode) getKey() Key {
	var ret []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	hdr.Data = uintptr(unsafe.Pointer(&n.flags)) + 1
	hdr.Len = int(n.klen)
	hdr.Cap = int(n.klen)
	return ret
}

func (n *memdbNode) getKeyFlags() NewKeyFlags {
	return NewKeyFlags(n.flags & nodeFlagsMask)
}

func (n *memdbNode) setKeyFlags(f NewKeyFlags) {
	n.flags = (^nodeFlagsMask & n.flags) | uint8(f)
}
