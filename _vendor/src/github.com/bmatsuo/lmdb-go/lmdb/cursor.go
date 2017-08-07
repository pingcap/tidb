package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
#include "lmdbgo.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// These flags are used exclusively for Cursor.Get.
const (
	// Flags for Cursor.Get
	//
	// See MDB_cursor_op.

	First        = C.MDB_FIRST          // The first item.
	FirstDup     = C.MDB_FIRST_DUP      // The first value of current key (DupSort).
	GetBoth      = C.MDB_GET_BOTH       // Get the key as well as the value (DupSort).
	GetBothRange = C.MDB_GET_BOTH_RANGE // Get the key and the nearsest value (DupSort).
	GetCurrent   = C.MDB_GET_CURRENT    // Get the key and value at the current position.
	GetMultiple  = C.MDB_GET_MULTIPLE   // Get up to a page dup values for key at current position (DupFixed).
	Last         = C.MDB_LAST           // Last item.
	LastDup      = C.MDB_LAST_DUP       // Position at last value of current key (DupSort).
	Next         = C.MDB_NEXT           // Next value.
	NextDup      = C.MDB_NEXT_DUP       // Next value of the current key (DupSort).
	NextMultiple = C.MDB_NEXT_MULTIPLE  // Get key and up to a page of values from the next cursor position (DupFixed).
	NextNoDup    = C.MDB_NEXT_NODUP     // The first value of the next key (DupSort).
	Prev         = C.MDB_PREV           // The previous item.
	PrevDup      = C.MDB_PREV_DUP       // The previous item of the current key (DupSort).
	PrevNoDup    = C.MDB_PREV_NODUP     // The last data item of the previous key (DupSort).
	Set          = C.MDB_SET            // The specified key.
	SetKey       = C.MDB_SET_KEY        // Get key and data at the specified key.
	SetRange     = C.MDB_SET_RANGE      // The first key no less than the specified key.
)

// The MDB_MULTIPLE and MDB_RESERVE flags are special and do not fit the
// calling pattern of other calls to Put.  They are not exported because they
// require special methods, PutMultiple and PutReserve in which the flag is
// implied and does not need to be passed.
const (
	// Flags for Txn.Put and Cursor.Put.
	//
	// See mdb_put and mdb_cursor_put.

	Current     = C.MDB_CURRENT     // Replace the item at the current key position (Cursor only)
	NoDupData   = C.MDB_NODUPDATA   // Store the key-value pair only if key is not present (DupSort).
	NoOverwrite = C.MDB_NOOVERWRITE // Store a new key-value pair only if key is not present.
	Append      = C.MDB_APPEND      // Append an item to the database.
	AppendDup   = C.MDB_APPENDDUP   // Append an item to the database (DupSort).
)

// Cursor operates on data inside a transaction and holds a position in the
// database.
//
// See MDB_cursor.
type Cursor struct {
	txn *Txn
	_c  *C.MDB_cursor
}

func openCursor(txn *Txn, db DBI) (*Cursor, error) {
	c := &Cursor{txn: txn}
	ret := C.mdb_cursor_open(txn._txn, C.MDB_dbi(db), &c._c)
	if ret != success {
		return nil, operrno("mdb_cursor_open", ret)
	}
	return c, nil
}

// Renew associates readonly cursor with txn.
//
// See mdb_cursor_renew.
func (c *Cursor) Renew(txn *Txn) error {
	ret := C.mdb_cursor_renew(txn._txn, c._c)
	err := operrno("mdb_cursor_renew", ret)
	if err != nil {
		return err
	}
	c.txn = txn
	return nil
}

func (c *Cursor) close() bool {
	if c._c != nil {
		if c.txn._txn == nil && !c.txn.readonly {
			// the cursor has already been released by LMDB.
		} else {
			C.mdb_cursor_close(c._c)
		}
		c.txn = nil
		c._c = nil
		return true
	}
	return false
}

// Close the cursor handle and clear the finalizer on c.  Cursors belonging to
// write transactions are closed automatically when the transaction is
// terminated.
//
// See mdb_cursor_close.
func (c *Cursor) Close() {
	if c.close() {
		runtime.SetFinalizer(c, nil)
	}
}

// Txn returns the cursor's transaction.
func (c *Cursor) Txn() *Txn {
	return c.txn
}

// DBI returns the cursor's database handle.  If c has been closed than an
// invalid DBI is returned.
func (c *Cursor) DBI() DBI {
	// dbiInvalid is an invalid DBI (the max value for the type).  it shouldn't
	// be possible to create a database handle with value dbiInvalid because
	// the process address space would be exhausted.  it is also impractical to
	// have many open databases in an environment.
	const dbiInvalid = ^DBI(0)

	// mdb_cursor_dbi segfaults when passed a nil value
	if c._c == nil {
		return dbiInvalid
	}
	return DBI(C.mdb_cursor_dbi(c._c))
}

// Get retrieves items from the database. If c.Txn().RawRead is true the slices
// returned by Get reference readonly sections of memory that must not be
// accessed after the transaction has terminated.
//
// In a Txn with RawRead set to true the Set op causes the returned key to
// share its memory with setkey (making it writable memory). In a Txn with
// RawRead set to false the Set op returns key values with memory distinct from
// setkey, as is always the case when using RawRead.
//
// Get ignores setval if setkey is empty.
//
// See mdb_cursor_get.
func (c *Cursor) Get(setkey, setval []byte, op uint) (key, val []byte, err error) {
	switch {
	case len(setkey) == 0:
		err = c.getVal0(op)
	case len(setval) == 0:
		err = c.getVal1(setkey, op)
	default:
		err = c.getVal2(setkey, setval, op)
	}
	if err != nil {
		*c.txn.key = C.MDB_val{}
		*c.txn.val = C.MDB_val{}
		return nil, nil, err
	}

	// When MDB_SET is passed to mdb_cursor_get its first argument will be
	// returned unchanged.  Unfortunately, the normal slice copy/extraction
	// routines will be bad for the Go runtime when operating on Go memory
	// (panic or potentially garbage memory reference).
	if op == Set {
		if c.txn.RawRead {
			key = setkey
		} else {
			p := make([]byte, len(setkey))
			copy(p, setkey)
			key = p
		}
	} else {
		key = c.txn.bytes(c.txn.key)
	}
	val = c.txn.bytes(c.txn.val)

	// Clear transaction storage record storage area for future use and to
	// prevent dangling references.
	*c.txn.key = C.MDB_val{}
	*c.txn.val = C.MDB_val{}

	return key, val, nil
}

// getVal0 retrieves items from the database without using given key or value
// data for reference (Next, First, Last, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal0(op uint) error {
	ret := C.mdb_cursor_get(c._c, c.txn.key, c.txn.val, C.MDB_cursor_op(op))
	return operrno("mdb_cursor_get", ret)
}

// getVal1 retrieves items from the database using key data for reference
// (Set, SetRange, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal1(setkey []byte, op uint) error {
	ret := C.lmdbgo_mdb_cursor_get1(
		c._c,
		(*C.char)(unsafe.Pointer(&setkey[0])), C.size_t(len(setkey)),
		c.txn.key, c.txn.val,
		C.MDB_cursor_op(op),
	)
	return operrno("mdb_cursor_get", ret)
}

// getVal2 retrieves items from the database using key and value data for
// reference (GetBoth, GetBothRange, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal2(setkey, setval []byte, op uint) error {
	ret := C.lmdbgo_mdb_cursor_get2(
		c._c,
		(*C.char)(unsafe.Pointer(&setkey[0])), C.size_t(len(setkey)),
		(*C.char)(unsafe.Pointer(&setval[0])), C.size_t(len(setval)),
		c.txn.key, c.txn.val,
		C.MDB_cursor_op(op),
	)
	return operrno("mdb_cursor_get", ret)
}

func (c *Cursor) putNilKey(flags uint) error {
	ret := C.lmdbgo_mdb_cursor_put2(c._c, nil, 0, nil, 0, C.uint(flags))
	return operrno("mdb_cursor_put", ret)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (c *Cursor) Put(key, val []byte, flags uint) error {
	if len(key) == 0 {
		return c.putNilKey(flags)
	}
	vn := len(val)
	if vn == 0 {
		val = []byte{0}
	}
	ret := C.lmdbgo_mdb_cursor_put2(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val)),
		C.uint(flags),
	)
	return operrno("mdb_cursor_put", ret)
}

// PutReserve returns a []byte of length n that can be written to, potentially
// avoiding a memcopy.  The returned byte slice is only valid in txn's thread,
// before it has terminated.
func (c *Cursor) PutReserve(key []byte, n int, flags uint) ([]byte, error) {
	if len(key) == 0 {
		return nil, c.putNilKey(flags)
	}

	c.txn.val.mv_size = C.size_t(n)
	ret := C.lmdbgo_mdb_cursor_put1(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		c.txn.val,
		C.uint(flags|C.MDB_RESERVE),
	)
	err := operrno("mdb_cursor_put", ret)
	if err != nil {
		*c.txn.val = C.MDB_val{}
		return nil, err
	}
	b := getBytes(c.txn.val)
	*c.txn.val = C.MDB_val{}
	return b, nil
}

// PutMulti stores a set of contiguous items with stride size under key.
// PutMulti panics if len(page) is not a multiple of stride.  The cursor's
// database must be DupFixed and DupSort.
//
// See mdb_cursor_put.
func (c *Cursor) PutMulti(key []byte, page []byte, stride int, flags uint) error {
	if len(key) == 0 {
		return c.putNilKey(flags)
	}
	if len(page) == 0 {
		page = []byte{0}
	}

	vn := WrapMulti(page, stride).Len()
	ret := C.lmdbgo_mdb_cursor_putmulti(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&page[0])), C.size_t(vn), C.size_t(stride),
		C.uint(flags|C.MDB_MULTIPLE),
	)
	return operrno("mdb_cursor_put", ret)
}

// Del deletes the item referred to by the cursor from the database.
//
// See mdb_cursor_del.
func (c *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(c._c, C.uint(flags))
	return operrno("mdb_cursor_del", ret)
}

// Count returns the number of duplicates for the current key.
//
// See mdb_cursor_count.
func (c *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdb_cursor_count(c._c, &_size)
	if ret != success {
		return 0, operrno("mdb_cursor_count", ret)
	}
	return uint64(_size), nil
}
