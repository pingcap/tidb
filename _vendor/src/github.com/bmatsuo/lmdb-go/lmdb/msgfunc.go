package lmdb

/*
#include "lmdbgo.h"
*/
import "C"
import (
	"sync"
	"sync/atomic"
)

// lmdbgoMDBMsgFuncBridge provides a static C function for handling MDB_msgfunc
// callbacks.  It performs string conversion and dynamic dispatch to a msgfunc
// provided to Env.ReaderList.  Any error returned by the msgfunc is cached and
// -1 is returned to terminate the iteration.

//export lmdbgoMDBMsgFuncBridge
func lmdbgoMDBMsgFuncBridge(cmsg C.lmdbgo_ConstCString, _ctx C.size_t) C.int {
	ctx := msgctx(_ctx).get()
	msg := C.GoString(cmsg.p)
	err := ctx.fn(msg)
	if err != nil {
		ctx.err = err
		return -1
	}
	return 0
}

type msgfunc func(string) error

// msgctx is the type used for context pointers passed to mdb_reader_list.  A
// msgctx stores its corresponding msgfunc, and any error encountered in an
// external map.  The corresponding function is called once for each
// mdb_reader_list entry using the msgctx.
//
// An External map is used because struct pointers passed to C functions must
// not contain pointers in their struct fields.  See the following language
// proposal which discusses the restrictions on passing pointers to C.
//
//		https://github.com/golang/proposal/blob/master/design/12416-cgo-pointers.md
type msgctx uintptr
type _msgctx struct {
	fn  msgfunc
	err error
}

var msgctxn uint32
var msgctxm = map[msgctx]*_msgctx{}
var msgctxmlock sync.RWMutex

func nextctx() msgctx {
	return msgctx(atomic.AddUint32(&msgctxn, 1))
}

func newMsgFunc(fn msgfunc) (ctx msgctx, done func()) {
	ctx = nextctx()
	ctx.register(fn)
	return ctx, ctx.deregister
}

func (ctx msgctx) register(fn msgfunc) {
	msgctxmlock.Lock()
	if _, ok := msgctxm[ctx]; ok {
		msgctxmlock.Unlock()
		panic("msgfunc conflict")
	}
	msgctxm[ctx] = &_msgctx{fn: fn}
	msgctxmlock.Unlock()
}

func (ctx msgctx) deregister() {
	msgctxmlock.Lock()
	delete(msgctxm, ctx)
	msgctxmlock.Unlock()
}

func (ctx msgctx) get() *_msgctx {
	msgctxmlock.RLock()
	_ctx := msgctxm[ctx]
	msgctxmlock.RUnlock()
	return _ctx
}
