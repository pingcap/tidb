package refers

import (
	"fmt"
	"sync/atomic"
)

type Refs[T any] struct {
	// refCount is composed of two fields:
	//
	//	[32-bit speculative references]:[32-bit real references]
	//
	// Speculative references are used for TryIncRef, to avoid a CompareAndSwap
	// loop. See IncRef, DecRef and TryIncRef for details of how these fields are
	// used.
	refCount atomic.Int64
}

// InitRefs initializes r with one reference and, if enabled, activates leak
// checking.
func (r *Refs[T]) InitRefs() {
	// We can use RacyStore because the refers can't be shared until after
	// InitRefs is called, and thus it's safe to use non-atomic operations.
	r.refCount.Store(1)
}

// RefType implements refers.CheckedObject.RefType.
func (r *Refs[T]) RefType() string {
	var obj *T
	return fmt.Sprintf("%T", obj)[1:]
}

// LeakMessage implements refers.CheckedObject.LeakMessage.
func (r *Refs[T]) LeakMessage() string {
	return fmt.Sprintf("[%s %p] reference count of %d instead of 0", r.RefType(), r, r.ReadRefs())
}

// ReadRefs returns the current number of references. The returned count is
// inherently racy and is unsafe to use without external synchronization.
func (r *Refs[T]) ReadRefs() int64 {
	return r.refCount.Load()
}

func (r *Refs[T]) IncRef() {
	v := r.refCount.Add(1)
	if v <= 1 {
		panic(fmt.Sprintf("Incrementing non-positive count %p on %s", r, r.RefType()))
	}
}

func (r *Refs[T]) DecRef(destroy func()) {
	v := r.refCount.Add(-1)
	switch {
	case v < 0:
		panic(fmt.Sprintf("Decrementing non-positive ref count %p, owned by %s", r, r.RefType()))
	case v == 0:
		// Call the destructor.
		if destroy != nil {
			destroy()
		}
	}
}
