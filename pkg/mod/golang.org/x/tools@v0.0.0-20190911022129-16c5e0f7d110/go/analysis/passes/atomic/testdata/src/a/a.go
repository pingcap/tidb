// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains tests for the atomic checker.

package a

import (
	"sync/atomic"
)

type Counter uint64

func AtomicTests() {
	x := uint64(1)
	x = atomic.AddUint64(&x, 1)        // want "direct assignment to atomic value"
	_, x = 10, atomic.AddUint64(&x, 1) // want "direct assignment to atomic value"
	x, _ = atomic.AddUint64(&x, 1), 10 // want "direct assignment to atomic value"

	y := &x
	*y = atomic.AddUint64(y, 1) // want "direct assignment to atomic value"

	var su struct{ Counter uint64 }
	su.Counter = atomic.AddUint64(&su.Counter, 1) // want "direct assignment to atomic value"
	z1 := atomic.AddUint64(&su.Counter, 1)
	_ = z1 // Avoid err "z declared and not used"

	var sp struct{ Counter *uint64 }
	*sp.Counter = atomic.AddUint64(sp.Counter, 1) // want "direct assignment to atomic value"
	z2 := atomic.AddUint64(sp.Counter, 1)
	_ = z2 // Avoid err "z declared and not used"

	au := []uint64{10, 20}
	au[0] = atomic.AddUint64(&au[0], 1) // want "direct assignment to atomic value"
	au[1] = atomic.AddUint64(&au[0], 1)

	ap := []*uint64{&au[0], &au[1]}
	*ap[0] = atomic.AddUint64(ap[0], 1) // want "direct assignment to atomic value"
	*ap[1] = atomic.AddUint64(ap[0], 1)

	x = atomic.AddUint64() // Used to make vet crash; now silently ignored.

	{
		// A variable declaration creates a new variable in the current scope.
		x := atomic.AddUint64(&x, 1)

		// Re-declaration assigns a new value.
		x, w := atomic.AddUint64(&x, 1), 10 // want "direct assignment to atomic value"
		_ = w
	}
}

type T struct{}

func (T) AddUint64(addr *uint64, delta uint64) uint64 { return 0 }

func NonAtomic() {
	x := uint64(1)
	var atomic T
	x = atomic.AddUint64(&x, 1) // ok; not the imported pkg
}
