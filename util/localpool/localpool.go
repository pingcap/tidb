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

package localpool

import (
	"runtime"
	_ "unsafe" // required by go:linkname
)

// LocalPool is an thread local object pool.
// It's similar to sync.Pool but has some difference
// - It can define the size of the pool.
// - It never get GCed.
type LocalPool struct {
	sizePerProc int
	slots       []*slot
	newFn       func() interface{}
	resetFn     func(obj interface{})
}

type slot struct {
	objs    []interface{}
	getHit  int
	getMiss int
	putHit  int
	putMiss int
}

// NewLocalPool creates a pool.
// The sizePerProc is pool size for each PROC, so total pool size is (GOMAXPROCS * sizePerProc)
// It can only be used when the GOMAXPROCS never change after the pool created.
// newFn is the function to create a new object.
// resetFn is the function called before put back to the pool, it can be nil.
func NewLocalPool(sizePerProc int, newFn func() interface{}, resetFn func(obj interface{})) *LocalPool {
	slots := make([]*slot, runtime.GOMAXPROCS(0))
	for i := 0; i < len(slots); i++ {
		slots[i] = &slot{
			objs: make([]interface{}, 0, sizePerProc),
		}
	}
	return &LocalPool{
		sizePerProc: sizePerProc,
		slots:       slots,
		newFn:       newFn,
		resetFn:     resetFn,
	}
}

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin() int
