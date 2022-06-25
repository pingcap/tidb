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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !race

package localpool

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

type Obj struct {
	// nolint:unused
	val int64 // nolint:structcheck // Dummy field to make it non-empty.
}

func TestPool(t *testing.T) {
	numWorkers := runtime.GOMAXPROCS(0)
	wg := new(util.WaitGroupWrapper)
	pool := NewLocalPool(16, func() interface{} {
		return new(Obj)
	}, nil)
	n := 1000
	for i := 0; i < numWorkers; i++ {
		wg.Run(func() {
			for j := 0; j < n; j++ {
				obj := pool.Get().(*Obj)
				obj.val = rand.Int63()
				pool.Put(obj)
			}
		})
	}
	wg.Wait()
	var getHit, getMiss, putHit, putMiss int
	for _, slot := range pool.slots {
		getHit += slot.getHit
		getMiss += slot.getMiss
		putHit += slot.putHit
		putMiss += slot.putMiss
	}
	require.Greater(t, getHit, getMiss)
	require.Greater(t, putHit, putMiss)
}

func GetAndPut(pool *LocalPool) {
	objs := make([]interface{}, rand.Intn(4)+1)
	for i := 0; i < len(objs); i++ {
		objs[i] = pool.Get()
	}
	runtime.Gosched()
	for _, obj := range objs {
		pool.Put(obj)
	}
}
