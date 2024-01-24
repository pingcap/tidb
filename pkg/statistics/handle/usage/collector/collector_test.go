// Copyright 2024 PingCAP, Inc.
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

package collector

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionSendDelta(t *testing.T) {
	num := 0
	mergeFn := func(delta int) {
		num += delta
	}

	g := NewGlobalCollector(mergeFn)
	g.StartWorker()
	s := g.SpawnSession()
	expect := 0
	for i := 0; i < 256; i++ {
		if s.SendDelta(1) {
			expect += 1
		}
	}

	g.Close()
	require.Equal(t, expect, num)
}

func TestSessionParallelSendDelta(t *testing.T) {
	num := 0
	var expect atomic.Int64
	mergeFn := func(delta int) {
		num += delta
	}

	g := NewGlobalCollector(mergeFn)
	g.StartWorker()
	sessionCount := 256
	var wg sync.WaitGroup
	for i := 0; i < sessionCount; i++ {
		s := g.SpawnSession()
		wg.Add(1)
		go func() {
			for i := 0; i < 256; i++ {
				if s.SendDelta(1) {
					expect.Add(1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	g.Close()
	require.Equal(t, expect.Load(), int64(num))
}

func TestSessionParallelSendDeltaSync(t *testing.T) {
	num := 0
	mergeFn := func(delta int) {
		num += delta
	}

	g := NewGlobalCollector(mergeFn)
	g.StartWorker()
	sessionCount := 256
	var wg sync.WaitGroup

	for i := 0; i < sessionCount; i++ {
		wg.Add(1)
		s := g.SpawnSession()
		go func() {
			for i := 0; i < 256; i++ {
				s.SendDeltaSync(1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	g.Close()
	require.Equal(t, sessionCount*256, num)
}
