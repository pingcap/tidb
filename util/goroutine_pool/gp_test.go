// Copyright 2017 PingCAP, Inc.
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
// +build !leak

package gp

import (
	"sync"
	"testing"
	"time"
)

func TestBasicAPI(t *testing.T) {
	gp := New(time.Second)
	var wg sync.WaitGroup
	wg.Add(1)
	// cover alloc()
	gp.Go(func() { wg.Done() })
	// cover put()
	wg.Wait()
	// cover get()
	gp.Go(func() {})
	gp.Close()
}

func TestGC(t *testing.T) {
	gp := New(200 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		idx := i
		gp.Go(func() {
			time.Sleep(time.Duration(idx+1) * time.Millisecond)
			wg.Done()
		})
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	gp.Close()
	gp.Go(func() {}) // To trigger count change.
	gp.Lock()
	count := gp.count
	gp.Unlock()
	if count > 1 {
		t.Error("all goroutines should be recycled", count)
	}
}

func TestRace(t *testing.T) {
	gp := New(8 * time.Millisecond)
	var wg sync.WaitGroup
	begin := make(chan struct{})
	wg.Add(500)
	for i := 0; i < 50; i++ {
		idxI := i
		go func() {
			<-begin
			for i := 0; i < 10; i++ {
				gp.Go(func() {
				idxJ := i
				res := gp.Go(func() {})
				if res != nil {
					t.Logf("fail to start work %d-%d", idxI, idxJ)
				}
				time.Sleep(5 * time.Millisecond)
					wg.Done()
				})
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	close(begin)
	wg.Wait()
	gp.Close()
	time.Sleep(1e9)
	gp.Lock()
	count := gp.count
	gp.Unlock()
	if count != 0 {
		t.Errorf("all goroutines should be recycled, count:%d\n", count)
	}
}

func BenchmarkGoPool(b *testing.B) {
	gp := New(20 * time.Second)
	for i := 0; i < b.N/2; i++ {
		gp.Go(func() {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gp.Go(dummy)
	}
	b.StopTimer()

	gp.Close()
}

func BenchmarkGo(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go dummy()
	}
}

func dummy() {
}

func BenchmarkMorestackPool(b *testing.B) {
	gp := New(5 * time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		gp.Go(func() {
			morestack(false)
			wg.Done()
		})
		wg.Wait()
	}
	b.StopTimer()

	gp.Close()
}

func BenchmarkMoreStack(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			morestack(false)
			wg.Done()
		}()
		wg.Wait()
	}
}

func morestack(f bool) {
	var stack [8 * 1024]byte
	if f {
		for i := 0; i < len(stack); i++ {
			stack[i] = 'a'
		}
	}
}
