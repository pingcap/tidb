// Copyright 2026 PingCAP, Inc.
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

package globalsort

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/stretchr/testify/require"
)

func TestParallelSort(t *testing.T) {
	type pair struct {
		key   int
		value int
	}
	compare := func(a, b pair) int { return cmp.Compare(a.key, b.key) }
	comparePair := func(a, b pair) int {
		if order := compare(a, b); order != 0 {
			return order
		}
		return cmp.Compare(a.value, b.value)
	}
	patterns := []string{"random", "sorted", "reverse", "equal", "repeated", "sorted-runs", "organ-pipe"}
	for _, size := range []int{
		0, 1, 2, minParallelSortSize - 1, minParallelSortSize, minParallelSortSize + 1,
		2*minParallelSortSize - 1, 2 * minParallelSortSize, 2*minParallelSortSize + 1,
		8*minParallelSortSize + 7,
	} {
		for _, pattern := range patterns {
			t.Run(fmt.Sprintf("%s/%d", pattern, size), func(t *testing.T) {
				random := rand.New(rand.NewPCG(1, 2))
				original := make([]pair, size)
				for i := range original {
					key := i
					switch pattern {
					case "random", "sorted-runs":
						key = random.Int()
					case "reverse":
						key = size - i
					case "equal":
						key = 1
					case "repeated":
						key = random.IntN(8)
					case "organ-pipe":
						key = min(i, size-i)
					}
					original[i] = pair{key: key, value: i}
				}
				if pattern == "sorted-runs" {
					for start := 0; start < size; start += 257 {
						slices.SortFunc(original[start:min(start+257, size)], compare)
					}
				}
				want := slices.Clone(original)
				slices.SortFunc(want, comparePair)
				for _, concurrency := range []int{-1, 0, 1, 2, 3, 8, math.MaxInt} {
					t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
						data := slices.Clone(original)
						parallelSort(data, compare, concurrency)
						require.True(t, slices.IsSortedFunc(data, compare))
						// Equal keys need not retain their order, but their values must stay attached.
						slices.SortFunc(data, comparePair)
						require.Equal(t, want, data)
					})
				}
			})
		}
	}
}

func TestParallelSortConcurrency(t *testing.T) {
	const size = 8 * minParallelSortSize
	type sortCall struct {
		budget    int
		data      []int
		active    atomic.Int64
		maxActive atomic.Int64
		entered   chan struct{}
	}
	var calls []*sortCall
	var workers sync.WaitGroup
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		unblock()
		workers.Wait()
	}()
	for _, budget := range []int{-1, 0, 1, 2, 3, 8, math.MaxInt} {
		call := &sortCall{budget: budget, data: make([]int, size), entered: make(chan struct{}, 2)}
		for i := range call.data {
			call.data[i] = i
		}
		calls = append(calls, call)
		workers.Add(1)
		go func() {
			defer workers.Done()
			var lowerEntered, upperEntered atomic.Bool
			parallelSort(call.data, func(a, b int) int {
				active := call.active.Add(1)
				for previous := call.maxActive.Load(); active > previous; previous = call.maxActive.Load() {
					if call.maxActive.CompareAndSwap(previous, active) {
						break
					}
				}
				// Hold work from separated value ranges so concurrent calls and
				// multiple workers within each parallel call overlap deterministically.
				// Let sparse pivot samples proceed until workers compare nearby values.
				nearby := a-b == 1 || b-a == 1
				lower := nearby && a < size/4 && b < size/4
				upper := nearby && a >= 3*size/4 && b >= 3*size/4
				if (lower && lowerEntered.CompareAndSwap(false, true)) ||
					(upper && upperEntered.CompareAndSwap(false, true)) {
					call.entered <- struct{}{}
					<-release
				}
				if a%64 == 0 {
					runtime.Gosched()
				}
				call.active.Add(-1)
				return cmp.Compare(a, b)
			}, budget)
		}()
	}

	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for _, call := range calls {
		for range min(max(call.budget, 1), 2) {
			select {
			case <-call.entered:
			case <-timeout.C:
				t.Fatalf("sort with concurrency %d did not reach its comparator barriers", call.budget)
			}
		}
	}
	unblock()
	workers.Wait()
	for _, call := range calls {
		require.LessOrEqual(t, call.maxActive.Load(), int64(max(call.budget, 1)), "concurrency %d", call.budget)
		if call.budget > 1 {
			require.Greater(t, call.maxActive.Load(), int64(1), "concurrency %d must allow parallel work", call.budget)
		}
		require.Zero(t, call.active.Load())
		for i, value := range call.data {
			require.Equal(t, i, value, "concurrency %d", call.budget)
		}
	}
}

func BenchmarkParallelSort(b *testing.B) {
	type kv struct {
		key   []byte
		value []byte
	}
	const size = 1 << 18
	compare := func(a, b kv) int { return bytes.Compare(a.key, b.key) }
	for _, keySize := range []int{16, 256} {
		keys := make([]byte, size*keySize)
		values := make([]byte, size*8)
		original := make([]kv, size)
		random := rand.New(rand.NewPCG(1, 2))
		for i := range original {
			key := keys[i*keySize : (i+1)*keySize]
			binary.BigEndian.PutUint64(key[keySize-8:], random.Uint64())
			value := values[i*8 : (i+1)*8]
			binary.BigEndian.PutUint64(value, uint64(i))
			original[i] = kv{key: key, value: value}
		}
		for _, pattern := range []string{"random", "sorted-runs"} {
			input := slices.Clone(original)
			if pattern == "sorted-runs" {
				for start := 0; start < size; start += size / 32 {
					slices.SortFunc(input[start:start+size/32], compare)
				}
			}
			for _, concurrency := range []int{1, 2, 8} {
				name := fmt.Sprintf("%s/key=%d/concurrency=%d", pattern, keySize, concurrency)
				b.Run(name+"/parallelSort", func(b *testing.B) {
					data := make([]kv, size)
					b.ReportAllocs()
					b.SetBytes(int64(size * keySize))
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						copy(data, input)
						b.StartTimer()
						parallelSort(data, compare, concurrency)
					}
				})
				b.Run(name+"/sorty", func(b *testing.B) {
					originalMaxGor := sorty.MaxGor
					sorty.MaxGor = uint64(concurrency)
					b.Cleanup(func() { sorty.MaxGor = originalMaxGor })
					data := make([]kv, size)
					lessSwap := func(i, k, r, s int) bool {
						if bytes.Compare(data[i].key, data[k].key) < 0 {
							if r != s {
								data[r], data[s] = data[s], data[r]
							}
							return true
						}
						return false
					}
					b.ReportAllocs()
					b.SetBytes(int64(size * keySize))
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						copy(data, input)
						b.StartTimer()
						sorty.Sort(len(data), lessSwap)
					}
				})
			}
		}
	}
}
