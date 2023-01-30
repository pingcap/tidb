// Copyright 2023 PingCAP, Inc.
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

package topk

import (
	"math"
	"math/rand"

	"github.com/pingcap/tidb/util/minheap"
	"github.com/twmb/murmur3"
)

// LookupTable is the size of lookup table
const LookupTable = 256

// HeavyKeeper is an accurate algorithm for finding top-k elephant flows
type HeavyKeeper struct {
	k           uint32
	width       uint32
	depth       uint32
	decay       float64
	lookupTable []float64

	r        *rand.Rand
	buckets  [][]bucket
	minHeap  *minheap.Heap[string]
	expelled chan string
}

// NewHeavyKeeper creates a new HeavyKeeper
func NewHeavyKeeper(k, width, depth uint32, decay float64) *HeavyKeeper {
	arrays := make([][]bucket, depth)
	for i := range arrays {
		arrays[i] = make([]bucket, width)
	}

	topk := &HeavyKeeper{
		k:           k,
		width:       width,
		depth:       depth,
		decay:       decay,
		lookupTable: make([]float64, LookupTable),
		buckets:     arrays,
		r:           rand.New(rand.NewSource(0)),
		minHeap:     minheap.NewHeap[string](k),
		expelled:    make(chan string, 32),
	}
	for i := 0; i < LookupTable; i++ {
		topk.lookupTable[i] = math.Pow(decay, float64(i))
	}
	return topk
}

// Expelled returns the expelled item
func (topk *HeavyKeeper) Expelled() <-chan string {
	return topk.expelled
}

// Contains returns if item is in topk
func (topk *HeavyKeeper) Contains(val string) (uint32, bool) {
	items := topk.minHeap.Sorted()
	for _, item := range items {
		if item.Key == val {
			return item.Count, true
		}
	}
	return 0, false
}

// Add add item into heavykeeper and return if item had beend add into minheap.
// if item had been add into minheap and some item was expelled, return the expelled item.
func (topk *HeavyKeeper) Add(key string, incr uint32) bool {
	keyBytes := []byte(key)
	itemFingerprint := murmur3.Sum32(keyBytes)
	var maxCount uint32

	// compute d hashes
	for i, row := range topk.buckets {
		bucketNumber := murmur3.SeedSum32(uint32(i), keyBytes) % topk.width
		fingerprint := row[bucketNumber].fingerprint
		count := row[bucketNumber].count

		if count == 0 {
			row[bucketNumber].fingerprint = itemFingerprint
			row[bucketNumber].count = incr
			maxCount = max(maxCount, incr)
		} else if fingerprint == itemFingerprint {
			row[bucketNumber].count += incr
			maxCount = max(maxCount, row[bucketNumber].count)
		} else {
			for localIncr := incr; localIncr > 0; localIncr-- {
				var decay float64
				curCount := row[bucketNumber].count
				if row[bucketNumber].count < LookupTable {
					decay = topk.lookupTable[curCount]
				} else {
					// decr pow caculate cost
					decay = topk.lookupTable[LookupTable-1]
				}
				if topk.r.Float64() < decay {
					row[bucketNumber].count--
					if row[bucketNumber].count == 0 {
						row[bucketNumber].fingerprint = itemFingerprint
						row[bucketNumber].count = localIncr
						maxCount = max(maxCount, localIncr)
						break
					}
				}
			}
		}
	}
	minHeap := topk.minHeap.Min()
	if len(topk.minHeap.Nodes) == int(topk.k) && maxCount < minHeap {
		return false
	}
	// update minheap
	itemHeapIdx, itemHeapExist := topk.minHeap.Find(key)
	if itemHeapExist {
		topk.minHeap.Fix(itemHeapIdx, maxCount)
		return true
	}
	expelled := topk.minHeap.Add(&minheap.Node[string]{Key: key, Count: maxCount})
	if expelled != nil {
		topk.expel(expelled.Key)
	}
	return true
}

func (topk *HeavyKeeper) expel(item string) {
	select {
	case topk.expelled <- item:
	default:
	}
}

type bucket struct {
	fingerprint uint32
	count       uint32
}

func (b *bucket) Get() (fingerprint, count uint32) {
	return b.fingerprint, b.count
}

func (b *bucket) Set(fingerprint, count uint32) {
	b.fingerprint = fingerprint
	b.count = count
}

func (b *bucket) Inc(val uint32) uint32 {
	b.count += val
	return b.count
}

func max(x, y uint32) uint32 {
	if x > y {
		return x
	}
	return y
}
