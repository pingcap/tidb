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

package statistics

import (
	"maps"
	"math"
)

// PartitionNDVMerger estimates global NDV from partition sketches.
//
// The estimate is the FMSketch NDV, but each kept hash h has a count. It is GEE
// with the scale of each partition p, because partitions differ in sampling rate:
//
//	NDV      = (mask + 1) * sum of count(h)
//	count(h) = sqrt(rows_p / samples_p)  if only p has h, and its sample has h once
//	count(h) = 1                         otherwise
type PartitionNDVMerger struct {
	// The sketch holds the merge state until sampled input needs per-hash
	// counts. After conversion, only its mask and maxSize remain in use.
	sketch *FMSketch
	// distinctPerHash maps each kept hash to the distinct values it counts for:
	// 1 for a known value, the partition's GEE scale for a sampled singleton.
	distinctPerHash map[uint64]float64
}

// Merge takes ownership of a partition sketch; callers must not use it afterward.
// GEE scales every singleton by one ratio, which fits only one sampling rate,
// so each singleton keeps its partition's scale here.
// Values from full-input partitions are known and are never scaled.
func (m *PartitionNDVMerger) Merge(sketch *FMSketch) {
	if sketch == nil {
		return
	}
	if m.distinctPerHash == nil {
		if m.sketch == nil {
			m.sketch = sketch
			return
		}
		if !m.sketch.Sampled() && !sketch.Sampled() {
			m.sketch.MergeFMSketch(sketch)
			return
		}
		m.initDistinctPerHash()
	}
	if m.sketch.mask < sketch.mask {
		m.sketch.mask = sketch.mask
		maps.DeleteFunc(m.distinctPerHash, func(h uint64, _ float64) bool { return h&m.sketch.mask != 0 })
	}
	scale := singletonScale(sketch)
	for hash, repeated := range sketch.hashset {
		if repeated {
			m.insertHash(hash, 1)
		} else {
			m.insertHash(hash, scale)
		}
	}
}

// NDV returns the estimated number of distinct values in the merged partitions.
func (m *PartitionNDVMerger) NDV() int64 {
	if m.distinctPerHash == nil {
		// This also preserves the sampled estimate's bounds when only one
		// partition has a sketch.
		return m.sketch.NDV()
	}
	var sum float64
	for _, distinct := range m.distinctPerHash {
		sum += distinct
	}
	return int64(math.Round(float64(m.sketch.mask+1) * sum))
}

// Sampled reports whether any merged partition sketch was sampled.
func (m *PartitionNDVMerger) Sampled() bool {
	return m.distinctPerHash != nil || m.sketch.Sampled()
}

// singletonScale is the GEE scale of a singleton, sqrt(N/n), or 1 for full
// input, whose values count once.
func singletonScale(sketch *FMSketch) float64 {
	// Only a sketch of sampled rows has ndvCounts. A merge can mix sampled and
	// full-input partition sketches, so both kinds reach this function.
	if sketch.ndvCounts == nil {
		return 1
	}
	// GEE counts a value that the sample has once as sqrt(rows/samples) values,
	// because the sample likely missed similar rare values.
	return math.Sqrt(float64(sketch.ndvCounts.rows) / float64(sketch.ndvCounts.samples))
}

func (m *PartitionNDVMerger) initDistinctPerHash() {
	m.distinctPerHash = make(map[uint64]float64, len(m.sketch.hashset))
	scale := singletonScale(m.sketch)
	for hash, repeated := range m.sketch.hashset {
		if repeated {
			m.distinctPerHash[hash] = 1
		} else {
			m.distinctPerHash[hash] = scale
		}
	}
	// distinctPerHash now holds these hashes and their scale, and later merges
	// update only it. Drop the sketch's copy to free memory and avoid stale reads.
	m.sketch.hashset = nil
	m.sketch.ndvCounts = nil
}

// insertHash adds a hash with the distinct values it counts for: the scale of
// the partition if one sampled partition saw it exactly once and no other
// partition saw it, or 1.
func (m *PartitionNDVMerger) insertHash(hash uint64, scale float64) {
	if hash&m.sketch.mask != 0 {
		return
	}
	if _, ok := m.distinctPerHash[hash]; ok {
		scale = 1
	}
	m.distinctPerHash[hash] = scale
	if len(m.distinctPerHash) > m.sketch.maxSize {
		m.sketch.mask = m.sketch.mask*2 + 1
		maps.DeleteFunc(m.distinctPerHash, func(h uint64, _ float64) bool { return h&m.sketch.mask != 0 })
	}
}
