// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may use this file except in compliance with the License.
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
	"github.com/dolthub/swiss"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/zeebo/xxh3"
)

// FMSketchVec is a vectorized version of FMSketch that can efficiently process
// multiple sketches and values in batch. It's optimized for statistics collection
// scenarios where we need to process multiple columns (each with its own sketch)
// simultaneously.
//
// Key optimizations:
// 1. Batch processing of multiple value-sketch pairs
// 2. Reusable encoding buffers to reduce allocations
// 3. Direct hash calculation using murmur3.Sum128 (no pool overhead)
// 4. Better cache locality by processing values sequentially
type FMSketchVec struct {
	// A set to store unique hashed values.
	hashset *swiss.Map[uint64, bool]
	// A binary mask used to track the maximum number of trailing zeroes in the hashed values.
	// Also used to track the level of the sketch.
	// Every time the size of the hashset exceeds the maximum size, the mask will be moved to the next level.
	mask uint64
	// The maximum size of the hashset. If the size exceeds this value, the mask will be moved to the next level.
	// And the hashset will only keep the hashed values with trailing zeroes greater than or equal to the new mask.
	maxSize int

	bufs     [][]byte
	hashVals []uint64
}

// NewFMSketchVec returns a new FM sketch vector.
func NewFMSketchVec(maxSize int) *FMSketchVec {
	return &FMSketchVec{
		hashset: swiss.NewMap[uint64, bool](uint32(128)),
		mask:    0,
		maxSize: MaxSketchSize,
	}
}

func (s *FMSketchVec) InsertValueVec(sc *stmtctx.StatementContext, values []types.Datum) (err error) {
	n := len(values)
	// vec encoding
	if len(s.bufs) < n {
		s.bufs = make([][]byte, n)
	}
	for i := 0; i < n; i++ {
		s.bufs[i] = s.bufs[i][:0]
	}
	s.bufs, err = codec.EncodeStringVec(s.bufs, values, false)
	if err != nil {
		return err
	}

	// vec hash
	if len(s.hashVals) < n {
		s.hashVals = make([]uint64, n)
	}
	xxHash3(s.bufs, s.hashVals)

	// vec insert
	s.insertHashValue(s.hashVals[:n])
	return nil
}

func (s *FMSketchVec) insertHashValue(hashVals []uint64) {
	extend := false
	for _, hashVal := range hashVals {
		if (hashVal & s.mask) != 0 {
			continue
		}
		s.hashset.Put(hashVal, true)
		if s.hashset.Count() > s.maxSize {
			extend = true
		}
	}
	if extend {
		// If the size of the hashset exceeds the maximum size, move the mask to the next level.
		s.mask = s.mask*2 + 1
		// Clean up the hashset by removing the hashed values with trailing zeroes less than the new mask.
		s.hashset.Iter(func(k uint64, _ bool) (stop bool) {
			if (k & s.mask) != 0 {
				s.hashset.Delete(k)
			}
			return false
		})
	}
}

// xxHash3 computes xxHash3 hash values for multiple byte slices in a vectorized manner.
// It hashes each byte slice in vals and stores the resulting 64-bit hash values in hashVals.
// The caller must ensure hashVals has at least len(vals) length to avoid panics.
func xxHash3(vals [][]byte, hashVals []uint64) {
	// Vectorized hashing: process each byte slice
	for i := 0; i < len(vals); i++ {
		hashVals[i] = xxh3.Hash(vals[i])
	}
}
