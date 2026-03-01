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
	"hash"
	"math"
	"math/bits"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	hllBucketBits  = 4
	hllBucketCount = 1 << hllBucketBits
	hllAlpha16     = 0.673
)

// HLLSketch is a fixed 16-bucket HyperLogLog sketch.
type HLLSketch struct {
	registers [hllBucketCount]uint8
}

// NewHLLSketch returns a new fixed-size HyperLogLog sketch.
func NewHLLSketch() *HLLSketch {
	return &HLLSketch{}
}

// Copy makes a copy for current HLLSketch.
func (s *HLLSketch) Copy() *HLLSketch {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func (s *HLLSketch) insertHashValue(hashVal uint64) {
	if s == nil {
		return
	}
	bucket := int(hashVal & uint64(hllBucketCount-1))
	w := hashVal >> hllBucketBits
	var rank uint8
	if w == 0 {
		rank = uint8(64 - hllBucketBits + 1)
	} else {
		rank = uint8(bits.LeadingZeros64(w) + 1)
	}
	if rank > s.registers[bucket] {
		s.registers[bucket] = rank
	}
}

// InsertValue inserts one datum to the sketch.
func (s *HLLSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	bytes, err := codec.EncodeValue(sc.TimeZone(), nil, value)
	err = sc.HandleError(err)
	if err != nil {
		return errors.Trace(err)
	}
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)
	_, err = hashFunc.Write(bytes)
	if err != nil {
		return errors.Trace(err)
	}
	s.insertHashValue(hashFunc.Sum64())
	return nil
}

// InsertRowValue inserts multi-column values to the sketch.
func (s *HLLSketch) InsertRowValue(sc *stmtctx.StatementContext, values []types.Datum) error {
	b := make([]byte, 0, 8)
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)

	errCtx := sc.ErrCtx()
	for _, v := range values {
		b = b[:0]
		b, err := codec.EncodeValue(sc.TimeZone(), b, v)
		err = errCtx.HandleError(err)
		if err != nil {
			return err
		}
		_, err = hashFunc.Write(b)
		if err != nil {
			return err
		}
	}
	s.insertHashValue(hashFunc.Sum64())
	return nil
}

// MergeHLLSketch merges two HLL sketches.
func (s *HLLSketch) MergeHLLSketch(rs *HLLSketch) {
	if s == nil || rs == nil {
		return
	}
	for i := range s.registers {
		if rs.registers[i] > s.registers[i] {
			s.registers[i] = rs.registers[i]
		}
	}
}

// NDV returns the estimated number of distinct values.
func (s *HLLSketch) NDV() int64 {
	if s == nil {
		return 0
	}
	m := float64(hllBucketCount)
	harmonic := 0.0
	zeroCount := 0
	for _, reg := range s.registers {
		harmonic += math.Pow(2, -float64(reg))
		if reg == 0 {
			zeroCount++
		}
	}
	estimate := hllAlpha16 * m * m / harmonic
	if estimate <= 2.5*m && zeroCount > 0 {
		estimate = m * math.Log(m/float64(zeroCount))
	}
	if estimate < 0 {
		return 0
	}
	return int64(estimate + 0.5)
}

// HLLSketchToProto converts HLLSketch to its protobuf representation.
func HLLSketchToProto(s *HLLSketch) *tipb.HllSketch {
	protoSketch := new(tipb.HllSketch)
	if s == nil {
		return protoSketch
	}
	protoSketch.BucketBits = hllBucketBits
	protoSketch.Registers = make([]uint32, 0, hllBucketCount)
	for _, reg := range s.registers {
		protoSketch.Registers = append(protoSketch.Registers, uint32(reg))
	}
	return protoSketch
}

// HLLSketchFromProto converts HLLSketch from its protobuf representation.
func HLLSketchFromProto(protoSketch *tipb.HllSketch) *HLLSketch {
	if protoSketch == nil {
		return nil
	}
	sketch := NewHLLSketch()
	for i, reg := range protoSketch.Registers {
		if i >= hllBucketCount {
			break
		}
		sketch.registers[i] = uint8(reg)
	}
	return sketch
}

// MemoryUsage returns the total memory usage of an HLLSketch.
func (s *HLLSketch) MemoryUsage() int64 {
	if s == nil {
		return 0
	}
	return int64(hllBucketCount)
}
