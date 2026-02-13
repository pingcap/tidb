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
	"math"
	"math/bits"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
)

// DefaultHLLPrecision is the default precision for HLL sketches.
// It matches the reference implementation used in the paper repository.
const DefaultHLLPrecision uint8 = 16

// HLLSketch is a HyperLogLog sketch for NDV estimation.
type HLLSketch struct {
	b         uint8
	m         uint32
	registers []uint8
}

// NewHLLSketch creates a new HLL sketch with the given precision.
func NewHLLSketch(b uint8) *HLLSketch {
	if b < 4 {
		b = 4
	} else if b > 30 {
		b = 30
	}
	m := uint32(1) << b
	return &HLLSketch{
		b:         b,
		m:         m,
		registers: make([]uint8, m),
	}
}

// Copy makes a copy of the sketch.
func (h *HLLSketch) Copy() *HLLSketch {
	if h == nil {
		return nil
	}
	cp := make([]uint8, len(h.registers))
	copy(cp, h.registers)
	return &HLLSketch{
		b:         h.b,
		m:         h.m,
		registers: cp,
	}
}

// InsertHash inserts a hashed value into the sketch.
func (h *HLLSketch) InsertHash(hashVal uint64) {
	if h == nil {
		return
	}
	shift := 64 - uint(h.b)
	idx := int(hashVal >> shift)
	w := hashVal << uint(h.b)
	rank := uint8(bits.LeadingZeros64(w) + 1)
	maxRank := uint8(64 - uint(h.b) + 1)
	if rank > maxRank {
		rank = maxRank
	}
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
}

// InsertValue inserts a value into the sketch.
func (h *HLLSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	hashVal, err := hashDatum(sc, value)
	if err != nil {
		return err
	}
	h.InsertHash(hashVal)
	return nil
}

// InsertRowValue inserts multi-column values into the sketch.
func (h *HLLSketch) InsertRowValue(sc *stmtctx.StatementContext, values []types.Datum) error {
	hashVal, err := hashRow(sc, values)
	if err != nil {
		return err
	}
	h.InsertHash(hashVal)
	return nil
}

// Merge merges another HLL sketch into this one.
func (h *HLLSketch) Merge(other *HLLSketch) {
	if h == nil || other == nil {
		return
	}
	if h.b != other.b || len(h.registers) != len(other.registers) {
		return
	}
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// NDV returns the estimated number of distinct values.
func (h *HLLSketch) NDV() uint64 {
	if h == nil {
		return 0
	}
	m := float64(h.m)
	sum := 0.0
	zeroCnt := 0
	for _, v := range h.registers {
		if v == 0 {
			zeroCnt++
		}
		sum += math.Pow(2.0, -float64(v))
	}
	alphaMM := hllAlpha(h.m) * m * m
	estimate := alphaMM / sum
	if estimate <= 2.5*m && zeroCnt > 0 {
		estimate = m * math.Log(m/float64(zeroCnt))
	}
	if estimate < 0 {
		return 0
	}
	return uint64(estimate + 0.5)
}

func hllAlpha(m uint32) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1.0 + 1.079/float64(m))
	}
}

// HLLSketchToProto converts HLLSketch to its protobuf representation.
func HLLSketchToProto(h *HLLSketch) *tipb.HLLSketch {
	if h == nil {
		return nil
	}
	protoSketch := &tipb.HLLSketch{
		B:         uint32(h.b),
		Registers: append([]byte(nil), h.registers...),
	}
	return protoSketch
}

// HLLSketchFromProto converts protobuf representation to HLLSketch.
func HLLSketchFromProto(pbSketch *tipb.HLLSketch) *HLLSketch {
	if pbSketch == nil {
		return nil
	}
	b := uint8(pbSketch.B)
	sketch := NewHLLSketch(b)
	if len(pbSketch.Registers) == len(sketch.registers) {
		copy(sketch.registers, pbSketch.Registers)
	}
	return sketch
}
