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

	"github.com/pingcap/tipb/go-tipb"
)

// HLL is a minimal HyperLogLog used to estimate per-region distinct/singleton
// counts for analyze NDV sub-sampling. TiKV builds these sketches (one NDV and
// one singleton HLL per region) and TiDB unions them across regions to estimate
// the global singleton (f1) count via a leave-one-out. Being fixed-size, one HLL
// per region keeps the retained state bounded, unlike per-region FM sketches.
//
// The register layout and estimator MUST stay byte-for-byte identical to the
// Rust implementation in TiKV (src/coprocessor/statistics/hll.rs): TiDB reads the
// raw register bytes a TiKV-built sketch carries, so a mismatch silently corrupts
// the estimate. There is one byte per register and registers.len() == 1<<precision.
type HLL struct {
	precision uint8
	registers []byte
}

// DefaultHLLPrecision is the precision used when building sketches: m = 1<<14 =
// 16384 one-byte registers (16 KiB), ~0.8% standard error. It must match the
// TiKV producer (src/coprocessor/statistics/hll.rs). The singleton estimate is a
// difference of close cardinalities, so the precision is deliberately not lower.
const DefaultHLLPrecision uint8 = 14

// NewHLL creates an empty HLL with the given precision (1<<precision registers).
func NewHLL(precision uint8) *HLL {
	return &HLL{precision: precision, registers: make([]byte, 1<<precision)}
}

// ToProto serializes the registers and precision. Used by the in-process sampler
// (unistore) to mimic the TiKV producer.
func (h *HLL) ToProto() *tipb.HLLSketch {
	registers := make([]byte, len(h.registers))
	copy(registers, h.registers)
	return &tipb.HLLSketch{Registers: registers, Precision: uint32(h.precision)}
}

// HLLFromProto decodes the register bytes built by TiKV. It returns nil for a nil
// or malformed proto (register length not a power of two matching precision).
func HLLFromProto(proto *tipb.HLLSketch) *HLL {
	if proto == nil {
		return nil
	}
	precision := uint8(proto.GetPrecision())
	registers := proto.GetRegisters()
	if precision == 0 || len(registers) != 1<<precision {
		return nil
	}
	return &HLL{precision: precision, registers: registers}
}

// InsertHash records a 64-bit hash. TiKV does this during sampling; TiDB only
// uses it in tests, but the routing must match TiKV exactly.
func (h *HLL) InsertHash(hash uint64) {
	p := uint(h.precision)
	idx := hash >> (64 - p)
	// Move the remaining 64-p bits to the top; the low p bits become zero.
	w := hash << p
	var rank byte
	if w == 0 {
		rank = byte(64 - p + 1)
	} else {
		rank = byte(bits.LeadingZeros64(w) + 1)
	}
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
}

// Merge takes the register-wise max (set union). other must have the same
// precision; mismatched precisions are ignored to stay defensive.
func (h *HLL) Merge(other *HLL) {
	if other == nil || other.precision != h.precision {
		return
	}
	for i, o := range other.registers {
		if o > h.registers[i] {
			h.registers[i] = o
		}
	}
}

// Clone returns a deep copy so a union does not mutate a retained sketch.
func (h *HLL) Clone() *HLL {
	registers := make([]byte, len(h.registers))
	copy(registers, h.registers)
	return &HLL{precision: h.precision, registers: registers}
}

// Count returns the estimated distinct count. Standard HyperLogLog with the
// small-range (linear counting) correction; the large-range correction is
// unnecessary for 64-bit hashes.
func (h *HLL) Count() uint64 {
	m := float64(len(h.registers))
	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += math.Exp2(-float64(r))
		if r == 0 {
			zeros++
		}
	}
	estimate := hllAlpha(len(h.registers)) * m * m / sum
	if estimate <= 2.5*m && zeros > 0 {
		// Linear counting is more accurate while many registers are still empty.
		return uint64(math.Round(m * math.Log(m/float64(zeros))))
	}
	return uint64(math.Round(estimate))
}

// hllAlpha is the HyperLogLog bias-correction constant alpha_m.
func hllAlpha(m int) float64 {
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
