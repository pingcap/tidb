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
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbspfreshbulk

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestVectorBytesPreserveCanonicalBits(t *testing.T) {
	for _, dimension := range []int{0, 1, 768, 960, 1024, int(MaxDimension)} {
		vector := make([]float32, dimension)
		canonical := make([]byte, 0, dimension*4)
		for i := range vector {
			bits := uint32(i) * 0x9e3779b9
			if i == 0 {
				bits = 0x80000000
			}
			vector[i] = math.Float32frombits(bits)
			canonical = binary.LittleEndian.AppendUint32(canonical, bits)
		}
		got := float32LittleEndianBytes(vector)
		if !bytes.Equal(canonical, got) {
			t.Fatalf("dimension %d: noncanonical f32 bits", dimension)
		}
		owned := append([]byte(nil), got...)
		clear(vector)
		if !bytes.Equal(canonical, owned) {
			t.Fatal("fragment copy retained borrowed input")
		}
	}
}
