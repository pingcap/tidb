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

package mydump

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/types"
)

var decimalBenchCases = func() []struct {
	name  string
	raw   []byte
	scale int
} {
	pattern := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	makeFull := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = pattern[i%len(pattern)]
		}
		return b
	}

	return []struct {
		name  string
		raw   []byte
		scale int
	}{
		{name: "SmallPos", raw: []byte{0x01}, scale: 3},
		{name: "SmallNeg", raw: []byte{0xff}, scale: 4},
		{name: "Medium", raw: makeFull(8), scale: 6},
		{name: "Long", raw: makeFull(16), scale: 8},
		{name: "LongNeg", raw: bytes.Repeat([]byte{0xff}, 16), scale: 12},
	}
}()

func BenchmarkParquetDecimalParsing(b *testing.B) {
	b.Run("FromParquetArray", func(b *testing.B) {
		b.ReportAllocs()
		for _, tc := range decimalBenchCases {
			b.Run(tc.name, func(b *testing.B) {
				buf := make([]byte, len(tc.raw))
				var dec types.MyDecimal
				for b.Loop() {
					copy(buf, tc.raw)
					if err := dec.FromParquetArray(buf, tc.scale); err != nil {
						b.Fatalf("case %q: %v", tc.name, err)
					}
				}
			})
		}
	})

	b.Run("ToStringAndFromStringNew", func(b *testing.B) {
		b.ReportAllocs()
		for _, tc := range decimalBenchCases {
			b.Run(tc.name, func(b *testing.B) {
				buf := make([]byte, len(tc.raw))
				var dec types.MyDecimal
				for b.Loop() {
					copy(buf, tc.raw)
					s := getStringFromParquetByte(buf, tc.scale)
					if err := dec.FromString(s); err != nil {
						b.Fatalf("case %q: %v", tc.name, err)
					}
				}
			})
		}
	})

	b.Run("ToStringAndFromStringOld", func(b *testing.B) {
		b.ReportAllocs()
		for _, tc := range decimalBenchCases {
			b.Run(tc.name, func(b *testing.B) {
				buf := make([]byte, len(tc.raw))
				var dec types.MyDecimal
				for b.Loop() {
					copy(buf, tc.raw)
					s := getStringFromParquetByteOld(buf, tc.scale)
					if err := dec.FromString([]byte(s)); err != nil {
						b.Fatalf("case %q: %v", tc.name, err)
					}
				}
			})
		}
	})
}
