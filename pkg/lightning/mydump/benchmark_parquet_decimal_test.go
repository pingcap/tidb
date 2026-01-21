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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

const (
	base1e9       = 1_000_000_000
	baseDigits    = 9
	maximumLength = mysql.MaxDecimalWidth + 2
)

func setDecimalFromBinaryViaBase1eN(
	rawBytes []byte, scale int, base uint64,
	baseDigits int, d *types.Datum,
) error {
	negative := (rawBytes[0] & 0x80) != 0
	if negative {
		for i := range rawBytes {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i]++
			if rawBytes[i] != 0 {
				break
			}
		}
	}

	var (
		digitsStack [maximumLength]byte
		n           int
		startIndex  = 0
		endIndex    = len(rawBytes)
	)

	for startIndex < endIndex && rawBytes[startIndex] == 0 {
		startIndex++
	}

	// Convert base-256 bytes to base-1eN.
	for startIndex < endIndex {
		var rem uint64
		for i := startIndex; i < endIndex; i++ {
			v := (rem << 8) | uint64(rawBytes[i])
			q := v / base
			rem = v % base
			rawBytes[i] = byte(q)
			if q == 0 && i == startIndex {
				startIndex++
			}
		}

		for range baseDigits {
			digitsStack[len(digitsStack)-1-n] = byte(48 + rem%10)
			n++
			rem /= 10
			if n == scale {
				digitsStack[maximumLength-1-n] = '.'
				n++
			}
			if startIndex == endIndex && rem == 0 {
				break
			}
		}
	}

	for n < scale+2 {
		digitsStack[maximumLength-1-n] = '0'
		n++
		if n == scale {
			digitsStack[maximumLength-1-n] = '.'
			n++
		}
	}

	if negative {
		digitsStack[maximumLength-1-n] = '-'
		n++
	}

	b := make([]byte, n)
	copy(b, digitsStack[len(digitsStack)-n:])
	dec := initializeMyDecimal(d)
	if err := dec.FromString(b); err != nil {
		return err
	}
	d.SetMysqlDecimal(dec)
	return nil
}

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

var decimalBenchMaxLen = func() int {
	maxLen := 0
	for _, tc := range decimalBenchCases {
		if len(tc.raw) > maxLen {
			maxLen = len(tc.raw)
		}
	}
	return maxLen
}()

func BenchmarkParquetDecimalParsing(b *testing.B) {
	b.Run("setDecimalFromBinaryViaBase1e9", func(b *testing.B) {
		b.ReportAllocs()
		var d types.Datum
		buf := make([]byte, decimalBenchMaxLen)
		for i := 0; i < b.N; i++ {
			tc := decimalBenchCases[i%len(decimalBenchCases)]
			work := buf[:len(tc.raw)]
			copy(work, tc.raw)
			if err := setDecimalFromBinaryViaBase1eN(work, tc.scale, base1e9, baseDigits, &d); err != nil {
				b.Fatalf("case %q: %v", tc.name, err)
			}
		}
	})

	b.Run("setDecimalFromBinaryViaBase10", func(b *testing.B) {
		b.ReportAllocs()
		var d types.Datum
		buf := make([]byte, decimalBenchMaxLen)
		for i := 0; i < b.N; i++ {
			tc := decimalBenchCases[i%len(decimalBenchCases)]
			work := buf[:len(tc.raw)]
			copy(work, tc.raw)
			if err := setDecimalFromBinaryViaBase1eN(work, tc.scale, 10, 1, &d); err != nil {
				b.Fatalf("case %q: %v", tc.name, err)
			}
		}
	})

	b.Run("FromParquetArray", func(b *testing.B) {
		b.ReportAllocs()
		var dec types.MyDecimal
		buf := make([]byte, decimalBenchMaxLen)
		for i := 0; i < b.N; i++ {
			tc := decimalBenchCases[i%len(decimalBenchCases)]
			work := buf[:len(tc.raw)]
			copy(work, tc.raw)
			if err := dec.FromParquetArray(work, tc.scale); err != nil {
				b.Fatalf("case %q: %v", tc.name, err)
			}
		}
	})

	b.Run("ToStringAndFromString", func(b *testing.B) {
		b.ReportAllocs()
		var dec types.MyDecimal
		buf := make([]byte, decimalBenchMaxLen)
		for i := 0; i < b.N; i++ {
			tc := decimalBenchCases[i%len(decimalBenchCases)]
			work := buf[:len(tc.raw)]
			copy(work, tc.raw)
			s := parquetArrayToStr(work, tc.scale)
			if err := dec.FromString([]byte(s)); err != nil {
				b.Fatalf("case %q: %v", tc.name, err)
			}
		}
	})
}
