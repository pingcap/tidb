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

//go:build ignore

// Emits Go's own bytes and float encodings so the Rust port can be compared
// against real TiDB output rather than against itself. The codec's Go tests
// are round-trips, so porting them faithfully still yields a suite that
// cannot see a shared encode/decode divergence; these vectors can.
//
// Regenerate with:
//
//	go run difftests/transaction-tests/fixtures/generate_bytes_float_vectors.go \
//	  > rust/difftests/transaction-tests/fixtures/bytes_float_vectors.hex
package main

import (
	"encoding/hex"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/util/codec"
)

func main() {
	// The group padding is what makes this encoding mem-comparable, so the
	// cases straddle every group boundary and include the two bytes the
	// marker arithmetic itself uses (0x00 padding and 0xff marker).
	byteCases := []struct {
		name  string
		value []byte
	}{
		{"empty", []byte{}},
		{"one", []byte("a")},
		{"seven", []byte("abcdefg")},
		{"eight", []byte("abcdefgh")},
		{"nine", []byte("abcdefghi")},
		{"fifteen", []byte("abcdefghijklmno")},
		{"sixteen", []byte("abcdefghijklmnop")},
		{"seventeen", []byte("abcdefghijklmnopq")},
		{"zeros", []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"zeros_nine", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"ff_run", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"mixed", []byte{0x00, 0xff, 0x01, 0xfe, 0x7f, 0x80, 0x00, 0xff, 0x2a}},
		{"utf8", []byte("你好, world")},
	}
	for _, c := range byteCases {
		emit("bytes_"+c.name, codec.EncodeBytes(nil, c.value))
		emit("bytes_desc_"+c.name, codec.EncodeBytesDesc(nil, c.value))
		emit("compact_bytes_"+c.name, codec.EncodeCompactBytes(nil, c.value))
	}

	// The sign bit is the subtle part: encodeFloatToCmpUint64 ORs it in for
	// every non-negative value, which LEAVES IT SET for -0.0, and
	// decodeCmpUintToFloat clears it again -- so -0.0 does not survive the
	// round trip. NaN is written as an explicit bit pattern because Go's
	// math.NaN() and Rust's f64::NAN differ in the low mantissa bit.
	floatCases := []struct {
		name  string
		value float64
	}{
		{"neg_one", -1.0},
		{"zero", 0.0},
		{"neg_zero", math.Copysign(0, -1)},
		{"one", 1.0},
		{"max", math.MaxFloat64},
		{"smallest_nonzero", math.SmallestNonzeroFloat64},
		{"neg_smallest_nonzero", -math.SmallestNonzeroFloat64},
		{"neg_max", -math.MaxFloat64},
		{"inf", math.Inf(1)},
		{"neg_inf", math.Inf(-1)},
		{"nan", math.Float64frombits(0x7FF8000000000001)},
		{"pi", 3.141592653589793},
		{"tiny", 1e-300},
		{"huge", 1e300},
	}
	for _, c := range floatCases {
		emit("float_"+c.name, codec.EncodeFloat(nil, c.value))
		emit("float_desc_"+c.name, codec.EncodeFloatDesc(nil, c.value))
	}
}

func emit(name string, encoded []byte) {
	fmt.Printf("%s=%s\n", name, hex.EncodeToString(encoded))
}
