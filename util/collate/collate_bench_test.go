// Copyright 2021 PingCAP, Inc.
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

package collate

import (
	"math/rand"
	_ "net/http/pprof"
	"testing"
)

const short = 2 << 4
const middle = 2 << 10
const long = 2 << 20

func generateData(length int) string {
	rs := []rune("ßss")
	r := make([]rune, length)
	for i := range r {
		r[i] = rs[rand.Intn(len(rs))]
	}

	return string(r)
}

func compare(b *testing.B, collator Collator, length int) {
	s1 := generateData(length)
	s2 := generateData(length)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collator.Compare(s1, s2)
	}
}

func key(b *testing.B, collator Collator, length int) {
	s := generateData(length)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collator.Key(s)
	}
}

func BenchmarkUtf8mb4Bin_CompareShort(b *testing.B) {
	compare(b, &binCollator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_CompareShort(b *testing.B) {
	compare(b, &generalCICollator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_CompareShort(b *testing.B) {
	compare(b, &unicodeCICollator{}, short)
}

func BenchmarkUtf8mb4Bin_CompareMid(b *testing.B) {
	compare(b, &binCollator{}, middle)
}

func BenchmarkUtf8mb4GeneralCI_CompareMid(b *testing.B) {
	compare(b, &generalCICollator{}, middle)
}

func BenchmarkUtf8mb4UnicodeCI_CompareMid(b *testing.B) {
	compare(b, &unicodeCICollator{}, middle)
}

func BenchmarkUtf8mb4Bin_CompareLong(b *testing.B) {
	compare(b, &binCollator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_CompareLong(b *testing.B) {
	compare(b, &generalCICollator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_CompareLong(b *testing.B) {
	compare(b, &unicodeCICollator{}, long)
}

func BenchmarkUtf8mb4Bin_KeyShort(b *testing.B) {
	key(b, &binCollator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_KeyShort(b *testing.B) {
	key(b, &generalCICollator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_KeyShort(b *testing.B) {
	key(b, &unicodeCICollator{}, short)
}

func BenchmarkUtf8mb4Bin_KeyMid(b *testing.B) {
	key(b, &binCollator{}, middle)
}

func BenchmarkUtf8mb4GeneralCI_KeyMid(b *testing.B) {
	key(b, &generalCICollator{}, middle)
}

func BenchmarkUtf8mb4UnicodeCI_KeyMid(b *testing.B) {
	key(b, &unicodeCICollator{}, middle)
}

func BenchmarkUtf8mb4Bin_KeyLong(b *testing.B) {
	key(b, &binCollator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_KeyLong(b *testing.B) {
	key(b, &generalCICollator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_KeyLong(b *testing.B) {
	key(b, &unicodeCICollator{}, long)
}
