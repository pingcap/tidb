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

package collate_test

import (
	"math/rand"
	_ "net/http/pprof"
	"testing"

	"github.com/pingcap/tidb/pkg/util/collate"
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

func compare(b *testing.B, collator collator, length int) {
	s1 := generateData(length)
	s2 := generateData(length)

	b.ResetTimer()
	for range b.N {
		collator.Compare(s1, s2)
	}
}

func key(b *testing.B, collator collator, length int) {
	s := generateData(length)

	b.ResetTimer()
	for range b.N {
		collator.Key(s)
	}
}

func immutableKey(b *testing.B, c any, length int) {
	s := generateData(length)

	b.ResetTimer()
	for range b.N {
		// Get the actual collator which has ImmutableKey method
		if collator, ok := c.(interface{ ImmutableKey(string) []byte }); ok {
			collator.ImmutableKey(s)
		}
	}
}

func BenchmarkUtf8mb4Bin_CompareShort(b *testing.B) {
	compare(b, &collate.ExportedBinPaddingCollator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_CompareShort(b *testing.B) {
	compare(b, &collate.ExportedGeneralCICollator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_CompareShort(b *testing.B) {
	compare(b, &collate.ExportedUnicodeCICollator{}, short)
}

func BenchmarkUtf8mb40900AICI_CompareShort(b *testing.B) {
	compare(b, &collate.ExportedUnicode0900AICICollator{}, short)
}

func BenchmarkUtf8mb40900Bin_CompareShort(b *testing.B) {
	compare(b, &collate.ExportedDerivedBinCollator{}, short)
}

func BenchmarkUtf8mb4Bin_CompareMid(b *testing.B) {
	compare(b, &collate.ExportedBinPaddingCollator{}, middle)
}

func BenchmarkUtf8mb4GeneralCI_CompareMid(b *testing.B) {
	compare(b, &collate.ExportedGeneralCICollator{}, middle)
}

func BenchmarkUtf8mb4UnicodeCI_CompareMid(b *testing.B) {
	compare(b, &collate.ExportedUnicodeCICollator{}, middle)
}

func BenchmarkUtf8mb40900AICI_CompareMid(b *testing.B) {
	compare(b, &collate.ExportedUnicode0900AICICollator{}, middle)
}

func BenchmarkUtf8mb40900Bin_CompareMid(b *testing.B) {
	compare(b, &collate.ExportedDerivedBinCollator{}, middle)
}

func BenchmarkUtf8mb4Bin_CompareLong(b *testing.B) {
	compare(b, &collate.ExportedBinPaddingCollator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_CompareLong(b *testing.B) {
	compare(b, &collate.ExportedGeneralCICollator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_CompareLong(b *testing.B) {
	compare(b, &collate.ExportedUnicodeCICollator{}, long)
}

func BenchmarkUtf8mb40900AICI_CompareLong(b *testing.B) {
	compare(b, &collate.ExportedUnicode0900AICICollator{}, long)
}

func BenchmarkUtf8mb40900Bin_CompareLong(b *testing.B) {
	compare(b, &collate.ExportedDerivedBinCollator{}, long)
}

func BenchmarkUtf8mb4Bin_KeyShort(b *testing.B) {
	key(b, &collate.ExportedBinPaddingCollator{}, short)
}

func BenchmarkUtf8mb4Bin_ImmutableKeyShort(b *testing.B) {
	immutableKey(b, &collate.ExportedBinPaddingCollator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_KeyShort(b *testing.B) {
	key(b, &collate.ExportedGeneralCICollator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_ImmutableKeyShort(b *testing.B) {
	immutableKey(b, &collate.ExportedGeneralCICollator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_KeyShort(b *testing.B) {
	key(b, &collate.ExportedUnicodeCICollator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_ImmutableKeyShort(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicodeCICollator{}, short)
}

func BenchmarkUtf8mb40900AICI_KeyShort(b *testing.B) {
	key(b, &collate.ExportedUnicode0900AICICollator{}, short)
}

func BenchmarkUtf8mb40900AICI_ImmutableKeyShort(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicode0900AICICollator{}, short)
}

func BenchmarkUtf8mb40900Bin_KeyShort(b *testing.B) {
	key(b, &collate.ExportedDerivedBinCollator{}, short)
}

func BenchmarkUtf8mb40900Bin_ImmutableKeyShort(b *testing.B) {
	immutableKey(b, &collate.ExportedDerivedBinCollator{}, short)
}

func BenchmarkUtf8mb4Bin_KeyMid(b *testing.B) {
	key(b, &collate.ExportedBinPaddingCollator{}, middle)
}

func BenchmarkUtf8mb4Bin_ImmutableKeyMid(b *testing.B) {
	immutableKey(b, &collate.ExportedBinPaddingCollator{}, middle)
}

func BenchmarkUtf8mb4GeneralCI_KeyMid(b *testing.B) {
	key(b, &collate.ExportedGeneralCICollator{}, middle)
}

func BenchmarkUtf8mb4GeneralCI_ImmutableKeyMid(b *testing.B) {
	immutableKey(b, &collate.ExportedGeneralCICollator{}, middle)
}

func BenchmarkUtf8mb4UnicodeCI_KeyMid(b *testing.B) {
	key(b, &collate.ExportedUnicodeCICollator{}, middle)
}

func BenchmarkUtf8mb4UnicodeCI_ImmutableKeyMid(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicodeCICollator{}, middle)
}

func BenchmarkUtf8mb40900AICI_KeyMid(b *testing.B) {
	key(b, &collate.ExportedUnicode0900AICICollator{}, middle)
}

func BenchmarkUtf8mb40900AICI_ImmutableKeyMid(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicode0900AICICollator{}, middle)
}

func BenchmarkUtf8mb40900Bin_KeyMid(b *testing.B) {
	key(b, &collate.ExportedDerivedBinCollator{}, middle)
}

func BenchmarkUtf8mb40900Bin_ImmutableKeyMid(b *testing.B) {
	immutableKey(b, &collate.ExportedDerivedBinCollator{}, middle)
}

func BenchmarkUtf8mb4Bin_KeyLong(b *testing.B) {
	key(b, &collate.ExportedBinPaddingCollator{}, long)
}

func BenchmarkUtf8mb4Bin_ImmutableKeyLong(b *testing.B) {
	immutableKey(b, &collate.ExportedBinPaddingCollator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_KeyLong(b *testing.B) {
	key(b, &collate.ExportedGeneralCICollator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_ImmutableKeyLong(b *testing.B) {
	immutableKey(b, &collate.ExportedGeneralCICollator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_KeyLong(b *testing.B) {
	key(b, &collate.ExportedUnicodeCICollator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_ImmutableKeyLong(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicodeCICollator{}, long)
}

func BenchmarkUtf8mb40900AICI_KeyLong(b *testing.B) {
	key(b, &collate.ExportedUnicode0900AICICollator{}, long)
}

func BenchmarkUtf8mb40900AICI_ImmutableKeyLong(b *testing.B) {
	immutableKey(b, &collate.ExportedUnicode0900AICICollator{}, long)
}

func BenchmarkUtf8mb40900Bin_KeyLong(b *testing.B) {
	key(b, &collate.ExportedDerivedBinCollator{}, long)
}

func BenchmarkUtf8mb40900Bin_ImmutableKeyLong(b *testing.B) {
	immutableKey(b, &collate.ExportedDerivedBinCollator{}, long)
}
