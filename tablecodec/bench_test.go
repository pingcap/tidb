// Copyright 2016 PingCAP, Inc.
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

package tablecodec

import (
	"testing"
)

func BenchmarkEncodeRowKeyWithHandle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, 100)
	}
}

func BenchmarkEncodeEndKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, 100)
		EncodeRowKeyWithHandle(100, 101)
	}
}

// BenchmarkEncodeRowKeyWithPrefixNex tests the performance of encoding row key with prefixNext
// PrefixNext() is slow than using EncodeRowKeyWithHandle.
// BenchmarkEncodeEndKey-4		20000000	        97.2 ns/op
// BenchmarkEncodeRowKeyWithPrefixNex-4	10000000	       121 ns/op
func BenchmarkEncodeRowKeyWithPrefixNex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sk := EncodeRowKeyWithHandle(100, 100)
		sk.PrefixNext()
	}
}

func BenchmarkDecodeRowKey(b *testing.B) {
	rowKey := EncodeRowKeyWithHandle(100, 100)
	for i := 0; i < b.N; i++ {
		DecodeRowKey(rowKey)
	}
}
