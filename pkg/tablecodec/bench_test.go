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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablecodec

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func BenchmarkEncodeRowKeyWithHandle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, kv.IntHandle(100))
	}
}

func BenchmarkEncodeEndKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, kv.IntHandle(100))
		EncodeRowKeyWithHandle(100, kv.IntHandle(101))
	}
}

// BenchmarkEncodeRowKeyWithPrefixNex tests the performance of encoding row key with prefixNext
// PrefixNext() is slow than using EncodeRowKeyWithHandle.
// BenchmarkEncodeEndKey-4		20000000	        97.2 ns/op
// BenchmarkEncodeRowKeyWithPrefixNex-4	10000000	       121 ns/op
func BenchmarkEncodeRowKeyWithPrefixNex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sk := EncodeRowKeyWithHandle(100, kv.IntHandle(100))
		sk.PrefixNext()
	}
}

func BenchmarkDecodeRowKey(b *testing.B) {
	rowKey := EncodeRowKeyWithHandle(100, kv.IntHandle(100))
	for i := 0; i < b.N; i++ {
		_, err := DecodeRowKey(rowKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeIndexKeyIntHandle(b *testing.B) {
	var idxVal []byte
	// When handle values greater than 255, it will have a memory alloc.
	idxVal = append(idxVal, EncodeHandleInUniqueIndexValue(kv.IntHandle(256), false)...)

	for i := 0; i < b.N; i++ {
		DecodeHandleInIndexValue(idxVal)
	}
}

func BenchmarkDecodeIndexKeyCommonHandle(b *testing.B) {
	var idxVal []byte
	idxVal = append(idxVal, 0)
	// index version
	idxVal = append(idxVal, IndexVersionFlag)
	idxVal = append(idxVal, byte(1))

	// common handle
	encoded, _ := codec.EncodeKey(time.UTC, nil, types.MakeDatums(1, 2)...)
	h, _ := kv.NewCommonHandle(encoded)
	idxVal = encodeCommonHandle(idxVal, h)

	for i := 0; i < b.N; i++ {
		DecodeHandleInIndexValue(idxVal)
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkEncodeRowKeyWithHandle,
		BenchmarkEncodeEndKey,
		BenchmarkEncodeRowKeyWithPrefixNex,
		BenchmarkDecodeRowKey,
		BenchmarkHasTablePrefix,
		BenchmarkHasTablePrefixBuiltin,
		BenchmarkEncodeValue,
	)
}
