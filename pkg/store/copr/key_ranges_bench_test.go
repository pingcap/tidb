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

package copr

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
)

var benchmarkSplitResult *KeyRanges

func BenchmarkKeyRangesSplitRetainedRight(b *testing.B) {
	const rangeCount = 510
	ranges := make([]kv.KeyRange, rangeCount)
	for i := range ranges {
		start := make([]byte, 8)
		end := make([]byte, 8)
		binary.BigEndian.PutUint64(start, uint64(i*2))
		binary.BigEndian.PutUint64(end, uint64(i*2+2))
		ranges[i] = kv.KeyRange{StartKey: start, EndKey: end}
	}
	keyRanges := NewKeyRanges(ranges)
	keys := map[string][]byte{
		"inside":   make([]byte, 8),
		"boundary": make([]byte, 8),
	}
	binary.BigEndian.PutUint64(keys["inside"], rangeCount+1)
	binary.BigEndian.PutUint64(keys["boundary"], rangeCount)
	factories := []struct {
		name string
		fn   func(*KeyRanges, []byte) *KeyRanges
	}{
		{name: "left/legacy", fn: func(ranges *KeyRanges, key []byte) *KeyRanges {
			left, _ := ranges.Split(key)
			return left
		}},
		{name: "left/one-sided", fn: func(ranges *KeyRanges, key []byte) *KeyRanges {
			return ranges.splitLeft(key)
		}},
		{name: "right/legacy", fn: func(ranges *KeyRanges, key []byte) *KeyRanges {
			_, right := ranges.Split(key)
			return right
		}},
		{name: "right/one-sided", fn: func(ranges *KeyRanges, key []byte) *KeyRanges {
			return ranges.splitRight(key)
		}},
	}

	for keyName, splitKey := range keys {
		for _, factory := range factories {
			b.Run(fmt.Sprintf("%s/%s", keyName, factory.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					benchmarkSplitResult = factory.fn(keyRanges, splitKey)
				}
			})
		}
	}
}
