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

package mvmap

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func BenchmarkMVMapPut(b *testing.B) {
	m := NewMVMap()
	buffer := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buffer, uint64(i))
		m.Put(buffer, buffer)
	}
}

func BenchmarkMVMapGet(b *testing.B) {
	m := NewMVMap()
	buffer := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buffer, uint64(i))
		m.Put(buffer, buffer)
	}
	val := make([][]byte, 0, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buffer, uint64(i))
		val = m.Get(buffer, val[:0])
		if len(val) != 1 || !bytes.Equal(val[0], buffer) {
			b.FailNow()
		}
	}
}
