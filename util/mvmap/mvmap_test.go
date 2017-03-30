// Copyright 2017 PingCAP, Inc.
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

package mvmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestMVMap(t *testing.T) {
	m := NewMVMap()
	m.Put([]byte("abc"), []byte("abc1"))
	m.Put([]byte("abc"), []byte("abc2"))
	m.Put([]byte("def"), []byte("def1"))
	m.Put([]byte("def"), []byte("def2"))
	vals := m.Get([]byte("abc"))
	if fmt.Sprintf("%s", vals) != "[abc2 abc1]" {
		t.FailNow()
	}
	vals = m.Get([]byte("def"))
	if fmt.Sprintf("%s", vals) != "[def2 def1]" {
		t.FailNow()
	}
}

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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buffer, uint64(i))
		val := m.Get(buffer)
		if len(val) != 1 || bytes.Compare(val[0], buffer) != 0 {
			b.FailNow()
		}
	}
}
