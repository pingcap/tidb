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
	"hash/fnv"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestMVMap(t *testing.T) {
	m := NewMVMap()
	var vals [][]byte
	m.Put([]byte("abc"), []byte("abc1"))
	m.Put([]byte("abc"), []byte("abc2"))
	m.Put([]byte("def"), []byte("def1"))
	m.Put([]byte("def"), []byte("def2"))
	vals = m.Get([]byte("abc"), vals[:0])
	if fmt.Sprintf("%s", vals) != "[abc1 abc2]" {
		t.FailNow()
	}
	vals = m.Get([]byte("def"), vals[:0])
	if fmt.Sprintf("%s", vals) != "[def1 def2]" {
		t.FailNow()
	}

	if m.Len() != 4 {
		t.FailNow()
	}

	results := []string{"abc abc1", "abc abc2", "def def1", "def def2"}
	it := m.NewIterator()
	for i := 0; i < 4; i++ {
		key, val := it.Next()
		if fmt.Sprintf("%s %s", key, val) != results[i] {
			t.FailNow()
		}
	}
	key, val := it.Next()
	if key != nil || val != nil {
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

func TestFNVHash(t *testing.T) {
	b := []byte{0xcb, 0xf2, 0x9c, 0xe4, 0x84, 0x22, 0x23, 0x25}
	sum1 := fnvHash64(b)
	hash := fnv.New64()
	hash.Reset()
	hash.Write(b)
	sum2 := hash.Sum64()
	if sum1 != sum2 {
		t.FailNow()
	}
}
