// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/goleveldb/leveldb/comparer"
)

func BenchmarkPut(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	b.ResetTimer()
	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}
}

func BenchmarkConcurrentPut(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			db := New(comparer.DefaultComparer, 0)
			for i := 0; i < len(buf); i++ {
				db.Put(buf[i][:], nil)
			}
			db.Free()
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	b.ResetTimer()
	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(buf[i][:])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(buf[rand.Int()%b.N][:])
	}
}
