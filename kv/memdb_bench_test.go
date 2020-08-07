// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

const (
	keySize   = 16
	valueSize = 128
)

func newMemDBForBench() *memdb {
	db := newMemDB()
	db.bufferSizeLimit = math.MaxUint64
	db.entrySizeLimit = math.MaxUint64
	return db
}

func BenchmarkLargeIndex(b *testing.B) {
	buf := make([][valueSize]byte, 10000000)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}
	db := newMemDBForBench()
	b.ResetTimer()

	for i := range buf {
		db.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPut(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := newMemDBForBench()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := newMemDBForBench()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := newMemDBForBench()
	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(context.TODO(), buf[i][:keySize])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := newMemDBForBench()
	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(context.TODO(), buf[i][:keySize])
	}
}

var opCnt = 100000

func BenchmarkMemDbBufferSequential(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferRandom(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbIter(b *testing.B) {
	buffer := newMemDB()
	benchIterator(b, buffer)
	b.ReportAllocs()
}

func BenchmarkMemDbCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		newMemDB()
	}
	b.ReportAllocs()
}

func shuffle(slc [][]byte) {
	N := len(slc)
	for i := 0; i < N; i++ {
		// choose index uniformly in [i, N-1]
		r := i + rand.Intn(N-i)
		slc[r], slc[i] = slc[i], slc[r]
	}
}
func benchmarkSetGet(b *testing.B, buffer MemBuffer, data [][]byte) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range data {
			buffer.Set(k, k)
		}
		for _, k := range data {
			buffer.Get(context.TODO(), k)
		}
	}
}

func benchIterator(b *testing.B, buffer MemBuffer) {
	for k := 0; k < opCnt; k++ {
		buffer.Set(encodeInt(k), encodeInt(k))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := buffer.Iter(nil, nil)
		if err != nil {
			b.Error(err)
		}
		for iter.Valid() {
			iter.Next()
		}
		iter.Close()
	}
}
