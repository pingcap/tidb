// Copyright 2019 PingCAP, Inc.
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

package memdb

import (
	"encoding/binary"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
)

const (
	keySize   = 16
	valueSize = 128
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testMemDBSuite struct{}

var _ = Suite(testMemDBSuite{})

func (s testMemDBSuite) TestGetSet(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)

	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v := p.Get(buf[:])
		c.Check(v, BytesEquals, buf[:])
	}
}

func (s testMemDBSuite) TestIterator(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)

	var buf [4]byte
	var i int
	it := p.NewIterator()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		c.Check(it.Value(), BytesEquals, buf[:])
		i++
	}
	c.Check(i, Equals, cnt)

	i--
	for it.SeekToLast(); it.Valid(); it.Prev() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		c.Check(it.Value(), BytesEquals, buf[:])
		i--
	}
	c.Check(i, Equals, -1)
}

func (s testMemDBSuite) TestOverwrite(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)
	var buf [4]byte

	for i := 0; i < cnt; i += 3 {
		var newBuf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		binary.BigEndian.PutUint32(newBuf[:], uint32(i*10))
		p.Put(buf[:], newBuf[:])
	}

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v := binary.BigEndian.Uint32(p.Get(buf[:]))
		if i%3 == 0 {
			c.Check(v, Equals, uint32(i*10))
		} else {
			c.Check(v, Equals, uint32(i))
		}
	}

	it := p.NewIterator()
	var i int

	for it.SeekToFirst(); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Check(v, Equals, uint32(i*10))
		} else {
			c.Check(v, Equals, uint32(i))
		}
		i++
	}
	c.Check(i, Equals, cnt)

	i--
	for it.SeekToLast(); it.Valid(); it.Prev() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Check(v, Equals, uint32(i*10))
		} else {
			c.Check(v, Equals, uint32(i))
		}
		i--
	}
	c.Check(i, Equals, -1)
}

func (s testMemDBSuite) TestDelete(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)
	var buf [4]byte

	for i := 0; i < cnt; i += 3 {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		p.Delete(buf[:])
	}

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v := p.Get(buf[:])
		if i%3 == 0 {
			c.Check(v, IsNil)
		} else {
			c.Check(v, BytesEquals, buf[:])
		}
	}

	it := p.NewIterator()
	var i int

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if i%3 == 0 {
			i++
		}
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		c.Check(it.Value(), BytesEquals, buf[:])
		i++
	}

	i--
	for it.SeekToLast(); it.Valid(); it.Prev() {
		if i%3 == 0 {
			i--
		}
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Check(it.Key(), BytesEquals, buf[:])
		c.Check(it.Value(), BytesEquals, buf[:])
		i--
	}
}

func (s testMemDBSuite) fillDB(cnt int) *DB {
	p := New(4 * 1024 * 1024)
	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		p.Put(buf[:], buf[:])
	}
	return p
}

func BenchmarkLargeIndex(b *testing.B) {
	buf := make([][valueSize]byte, 10000000)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}
	p := New(4 * 1024 * 1024)
	b.ResetTimer()

	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPut(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(4 * 1024 * 1024)
	b.ResetTimer()

	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := New(4 * 1024 * 1024)
	b.ResetTimer()

	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(4 * 1024 * 1024)
	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(buf[i][:])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(4 * 1024 * 1024)
	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(buf[rand.Int()%b.N][:])
	}
}
