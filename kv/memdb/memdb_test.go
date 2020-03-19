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
	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/memdb"
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

	sz := p.Size()
	for i := 0; i < cnt; i += 3 {
		var newBuf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		binary.BigEndian.PutUint32(newBuf[:], uint32(i*10))
		p.Put(buf[:], newBuf[:])
	}
	c.Check(p.Len(), Equals, cnt)
	c.Check(p.Size(), Equals, sz)

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

func (s testMemDBSuite) TestKVLargeThanBlock(c *C) {
	p := New(4 * 1024)
	p.Put([]byte{1}, make([]byte, 1))
	p.Put([]byte{2}, make([]byte, 4096))
	c.Check(len(p.arena.blocks), Equals, 2)
	p.Put([]byte{3}, make([]byte, 3000))
	c.Check(len(p.arena.blocks), Equals, 2)
	c.Check(len(p.Get([]byte{3})), Equals, 3000)
}

func (s testMemDBSuite) TestEmptyDB(c *C) {
	p := New(4 * 1024)
	c.Check(p.Get([]byte{0}), IsNil)
	c.Check(p.Delete([]byte{0}), IsFalse)
	it := p.NewIterator()
	it.SeekToFirst()
	c.Check(it.Valid(), IsFalse)
	it.SeekToLast()
	c.Check(it.Valid(), IsFalse)
	it.SeekForPrev([]byte{0})
	c.Check(it.Valid(), IsFalse)
	it.SeekForExclusivePrev([]byte{0})
	c.Check(it.Valid(), IsFalse)
	it.Seek([]byte{0xff})
	c.Check(it.Valid(), IsFalse)
}

func (s testMemDBSuite) TestReset(c *C) {
	p := s.fillDB(10000)
	p.Reset()
	c.Check(p.Get([]byte{0}), IsNil)
	c.Check(p.Delete([]byte{0}), IsFalse)
	c.Check(p.Size(), Equals, 0)
	c.Check(p.Len(), Equals, 0)

	key := []byte{0}
	p.Put(key, key)
	c.Check(p.Get(key), BytesEquals, key)
	c.Check(p.arena.availIdx, Equals, 0)

	it := p.NewIterator()
	it.SeekToFirst()
	c.Check(it.Key(), BytesEquals, key)
	c.Check(it.Value(), BytesEquals, key)
	it.Next()
	c.Check(it.Valid(), IsFalse)

	it.SeekToLast()
	c.Check(it.Key(), BytesEquals, key)
	c.Check(it.Value(), BytesEquals, key)
	it.Prev()
	c.Check(it.Valid(), IsFalse)
}

func (s testMemDBSuite) TestRandom(c *C) {
	const cnt = 500000
	keys := make([][]byte, cnt)
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(19)+1)
		rand.Read(keys[i])
	}

	p1 := New(4 * 1024)
	p2 := memdb.New(comparer.DefaultComparer, 4*1024)
	for _, k := range keys {
		p1.Put(k, k)
		_ = p2.Put(k, k)
	}

	c.Check(p1.Len(), Equals, p2.Len())
	c.Check(p1.Size(), Equals, p2.Size())

	rand.Shuffle(cnt, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		switch rand.Intn(4) {
		case 0, 1:
			newValue := make([]byte, rand.Intn(19)+1)
			rand.Read(newValue)
			p1.Put(k, newValue)
			_ = p2.Put(k, newValue)
		case 2:
			p1.Delete(k)
			_ = p2.Delete(k)
		}
	}

	c.Check(p1.Len(), Equals, p2.Len())
	c.Check(p1.Size(), Equals, p2.Size())

	it1 := p1.NewIterator()
	it1.SeekToFirst()

	it2 := p2.NewIterator(nil)

	var prevKey, prevVal []byte
	for it2.First(); it2.Valid(); it2.Next() {
		c.Check(it1.Key(), BytesEquals, it2.Key())
		c.Check(it1.Value(), BytesEquals, it2.Value())

		it := p1.NewIterator()
		it.Seek(it2.Key())
		c.Check(it.Key(), BytesEquals, it2.Key())
		c.Check(it.Value(), BytesEquals, it2.Value())

		it.SeekForPrev(it2.Key())
		c.Check(it.Key(), BytesEquals, it2.Key())
		c.Check(it.Value(), BytesEquals, it2.Value())

		if prevKey != nil {
			it.SeekForExclusivePrev(it2.Key())
			c.Check(it.Key(), BytesEquals, prevKey)
			c.Check(it.Value(), BytesEquals, prevVal)
		}

		it1.Next()
		prevKey = it2.Key()
		prevVal = it2.Value()
	}

	it1.SeekToLast()
	for it2.Last(); it2.Valid(); it2.Prev() {
		c.Check(it1.Key(), BytesEquals, it2.Key())
		c.Check(it1.Value(), BytesEquals, it2.Value())
		it1.Prev()
	}
}

func (s testMemDBSuite) fillDB(cnt int) *DB {
	p := New(4 * 1024)
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
