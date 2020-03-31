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

func (s testMemDBSuite) TestReset(c *C) {
	p := New(4 * 1024)
	p.Reset()
	s.deriveAndFill(0, 10000, 0, &p.root).Flush()
	p.Reset()
	c.Check(p.Get([]byte{0}), IsNil)
	c.Check(p.Size(), Equals, 0)
	c.Check(p.Len(), Equals, 0)

	key := []byte{0}
	p.Put(key, key)
	c.Check(p.Get(key), BytesEquals, key)
	c.Check(p.root.arena.availIdx, Equals, 0)

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

func (s testMemDBSuite) TestDiscard(c *C) {
	const cnt = 10000
	p := NewSandbox(4 * 1024)
	s.deriveAndFill(0, cnt, 0, p).Flush()
	sz := p.Size()

	s.deriveAndFill(0, cnt, 1, p).Discard()
	c.Check(p.Len(), Equals, cnt)
	c.Check(p.Size(), Equals, sz)

	var buf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v := p.Get(buf[:])
		c.Check(v, BytesEquals, buf[:])
	}

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

func (s testMemDBSuite) TestFlushOverwrite(c *C) {
	const cnt = 10000
	p := NewSandbox(4 * 1024)
	s.deriveAndFill(0, cnt, 0, p).Flush()
	sz := p.Size()

	s.deriveAndFill(0, cnt, 1, p).Flush()

	c.Check(p.Len(), Equals, cnt)
	c.Check(p.Size(), Equals, sz)

	var kbuf, vbuf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		v := p.Get(kbuf[:])
		c.Check(v, BytesEquals, vbuf[:])
	}

	var i int
	it := p.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Check(it.Key(), BytesEquals, kbuf[:])
		c.Check(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Check(i, Equals, cnt)

	i--
	for it.SeekToLast(); it.Valid(); it.Prev() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Check(it.Key(), BytesEquals, kbuf[:])
		c.Check(it.Value(), BytesEquals, vbuf[:])
		i--
	}
	c.Check(i, Equals, -1)
}

func (s testMemDBSuite) TestComplexUpdate(c *C) {
	const (
		keep      = 3000
		overwrite = 6000
		insert    = 9000
	)

	p := NewSandbox(4 * 1024)
	s.deriveAndFill(0, overwrite, 0, p).Flush()
	c.Check(p.Len(), Equals, overwrite)
	s.deriveAndFill(keep, insert, 1, p).Flush()
	c.Check(p.Len(), Equals, insert)

	var kbuf, vbuf [4]byte

	for i := 0; i < insert; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i >= keep {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v := p.Get(kbuf[:])
		c.Check(v, BytesEquals, vbuf[:])
	}
}

func (s testMemDBSuite) TestNestedSandbox(c *C) {
	p := NewSandbox(4 * 1024)
	sb0 := s.deriveAndFill(0, 200, 0, p)
	sb1 := s.deriveAndFill(0, 100, 1, sb0)
	sb2 := s.deriveAndFill(50, 150, 2, sb1)
	sb3 := s.deriveAndFill(100, 120, 3, sb2)
	sb4 := s.deriveAndFill(0, 150, 4, sb3)
	sb4.Discard() // Discard (0..150 -> 4)
	sb3.Flush()   // Flush (100..120 -> 3)
	sb2.Discard() // Discard (100..120 -> 3) & (50..150 -> 2)
	sb1.Flush()   // Flush (0..100 -> 1)
	sb0.Flush()   // Flush (0..100 -> 1) & (0..200 -> 0)
	// The final result should be (0..100 -> 1) & (101..200 -> 0)

	var kbuf, vbuf [4]byte

	for i := 0; i < 200; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v := p.Get(kbuf[:])
		c.Check(v, BytesEquals, vbuf[:])
	}

	var i int
	it := p.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Check(it.Key(), BytesEquals, kbuf[:])
		c.Check(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Check(i, Equals, 200)

	i--
	for it.SeekToLast(); it.Valid(); it.Prev() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Check(it.Key(), BytesEquals, kbuf[:])
		c.Check(it.Value(), BytesEquals, vbuf[:])
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

func (s testMemDBSuite) TestKVLargeThanBlock(c *C) {
	p := NewSandbox(4 * 1024)
	p.Put([]byte{1}, make([]byte, 1))
	p.Put([]byte{2}, make([]byte, 4096))
	c.Check(len(p.arena.blocks), Equals, 2)
	p.Put([]byte{3}, make([]byte, 3000))
	c.Check(len(p.arena.blocks), Equals, 2)
	c.Check(len(p.Get([]byte{3})), Equals, 3000)
}

func (s testMemDBSuite) TestEmptyDB(c *C) {
	p := NewSandbox(4 * 1024)
	c.Check(p.Get([]byte{0}), IsNil)
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

func (s testMemDBSuite) TestRandom(c *C) {
	const cnt = 500000
	keys := make([][]byte, cnt)
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(19)+1)
		rand.Read(keys[i])
	}

	p1 := NewSandbox(4 * 1024)
	p2 := memdb.New(comparer.DefaultComparer, 4*1024)
	for _, k := range keys {
		p1.Put(k, k)
		_ = p2.Put(k, k)
	}

	c.Check(p1.Len(), Equals, p2.Len())
	c.Check(p1.Size(), Equals, p2.Size())

	rand.Shuffle(cnt, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		newValue := make([]byte, rand.Intn(19)+1)
		rand.Read(newValue)
		p1.Put(k, newValue)
		_ = p2.Put(k, newValue)
	}
	s.checkConsist(c, p1, p2)
}

func (s testMemDBSuite) TestRandomDerive(c *C) {
	s.testRandomDeriveRecur(c, NewSandbox(4*1024), memdb.New(comparer.DefaultComparer, 4*1024), 0)
}

func (s testMemDBSuite) testRandomDeriveRecur(c *C, sb *Sandbox, db *memdb.DB, depth int) {
	var keys [][]byte
	if rand.Float64() < 0.5 {
		start, end := rand.Intn(512), rand.Intn(512)+512
		cnt := end - start
		keys = make([][]byte, cnt)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(start+i))
		}
	} else {
		keys = make([][]byte, rand.Intn(512)+512)
		for i := range keys {
			keys[i] = make([]byte, rand.Intn(19)+1)
			rand.Read(keys[i])
		}
	}

	vals := make([][]byte, len(keys))
	for i := range vals {
		vals[i] = make([]byte, rand.Intn(255)+1)
		rand.Read(vals[i])
	}

	sbBuf := sb.Derive()
	dbBuf := memdb.New(comparer.DefaultComparer, 4*1024)
	for i := range keys {
		sbBuf.Put(keys[i], vals[i])
		_ = dbBuf.Put(keys[i], vals[i])
	}

	if depth < 1000 {
		s.testRandomDeriveRecur(c, sbBuf, dbBuf, depth+1)
	}

	if rand.Float64() < 0.3 && depth > 0 {
		sbBuf.Discard()
	} else {
		sbBuf.Flush()
		it := dbBuf.NewIterator(nil)
		for it.First(); it.Valid(); it.Next() {
			_ = db.Put(it.Key(), it.Value())
		}
	}
	s.checkConsist(c, sb, db)
}

func (s testMemDBSuite) checkConsist(c *C, p1 *Sandbox, p2 *memdb.DB) {
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

func (s testMemDBSuite) fillDB(cnt int) *Sandbox {
	p := NewSandbox(4 * 1024)
	s.deriveAndFill(0, cnt, 0, p).Flush()
	return p
}

func (s testMemDBSuite) deriveAndFill(start, end, valueBase int, parent *Sandbox) *Sandbox {
	sb := parent.Derive()
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		sb.Put(kbuf[:], vbuf[:])
	}
	return sb
}

func BenchmarkLargeIndex(b *testing.B) {
	buf := make([][valueSize]byte, 10000000)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}
	p := NewSandbox(4 * 1024 * 1024)
	b.ResetTimer()

	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkFlush(b *testing.B) {
	const size = 10000
	buf := make([][valueSize]byte, size)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	b.Run("naive-insert", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			p1 := NewSandbox(4 * 1024)
			p2 := NewSandbox(4 * 1024)
			for i := range buf[:len(buf)/2] {
				p1.Put(buf[i][:keySize], buf[i][:])
			}
			for i := range buf[len(buf)/2:] {
				p2.Put(buf[i][:keySize], buf[i][:])
			}
			b.StartTimer()
			it := p2.NewIterator()
			for it.SeekToFirst(); it.Valid(); it.Next() {
				p1.Put(it.Key(), it.Value())
			}
		}
	})

	b.Run("sandbox-flush", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			p1 := NewSandbox(4 * 1024)
			for i := range buf[:len(buf)/2] {
				p1.Put(buf[i][:keySize], buf[i][:])
			}

			p2 := p1.Derive()
			for i := range buf[len(buf)/2:] {
				p2.Put(buf[i][:keySize], buf[i][:])
			}
			b.StartTimer()
			p2.Flush()
		}
	})
}

func BenchmarkPut(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := NewSandbox(4 * 1024 * 1024)
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

	p := NewSandbox(4 * 1024 * 1024)
	b.ResetTimer()

	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := NewSandbox(4 * 1024 * 1024)
	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(buf[i][:keySize])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := NewSandbox(4 * 1024 * 1024)
	for i := range buf {
		p.Put(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(buf[i][:keySize])
	}
}
