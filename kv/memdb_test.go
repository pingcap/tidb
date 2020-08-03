// Copyright 2020 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
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
	"fmt"
	"math"
	"testing"

	. "github.com/pingcap/check"
	leveldb "github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/pingcap/tidb/util/testleak"
)

func init() {
	testMode = true
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var (
	_ = Suite(&testKVSuite{})
	_ = Suite(&testMemDBSuite{})
)

type testMemDBSuite struct{}

// DeleteKey is used in test to verify the `deleteNode` used in `vlog.revertToCheckpoint`.
func (db *memdb) DeleteKey(key []byte) {
	x := db.tranverse(key, false)
	if x.isNull() {
		return
	}
	db.count--
	db.size -= (len(db.vlog.getValue(x.vptr)) + int(x.klen))
	db.deleteNode(x)
}

func (s *testMemDBSuite) TestGetSet(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)

	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := p.Get(context.TODO(), buf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, buf[:])
	}
}

func (s *testMemDBSuite) TestBigKV(c *C) {
	db := newMemDB()
	db.entrySizeLimit = math.MaxUint64
	db.bufferSizeLimit = math.MaxUint64
	db.Set([]byte{1}, make([]byte, 80<<20))
	c.Assert(db.vlog.blockSize, Equals, maxBlockSize)
	c.Assert(len(db.vlog.blocks), Equals, 1)
	h := db.Staging()
	db.Set([]byte{2}, make([]byte, 127<<20))
	db.Release(h)
	c.Assert(db.vlog.blockSize, Equals, maxBlockSize)
	c.Assert(len(db.vlog.blocks), Equals, 2)
	c.Assert(func() { db.Set([]byte{3}, make([]byte, maxBlockSize+1)) }, Panics, "alloc size is larger than max block size")
}

func (s *testMemDBSuite) TestIterator(c *C) {
	const cnt = 10000
	db := s.fillDB(cnt)

	var buf [4]byte
	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestDiscard(c *C) {
	const cnt = 10000
	db := newMemDB()
	base := s.deriveAndFill(0, cnt, 0, db)
	sz := db.Size()

	db.Cleanup(s.deriveAndFill(0, cnt, 1, db))
	c.Assert(db.Len(), Equals, cnt)
	c.Assert(db.Size(), Equals, sz)

	var buf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := db.Get(context.TODO(), buf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, buf[:])
	}

	var i int
	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i--
	}
	c.Assert(i, Equals, -1)

	db.Cleanup(base)
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		_, err := db.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)
	}
	it1, _ := db.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestFlushOverwrite(c *C) {
	const cnt = 10000
	db := newMemDB()
	db.Release(s.deriveAndFill(0, cnt, 0, db))
	sz := db.Size()

	db.Release(s.deriveAndFill(0, cnt, 1, db))

	c.Assert(db.Len(), Equals, cnt)
	c.Assert(db.Size(), Equals, sz)

	var kbuf, vbuf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		v, err := db.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, vbuf[:])
	}

	var i int
	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestComplexUpdate(c *C) {
	const (
		keep      = 3000
		overwrite = 6000
		insert    = 9000
	)

	db := newMemDB()
	db.Release(s.deriveAndFill(0, overwrite, 0, db))
	c.Assert(db.Len(), Equals, overwrite)
	db.Release(s.deriveAndFill(keep, insert, 1, db))
	c.Assert(db.Len(), Equals, insert)

	var kbuf, vbuf [4]byte

	for i := 0; i < insert; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i >= keep {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := db.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, vbuf[:])
	}
}

func (s *testMemDBSuite) TestNestedSandbox(c *C) {
	db := newMemDB()
	h0 := s.deriveAndFill(0, 200, 0, db)
	h1 := s.deriveAndFill(0, 100, 1, db)
	h2 := s.deriveAndFill(50, 150, 2, db)
	h3 := s.deriveAndFill(100, 120, 3, db)
	h4 := s.deriveAndFill(0, 150, 4, db)
	db.Cleanup(h4) // Discard (0..150 -> 4)
	db.Release(h3) // Flush (100..120 -> 3)
	db.Cleanup(h2) // Discard (100..120 -> 3) & (50..150 -> 2)
	db.Release(h1) // Flush (0..100 -> 1)
	db.Release(h0) // Flush (0..100 -> 1) & (0..200 -> 0)
	// The final result should be (0..100 -> 1) & (101..200 -> 0)

	var kbuf, vbuf [4]byte

	for i := 0; i < 200; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := db.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, vbuf[:])
	}

	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Assert(i, Equals, 200)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestOverwrite(c *C) {
	const cnt = 10000
	db := s.fillDB(cnt)
	var buf [4]byte

	sz := db.Size()
	for i := 0; i < cnt; i += 3 {
		var newBuf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		binary.BigEndian.PutUint32(newBuf[:], uint32(i*10))
		db.Set(buf[:], newBuf[:])
	}
	c.Assert(db.Len(), Equals, cnt)
	c.Assert(db.Size(), Equals, sz)

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		val, _ := db.Get(context.TODO(), buf[:])
		v := binary.BigEndian.Uint32(val)
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
	}

	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestKVLargeThanBlock(c *C) {
	db := newMemDB()
	db.Set([]byte{1}, make([]byte, 1))
	db.Set([]byte{2}, make([]byte, 4096))
	c.Assert(len(db.vlog.blocks), Equals, 2)
	db.Set([]byte{3}, make([]byte, 3000))
	c.Assert(len(db.vlog.blocks), Equals, 2)
	val, err := db.Get(context.TODO(), []byte{3})
	c.Assert(err, IsNil)
	c.Assert(len(val), Equals, 3000)
}

func (s *testMemDBSuite) TestEmptyDB(c *C) {
	db := newMemDB()
	_, err := db.Get(context.TODO(), []byte{0})
	c.Assert(err, NotNil)
	it1, _ := db.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestReset(c *C) {
	db := s.fillDB(1000)
	db.Reset()
	_, err := db.Get(context.TODO(), []byte{0, 0, 0, 0})
	c.Assert(err, NotNil)
	it1, _ := db.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestInspectStage(c *C) {
	db := newMemDB()
	h1 := s.deriveAndFill(0, 1000, 0, db)
	h2 := s.deriveAndFill(500, 1000, 1, db)
	for i := 500; i < 1500; i++ {
		var kbuf [4]byte
		// don't update in place
		var vbuf [5]byte
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+2))
		db.Set(kbuf[:], vbuf[:])
	}
	h3 := s.deriveAndFill(1000, 2000, 3, db)

	db.InspectStage(h3, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 1000 && k < 2000, IsTrue)
		c.Assert(v-k, DeepEquals, 3)
	})

	db.InspectStage(h2, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 500 && k < 2000, IsTrue)
		if k < 1000 {
			c.Assert(v-k, Equals, 2)
		} else {
			c.Assert(v-k, Equals, 3)
		}
	})

	db.Cleanup(h3)
	db.Release(h2)

	db.InspectStage(h1, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 0 && k < 1500, IsTrue)
		if k < 500 {
			c.Assert(v-k, Equals, 0)
		} else {
			c.Assert(v-k, Equals, 2)
		}
	})

	db.Release(h1)
}

func (s *testMemDBSuite) TestDirty(c *C) {
	db := newMemDB()
	db.Set([]byte{1}, []byte{1})
	c.Assert(db.Dirty(), IsTrue)

	db = newMemDB()
	h := db.Staging()
	db.Set([]byte{1}, []byte{1})
	db.Cleanup(h)
	c.Assert(db.Dirty(), IsFalse)

	h = db.Staging()
	db.Set([]byte{1}, []byte{1})
	db.Release(h)
	c.Assert(db.Dirty(), IsTrue)

	// persistent flags will make memdb dirty.
	db = newMemDB()
	h = db.Staging()
	db.SetWithFlags([]byte{1}, []byte{1}, SetPessimisticLock)
	db.Cleanup(h)
	c.Assert(db.Dirty(), IsTrue)

	// non-persistent flags will not make memdb dirty.
	db = newMemDB()
	h = db.Staging()
	db.SetWithFlags([]byte{1}, []byte{1}, SetPresumeKeyNotExists)
	db.Cleanup(h)
	c.Assert(db.Dirty(), IsFalse)
}

func (s *testMemDBSuite) TestFlags(c *C) {
	const cnt = 10000
	db := newMemDB()
	h := db.Staging()
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		if i%2 == 0 {
			db.SetWithFlags(buf[:], buf[:], SetPresumeKeyNotExists, SetPessimisticLock)
		} else {
			db.SetWithFlags(buf[:], buf[:], SetPresumeKeyNotExists)
		}
	}
	db.Cleanup(h)

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := db.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)
		flags, err := db.GetFlags(buf[:])
		if i%2 == 0 {
			c.Assert(err, IsNil)
			c.Assert(flags.HasPessimisticLock(), IsTrue)
			c.Assert(flags.HasPresumeKeyNotExists(), IsFalse)
		} else {
			c.Assert(err, NotNil)
		}
	}

	c.Assert(db.Len(), Equals, 0)
	c.Assert(db.Size(), Equals, 0)

	it1, _ := db.Iter(nil, nil)
	it := it1.(*memdbIterator)
	c.Assert(it.Valid(), IsFalse)

	it.includeFlags = true
	it.init()

	for ; it.Valid(); it.Next() {
		k := binary.BigEndian.Uint32(it.Key())
		c.Assert(k%2 == 0, IsTrue)
	}

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		db.UpdateFlags(buf[:], DelPessimisticLock)
	}
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := db.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)

		// UpdateFlags will create missing node.
		flags, err := db.GetFlags(buf[:])
		c.Assert(err, IsNil)
		c.Assert(flags.HasPessimisticLock(), IsFalse)
	}
}

func (s *testMemDBSuite) checkConsist(c *C, p1 *memdb, p2 *leveldb.DB) {
	c.Assert(p1.Len(), Equals, p2.Len())
	c.Assert(p1.Size(), Equals, p2.Size())

	it1, _ := p1.Iter(nil, nil)
	it2 := p2.NewIterator(nil)

	var prevKey, prevVal []byte
	for it2.First(); it2.Valid(); it2.Next() {
		v, err := p1.Get(context.TODO(), it2.Key())
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, it2.Value())

		c.Assert([]byte(it1.Key()), BytesEquals, it2.Key())
		c.Assert(it1.Value(), BytesEquals, it2.Value())

		it, _ := p1.Iter(it2.Key(), nil)
		c.Assert([]byte(it.Key()), BytesEquals, it2.Key())
		c.Assert(it.Value(), BytesEquals, it2.Value())

		if prevKey != nil {
			it, _ = p1.IterReverse(it2.Key())
			c.Assert([]byte(it.Key()), BytesEquals, prevKey)
			c.Assert(it.Value(), BytesEquals, prevVal)
		}

		it1.Next()
		prevKey = it2.Key()
		prevVal = it2.Value()
	}

	it1, _ = p1.IterReverse(nil)
	for it2.Last(); it2.Valid(); it2.Prev() {
		c.Assert([]byte(it1.Key()), BytesEquals, it2.Key())
		c.Assert(it1.Value(), BytesEquals, it2.Value())
		it1.Next()
	}
}

func (s *testMemDBSuite) fillDB(cnt int) *memdb {
	db := newMemDB()
	h := s.deriveAndFill(0, cnt, 0, db)
	db.Release(h)
	return db
}

func (s *testMemDBSuite) deriveAndFill(start, end, valueBase int, db *memdb) StagingHandle {
	h := db.Staging()
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		db.Set(kbuf[:], vbuf[:])
	}
	return h
}

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

type testKVSuite struct {
	bs []MemBuffer
}

func (s *testKVSuite) SetUpSuite(c *C) {
	s.bs = make([]MemBuffer, 1)
	s.bs[0] = newMemDB()
}

func (s *testKVSuite) ResetMembuffers() {
	s.bs[0] = newMemDB()
}

func insertData(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := buffer.Set(val, val)
		c.Assert(err, IsNil)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(c *C, iter Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkNewIterator(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(valToStr(c, iter), Equals, string(val))

		err = iter.Next()
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsTrue)

		val = encodeInt((i + 1) * indexStep)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(valToStr(c, iter), Equals, string(val))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter, err := buffer.Iter(encodeInt(testCount*indexStep), nil)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = buffer.Iter(inBetween, nil)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert([]byte(iter.Key()), Not(BytesEquals), inBetween)
	c.Assert([]byte(iter.Key()), BytesEquals, last)
	iter.Close()
}

func mustGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := buffer.Get(context.TODO(), s)
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, string(s))
	}
}

func (s *testKVSuite) TestGetSet(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		insertData(c, buffer)
		mustGet(c, buffer)
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		// should be invalid
		iter, err := buffer.Iter(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsFalse)

		insertData(c, buffer)
		checkNewIterator(c, buffer)
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestIterNextUntil(c *C) {
	defer testleak.AfterTest(c)()
	buffer := newMemDB()
	insertData(c, buffer)

	iter, err := buffer.Iter(nil, nil)
	c.Assert(err, IsNil)

	err = NextUntil(iter, func(k Key) bool {
		return false
	})
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
}

func (s *testKVSuite) TestBasicNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		it, err := buffer.Iter([]byte("2"), nil)
		c.Assert(err, IsNil)
		c.Assert(it.Valid(), IsFalse)
	}
}

func (s *testKVSuite) TestNewIteratorMin(c *C) {
	defer testleak.AfterTest(c)()
	kvs := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}
	for _, buffer := range s.bs {
		for _, kv := range kvs {
			err := buffer.Set([]byte(kv.key), []byte(kv.value))
			c.Assert(err, IsNil)
		}

		cnt := 0
		it, err := buffer.Iter(nil, nil)
		c.Assert(err, IsNil)
		for it.Valid() {
			cnt++
			err := it.Next()
			c.Assert(err, IsNil)
		}
		c.Assert(cnt, Equals, 6)

		it, err = buffer.Iter([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"), nil)
		c.Assert(err, IsNil)
		c.Assert(string([]byte(it.Key())), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestMemDBStaging(c *C) {
	buffer := newMemDB()
	err := buffer.Set([]byte("x"), make([]byte, 2))
	c.Assert(err, IsNil)

	h1 := buffer.Staging()
	err = buffer.Set([]byte("x"), make([]byte, 3))
	c.Assert(err, IsNil)

	h2 := buffer.Staging()
	err = buffer.Set([]byte("yz"), make([]byte, 1))
	c.Assert(err, IsNil)

	v, _ := buffer.Get(context.Background(), []byte("x"))
	c.Assert(len(v), Equals, 3)

	buffer.Release(h2)

	v, _ = buffer.Get(context.Background(), []byte("yz"))
	c.Assert(len(v), Equals, 1)

	buffer.Cleanup(h1)

	v, _ = buffer.Get(context.Background(), []byte("x"))
	c.Assert(len(v), Equals, 2)
}

func (s *testKVSuite) TestBufferLimit(c *C) {
	buffer := newMemDB()
	buffer.bufferSizeLimit = 1000
	buffer.entrySizeLimit = 500

	err := buffer.Set([]byte("x"), make([]byte, 500))
	c.Assert(err, NotNil) // entry size limit

	err = buffer.Set([]byte("x"), make([]byte, 499))
	c.Assert(err, IsNil)
	err = buffer.Set([]byte("yz"), make([]byte, 499))
	c.Assert(err, NotNil) // buffer size limit

	err = buffer.Delete(make([]byte, 499))
	c.Assert(err, IsNil)

	err = buffer.Delete(make([]byte, 500))
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestBufferBatchGetter(c *C) {
	snap := &mockSnapshot{store: newMemDB()}
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	snap.store.Set(ka, ka)
	snap.store.Set(kb, kb)
	snap.store.Set(kc, kc)
	snap.store.Set(kd, kd)

	// middle value is the same as snap
	middle := newMemDB()
	middle.Set(ka, []byte("a1"))
	middle.Set(kc, []byte("c1"))

	buffer := newMemDB()
	buffer.Set(ka, []byte("a2"))
	buffer.Delete(kb)

	batchGetter := NewBufferBatchGetter(buffer, middle, snap)
	result, err := batchGetter.BatchGet(context.Background(), []Key{ka, kb, kc, kd})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(string(result[string(ka)]), Equals, "a2")
	c.Assert(string(result[string(kc)]), Equals, "c1")
	c.Assert(string(result[string(kd)]), Equals, "d")
}
