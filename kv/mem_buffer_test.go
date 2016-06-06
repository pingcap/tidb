// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
	bs []MemBuffer
}

func (s *testKVSuite) SetUpSuite(c *C) {
	s.bs = make([]MemBuffer, 2)
	s.bs[0] = NewRBTreeBuffer()
	s.bs[1] = NewMemDbBuffer()
}

func (s *testKVSuite) TearDownSuite(c *C) {
	for _, buffer := range s.bs {
		buffer.Release()
	}
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
		iter, err := buffer.Seek(val)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Seek(val)
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
	iter, err := buffer.Seek(encodeInt(testCount * indexStep))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = buffer.Seek(inBetween)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert([]byte(iter.Key()), Not(BytesEquals), inBetween)
	c.Assert([]byte(iter.Key()), BytesEquals, last)
	iter.Close()
}

func mustNotGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		_, err := buffer.Get(s)
		c.Assert(err, NotNil)
	}
}

func mustGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := buffer.Get(s)
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, string(s))
	}
}

func (s *testKVSuite) TestGetSet(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		insertData(c, buffer)
		mustGet(c, buffer)
		buffer.Release()
	}
}

func (s *testKVSuite) TestNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		// should be invalid
		iter, err := buffer.Seek(nil)
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsFalse)

		insertData(c, buffer)
		checkNewIterator(c, buffer)
		buffer.Release()
	}
}

func (s *testKVSuite) TestBasicNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		it, err := buffer.Seek([]byte("2"))
		c.Assert(err, IsNil)
		c.Assert(it.Valid(), IsFalse)
		buffer.Release()
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
			buffer.Set([]byte(kv.key), []byte(kv.value))
		}

		cnt := 0
		it, err := buffer.Seek(nil)
		c.Assert(err, IsNil)
		for it.Valid() {
			cnt++
			it.Next()
		}
		c.Assert(cnt, Equals, 6)

		it, err = buffer.Seek([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"))
		c.Assert(err, IsNil)
		c.Assert(string(it.Key()), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")

		buffer.Release()
	}
}

var opCnt = 100000

func BenchmarkRBTreeBufferSequential(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := NewRBTreeBuffer()
	benchmarkSetGet(b, buffer, data)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkRBTreeBufferRandom(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := NewRBTreeBuffer()
	benchmarkSetGet(b, buffer, data)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkMemDbBufferSequential(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := NewMemDbBuffer()
	benchmarkSetGet(b, buffer, data)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkMemDbBufferRandom(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := NewMemDbBuffer()
	benchmarkSetGet(b, buffer, data)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkRBTreeIter(b *testing.B) {
	buffer := NewRBTreeBuffer()
	benchIterator(b, buffer)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkMemDbIter(b *testing.B) {
	buffer := NewMemDbBuffer()
	benchIterator(b, buffer)
	buffer.Release()
	b.ReportAllocs()
}

func BenchmarkRBTreeCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := NewRBTreeBuffer()
		buffer.Release()
	}
	b.ReportAllocs()
}

func BenchmarkMemDbCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := NewMemDbBuffer()
		buffer.Release()
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
			buffer.Get(k)
		}
	}
}

func benchIterator(b *testing.B, buffer MemBuffer) {
	for k := 0; k < opCnt; k++ {
		buffer.Set(encodeInt(k), encodeInt(k))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := buffer.Seek(nil)
		if err != nil {
			b.Error(err)
		}
		for iter.Valid() {
			iter.Next()
		}
		iter.Close()
	}
}
