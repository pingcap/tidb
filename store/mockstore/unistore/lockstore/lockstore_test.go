// Copyright 2019-present PingCAP, Inc.
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

package lockstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (ts testSuite) SetUpSuite(c *C) {}

func (ts testSuite) TearDownSuite(c *C) {}

var _ = Suite(testSuite{})

func (ts testSuite) TestMemStore(c *C) {
	prefix := "ls"
	n := 30000
	ls := NewMemStore(1 << 10)
	val := ls.Get([]byte("a"), nil)
	c.Assert(val, HasLen, 0)
	insertMemStore(ls, prefix, "", n)
	numBlocks := len(ls.getArena().blocks)
	checkMemStore(c, ls, prefix, "", n)
	deleteMemStore(c, ls, prefix, n)
	c.Assert(len(ls.getArena().blocks), Equals, numBlocks)
	time.Sleep(reuseSafeDuration)
	insertMemStore(ls, prefix, "", n)
	// Because the height is random, we insert again, the block number may be different.
	diff := len(ls.getArena().blocks) - numBlocks
	c.Assert(diff < numBlocks/100, IsTrue)
	c.Assert(ls.Get(numToKey(n), nil), HasLen, 0)
	c.Assert(ls.Get([]byte("abc"), nil), HasLen, 0)
}

const keyFormat = "%s%020d"

func insertMemStore(ls *MemStore, prefix, valPrefix string, n int) *MemStore {
	perms := rand.Perm(n)
	hint := new(Hint)
	for _, v := range perms {
		keyStr := fmt.Sprintf(keyFormat, prefix, v)
		key := []byte(keyStr)
		val := []byte(valPrefix + keyStr)
		ls.PutWithHint(key, val, hint)
	}
	return ls
}

func checkMemStore(c *C, ls *MemStore, prefix, valPrefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		val := ls.Get(key, nil)
		c.Assert(bytes.Equal(val[:len(valPrefix)], []byte(valPrefix)), IsTrue)
		c.Assert(bytes.Equal(key, val[len(valPrefix):]), IsTrue)
	}
}

func deleteMemStore(c *C, ls *MemStore, prefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		c.Assert(ls.Delete(key), IsTrue)
	}
}

func (ts testSuite) TestIterator(c *C) {
	_ = checkKey
	c.Skip("Skip this unstable test(#26235) and bring it back before 2021-07-29.")
	ls := NewMemStore(1 << 10)
	hint := new(Hint)
	for i := 10; i < 1000; i += 10 {
		key := []byte(fmt.Sprintf(keyFormat, "ls", i))
		ls.PutWithHint(key, bytes.Repeat(key, 10), hint)
	}
	c.Assert(ls.getArena().blocks, HasLen, 33)
	it := ls.NewIterator()
	it.SeekToFirst()
	checkKey(c, it, 10)
	it.Next()
	checkKey(c, it, 20)
	it.SeekToFirst()
	checkKey(c, it, 10)
	it.SeekToLast()
	checkKey(c, it, 990)
	it.Seek(numToKey(11))
	checkKey(c, it, 20)
	it.Seek(numToKey(989))
	checkKey(c, it, 990)
	it.Seek(numToKey(0))
	checkKey(c, it, 10)

	it.Seek(numToKey(2000))
	c.Assert(it.Valid(), IsFalse)
	it.Seek(numToKey(500))
	checkKey(c, it, 500)
	it.Prev()
	checkKey(c, it, 490)
	it.SeekForPrev(numToKey(100))
	checkKey(c, it, 100)
	it.SeekForPrev(numToKey(99))
	checkKey(c, it, 90)

	it.SeekForPrev(numToKey(2000))
	checkKey(c, it, 990)
}

func checkKey(c *C, it *Iterator, n int) {
	c.Assert(it.Valid(), IsTrue)
	c.Assert(bytes.Equal(it.Key(), []byte(fmt.Sprintf(keyFormat, "ls", n))), IsTrue)
	c.Assert(bytes.Equal(it.Value(), bytes.Repeat(it.Key(), 10)), IsTrue)
}

func numToKey(n int) []byte {
	return []byte(fmt.Sprintf(keyFormat, "ls", n))
}

func (ts testSuite) TestReplace(c *C) {
	prefix := "ls"
	n := 30000
	ls := NewMemStore(1 << 10)
	insertMemStore(ls, prefix, "old", n)
	checkMemStore(c, ls, prefix, "old", n)
	insertMemStore(ls, prefix, "new", n)
	checkMemStore(c, ls, prefix, "new", n)
}

func (ts testSuite) TestConcurrent(c *C) {
	keyRange := 10
	concurrentKeys := make([][]byte, keyRange)
	for i := 0; i < keyRange; i++ {
		concurrentKeys[i] = numToKey(i)
	}

	lock := sync.RWMutex{}
	ls := NewMemStore(1 << 20)
	// Starts 10 readers and 1 writer.
	closeCh := make(chan bool)
	for i := 0; i < keyRange; i++ {
		go runReader(ls, &lock, closeCh, i)
	}
	ran := rand.New(rand.NewSource(time.Now().Unix()))
	start := time.Now()
	var totalInsert, totalDelete int
	hint := new(Hint)
	for {
		if totalInsert%128 == 0 && time.Since(start) > time.Second*10 {
			break
		}
		n := ran.Intn(keyRange)
		key := concurrentKeys[n]
		lock.Lock()
		if ls.PutWithHint(key, key, hint) {
			totalInsert++
		}
		lock.Unlock()
		n = ran.Intn(keyRange)
		key = concurrentKeys[n]
		lock.Lock()
		if ls.DeleteWithHint(key, hint) {
			totalDelete++
		}
		lock.Unlock()
	}
	close(closeCh)
	time.Sleep(time.Millisecond * 100)
	arena := ls.getArena()
	fmt.Println("total insert", totalInsert, "total delete", totalDelete)
	fmt.Println(len(arena.pendingBlocks), len(arena.writableQueue), len(arena.blocks))
}

func runReader(ls *MemStore, lock *sync.RWMutex, closeCh chan bool, i int) {
	key := numToKey(i)
	buf := make([]byte, 100)
	var n int
	for {
		n++
		if n%128 == 0 {
			select {
			case <-closeCh:
				fmt.Println("read", n)
				return
			default:
			}
		}
		lock.RLock()
		result := ls.Get(key, buf)
		lock.RUnlock()
		if len(result) > 0 && !bytes.Equal(key, result) {
			panic("data corruption")
		}
	}
}

func BenchmarkMemStoreDeleteInsertGet(b *testing.B) {
	ls := NewMemStore(1 << 23)
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = numToKey(i)
		ls.Put(keys[i], keys[i])
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := r.Intn(10000)
		ls.Delete(keys[n])
		ls.Put(keys[n], keys[n])
		ls.Get(keys[n], buf)
	}
}

func BenchmarkMemStoreIterate(b *testing.B) {
	ls := NewMemStore(1 << 23)
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = numToKey(i)
		ls.Put(keys[i], keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := ls.NewIterator()
		it.SeekToFirst()
		for it.Valid() {
			it.Next()
		}
	}
}

func BenchmarkPutWithHint(b *testing.B) {
	ls := NewMemStore(1 << 20)
	numKeys := 100000
	keys := make([][]byte, numKeys)
	hint := new(Hint)
	for i := 0; i < numKeys; i++ {
		keys[i] = numToKey(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		ls.PutWithHint(keys[idx], keys[idx], hint)
	}
}

func BenchmarkPut(b *testing.B) {
	ls := NewMemStore(1 << 20)
	numKeys := 100000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = numToKey(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		ls.Put(keys[idx], keys[idx])
	}
}
