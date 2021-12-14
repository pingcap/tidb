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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

	"github.com/stretchr/testify/require"
)

func TestMemStore(t *testing.T) {
	prefix := "ls"
	n := 30000
	ls := NewMemStore(1 << 10)
	val := ls.Get([]byte("a"), nil)
	require.Len(t, val, 0)
	insertMemStore(ls, prefix, "", n)
	numBlocks := len(ls.getArena().blocks)
	checkMemStore(t, ls, prefix, "", n)
	deleteMemStore(t, ls, prefix, n)
	require.Len(t, ls.getArena().blocks, numBlocks)
	time.Sleep(reuseSafeDuration)
	insertMemStore(ls, prefix, "", n)
	// Because the height is random, we insert again, the block number may be different.
	diff := len(ls.getArena().blocks) - numBlocks
	require.True(t, diff < numBlocks/100)
	require.Len(t, ls.Get(numToKey(n), nil), 0)
	require.Len(t, ls.Get([]byte("abc"), nil), 0)
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

func checkMemStore(t *testing.T, ls *MemStore, prefix, valPrefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		val := ls.Get(key, nil)
		require.True(t, bytes.Equal(val[:len(valPrefix)], []byte(valPrefix)))
		require.True(t, bytes.Equal(key, val[len(valPrefix):]))
	}
}

func deleteMemStore(t *testing.T, ls *MemStore, prefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		require.True(t, ls.Delete(key))
	}
}

func TestIterator(t *testing.T) {
	_ = checkKey
	t.Skip("Skip this unstable test(#26235) and bring it back before 2021-07-29.")
	ls := NewMemStore(1 << 10)
	hint := new(Hint)
	for i := 10; i < 1000; i += 10 {
		key := []byte(fmt.Sprintf(keyFormat, "ls", i))
		ls.PutWithHint(key, bytes.Repeat(key, 10), hint)
	}
	require.Len(t, ls.getArena().blocks, 33)
	it := ls.NewIterator()
	it.SeekToFirst()
	checkKey(t, it, 10)
	it.Next()
	checkKey(t, it, 20)
	it.SeekToFirst()
	checkKey(t, it, 10)
	it.SeekToLast()
	checkKey(t, it, 990)
	it.Seek(numToKey(11))
	checkKey(t, it, 20)
	it.Seek(numToKey(989))
	checkKey(t, it, 990)
	it.Seek(numToKey(0))
	checkKey(t, it, 10)

	it.Seek(numToKey(2000))
	require.False(t, it.Valid())
	it.Seek(numToKey(500))
	checkKey(t, it, 500)
	it.Prev()
	checkKey(t, it, 490)
	it.SeekForPrev(numToKey(100))
	checkKey(t, it, 100)
	it.SeekForPrev(numToKey(99))
	checkKey(t, it, 90)

	it.SeekForPrev(numToKey(2000))
	checkKey(t, it, 990)
}

func checkKey(t *testing.T, it *Iterator, n int) {
	require.True(t, it.Valid())
	require.True(t, bytes.Equal(it.Key(), []byte(fmt.Sprintf(keyFormat, "ls", n))))
	require.True(t, bytes.Equal(it.Value(), bytes.Repeat(it.Key(), 10)))
}

func numToKey(n int) []byte {
	return []byte(fmt.Sprintf(keyFormat, "ls", n))
}

func TestReplace(t *testing.T) {
	prefix := "ls"
	n := 30000
	ls := NewMemStore(1 << 10)
	insertMemStore(ls, prefix, "old", n)
	checkMemStore(t, ls, prefix, "old", n)
	insertMemStore(ls, prefix, "new", n)
	checkMemStore(t, ls, prefix, "new", n)
}

func TestMemStoreConcurrent(t *testing.T) {
	keyRange := 10
	concurrentKeys := make([][]byte, keyRange)
	for i := 0; i < keyRange; i++ {
		concurrentKeys[i] = numToKey(i)
	}

	lock := sync.RWMutex{}
	ls := NewMemStore(1 << 20)
	// Starts 10 readers and 1 writer.
	closeCh := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(keyRange)
	for i := 0; i < keyRange; i++ {
		go runReader(ls, &lock, closeCh, i, wg)
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
	wg.Wait()
	arena := ls.getArena()
	fmt.Println("total insert", totalInsert, "total delete", totalDelete)
	fmt.Println(len(arena.pendingBlocks), len(arena.writableQueue), len(arena.blocks))
}

func runReader(ls *MemStore, lock *sync.RWMutex, closeCh chan bool, i int, wg *sync.WaitGroup) {
	defer wg.Done()
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
