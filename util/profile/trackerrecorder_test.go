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

package profile

import (
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/util/kvcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeapProfileRecorder(t *testing.T) {
	t.Parallel()

	// As runtime.MemProfileRate default values is 512 KB , so the num should be greater than 60000
	// that the memory usage of the values would be greater than 512 KB.
	num := 60000
	lru := kvcache.NewSimpleLRUCache(uint(num), 0, 0)

	keys := make([]*mockCacheKey, num)
	for i := 0; i < num; i++ {
		keys[i] = newMockHashKey(int64(i))
		v := getRandomString(10)
		lru.Put(keys[i], v)
	}
	bytes, err := col.getFuncMemUsage(kvcache.ProfileName)
	require.Nil(t, err)

	valueSize := int(unsafe.Sizeof(getRandomString(10)))
	// ensure that the consumed bytes is at least larger than num * size of value
	assert.LessOrEqual(t, int64(valueSize*num), bytes)
	// we should assert lru size last and value size to reference lru in order to avoid gc
	for _, k := range lru.Keys() {
		assert.Len(t, k.Hash(), 8)
	}
	for _, v := range lru.Values() {
		assert.Len(t, v.(string), 10)
	}
}

type mockCacheKey struct {
	hash []byte
	key  int64
}

func (mk *mockCacheKey) Hash() []byte {
	if mk.hash != nil {
		return mk.hash
	}
	mk.hash = make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		mk.hash[i] = byte((mk.key >> ((i - 1) * 8)) & 0xff)
	}
	return mk.hash
}

func newMockHashKey(key int64) *mockCacheKey {
	return &mockCacheKey{
		key: key,
	}
}

func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}
