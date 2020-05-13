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
	"time"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/kvcache"
)

var _ = Suite(&trackRecorderSuite{})

type trackRecorderSuite struct {
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

func (t *trackRecorderSuite) TestHeapProfileRecorder(c *C) {
	lru := kvcache.NewSimpleLRUCache(10000, 0, 0)

	keys := make([]*mockCacheKey, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = newMockHashKey(int64(i))
		v := getRandomString(10)
		lru.Put(keys[i], v)
	}

	bytes, err := col.getFuncMemUsage(kvcache.ProfileName)
	c.Assert(err, IsNil)
	valueSize := int(unsafe.Sizeof(getRandomString(10)))
	// ensure that the consumed bytes is at least larger than 10000 * size of value
	c.Assert(int64(valueSize*10000), LessEqual, bytes)
	// we should assert lru size last to reference lru in order to avoid gc
	c.Assert(lru.Size(), Equals, 10000)
}
