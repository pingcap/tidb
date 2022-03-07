// Copyright 2022 PingCAP, Inc.
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

package bindinfo

import (
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/require"
)

func TestBindCache(t *testing.T) {
	variable.MemQuotaBindCache.Store(200)
	bindCache := newBindCache()

	value := make([][]*BindRecord, 3)
	key := make([]bindCacheKey, 3)
	var bigKey string
	for i := 0; i < 3; i++ {
		cacheKey := strings.Repeat(strconv.Itoa(i), 50)
		key[i] = bindCacheKey(hack.Slice(cacheKey))
		record := &BindRecord{OriginalSQL: cacheKey, Db: ""}
		value[i] = []*BindRecord{record}
		bigKey += cacheKey

		require.Equal(t, int64(100), calcBindCacheKVMem(key[i], value[i]))
	}

	ok, err := bindCache.set(key[0], value[0])
	require.True(t, ok)
	require.Nil(t, err)
	result := bindCache.get(key[0])
	require.NotNil(t, result)

	ok, err = bindCache.set(key[1], value[1])
	require.True(t, ok)
	require.Nil(t, err)
	result = bindCache.get(key[1])
	require.NotNil(t, result)

	ok, err = bindCache.set(key[2], value[2])
	require.True(t, ok)
	require.NotNil(t, err)
	result = bindCache.get(key[2])
	require.NotNil(t, result)

	// key[0] is not in the cache
	result = bindCache.get(key[0])
	require.Nil(t, result)

	// key[1] is still in the cache
	result = bindCache.get(key[1])
	require.NotNil(t, result)

	bigBindCacheKey := bindCacheKey(hack.Slice(bigKey))
	bigRecord := &BindRecord{OriginalSQL: bigKey, Db: ""}
	bigBindCacheValue := []*BindRecord{bigRecord}
	require.Equal(t, int64(300), calcBindCacheKVMem(bigBindCacheKey, bigBindCacheValue))
	ok, err = bindCache.set(bigBindCacheKey, bigBindCacheValue)
	require.False(t, ok)
	require.NotNil(t, err)
	result = bindCache.get(bigBindCacheKey)
	require.Nil(t, result)
}
