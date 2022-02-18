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

	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestBindCache(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuotaBindCache = 100
	bindCache := newBindCache(ctx)

	value := make([][]*BindRecord, 3)
	key := make([]bindCacheKey, 3)
	for i := 0; i < 3; i++ {
		cacheKey := strings.Repeat(strconv.Itoa(i), 50)
		key[i] = bindCacheKey(hack.Slice(cacheKey))
		record := &BindRecord{OriginalSQL: cacheKey, Db: ""}
		value[i] = []*BindRecord{record}

		require.Equal(t, int64(100), bindCacheKVMem(key[i], value[i]))
	}

	ok := bindCache.set(key[0], value[0])
	require.True(t, ok)
	result := bindCache.get(key[0])
	require.NotNil(t, result)

	ok = bindCache.set(key[1], value[1])
	require.True(t, ok)
	result = bindCache.get(key[1])
	require.NotNil(t, result)

	ok = bindCache.set(key[2], value[2])
	require.True(t, ok)
	result = bindCache.get(key[2])
	require.NotNil(t, result)

	// Both key[0] and key[1] are not in the cache
	result = bindCache.get(key[0])
	require.Nil(t, result)

	result = bindCache.get(key[1])
	require.Nil(t, result)
}
