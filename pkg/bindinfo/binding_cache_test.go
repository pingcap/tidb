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

	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/stretchr/testify/require"
)

func bindingFuzzyDigest(t *testing.T, b Binding) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(b.BindSQL, b.Charset, b.Collation)
	require.NoError(t, err)
	_, fuzzyDigest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
	return fuzzyDigest
}

func TestFuzzyBindingCache(t *testing.T) {
	fbc := newFuzzyBindingCache(nil).(*fuzzyBindingCache)
	b1 := Binding{BindSQL: "SELECT * FROM db1.t1", SQLDigest: "b1"}
	fDigest1 := bindingFuzzyDigest(t, b1)
	b2 := Binding{BindSQL: "SELECT * FROM db2.t1", SQLDigest: "b2"}
	b3 := Binding{BindSQL: "SELECT * FROM db2.t3", SQLDigest: "b3"}
	fDigest3 := bindingFuzzyDigest(t, b3)

	// add 3 bindings and b1 and b2 have the same fuzzy digest
	require.NoError(t, fbc.SetBinding(b1.SQLDigest, []Binding{b1}))
	require.NoError(t, fbc.SetBinding(b2.SQLDigest, []Binding{b2}))
	require.NoError(t, fbc.SetBinding(b3.SQLDigest, []Binding{b3}))
	require.Equal(t, len(fbc.fuzzy2SQLDigests), 2) // b1 and b2 have the same fuzzy digest
	require.Equal(t, len(fbc.fuzzy2SQLDigests[fDigest1]), 2)
	require.Equal(t, len(fbc.fuzzy2SQLDigests[fDigest3]), 1)
	require.Equal(t, len(fbc.sql2FuzzyDigest), 3)
	_, ok := fbc.sql2FuzzyDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.sql2FuzzyDigest[b2.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.sql2FuzzyDigest[b3.SQLDigest]
	require.True(t, ok)

	// remove b2
	fbc.RemoveBinding(b2.SQLDigest)
	require.Equal(t, len(fbc.fuzzy2SQLDigests), 2)
	require.Equal(t, len(fbc.fuzzy2SQLDigests[fDigest1]), 1)
	require.Equal(t, len(fbc.fuzzy2SQLDigests[fDigest3]), 1)
	require.Equal(t, len(fbc.sql2FuzzyDigest), 2)
	_, ok = fbc.sql2FuzzyDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.sql2FuzzyDigest[b2.SQLDigest]
	require.False(t, ok) // can't find b2 now
	_, ok = fbc.sql2FuzzyDigest[b3.SQLDigest]
	require.True(t, ok)

	// test deep copy
	newCache, err := fbc.Copy()
	require.NoError(t, err)
	newFBC := newCache.(*fuzzyBindingCache)
	newFBC.fuzzy2SQLDigests[fDigest1] = nil
	delete(newFBC.sql2FuzzyDigest, b1.SQLDigest)
	require.Equal(t, len(fbc.fuzzy2SQLDigests[fDigest1]), 1) // no impact to the original cache
	_, ok = fbc.sql2FuzzyDigest[b1.SQLDigest]
	require.True(t, ok)
}

func TestBindCache(t *testing.T) {
	variable.MemQuotaBindingCache.Store(250)
	bindCache := newBindCache().(*bindingCache)

	value := make([]Bindings, 3)
	key := make([]bindingCacheKey, 3)
	var bigKey string
	for i := 0; i < 3; i++ {
		cacheKey := strings.Repeat(strconv.Itoa(i), 50)
		key[i] = bindingCacheKey(hack.Slice(cacheKey))
		value[i] = []Binding{{OriginalSQL: cacheKey}}
		bigKey += cacheKey

		require.Equal(t, int64(116), calcBindCacheKVMem(key[i], value[i]))
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
	require.NotNil(t, err) // exceed the memory limit
	result = bindCache.get(key[2])
	require.NotNil(t, result)

	// key[0] is not in the cache
	result = bindCache.get(key[0])
	require.Nil(t, result)

	// key[1] is still in the cache
	result = bindCache.get(key[1])
	require.NotNil(t, result)

	bigBindCacheKey := bindingCacheKey(hack.Slice(bigKey))
	bigBindCacheValue := []Binding{{OriginalSQL: strings.Repeat("x", 100)}}
	require.Equal(t, int64(266), calcBindCacheKVMem(bigBindCacheKey, bigBindCacheValue))
	ok, err = bindCache.set(bigBindCacheKey, bigBindCacheValue)
	require.False(t, ok) // the key-value pair is too big to be cached
	require.NotNil(t, err)
	result = bindCache.get(bigBindCacheKey)
	require.Nil(t, result)
}
