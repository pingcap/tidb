// Package core Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func randomPlanCacheKey() string {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("%v", random.Int())
}

func randomPlanCacheValue(types []*types.FieldType) *PlanCacheValue {
	plans := []base.Plan{&Insert{}, &Update{}, &Delete{}, &PhysicalTableScan{}, &PhysicalTableDual{}, &PhysicalTableReader{},
		&PhysicalTableScan{}, &PhysicalIndexJoin{}, &PhysicalIndexHashJoin{}, &PhysicalIndexMergeJoin{}, &PhysicalIndexMergeReader{},
		&PhysicalIndexLookUpReader{}, &PhysicalApply{}, &PhysicalApply{}, &PhysicalLimit{}}
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &PlanCacheValue{
		Plan:       plans[random.Int()%len(plans)],
		ParamTypes: types,
	}
}

func TestLRUPCPut(t *testing.T) {
	// test initialize
	mockCtx := MockContext()
	mockCtx.GetSessionVars().EnablePlanCacheForParamLimit = true
	defer func() {
		domain.GetDomain(mockCtx).StatsHandle().Close()
	}()
	lruA := NewLRUPlanCache(0, 0, 0, mockCtx, false)
	require.Equal(t, lruA.capacity, uint(100))

	dropCnt := 0
	lru := NewLRUPlanCache(3, 0, 0, mockCtx, false)
	lru.onEvict = func(key string, value any) {
		dropCnt++
	}
	require.Equal(t, uint(3), lru.capacity)

	keys := make([]string, 5)
	vals := make([]*PlanCacheValue, 5)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeLong)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeInt24)},
	}

	// one key corresponding to multi values
	for i := 0; i < 5; i++ {
		keys[i] = "key-1"
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{
			ParamTypes: opts,
		}
		lru.Put(keys[i], vals[i], opts)
	}
	require.Equal(t, lru.size, lru.capacity)
	require.Equal(t, uint(3), lru.size)

	// test for non-existent elements
	require.Equal(t, dropCnt, 2)
	for i := 0; i < 2; i++ {
		bucket, exist := lru.buckets[keys[i]]
		require.True(t, exist)
		for element := range bucket {
			require.NotEqual(t, vals[i], element.Value.(*planCacheEntry).PlanValue)
		}
	}

	// test for existent elements
	root := lru.lruList.Front()
	require.NotNil(t, root)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*planCacheEntry)
		require.True(t, ok)
		require.NotNil(t, entry)

		// test key
		key := entry.PlanKey
		require.NotNil(t, key)
		require.Equal(t, keys[i], key)

		bucket, exist := lru.buckets[keys[i]]
		require.True(t, exist)
		matchOpts := pTypes[i]
		element, exist := lru.pickFromBucket(bucket, matchOpts)
		require.NotNil(t, element)
		require.True(t, exist)
		require.Equal(t, root, element)

		// test value
		value, ok := entry.PlanValue.(*PlanCacheValue)
		require.True(t, ok)
		require.Equal(t, vals[i], value)

		root = root.Next()
	}

	// test for end of double-linked list
	require.Nil(t, root)
}

func TestLRUPCGet(t *testing.T) {
	mockCtx := MockContext()
	mockCtx.GetSessionVars().EnablePlanCacheForParamLimit = true
	defer func() {
		domain.GetDomain(mockCtx).StatsHandle().Close()
	}()
	lru := NewLRUPlanCache(3, 0, 0, mockCtx, false)

	keys := make([]string, 5)
	vals := make([]*PlanCacheValue, 5)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeLong)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeInt24)},
	}
	// 5 bucket
	for i := 0; i < 5; i++ {
		keys[i] = fmt.Sprintf("key-%v", i%4)
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{
			ParamTypes: opts,
		}
		lru.Put(keys[i], vals[i], opts)
	}

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		opts := pTypes[i]
		value, exists := lru.Get(keys[i], opts)
		require.False(t, exists)
		require.Nil(t, value)
	}

	for i := 2; i < 5; i++ {
		opts := pTypes[i]
		value, exists := lru.Get(keys[i], opts)
		require.True(t, exists)
		require.NotNil(t, value)
		require.Equal(t, vals[i], value)
		require.Equal(t, uint(3), lru.size)
		require.Equal(t, uint(3), lru.capacity)

		root := lru.lruList.Front()
		require.NotNil(t, root)

		entry, ok := root.Value.(*planCacheEntry)
		require.True(t, ok)
		require.Equal(t, keys[i], entry.PlanKey)

		value, ok = entry.PlanValue.(*PlanCacheValue)
		require.True(t, ok)
		require.Equal(t, vals[i], value)
	}
}

func TestLRUPCDelete(t *testing.T) {
	mockCtx := MockContext()
	mockCtx.GetSessionVars().EnablePlanCacheForParamLimit = true
	defer func() {
		domain.GetDomain(mockCtx).StatsHandle().Close()
	}()
	lru := NewLRUPlanCache(3, 0, 0, mockCtx, false)

	keys := make([]string, 3)
	vals := make([]*PlanCacheValue, 3)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
	}
	for i := 0; i < 3; i++ {
		keys[i] = fmt.Sprintf("key-%v", i)
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{
			ParamTypes: opts,
		}
		lru.Put(keys[i], vals[i], opts)
	}
	require.Equal(t, 3, int(lru.size))

	lru.Delete(keys[1])

	value, exists := lru.Get(keys[1], pTypes[1])
	require.False(t, exists)
	require.Nil(t, value)
	require.Equal(t, 2, int(lru.size))

	_, exists = lru.Get(keys[0], pTypes[0])
	require.True(t, exists)

	_, exists = lru.Get(keys[2], pTypes[2])
	require.True(t, exists)
}

func TestLRUPCDeleteAll(t *testing.T) {
	ctx := MockContext()
	lru := NewLRUPlanCache(3, 0, 0, ctx, false)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	keys := make([]string, 3)
	vals := make([]*PlanCacheValue, 3)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
	}
	for i := 0; i < 3; i++ {
		keys[i] = fmt.Sprintf("key-%v", i)
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{
			ParamTypes: opts,
		}
		lru.Put(keys[i], vals[i], opts)
	}
	require.Equal(t, 3, int(lru.size))

	lru.DeleteAll()

	for i := 0; i < 3; i++ {
		opts := pTypes[i]
		value, exists := lru.Get(keys[i], opts)
		require.False(t, exists)
		require.Nil(t, value)
		require.Equal(t, 0, int(lru.size))
	}
}

func TestLRUPCSetCapacity(t *testing.T) {
	ctx := MockContext()
	lru := NewLRUPlanCache(5, 0, 0, ctx, false)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	dropCnt := 0
	lru.onEvict = func(key string, value any) {
		dropCnt++
	}
	require.Equal(t, uint(5), lru.capacity)

	keys := make([]string, 5)
	vals := make([]*PlanCacheValue, 5)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeLong)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeInt24)},
	}

	// one key corresponding to multi values
	for i := 0; i < 5; i++ {
		keys[i] = "key-1"
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{
			ParamTypes: opts,
		}
		lru.Put(keys[i], vals[i], opts)
	}
	require.Equal(t, lru.size, lru.capacity)
	require.Equal(t, uint(5), lru.size)

	err := lru.SetCapacity(3)
	require.NoError(t, err)

	// test for non-existent elements
	require.Equal(t, dropCnt, 2)
	for i := 0; i < 2; i++ {
		bucket, exist := lru.buckets[keys[i]]
		require.True(t, exist)
		for element := range bucket {
			require.NotEqual(t, vals[i], element.Value.(*planCacheEntry).PlanValue)
		}
	}

	// test for existent elements
	root := lru.lruList.Front()
	require.NotNil(t, root)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*planCacheEntry)
		require.True(t, ok)
		require.NotNil(t, entry)

		// test value
		value, ok := entry.PlanValue.(*PlanCacheValue)
		require.True(t, ok)
		require.Equal(t, vals[i], value)

		root = root.Next()
	}

	// test for end of double-linked list
	require.Nil(t, root)

	err = lru.SetCapacity(0)
	require.ErrorContains(t, err, "capacity of LRU cache should be at least 1")
}

func TestIssue37914(t *testing.T) {
	ctx := MockContext()
	lru := NewLRUPlanCache(3, 0.1, 1, ctx, false)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	pTypes := []*types.FieldType{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)}
	key := "key-1"
	opts := pTypes
	val := &PlanCacheValue{ParamTypes: opts}

	require.NotPanics(t, func() {
		lru.Put(key, val, opts)
	})
}

func TestIssue38244(t *testing.T) {
	ctx := MockContext()
	lru := NewLRUPlanCache(3, 0, 0, ctx, false)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	require.Equal(t, uint(3), lru.capacity)

	keys := make([]string, 5)
	vals := make([]*PlanCacheValue, 5)
	pTypes := [][]*types.FieldType{{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeEnum)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDate)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeLong)},
		{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeInt24)},
	}

	// one key corresponding to multi values
	for i := 0; i < 5; i++ {
		keys[i] = fmt.Sprintf("key-%v", i)
		opts := pTypes[i]
		vals[i] = &PlanCacheValue{ParamTypes: opts}
		lru.Put(keys[i], vals[i], opts)
	}
	require.Equal(t, lru.size, lru.capacity)
	require.Equal(t, uint(3), lru.size)
	require.Equal(t, len(lru.buckets), 3)
}

func TestLRUPlanCacheMemoryUsage(t *testing.T) {
	pTypes := []*types.FieldType{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeDouble)}
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	ctx.GetSessionVars().EnablePreparedPlanCacheMemoryMonitor = true
	lru := NewLRUPlanCache(3, 0, 0, ctx, false)
	evict := make(map[string]any)
	lru.onEvict = func(key string, value any) {
		evict[key] = value
	}
	var res int64 = 0
	// put
	for i := 0; i < 3; i++ {
		k := randomPlanCacheKey()
		v := randomPlanCacheValue(pTypes)
		opts := pTypes
		lru.Put(k, v, opts)
		res += int64(len(k)) + v.MemoryUsage()
		require.Equal(t, lru.MemoryUsage(), res)
	}
	// evict
	p := &PhysicalTableScan{}
	k := "key-3"
	v := &PlanCacheValue{Plan: p}
	opts := pTypes
	lru.Put(k, v, opts)
	res += int64(len(k)) + v.MemoryUsage()
	for kk, vv := range evict {
		res -= int64(len(kk)) + vv.(*PlanCacheValue).MemoryUsage()
	}
	require.Equal(t, lru.MemoryUsage(), res)
	// delete
	lru.Delete(k)
	res -= int64(len(k)) + v.MemoryUsage()
	require.Equal(t, lru.MemoryUsage(), res)
	// delete all
	lru.DeleteAll()
	require.Equal(t, lru.MemoryUsage(), int64(0))
}
