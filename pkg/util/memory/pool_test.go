// Copyright 2025 PingCAP, Inc.
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

package memory

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func (p *ResourcePool) setLimit(newLimit int64) {
	atomic.StoreInt64(&p.limit, newLimit)
}

func TestPoolAllocations(t *testing.T) {
	maxs := []int64{1, 9, 10, 11, 99, 100, 101, 0}
	factors := []int{1, 2, 10, 10000}
	poolAllocSizes := []int64{1, 2, 9, 10, 11, 100}
	preBudgets := []int64{0, 1, 2, 9, 10, 11, 100}

	rnd := rand.New(rand.NewSource(1))

	var pool *ResourcePool

	m := NewResourcePoolDefault("test", 0)
	m.StartNoReserved(nil)
	accs := make([]Budget, 4)
	for i := range accs {
		accs[i] = m.CreateBudget()
	}

	checkInvariants := func() {
		var sum int64
		fail := false
		for accI := range accs {
			if accs[accI].used < 0 {
				t.Errorf("budget %d < 0: %d", accI, accs[accI].used)
				fail = true
			}
			sum += accs[accI].Capacity()
		}
		if m.mu.allocated < 0 {
			t.Errorf("pool size < 0: %d", m.mu.allocated)
			fail = true
		}
		if sum != m.mu.allocated {
			t.Errorf("total budget sum %d different from pool size %d", sum, m.mu.allocated)
			fail = true
		}
		if m.mu.budget.used < 0 {
			t.Errorf("pool budget < 0: %d", m.mu.budget.used)
			fail = true
		}
		avail := m.mu.budget.Capacity() + m.reserved
		if sum > avail {
			t.Errorf("total budget sum %d greater than total pool budget %d", sum, avail)
			fail = true
		}
		if pool.mu.allocated > pool.reserved {
			t.Errorf("pool cur %d exceeds max %d", pool.mu.allocated, pool.reserved)
			fail = true
		}
		if m.mu.budget.Capacity() != pool.mu.allocated {
			t.Errorf("pool budget %d different from pool cur %d", m.mu.budget.used, pool.mu.allocated)
			fail = true
		}

		if fail {
			t.Fatal("invariants not preserved")
		}
	}

	const numBudgetOps = 200

	for _, max := range maxs {
		pool = NewResourcePoolDefault("test", 1)
		pool.Start(nil, (max))

		for _, hf := range factors {
			pool.maxUnusedBlocks = int64(hf)

			for _, pb := range preBudgets {
				mmax := pb + max

				for _, pa := range poolAllocSizes {
					m = NewResourcePoolDefault("test", pa)
					m.Start(pool, (pb))

					for range numBudgetOps {
						accI := rnd.Intn(len(accs))
						switch rnd.Intn(3) {
						case 0:
							sz := randomSize(rnd, mmax)
							checkInvariants()
							accs[accI].Grow(sz)
							checkInvariants()
						case 1:
							checkInvariants()
							accs[accI].Clear()
							checkInvariants()
						case 2:
							osz := rnd.Int63n(accs[accI].used + 1)
							nsz := randomSize(rnd, mmax)
							checkInvariants()
							accs[accI].resize(osz, nsz)
							checkInvariants()
						}
					}

					for accI := range accs {
						checkInvariants()
						accs[accI].Clear()
						checkInvariants()
					}

					m.Stop()
					if pool.mu.allocated != 0 {
						t.Fatalf("pool not empty after pool close: %d", pool.mu.allocated)
					}
				}
			}
		}
		pool.Stop()
	}
}

func TestBudget(t *testing.T) {
	p := NewResourcePoolDefault("test", 1)
	p.Start(nil, (100))
	p.allocAlignSize = 1
	p.maxUnusedBlocks = 1

	a1 := p.CreateBudget()
	a2 := p.CreateBudget()
	if err := a1.Grow(10); err != nil {
		t.Fatalf("pool refused allocation: %v", err)
	}

	if err := a2.Grow(30); err != nil {
		t.Fatalf("pool refused allocation: %v", err)
	}

	if err := a1.Grow(61); err == nil {
		t.Fatalf("pool accepted excessive allocation")
	}

	if err := a2.Grow(61); err == nil {
		t.Fatalf("pool accepted excessive allocation")
	}

	a1.Clear()

	if err := a2.Grow(61); err != nil {
		t.Fatalf("pool refused allocation: %v", err)
	}

	if err := a2.resize(50, 60); err == nil {
		t.Fatalf("pool accepted excessive allocation")
	}

	if err := a1.resize(0, 5); err != nil {
		t.Fatalf("pool refused allocation: %v", err)
	}

	if err := a2.resize(a2.used, 40); err != nil {
		t.Fatalf("pool refused reset + allocation: %v", err)
	}

	a1.Clear()
	a2.Clear()

	if p.mu.allocated != 0 {
		t.Fatal("closing spans leaves bytes in pool")
	}

	if m2 := a1.Pool(); m2 != p {
		t.Fatalf("a1.Pool() returned %v, wanted %v", m2, &p)
	}

	p.Stop()
}

func TestNilBudget(t *testing.T) {
	var ba *Budget
	_ = ba.Used()
	_ = ba.Pool()
	_ = ba.Capacity()
	ba.Empty()
	ba.Clear()
	ba.Clear()
	require.Nil(t, ba.resize(0, 10))
	require.Nil(t, ba.ResizeTo(10))
	require.Nil(t, ba.Grow(10))
	ba.Shrink(10)
}

func TestResourcePool(t *testing.T) {
	p := NewResourcePoolDefault("test", 1)
	p.Start(nil, (100))
	p.maxUnusedBlocks = 1

	if err := p.allocate(10); err != nil {
		t.Fatalf("pool refused small allocation: %v", err)
	}
	if err := p.allocate(91); err == nil {
		t.Fatalf("pool accepted excessive allocation: %v", err)
	}
	if err := p.allocate(90); err != nil {
		t.Fatalf("pool refused top allocation: %v", err)
	}
	if p.mu.allocated != 100 {
		t.Fatalf("incorrect allocation: got %d, expected %d", p.mu.allocated, 100)
	}

	p.release(90)
	if p.mu.allocated != 10 {
		t.Fatalf("incorrect allocation: got %d, expected %d", p.mu.allocated, 10)
	}
	if p.mu.maxAllocated != 100 {
		t.Fatalf("incorrect max allocation: got %d, expected %d", p.mu.maxAllocated, 100)
	}
	if p.MaxAllocated() != 100 {
		t.Fatalf("incorrect MaximumBytes(): got %d, expected %d", p.mu.maxAllocated, 100)
	}

	p.release(10)
	if p.mu.allocated != 0 {
		t.Fatalf("incorrect allocation: got %d, expected %d", p.mu.allocated, 0)
	}

	limitedPool := newResourcePoolWithLimit(
		"testlimit", 10, 1)
	limitedPool.StartNoReserved(p)

	if err := limitedPool.allocate(10); err != nil {
		t.Fatalf("limited pool refused small allocation: %v", err)
	}
	if err := limitedPool.allocate(1); err == nil {
		t.Fatal("limited pool allowed allocation over limit")
	}

	limitedPool.release(10)

	limitedPool.Stop()
	p.Stop()
}

func newResourcePoolWithLimit(name string, limiit, allocAlignSize int64) *ResourcePool {
	return NewResourcePool(
		newPoolUID(), name, limiit, allocAlignSize, DefMaxUnusedBlocks, PoolActions{})
}

func TestMemoryAllocationEdgeCases(t *testing.T) {
	m := NewResourcePoolDefault("test", 1e9)
	m.Start(nil, (1e9))

	a := m.CreateBudget()
	if err := a.Grow(1); err != nil {
		t.Fatal(err)
	}
	if err := a.Grow(math.MaxInt64); err == nil {
		t.Fatalf("expected error, but found success")
	}

	a.Clear()
	m.Stop()
}

func TestMultiSharedGauge(t *testing.T) {
	minAllocation := int64(1000)

	parent := NewResourcePoolDefault("root", minAllocation)
	parent.Start(nil, (100000))

	child := parent.NewResourcePoolInheritWithLimit("child", 20000)
	child.StartNoReserved(parent)

	acc := child.CreateBudget()
	require.NoError(t, acc.Grow(100))

	require.Equal(t, minAllocation, parent.Allocated())
}

func TestActions(t *testing.T) {
	{
		root := NewResourcePoolDefault("root", 1000)
		root.Start(nil, math.MaxInt64)
		p1 := NewResourcePoolDefault("p1", 666)
		p1.StartNoReserved(root)
		require.NoError(t, p1.ExplicitReserve(1002))
		require.Equal(t, p1.Capacity(), root.allocAlignSize*2)
		require.True(t, p1.mu.budget.explicitReserved == 1002)
		pb1 := p1.CreateBudget()
		require.NoError(t, pb1.Reserve(123))
		require.Equal(t, p1.Capacity(), root.allocAlignSize*2)
		require.Equal(t, pb1.Capacity(), p1.allocAlignSize)
		require.True(t, pb1.explicitReserved == 123)
		require.NoError(t, pb1.Grow(123))
		pb1.Shrink(123)
		require.Equal(t, p1.Capacity(), root.allocAlignSize*2)
		pb1.Clear()
	}

	{
		name := "root"
		outOfCapacityActionNum := 0
		root := NewResourcePoolDefault(
			name,
			1,
		)
		root.StartNoReserved(nil)

		b := root.CreateBudget()

		{
			err := b.Grow(1)
			require.Error(t, err)
		}

		root.SetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
			require.Equal(t, s.Pool.name, name)
			s.Pool.forceAddCap(s.Request)
			outOfCapacityActionNum++
			return nil
		})

		require.NoError(t, b.Grow(1))

		require.Equal(t, outOfCapacityActionNum, 1)

		require.NoError(t, b.Grow(10))

		require.Equal(t, outOfCapacityActionNum, 2)

		require.Equal(t, root.mu.budget.used, b.Used())

		b.Clear()

		require.NoError(t, b.Grow(5))

		require.Equal(t, outOfCapacityActionNum, 2)

		b.Clear()

		outOfLimitActionCnt := 0
		root.SetOutOfLimitAction(func(r *ResourcePool) error {
			outOfLimitActionCnt++
			root.setLimit(root.capacity())
			return fmt.Errorf("")
		})
		root.SetLimit(1)
		require.True(t, root.Limit() == 1)
		require.Error(t, b.Grow(2))
		require.Equal(t, outOfLimitActionCnt, 1)
		err := b.Grow(root.capacity() - root.allocated())
		require.NoError(t, err)
		require.Equal(t, outOfLimitActionCnt, 1)
	}
}

func genPool(
	name string, parent *ResourcePool,
) *ResourcePool {
	var reservedBytes int64
	if parent == nil {
		reservedBytes = math.MaxInt64
	}
	return getPoolImpl(name, parent, reservedBytes)
}

func getPoolImpl(
	name string, parent *ResourcePool, reservedBytes int64,
) *ResourcePool {
	m := NewResourcePoolDefault(name, 1)
	m.Start(parent, (reservedBytes))
	return m
}

func getPoolUsed(
	t *testing.T,
	name string,
	parent *ResourcePool,
	usedBytes, reservedBytes int64,
) *ResourcePool {
	m := getPoolImpl(name, parent, reservedBytes)
	if usedBytes != 0 {
		acc := m.CreateBudget()
		if err := acc.Grow(usedBytes); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func TestResourcePoolTree(t *testing.T) {
	export := func(p *ResourcePool) string {
		var pools []ResourcePoolState
		_ = p.Traverse(func(pool ResourcePoolState) error {
			pools = append(pools, pool)
			return nil
		})
		var sb strings.Builder
		for _, e := range pools {
			for range e.Level {
				sb.WriteString("-")
			}
			sb.WriteString(e.Name + "\n")
		}
		return sb.String()
	}

	parent := genPool("parent", nil)
	child1 := genPool("child1", parent)
	child2 := genPool("child2", parent)
	require.Equal(t, "parent\n-child2\n-child1\n", export(parent))
	require.Equal(t, "child1\n", export(child1))
	require.Equal(t, "child2\n", export(child2))

	grandchild1 := genPool("grandchild1", child1)
	grandchild2 := genPool("grandchild2", child2)
	require.Equal(t, "parent\n-child2\n--grandchild2\n-child1\n--grandchild1\n", export(parent))
	require.Equal(t, "child1\n-grandchild1\n", export(child1))
	require.Equal(t, "child2\n-grandchild2\n", export(child2))

	grandchild2.Stop()
	child2.Stop()

	require.Equal(t, "parent\n-child1\n--grandchild1\n", export(parent))
	require.Equal(t, "child1\n-grandchild1\n", export(child1))

	grandchild1.Stop()
	child1.Stop()

	require.Equal(t, "parent\n", export(parent))
	parent.Stop()
}

func TestResourcePoolUsedFromReserved(t *testing.T) {
	root := genPool("root", nil)
	const usedBytes = int64(1 << 10)
	child := getPoolUsed(t, "child", root, usedBytes, 2*usedBytes)

	require.NoError(t, root.Traverse(func(s ResourcePoolState) error {
		if s.Name == child.Name() {
			require.Equal(t, usedBytes, s.Used)
		}
		return nil
	}))
}

func TestResourcePoolNoDeadlocks(t *testing.T) {
	root := genPool("root", nil)
	defer root.Stop()

	var wg sync.WaitGroup
	const numGoroutines = 10
	done := make(chan struct{})
	for i := range numGoroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(1))
			for {
				select {
				case <-done:
					return
				default:
					func() {
						m := genPool(fmt.Sprintf("m%d", i), root)
						defer m.Stop()
						numOps := rng.Intn(10 + 1)
						var reserved int64
						defer func() {
							m.release(reserved)
						}()
						for op := 0; op < numOps; op++ {
							if reserved > 0 && rng.Float64() < 0.5 {
								toRelease := int64(rng.Intn(int(reserved))) + 1
								m.release(toRelease)
								reserved -= toRelease
							} else {
								toReserve := int64(rng.Intn(1000) + 1)
								_ = m.allocate(toReserve)
								reserved += toReserve
							}
							time.Sleep(time.Duration(rng.Intn(1000)) * time.Microsecond)
						}
					}()
					time.Sleep(time.Duration(rng.Intn(2000)) * time.Microsecond)
				}
			}
		}(i)
	}

	rng := rand.New(rand.NewSource(1))
	for range 1000 {
		var pools []ResourcePoolState
		_ = root.Traverse(func(pool ResourcePoolState) error {
			pools = append(pools, pool)
			return nil
		})
		for _, m := range pools {
			require.NotEqual(t, ResourcePoolState{}, m)
		}
		time.Sleep(time.Duration(rng.Intn(3000)) * time.Microsecond)
	}
	close(done)
	wg.Wait()
}

func BenchmarkBudgetGrow(b *testing.B) {
	m := NewResourcePoolDefault("test",
		1e9,
	)
	m.Start(nil, (1e9))

	a := m.CreateBudget()
	for range b.N {
		_ = a.Grow(1)
	}
}

func BenchmarkTraverseTree(b *testing.B) {
	makePoolTree := func(numLevels int, numChildrenPerPool int) (root *ResourcePool, cleanup func()) {
		allPools := make([][]*ResourcePool, numLevels)
		allPools[0] = []*ResourcePool{genPool("root", nil)}
		for level := 1; level < numLevels; level++ {
			allPools[level] = make([]*ResourcePool, 0, len(allPools[level-1])*numChildrenPerPool)
			for parent, parentMon := range allPools[level-1] {
				for child := range numChildrenPerPool {
					name := fmt.Sprintf("child%d_parent%d", child, parent)
					allPools[level] = append(allPools[level], genPool(name, parentMon))
				}
			}
		}
		cleanup = func() {
			for i := len(allPools) - 1; i >= 0; i-- {
				for _, m := range allPools[i] {
					m.Stop()
				}
			}
		}
		return allPools[0][0], cleanup
	}
	for _, numLevels := range []int{2, 4, 8} {
		for _, numChildrenPerPool := range []int{2, 4, 8} {
			b.Run(fmt.Sprintf("levels=%d/children=%d", numLevels, numChildrenPerPool), func(b *testing.B) {
				root, cleanup := makePoolTree(numLevels, numChildrenPerPool)
				defer cleanup()
				b.ResetTimer()
				for range b.N {
					var numPools int
					_ = root.Traverse(func(ResourcePoolState) error {
						numPools++
						return nil
					})
				}
			})
		}
	}
}

func randomSize(rnd *rand.Rand, mag int64) int64 {
	return int64(rnd.ExpFloat64() * float64(mag) * 0.3679)
}
