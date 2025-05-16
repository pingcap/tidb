// Copyright 2018 PingCAP, Inc.
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

// randomSize generates a size greater or equal to zero, with a random
// distribution that is skewed towards zero and ensures that most
// generated values are smaller than `mag`.
func randomSize(rnd *rand.Rand, mag int64) int64 {
	return int64(rnd.ExpFloat64() * float64(mag) * 0.3679)
}

func TestMemoryAllocations(t *testing.T) {
	maxs := []int64{1, 9, 10, 11, 99, 100, 101, 0}
	hysteresisFactors := []int{1, 2, 10, 10000}
	poolAllocSizes := []int64{1, 2, 9, 10, 11, 100}
	preBudgets := []int64{0, 1, 2, 9, 10, 11, 100}

	rnd := rand.New(rand.NewSource(1))

	var pool *ResourcePool
	var paramHeader func()

	m := NewResourcePoolDefault("test", /* name */
		0 /* pool allocation size */)
	m.StartNoReserved(nil /* pool */)
	accs := make([]Budget, 4)
	for i := range accs {
		accs[i] = m.CreateBudget()
	}

	// The following invariants will be checked at every step of the
	// test underneath.
	checkInvariants := func() {
		t.Helper()

		var sum int64
		fail := false
		for accI := range accs {
			if accs[accI].used < 0 {
				t.Errorf("account %d went negative: %d", accI, accs[accI].used)
				fail = true
			}
			sum += accs[accI].Capacity()
		}
		if m.mu.allocated < 0 {
			t.Errorf("pool current count went negative: %d", m.mu.allocated)
			fail = true
		}
		if sum != m.mu.allocated {
			t.Errorf("total account sum %d different from pool count %d", sum, m.mu.allocated)
			fail = true
		}
		if m.mu.budget.used < 0 {
			t.Errorf("pool current budget went negative: %d", m.mu.budget.used)
			fail = true
		}
		avail := m.mu.budget.Capacity() + m.reserved
		if sum > avail {
			t.Errorf("total account sum %d greater than total pool budget %d", sum, avail)
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

	const numAccountOps = 200
	var linesBetweenHeaderReminders int
	var generateHeader func()
	var reportAndCheck func(string, ...any)
	{
		// Detailed output: report the intermediate values of the
		// important variables at every stage of the test.
		linesBetweenHeaderReminders = 5
		generateHeader = func() {
			fmt.Println("")
			paramHeader()
			fmt.Printf(" mcur  mbud  mpre  pool ")
			for accI := range accs {
				fmt.Printf("%5s ", fmt.Sprintf("a%d", accI))
			}
			fmt.Println("")
		}
		reportAndCheck = func(extraFmt string, extras ...any) {
			t.Helper()
			fmt.Printf("%5d %5d %5d %5d ", m.mu.allocated, m.mu.budget.used, m.reserved, pool.mu.allocated)
			for accI := range accs {
				fmt.Printf("%5d ", accs[accI].used)
			}
			fmt.Print("\t")
			fmt.Printf(extraFmt, extras...)
			fmt.Println("")
			checkInvariants()
		}
	}

	for _, max := range maxs {
		pool = NewResourcePoolDefault("test", 1)
		pool.Start(nil, (max))

		for _, hf := range hysteresisFactors {
			pool.maxUnusedBlocks = int64(hf)

			for _, pb := range preBudgets {
				mmax := pb + max

				for _, pa := range poolAllocSizes {
					paramHeader = func() { fmt.Printf("max %d, pb %d, as %d, hf %d\n", max, pb, pa, hf) }

					// We start with a fresh pool for every set of
					// parameters.
					m = NewResourcePoolDefault("test", pa)
					m.Start(pool, (pb))

					for i := 0; i < numAccountOps; i++ {
						if i%linesBetweenHeaderReminders == 0 {
							generateHeader()
						}

						// The following implements a random operation generator.
						// At every test iteration a random account is selected
						// and then a random operation is performed for that
						// account.

						accI := rnd.Intn(len(accs))
						switch rnd.Intn(3 /* number of states below */) {
						case 0:
							sz := randomSize(rnd, mmax)
							reportAndCheck("G [%5d] %5d", accI, sz)
							err := accs[accI].Grow(sz)
							if err == nil {
								reportAndCheck("G [%5d] ok", accI)
							} else {
								reportAndCheck("G [%5d] %s", accI, err)
							}
						case 1:
							reportAndCheck("C [%5d]", accI)
							accs[accI].Clear()
							reportAndCheck("C [%5d]", accI)
						case 2:
							osz := rnd.Int63n(accs[accI].used + 1)
							nsz := randomSize(rnd, mmax)
							reportAndCheck("R [%5d] %5d %5d", accI, osz, nsz)
							err := accs[accI].resize(osz, nsz)
							if err == nil {
								reportAndCheck("R [%5d] ok", accI)
							} else {
								reportAndCheck("R [%5d] %s", accI, err)
							}
						}
					}

					// After all operations have been performed, ensure
					// that closing everything comes back to the initial situation.
					for accI := range accs {
						reportAndCheck("CL[%5d]", accI)
						accs[accI].Clear()
						reportAndCheck("CL[%5d]", accI)
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

func TestBytesPool(t *testing.T) {
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
		t.Fatalf("incorrect current allocation: got %d, expected %d", p.mu.allocated, 100)
	}

	p.release(90) // Should succeed without panic.
	if p.mu.allocated != 10 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", p.mu.allocated, 10)
	}
	if p.mu.maxAllocated != 100 {
		t.Fatalf("incorrect max allocation: got %d, expected %d", p.mu.maxAllocated, 100)
	}
	if p.MaxAllocated() != 100 {
		t.Fatalf("incorrect MaximumBytes(): got %d, expected %d", p.mu.maxAllocated, 100)
	}

	p.release(10) // Should succeed without panic.
	if p.mu.allocated != 0 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", p.mu.allocated, 0)
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
	// defer leaktest.AfterTest(t)()

	m := NewResourcePoolDefault("test",
		1e9 /* increment */)
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
		noteActionTriggerCnt := 0
		root := NewResourcePool(
			newPoolUID(),
			name /* name */, math.MaxInt64,
			1, /* increment */
			DefMaxUnusedBlocks,
			PoolActions{
				NoteAction: NoteAction{
					Threshold: 101,
					CB: func(s NoteActionState) {
						require.Equal(t, s.Pool.name, name)
						noteActionTriggerCnt += 1
					},
				},
			},
		)
		root.Start(nil /* pool */, (math.MaxInt64))
		root.maxUnusedBlocks = 0

		// Pre-reserve a budget of 100 bytes.
		b := root.CreateBudget()
		require.NoError(t, b.Grow(100))

		require.Equal(t, b.Used(), int64(100))
		require.Equal(t, noteActionTriggerCnt, 0)

		require.NoError(t, b.Grow(2))
		require.Equal(t, b.Used(), int64(102))
		require.Equal(t, noteActionTriggerCnt, 1)
	}

	{
		name := "root"
		outOfCapacityActionNum := 0
		root := NewResourcePoolDefault(
			name, /* name */
			1,    /* increment */
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

		root.ApproxAvailable()

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

func getPool(
	name string, parent *ResourcePool,
) *ResourcePool {
	var reservedBytes int64
	if parent == nil {
		reservedBytes = math.MaxInt64
	}
	return getPoolEx(name, parent, reservedBytes)
}

func getPoolEx(
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
	m := getPoolEx(name, parent, reservedBytes)
	if usedBytes != 0 {
		acc := m.CreateBudget()
		if err := acc.Grow(usedBytes); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

// TestBytesPoolTree is a sanity check that the tree structure of related
// pools is maintained and traversed as expected.
func TestBytesPoolTree(t *testing.T) {
	// defer leaktest.AfterTest(t)()

	export := func(p *ResourcePool) string {
		var pools []ResourcePoolState
		_ = p.Traverse(func(pool ResourcePoolState) error {
			pools = append(pools, pool)
			return nil
		})
		var sb strings.Builder
		for _, e := range pools {
			for i := 0; i < e.Level; i++ {
				sb.WriteString("-")
			}
			sb.WriteString(e.Name + "\n")
		}
		return sb.String()
	}

	parent := getPool("parent", nil /* parent */)
	child1 := getPool("child1", parent)
	child2 := getPool("child2", parent)
	require.Equal(t, "parent\n-child2\n-child1\n", export(parent))
	require.Equal(t, "child1\n", export(child1))
	require.Equal(t, "child2\n", export(child2))

	grandchild1 := getPool("grandchild1", child1)
	grandchild2 := getPool("grandchild2", child2)
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

// TestBytesPoolUsedFromReserved verifies that the correct memory usage is
// reported by TraverseTree if that usage counts towards the reserved account.
func TestBytesPoolUsedFromReserved(t *testing.T) {
	// defer leaktest.AfterTest(t)()

	root := getPool("root", nil /* parent */)
	const usedBytes = int64(1 << 10)
	child := getPoolUsed(t, "child", root, usedBytes, 2*usedBytes /* reservedBytes */)

	require.NoError(t, root.Traverse(func(s ResourcePoolState) error {
		if s.Name == child.Name() {
			require.Equal(t, usedBytes, s.Used)
		}
		return nil
	}))
}

// TestBytesPoolNoDeadlocks ensures that no deadlocks can occur when pools
// are started and stopped concurrently with the pool tree traversal.
func TestBytesPoolNoDeadlocks(t *testing.T) {
	// defer leaktest.AfterTest(t)()

	root := getPool("root", nil /* parent */)
	defer root.Stop()

	// Spin up 10 goroutines that repeatedly start and stop child pools while
	// also making reservations against them.
	var wg sync.WaitGroup
	const numGoroutines = 10
	// done will be closed when the concurrent goroutines should exit.
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
						m := getPool(fmt.Sprintf("m%d", i), root)
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
								// We shouldn't hit any errors since we have an
								// unlimited root budget.
								_ = m.allocate(toReserve)
								reserved += toReserve
							}
							// Sleep up to 1ms in-between operations.
							time.Sleep(time.Duration(rng.Intn(1000)) * time.Microsecond)
						}
					}()
					// Sleep up to 2ms after having stopped the pool.
					time.Sleep(time.Duration(rng.Intn(2000)) * time.Microsecond)
				}
			}
		}(i)
	}

	// In the main goroutine, perform the tree traversal several times with
	// sleeps in-between.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		// We mainly want to ensure that no deadlocks nor data races are
		// occurring, but also we do a sanity check that each "row" in the
		// output of TraverseTree() is a non-empty PoolState.
		var pools []ResourcePoolState
		_ = root.Traverse(func(pool ResourcePoolState) error {
			pools = append(pools, pool)
			return nil
		})
		for _, m := range pools {
			require.NotEqual(t, ResourcePoolState{}, m)
		}
		// Sleep up to 3ms.
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
	for i := 0; i < b.N; i++ {
		_ = a.Grow(1)
	}
}

func BenchmarkTraverseTree(b *testing.B) {
	makePoolTree := func(numLevels int, numChildrenPerPool int) (root *ResourcePool, cleanup func()) {
		allPools := make([][]*ResourcePool, numLevels)
		allPools[0] = []*ResourcePool{getPool("root", nil /* parent */)}
		for level := 1; level < numLevels; level++ {
			allPools[level] = make([]*ResourcePool, 0, len(allPools[level-1])*numChildrenPerPool)
			for parent, parentMon := range allPools[level-1] {
				for child := 0; child < numChildrenPerPool; child++ {
					name := fmt.Sprintf("child%d_parent%d", child, parent)
					allPools[level] = append(allPools[level], getPool(name, parentMon))
				}
			}
		}
		cleanup = func() {
			// Simulate the production setting where we stop the children before
			// their parent (this is not strictly necessary since we don't
			// reserve budget from the pools below).
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
				for i := 0; i < b.N; i++ {
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
