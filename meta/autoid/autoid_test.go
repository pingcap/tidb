// Copyright 2015 PingCAP, Inc.
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

package autoid_test

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/mockstore"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)

	globalAutoID, err := alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	_, id, err := alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
	_, _, err = alloc.Alloc(0, 1)
	c.Assert(err, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(autoid.GetStep()+1))

	// rebase
	err = alloc.Rebase(1, int64(1), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3))
	err = alloc.Rebase(1, int64(3), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(4))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(11))
	err = alloc.Rebase(1, int64(3010), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3011))

	alloc = autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(autoid.GetStep()+1))

	alloc = autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(2, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	alloc = autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3000), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(3, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3211))
	err = alloc.Rebase(3, int64(6543), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(3, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(6544))

	// Test the MaxInt64 is the upper bound of `alloc` function but not `rebase`.
	err = alloc.Rebase(3, int64(math.MaxInt64-1), true)
	c.Assert(err, IsNil)
	_, _, err = alloc.Alloc(3, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(math.MaxInt64), true)
	c.Assert(err, IsNil)

	// alloc N for signed
	alloc = autoid.NewAllocator(store, 1, false)
	c.Assert(alloc, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(4)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	min, max, err := alloc.Alloc(4, 1)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(1))
	c.Assert(min+1, Equals, int64(1))

	min, max, err = alloc.Alloc(4, 2)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(2))
	c.Assert(min+1, Equals, int64(2))
	c.Assert(max, Equals, int64(3))

	min, max, err = alloc.Alloc(4, 100)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(100))
	expected := int64(4)
	for i := min + 1; i <= max; i++ {
		c.Assert(i, Equals, expected)
		expected++
	}

	err = alloc.Rebase(4, int64(1000), false)
	c.Assert(err, IsNil)
	min, max, err = alloc.Alloc(4, 3)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(3))
	c.Assert(min+1, Equals, int64(1001))
	c.Assert(min+2, Equals, int64(1002))
	c.Assert(max, Equals, int64(1003))

	lastRemainOne := alloc.End()
	err = alloc.Rebase(4, alloc.End()-2, false)
	c.Assert(err, IsNil)
	min, max, err = alloc.Alloc(4, 5)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(5))
	c.Assert(min+1, Greater, lastRemainOne)
}

func (*testSuite) TestUnsignedAutoid(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)

	globalAutoID, err := alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	_, id, err := alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
	_, _, err = alloc.Alloc(0, 1)
	c.Assert(err, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(autoid.GetStep()+1))

	// rebase
	err = alloc.Rebase(1, int64(1), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3))
	err = alloc.Rebase(1, int64(3), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(4))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(11))
	err = alloc.Rebase(1, int64(3010), true)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3011))

	alloc = autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)
	_, id, err = alloc.Alloc(1, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(autoid.GetStep()+1))

	alloc = autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(2, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	alloc = autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3000), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(3, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3211))
	err = alloc.Rebase(3, int64(6543), false)
	c.Assert(err, IsNil)
	_, id, err = alloc.Alloc(3, 1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(6544))

	// Test the MaxUint64 is the upper bound of `alloc` func but not `rebase`.
	var n uint64 = math.MaxUint64 - 1
	un := int64(n)
	err = alloc.Rebase(3, un, true)
	c.Assert(err, IsNil)
	_, _, err = alloc.Alloc(3, 1)
	c.Assert(err, NotNil)
	un = int64(n + 1)
	err = alloc.Rebase(3, un, true)
	c.Assert(err, IsNil)

	// alloc N for unsigned
	alloc = autoid.NewAllocator(store, 1, true)
	c.Assert(alloc, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(4)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))

	min, max, err := alloc.Alloc(4, 2)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(2))
	c.Assert(min+1, Equals, int64(1))
	c.Assert(max, Equals, int64(2))

	err = alloc.Rebase(4, int64(500), true)
	c.Assert(err, IsNil)
	min, max, err = alloc.Alloc(4, 2)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(2))
	c.Assert(min+1, Equals, int64(501))
	c.Assert(max, Equals, int64(502))

	lastRemainOne := alloc.End()
	err = alloc.Rebase(4, alloc.End()-2, false)
	c.Assert(err, IsNil)
	min, max, err = alloc.Alloc(4, 5)
	c.Assert(err, IsNil)
	c.Assert(max-min, Equals, int64(5))
	c.Assert(min+1, Greater, lastRemainOne)
}

// TestConcurrentAlloc is used for the test that
// multiple alloctors allocate ID with the same table ID concurrently.
func (*testSuite) TestConcurrentAlloc(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	autoid.SetStep(100)
	defer func() {
		autoid.SetStep(5000)
	}()

	dbID := int64(2)
	tblID := int64(100)
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTable(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocIDs := func() {
		alloc := autoid.NewAllocator(store, dbID, false)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			_, id, err1 := alloc.Alloc(tblID, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			mu.Lock()
			if _, ok := m[id]; ok {
				errCh <- fmt.Errorf("duplicate id:%v", id)
				mu.Unlock()
				break
			}
			m[id] = struct{}{}
			mu.Unlock()

			//test Alloc N
			N := rand.Uint64() % 100
			min, max, err1 := alloc.Alloc(tblID, N)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			for i := min + 1; i <= max; i++ {
				if _, ok := m[i]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", i)
					errFlag = true
					mu.Unlock()
					break
				}
				m[i] = struct{}{}
			}
			if errFlag {
				break
			}
			mu.Unlock()
		}
	}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocIDs()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	c.Assert(err, IsNil)
}

// TestRollbackAlloc tests that when the allocation transaction commit failed,
// the local variable base and end doesn't change.
func (*testSuite) TestRollbackAlloc(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTable(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	injectConf := new(kv.InjectionConfig)
	injectConf.SetCommitError(errors.New("injected"))
	injectedStore := kv.NewInjectedStore(store, injectConf)
	alloc := autoid.NewAllocator(injectedStore, 1, false)
	_, _, err = alloc.Alloc(2, 1)
	c.Assert(err, NotNil)
	c.Assert(alloc.Base(), Equals, int64(0))
	c.Assert(alloc.End(), Equals, int64(0))

	err = alloc.Rebase(2, 100, true)
	c.Assert(err, NotNil)
	c.Assert(alloc.Base(), Equals, int64(0))
	c.Assert(alloc.End(), Equals, int64(0))
}

// TestNextStep tests generate next auto id step.
func (*testSuite) TestNextStep(c *C) {
	nextStep := autoid.NextStep(2000000, 1*time.Nanosecond)
	c.Assert(nextStep, Equals, int64(2000000))
	nextStep = autoid.NextStep(678910, 10*time.Second)
	c.Assert(nextStep, Equals, int64(678910))
	nextStep = autoid.NextStep(50000, 10*time.Minute)
	c.Assert(nextStep, Equals, int64(30000))
}

func BenchmarkAllocator_Alloc(b *testing.B) {
	b.StopTimer()
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return
	}
	defer store.Close()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		if err != nil {
			return err
		}
		err = m.CreateTable(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	alloc := autoid.NewAllocator(store, 1, false)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		alloc.Alloc(2, 1)
	}
}

// Fix a computation logic bug in allocator computation.
func (*testSuite) TestAllocComputationIssue(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize"), IsNil)
	}()

	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTable(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	unsignedAlloc := autoid.NewAllocator(store, 1, true)
	c.Assert(unsignedAlloc, NotNil)
	signedAlloc := autoid.NewAllocator(store, 1, false)
	c.Assert(signedAlloc, NotNil)

	// the next valid two value must be 13 & 16, batch size = 6.
	err = unsignedAlloc.Rebase(1, 10, false)
	c.Assert(err, IsNil)
	// the next valid two value must be 10 & 13, batch size = 6.
	err = signedAlloc.Rebase(2, 7, false)
	c.Assert(err, IsNil)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(unsignedAlloc, 9, 9)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(signedAlloc, 4, 6)

	// Here adjust the alloced N larger than step mocked step length equal to 3.
	min, max, err := unsignedAlloc.Alloc(1, 4)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, int64(10))
	c.Assert(max, Equals, int64(14))
	min, max, err = signedAlloc.Alloc(2, 4)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, int64(7))
	c.Assert(max, Equals, int64(11))
}
