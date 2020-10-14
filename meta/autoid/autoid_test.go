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

var _ = SerialSuites(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 5, Name: model.NewCIStr("t3")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	alloc := autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)

	globalAutoID, err := alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	iter, err := alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(1))
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(2))
	_, err = alloc.Alloc(0, 1, 1, 1)
	c.Assert(err, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, autoid.GetStep()+1)

	// rebase
	err = alloc.Rebase(1, int64(1), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3))
	err = alloc.Rebase(1, int64(3), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(4))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(11))
	err = alloc.Rebase(1, int64(3010), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3011))

	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, autoid.GetStep()+1)

	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(2, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(2))

	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3000), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3211))
	err = alloc.Rebase(3, int64(6543), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(6544))

	// Test the MaxInt64 is the upper bound of `alloc` function but not `rebase`.
	err = alloc.Rebase(3, int64(math.MaxInt64-1), true)
	c.Assert(err, IsNil)
	_, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(math.MaxInt64), true)
	c.Assert(err, IsNil)

	// alloc N for signed
	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(4)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	iter, err = alloc.Alloc(4, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 1, IsTrue)
	c.Assert(iter.Current(), Equals, int64(1))

	iter, err = alloc.Alloc(4, 2, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	c.Assert(iter.Current(), Equals, int64(2))
	c.Assert(iter.Last(), Equals, int64(3))

	iter, err = alloc.Alloc(4, 100, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 100, IsTrue)
	expected := int64(4)
	for {
		hasNext, i := iter.Next()
		if !hasNext {
			break
		}
		c.Assert(i, Equals, expected)
		expected++
	}

	err = alloc.Rebase(4, int64(1000), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(4, 3, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 3, IsTrue)
	c.Assert(iter.Current(), Equals, int64(1001))
	c.Assert(iter.Skip().Current(), Equals, int64(1002))
	c.Assert(iter.Skip().Current(), Equals, int64(1003))

	lastRemainOne := alloc.End()
	err = alloc.Rebase(4, alloc.End()-2, false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(4, 5, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 5, IsTrue)
	c.Assert(iter.Current(), Greater, lastRemainOne)

	// Test for increment & offset for signed.
	alloc = autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)

	increment := int64(2)
	offset := int64(100)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	iter, err = alloc.Alloc(5, 1, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(100))

	iter, err = alloc.Alloc(5, 2, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	c.Assert(iter.Current(), Equals, int64(102))
	c.Assert(iter.Skip().Current(), Equals, int64(104))

	increment = int64(5)
	iter, err = alloc.Alloc(5, 3, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 3, IsTrue)
	c.Assert(iter.Current(), Equals, int64(105))
	c.Assert(iter.Last(), Equals, int64(115))

	increment = int64(15)
	iter, err = alloc.Alloc(5, 2, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	c.Assert(iter.Current(), Equals, int64(130))
	c.Assert(iter.Last(), Equals, int64(145))

	offset = int64(200)
	iter, err = alloc.Alloc(5, 2, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	// offset-1 > base will cause alloc rebase to offset-1.
	c.Assert(iter.Current(), Equals, int64(200))
	c.Assert(iter.Last(), Equals, int64(215))
}

func (*testSuite) TestUnsignedAutoid(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 5, Name: model.NewCIStr("t3")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)

	globalAutoID, err := alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))
	iter, err := alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(1))
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(2))
	_, err = alloc.Alloc(0, 1, 1, 1)
	c.Assert(err, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, autoid.GetStep()+1)

	// rebase
	err = alloc.Rebase(1, int64(1), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3))
	err = alloc.Rebase(1, int64(3), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(4))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(11))
	err = alloc.Rebase(1, int64(3010), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3011))

	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	iter, err = alloc.Alloc(1, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, autoid.GetStep()+1)

	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(2, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(2))

	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3000), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(3211))
	err = alloc.Rebase(3, int64(6543), false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Current(), Equals, int64(6544))

	// Test the MaxUint64 is the upper bound of `alloc` func but not `rebase`.
	var n uint64 = math.MaxUint64
	un := int64(n)
	err = alloc.Rebase(3, un, true)
	c.Assert(err, IsNil)
	_, err = alloc.Alloc(3, 1, 1, 1)
	c.Assert(err, NotNil)
	un = int64(n + 1)
	err = alloc.Rebase(3, un, true)
	c.Assert(err, IsNil)

	// alloc N for unsigned
	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)
	globalAutoID, err = alloc.NextGlobalAutoID(4)
	c.Assert(err, IsNil)
	c.Assert(globalAutoID, Equals, int64(1))

	iter, err = alloc.Alloc(4, 2, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	c.Assert(iter.Current(), Equals, int64(1))
	c.Assert(iter.Last(), Equals, int64(2))

	err = alloc.Rebase(4, int64(500), true)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(4, 2, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 2, IsTrue)
	c.Assert(iter.Current(), Equals, int64(501))
	c.Assert(iter.Last(), Equals, int64(502))

	lastRemainOne := alloc.End()
	err = alloc.Rebase(4, alloc.End()-2, false)
	c.Assert(err, IsNil)
	iter, err = alloc.Alloc(4, 5, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count() == 5, IsTrue)
	c.Assert(iter.Current(), Greater, lastRemainOne)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/skipIncrementOffsetValidation", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/skipIncrementOffsetValidation"), IsNil)
	}()
	alloc = autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(alloc, NotNil)

	increment := int64(2)
	n = math.MaxUint64 - 100
	offset := int64(n)

	iter, err = alloc.Alloc(5, 2, increment, offset)
	c.Assert(err, IsNil)
	c.Assert(uint64(iter.Current()), Equals, n)
	c.Assert(uint64(iter.Last()), Equals, n+2)
}

// TestConcurrentAlloc is used for the test that
// multiple allocators allocate ID with the same table ID concurrently.
func (*testSuite) TestConcurrentAlloc(c *C) {
	store, err := mockstore.NewMockStore()
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
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
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
		alloc := autoid.NewAllocator(store, dbID, false, autoid.RowIDAllocType)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			iter, err1 := alloc.Alloc(tblID, 1, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}
			id := iter.Current()

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
			iter, err1 = alloc.Alloc(tblID, N, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			for {
				hasNext, id := iter.Next()
				if !hasNext {
					break
				}
				if _, ok := m[id]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", id)
					errFlag = true
					mu.Unlock()
					break
				}
				m[id] = struct{}{}
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
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	injectConf := new(kv.InjectionConfig)
	injectConf.SetCommitError(errors.New("injected"))
	injectedStore := kv.NewInjectedStore(store, injectConf)
	alloc := autoid.NewAllocator(injectedStore, 1, false, autoid.RowIDAllocType)
	_, err = alloc.Alloc(2, 1, 1, 1)
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
	store, err := mockstore.NewMockStore()
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
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	alloc := autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		alloc.Alloc(2, 1, 1, 1)
	}
}

func BenchmarkAllocator_SequenceAlloc(b *testing.B) {
	b.StopTimer()
	store, err := mockstore.NewMockStore()
	if err != nil {
		return
	}
	defer store.Close()
	var seq *model.SequenceInfo
	var sequenceBase int64
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		if err != nil {
			return err
		}
		seq = &model.SequenceInfo{
			Start:      1,
			Cycle:      true,
			Cache:      false,
			MinValue:   -10,
			MaxValue:   math.MaxInt64,
			Increment:  2,
			CacheValue: 2000000,
		}
		seqTable := &model.TableInfo{
			ID:       1,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		sequenceBase = seq.Start - 1
		err = m.CreateSequenceAndSetSeqValue(1, seqTable, sequenceBase)
		return err
	})
	if err != nil {
		return
	}
	alloc := autoid.NewSequenceAllocator(store, 1, seq)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := alloc.AllocSeqCache(1)
		if err != nil {
			fmt.Println("err")
		}
	}
}

func BenchmarkAllocator_Seek(b *testing.B) {
	base := int64(21421948021)
	offset := int64(-351354365326)
	increment := int64(3)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		autoid.CalcSequenceBatchSize(base, 3, increment, offset, math.MinInt64, math.MaxInt64)
	}
}

func (*testSuite) TestSequenceAutoid(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()

	var seq *model.SequenceInfo
	var sequenceBase int64
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		seq = &model.SequenceInfo{
			Start:      1,
			Cycle:      true,
			Cache:      true,
			MinValue:   -10,
			MaxValue:   10,
			Increment:  2,
			CacheValue: 3,
		}
		seqTable := &model.TableInfo{
			ID:       1,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		sequenceBase = seq.Start - 1
		err = m.CreateSequenceAndSetSeqValue(1, seqTable, sequenceBase)
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewSequenceAllocator(store, 1, seq)
	c.Assert(alloc, NotNil)

	// allocate sequence cache.
	base, end, round, err := alloc.AllocSeqCache(1)
	c.Assert(err, IsNil)
	c.Assert(base, Equals, int64(0))
	c.Assert(end, Equals, int64(5))
	c.Assert(round, Equals, int64(0))

	// test the sequence batch size.
	offset := seq.Start
	size, err := autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	c.Assert(err, IsNil)
	c.Assert(size, Equals, end-base)

	// simulate the next value allocation.
	nextVal, ok := autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(1))
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(3))
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(5))

	base, end, round, err = alloc.AllocSeqCache(1)
	c.Assert(err, IsNil)
	c.Assert(base, Equals, int64(5))
	c.Assert(end, Equals, int64(10))
	c.Assert(round, Equals, int64(0))

	// test the sequence batch size.
	size, err = autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	c.Assert(err, IsNil)
	c.Assert(size, Equals, end-base)

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(7))
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(9))
	base = nextVal

	_, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	// the rest in cache in not enough for next value.
	c.Assert(ok, Equals, false)

	base, end, round, err = alloc.AllocSeqCache(1)
	c.Assert(err, IsNil)
	c.Assert(base, Equals, int64(-11))
	c.Assert(end, Equals, int64(-6))
	// the round is already in cycle.
	c.Assert(round, Equals, int64(1))

	// test the sequence batch size.
	size, err = autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	c.Assert(err, IsNil)
	c.Assert(size, Equals, end-base)

	offset = seq.MinValue
	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(-10))
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(-8))
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	c.Assert(ok, Equals, true)
	c.Assert(nextVal, Equals, int64(-6))
	base = nextVal

	_, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	// the cache is already empty.
	c.Assert(ok, Equals, false)
}

func (*testSuite) TestConcurrentAllocSequence(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()

	var seq *model.SequenceInfo
	var sequenceBase int64
	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err1 := m.CreateDatabase(&model.DBInfo{ID: 2, Name: model.NewCIStr("a")})
		c.Assert(err1, IsNil)
		seq = &model.SequenceInfo{
			Start:      100,
			Cycle:      false,
			Cache:      true,
			MinValue:   -100,
			MaxValue:   100,
			Increment:  -2,
			CacheValue: 3,
		}
		seqTable := &model.TableInfo{
			ID:       2,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		if seq.Increment >= 0 {
			sequenceBase = seq.Start - 1
		} else {
			sequenceBase = seq.Start + 1
		}
		err1 = m.CreateSequenceAndSetSeqValue(2, seqTable, sequenceBase)
		c.Assert(err1, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocSequence := func() {
		alloc := autoid.NewSequenceAllocator(store, 2, seq)
		for j := 0; j < 3; j++ {
			base, end, _, err1 := alloc.AllocSeqCache(2)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			// sequence is negative-growth here.
			for i := base - 1; i >= end; i-- {
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
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocSequence()
			wg.Done()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	c.Assert(err, IsNil)
}

// Fix a computation logic bug in allocator computation.
func (*testSuite) TestAllocComputationIssue(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize"), IsNil)
	}()

	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		c.Assert(err, IsNil)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	unsignedAlloc := autoid.NewAllocator(store, 1, true, autoid.RowIDAllocType)
	c.Assert(unsignedAlloc, NotNil)
	signedAlloc := autoid.NewAllocator(store, 1, false, autoid.RowIDAllocType)
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

	// Here will recompute the new allocator batch size base on new base = 10, which will get 6.
	iter, err := unsignedAlloc.Alloc(1, 2, 3, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count(), Equals, uint64(2))
	c.Assert(iter.Current(), Equals, int64(13))
	c.Assert(iter.Last(), Equals, int64(16))
	iter, err = signedAlloc.Alloc(2, 2, 3, 1)
	c.Assert(err, IsNil)
	c.Assert(iter.Count(), Equals, uint64(2))
	c.Assert(iter.Current(), Equals, int64(10))
	c.Assert(iter.Last(), Equals, int64(13))
}
