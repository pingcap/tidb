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
	"sync"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	store, err := tikv.NewMockTikvStore()
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
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewAllocator(store, 1)
	c.Assert(alloc, NotNil)

	globalAutoId, err := alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoId, Equals, int64(1))
	id, err := alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
	id, err = alloc.Alloc(0)
	c.Assert(err, NotNil)
	globalAutoId, err = alloc.NextGlobalAutoID(1)
	c.Assert(err, IsNil)
	c.Assert(globalAutoId, Equals, int64(autoid.GetStep()+1))

	// rebase
	err = alloc.Rebase(1, int64(1), true)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3))
	err = alloc.Rebase(1, int64(3), true)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(4))
	err = alloc.Rebase(1, int64(10), true)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(11))
	err = alloc.Rebase(1, int64(3010), true)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3011))

	alloc = autoid.NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(autoid.GetStep()+1))

	alloc = autoid.NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(2)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	alloc = autoid.NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = autoid.NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3000), false)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(3)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3211))
	err = alloc.Rebase(3, int64(6543), false)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(3)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(6544))
}

// TestConcurrentAlloc is used for the test that
// multiple alloctors allocate ID with the same table ID concurrently.
func (*testSuite) TestConcurrentAlloc(c *C) {
	store, err := tikv.NewMockTikvStore()
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
	count := 100
	errCh := make(chan error, count)

	allocIDs := func() {
		alloc := autoid.NewAllocator(store, dbID)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			id, err1 := alloc.Alloc(tblID)
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
	store, err := tikv.NewMockTikvStore()
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
	alloc := autoid.NewAllocator(injectedStore, 1)
	_, err = alloc.Alloc(2)
	c.Assert(err, NotNil)
	c.Assert(alloc.Base(), Equals, int64(0))
	c.Assert(alloc.End(), Equals, int64(0))

	err = alloc.Rebase(2, 100, true)
	c.Assert(err, NotNil)
	c.Assert(alloc.Base(), Equals, int64(0))
	c.Assert(alloc.End(), Equals, int64(0))
}
