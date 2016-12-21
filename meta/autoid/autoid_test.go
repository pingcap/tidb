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

package autoid

import (
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
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

	alloc := NewAllocator(store, 1)
	c.Assert(alloc, NotNil)

	id, err := alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
	id, err = alloc.Alloc(0)
	c.Assert(err, NotNil)

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

	alloc = NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(GetStep()+1))

	alloc = NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(2, int64(1), false)
	c.Assert(err, IsNil)
	id, err = alloc.Alloc(2)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	alloc = NewAllocator(store, 1)
	c.Assert(alloc, NotNil)
	err = alloc.Rebase(3, int64(3210), false)
	c.Assert(err, IsNil)
	alloc = NewAllocator(store, 1)
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
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer store.Close()
	step = 100
	defer func() {
		step = 5000
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
		alloc := NewAllocator(store, dbID)
		for j := 0; j < int(step)+5; j++ {
			id, err := alloc.Alloc(tblID)
			if err != nil {
				errCh <- err
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
