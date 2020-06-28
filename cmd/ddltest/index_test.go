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

package ddltest

import (
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	goctx "golang.org/x/net/context"
)

func getIndex(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		if idx.Meta().Name.O == name {
			return idx
		}
	}

	return nil
}

func (s *TestDDLSuite) checkAddIndex(c *C, indexInfo *model.IndexInfo) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	tbl := s.getTable(c, "test_index")

	// read handles form table
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(),
		func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			handles.Set(h, struct{}{})
			return true, nil
		})
	c.Assert(err, IsNil)

	// read handles from index
	idx := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), indexInfo)
	err = ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(false)
	c.Assert(err, IsNil)
	defer func() {
		txn.Rollback()
	}()

	it, err := idx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		_, ok := handles.Get(h)
		c.Assert(ok, IsTrue)
		handles.Delete(h)
	}

	c.Assert(handles.Len(), Equals, 0)
}

func (s *TestDDLSuite) checkDropIndex(c *C, indexInfo *model.IndexInfo) {
	gcWorker, err := gcworker.NewMockGCWorker(s.store.(tikv.Storage))
	c.Assert(err, IsNil)
	err = gcWorker.DeleteRanges(goctx.Background(), uint64(math.MaxInt32))
	c.Assert(err, IsNil)

	ctx := s.ctx
	err = ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	tbl := s.getTable(c, "test_index")

	// read handles from index
	idx := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), indexInfo)
	err = ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(false)
	c.Assert(err, IsNil)
	defer txn.Rollback()

	it, err := idx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	handles := kv.NewHandleMap()
	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		handles.Set(h, struct{}{})
	}

	// TODO: Uncomment this after apply pool is finished
	// c.Assert(handles.Len(), Equals, 0)
}

// TestIndex operations on table test_index (c int, c1 bigint, c2 double, c3 varchar(256), primary key(c)).
func (s *TestDDLSuite) TestIndex(c *C) {
	// first add many data
	workerNum := 10
	base := *dataNum / workerNum
	var wg sync.WaitGroup
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < base; j++ {
				k := base*i + j
				s.execInsert(c,
					fmt.Sprintf("insert into test_index values (%d, %d, %f, '%s')",
						k, randomInt(), randomFloat(), randomString(10)))
			}
		}(i)
	}
	wg.Wait()

	tbl := []struct {
		Query     string
		IndexName string
		Add       bool
	}{
		{"create index c1_index on test_index (c1)", "c1_index", true},
		{"drop index c1_index on test_index", "c1_index", false},
		{"create index c2_index on test_index (c2)", "c2_index", true},
		{"drop index c2_index on test_index", "c2_index", false},
		{"create index c3_index on test_index (c3)", "c3_index", true},
		{"drop index c3_index on test_index", "c3_index", false},
	}

	insertID := int64(*dataNum)
	var oldIndex table.Index
	for _, t := range tbl {
		c.Logf("run DDL sql %s", t.Query)
		done := s.runDDL(t.Query)

		ticker := time.NewTicker(time.Duration(*lease) * time.Second / 2)
		defer ticker.Stop()
	LOOP:
		for {
			select {
			case err := <-done:
				c.Assert(err, IsNil)
				break LOOP
			case <-ticker.C:
				// add count new data
				// delete count old data randomly
				// update count old data randomly
				count := 10
				s.execIndexOperations(c, workerNum, count, &insertID)
			}
		}

		tbl := s.getTable(c, "test_index")
		index := getIndex(tbl, t.IndexName)
		if t.Add {
			c.Assert(index, NotNil)
			oldIndex = index
			s.checkAddIndex(c, index.Meta())
		} else {
			c.Assert(index, IsNil)
			s.checkDropIndex(c, oldIndex.Meta())
		}
	}
}

func (s *TestDDLSuite) execIndexOperations(c *C, workerNum, count int, insertID *int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				id := atomic.AddInt64(insertID, 1)
				sql := fmt.Sprintf("insert into test_index values (%d, %d, %f, '%s')", id, randomInt(), randomFloat(), randomString(10))
				s.execInsert(c, sql)
				c.Logf("sql %s", sql)
				sql = fmt.Sprintf("delete from test_index where c = %d", randomIntn(int(id)))
				s.mustExec(c, sql)
				c.Logf("sql %s", sql)
				sql = fmt.Sprintf("update test_index set c1 = %d, c2 = %f, c3 = '%s' where c = %d", randomInt(), randomFloat(), randomString(10), randomIntn(int(id)))
				s.mustExec(c, sql)
				c.Logf("sql %s", sql)

			}
		}()
	}
	wg.Wait()
}
