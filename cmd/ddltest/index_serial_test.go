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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddltest

import (
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
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

func (s *ddlSuite) checkAddIndex(t *testing.T, indexInfo *model.IndexInfo) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	require.NoError(t, err)
	tbl := s.getTable(t, "test_index")

	// read handles form table
	handles := kv.NewHandleMap()
	err = tables.IterRecords(tbl, ctx, tbl.Cols(),
		func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			handles.Set(h, struct{}{})
			return true, nil
		})
	require.NoError(t, err)

	// read handles from index
	idx := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), indexInfo)
	err = ctx.NewTxn(goctx.Background())
	require.NoError(t, err)
	txn, err := ctx.Txn(false)
	require.NoError(t, err)
	defer func() {
		err = txn.Rollback()
		require.NoError(t, err)
	}()

	it, err := idx.SeekFirst(txn)
	require.NoError(t, err)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		_, ok := handles.Get(h)
		require.True(t, ok)
		handles.Delete(h)
	}

	require.Equal(t, 0, handles.Len())
}

func (s *ddlSuite) checkDropIndex(t *testing.T, indexInfo *model.IndexInfo) {
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	require.NoError(t, err)
	err = gcWorker.DeleteRanges(goctx.Background(), uint64(math.MaxInt32))
	require.NoError(t, err)

	ctx := s.ctx
	err = ctx.NewTxn(goctx.Background())
	require.NoError(t, err)
	tbl := s.getTable(t, "test_index")

	// read handles from index
	idx := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), indexInfo)
	err = ctx.NewTxn(goctx.Background())
	require.NoError(t, err)
	txn, err := ctx.Txn(false)
	require.NoError(t, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(t, err)
	}()

	it, err := idx.SeekFirst(txn)
	require.NoError(t, err)
	defer it.Close()

	handles := kv.NewHandleMap()
	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		handles.Set(h, struct{}{})
	}

	// TODO: Uncomment this after apply pool is finished
	// c.Assert(handles.Len(), Equals, 0)
}

// TestIndex operations on table test_index (c int, c1 bigint, c2 double, c3 varchar(256), primary key(c)).
func TestIndex(t *testing.T) {
	s := createDDLSuite(t)
	defer s.teardown(t)

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
				s.execInsert(
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
	for _, col := range tbl {
		t.Run(col.Query, func(t *testing.T) {
			done := s.runDDL(col.Query)

			ticker := time.NewTicker(time.Duration(*lease) * time.Second / 2)
			defer ticker.Stop()
		LOOP:
			for {
				select {
				case err := <-done:
					require.NoError(t, err)
					break LOOP
				case <-ticker.C:
					// add count new data
					// delete count old data randomly
					// update count old data randomly
					count := 10
					s.execIndexOperations(t, workerNum, count, &insertID)
				}
			}

			tbl := s.getTable(t, "test_index")
			index := getIndex(tbl, col.IndexName)
			if col.Add {
				require.NotNil(t, index)
				oldIndex = index
				s.checkAddIndex(t, index.Meta())
			} else {
				require.Nil(t, index)
				s.checkDropIndex(t, oldIndex.Meta())
			}
		})
	}
}

func (s *ddlSuite) execIndexOperations(t *testing.T, workerNum, count int, insertID *int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				id := atomic.AddInt64(insertID, 1)
				sql := fmt.Sprintf("insert into test_index values (%d, %d, %f, '%s')", id, randomInt(), randomFloat(), randomString(10))
				s.execInsert(sql)
				t.Logf("sql %s", sql)
				sql = fmt.Sprintf("delete from test_index where c = %d", randomIntn(int(id)))
				s.mustExec(sql)
				t.Logf("sql %s", sql)
				sql = fmt.Sprintf("update test_index set c1 = %d, c2 = %f, c3 = '%s' where c = %d", randomInt(), randomFloat(), randomString(10), randomIntn(int(id)))
				s.mustExec(sql)
				t.Logf("sql %s", sql)
			}
		}()
	}
	wg.Wait()
}
