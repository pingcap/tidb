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
	goctx "context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// After add column finished, check the records in the table.
func (s *ddlSuite) checkAddColumn(t *testing.T, rowID int64, defaultVal interface{}, updatedVal interface{}) {
	ctx := s.ctx
	err := sessiontxn.NewTxn(goctx.Background(), ctx)
	require.NoError(t, err)

	tbl := s.getTable(t, "test_column")
	oldInsertCount := int64(0)
	newInsertCount := int64(0)
	oldUpdateCount := int64(0)
	newUpdateCount := int64(0)
	err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		col1Val := data[0].GetValue()
		col2Val := data[1].GetValue()
		col3Val := data[2].GetValue()
		// Check inserted row.
		if reflect.DeepEqual(col1Val, col2Val) {
			if reflect.DeepEqual(col3Val, defaultVal) {
				// When insert a row with 2 columns, the third column will be default value.
				oldInsertCount++
			} else if reflect.DeepEqual(col3Val, col1Val) {
				// When insert a row with 3 columns, the third column value will be the first column value.
				newInsertCount++
			} else {
				log.Fatal("[checkAddColumn fail]invalid row", zap.Any("row", data))
			}
		}

		// Check updated row.
		if reflect.DeepEqual(col2Val, updatedVal) {
			if reflect.DeepEqual(col3Val, defaultVal) || reflect.DeepEqual(col3Val, col1Val) {
				oldUpdateCount++
			} else if reflect.DeepEqual(col3Val, updatedVal) {
				newUpdateCount++
			} else {
				log.Fatal("[checkAddColumn fail]invalid row", zap.Any("row", data))
			}
		}

		return true, nil
	})
	require.NoError(t, err)

	deleteCount := rowID - oldInsertCount - newInsertCount - oldUpdateCount - newUpdateCount
	require.GreaterOrEqual(t, oldInsertCount, int64(0))
	require.GreaterOrEqual(t, newInsertCount, int64(0))
	require.Greater(t, oldUpdateCount, int64(0))
	require.Greater(t, newUpdateCount, int64(0))
	require.Greater(t, deleteCount, int64(0))
}

func (s *ddlSuite) checkDropColumn(t *testing.T, rowID int64, alterColumn *table.Column, updateDefault interface{}) {
	ctx := s.ctx
	err := sessiontxn.NewTxn(goctx.Background(), ctx)
	require.NoError(t, err)

	tbl := s.getTable(t, "test_column")
	for _, col := range tbl.Cols() {
		require.NotEqual(t, alterColumn.ID, col.ID)
	}
	insertCount := int64(0)
	updateCount := int64(0)
	err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		if reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) {
			// Check inserted row.
			insertCount++
		} else if reflect.DeepEqual(data[1].GetValue(), updateDefault) {
			// Check updated row.
			updateCount++
		} else {
			log.Fatal("[checkDropColumn fail]invalid row", zap.Any("row", data))
		}
		return true, nil
	})
	require.NoError(t, err)

	deleteCount := rowID - insertCount - updateCount
	require.Greater(t, insertCount, int64(0))
	require.Greater(t, updateCount, int64(0))
	require.Greater(t, deleteCount, int64(0))
}

func TestColumn(t *testing.T) {
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
				s.execInsert(fmt.Sprintf("insert into test_column values (%d, %d)", k, k))
			}
		}(i)
	}
	wg.Wait()

	tbl := []struct {
		Query      string
		ColumnName string
		Add        bool
		Default    interface{}
	}{
		{"alter table test_column add column c3 int default -1", "c3", true, int64(-1)},
		{"alter table test_column drop column c3", "c3", false, nil},
	}

	rowID := int64(*dataNum)
	updateDefault := int64(-2)
	var alterColumn *table.Column

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
					count := 10
					s.execColumnOperations(t, workerNum, count, &rowID, updateDefault)
				}
			}

			if col.Add {
				s.checkAddColumn(t, rowID, col.Default, updateDefault)
			} else {
				s.checkDropColumn(t, rowID, alterColumn, updateDefault)
			}

			tbl := s.getTable(t, "test_column")
			alterColumn = table.FindCol(tbl.Cols(), col.ColumnName)
			if col.Add {
				require.NotNil(t, alterColumn)
			} else {
				require.Nil(t, alterColumn)
			}
		})
	}
}

func (s *ddlSuite) execColumnOperations(t *testing.T, workerNum, count int, rowID *int64, updateDefault int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				key := int(atomic.AddInt64(rowID, 2))
				s.execInsert(fmt.Sprintf("insert into test_column (c1, c2) values (%d, %d)",
					key-1, key-1))
				_, _ = s.exec(fmt.Sprintf("insert into test_column values (%d, %d, %d)", key, key, key))
				s.mustExec(fmt.Sprintf("update test_column set c2 = %d where c1 = %d",
					updateDefault, randomNum(key)))
				_, _ = s.exec(fmt.Sprintf("update test_column set c2 = %d, c3 = %d where c1 = %d",
					updateDefault, updateDefault, randomNum(key)))
				s.mustExec(fmt.Sprintf("delete from test_column where c1 = %d", randomNum(key)))
			}
		}()
	}
	wg.Wait()
}
