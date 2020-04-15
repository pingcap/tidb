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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	plannercore "github.com/pingcap/tidb/v4/planner/core"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/table"
	"github.com/pingcap/tidb/v4/types"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

// After add column finished, check the records in the table.
func (s *TestDDLSuite) checkAddColumn(c *C, rowID int64, defaultVal interface{}, updatedVal interface{}) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_column")
	oldInsertCount := int64(0)
	newInsertCount := int64(0)
	oldUpdateCount := int64(0)
	newUpdateCount := int64(0)
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
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
				log.Fatalf("[checkAddColumn fail]invalid row: %v", data)
			}
		}

		// Check updated row.
		if reflect.DeepEqual(col2Val, updatedVal) {
			if reflect.DeepEqual(col3Val, defaultVal) || reflect.DeepEqual(col3Val, col1Val) {
				oldUpdateCount++
			} else if reflect.DeepEqual(col3Val, updatedVal) {
				newUpdateCount++
			} else {
				log.Fatalf("[checkAddColumn fail]invalid row: %v", data)
			}
		}

		return true, nil
	})
	c.Assert(err, IsNil)

	deleteCount := rowID - oldInsertCount - newInsertCount - oldUpdateCount - newUpdateCount
	c.Assert(oldInsertCount, GreaterEqual, int64(0))
	c.Assert(newInsertCount, GreaterEqual, int64(0))
	c.Assert(oldUpdateCount, Greater, int64(0))
	c.Assert(newUpdateCount, Greater, int64(0))
	c.Assert(deleteCount, Greater, int64(0))
}

func (s *TestDDLSuite) checkDropColumn(c *C, rowID int64, alterColumn *table.Column, updateDefault interface{}) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_column")
	for _, col := range tbl.Cols() {
		c.Assert(col.ID, Not(Equals), alterColumn.ID)
	}
	insertCount := int64(0)
	updateCount := int64(0)
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) {
			// Check inserted row.
			insertCount++
		} else if reflect.DeepEqual(data[1].GetValue(), updateDefault) {
			// Check updated row.
			updateCount++
		} else {
			log.Fatalf("[checkDropColumn fail]invalid row: %v", data)
		}
		return true, nil
	})
	c.Assert(err, IsNil)

	deleteCount := rowID - insertCount - updateCount
	c.Assert(insertCount, Greater, int64(0))
	c.Assert(updateCount, Greater, int64(0))
	c.Assert(deleteCount, Greater, int64(0))
}

func (s *TestDDLSuite) TestColumn(c *C) {
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
				s.execInsert(c, fmt.Sprintf("insert into test_column values (%d, %d)", k, k))
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

	for _, t := range tbl {
		c.Logf("run DDL %s", t.Query)
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
				count := 10
				s.execColumnOperations(c, workerNum, count, &rowID, updateDefault)
			}
		}

		if t.Add {
			s.checkAddColumn(c, rowID, t.Default, updateDefault)
		} else {
			s.checkDropColumn(c, rowID, alterColumn, updateDefault)
		}

		tbl := s.getTable(c, "test_column")
		alterColumn = table.FindCol(tbl.Cols(), t.ColumnName)
		if t.Add {
			c.Assert(alterColumn, NotNil)
		} else {
			c.Assert(alterColumn, IsNil)
		}
	}
}

func (s *TestDDLSuite) execColumnOperations(c *C, workerNum, count int, rowID *int64, updateDefault int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				key := int(atomic.AddInt64(rowID, 2))
				s.execInsert(c, fmt.Sprintf("insert into test_column (c1, c2) values (%d, %d)",
					key-1, key-1))
				s.exec(fmt.Sprintf("insert into test_column values (%d, %d, %d)", key, key, key))
				s.mustExec(c, fmt.Sprintf("update test_column set c2 = %d where c1 = %d",
					updateDefault, randomNum(key)))
				s.exec(fmt.Sprintf("update test_column set c2 = %d, c3 = %d where c1 = %d",
					updateDefault, updateDefault, randomNum(key)))
				s.mustExec(c, fmt.Sprintf("delete from test_column where c1 = %d", randomNum(key)))
			}
		}()
	}
	wg.Wait()
}

func (s *TestDDLSuite) TestCommitWhenSchemaChanged(c *C) {
	s.mustExec(c, "drop table if exists test_commit")
	s.mustExec(c, "create table test_commit (a int, b int)")
	s.mustExec(c, "insert into test_commit values (1, 1)")
	s.mustExec(c, "insert into test_commit values (2, 2)")

	s1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	ctx := goctx.Background()
	_, err = s1.Execute(ctx, "use test_ddl")
	c.Assert(err, IsNil)
	s1.Execute(ctx, "begin")
	s1.Execute(ctx, "insert into test_commit values (3, 3)")

	s.mustExec(c, "alter table test_commit drop column b")

	// When this transaction commit, it will find schema already changed.
	s1.Execute(ctx, "insert into test_commit values (4, 4)")
	_, err = s1.Execute(ctx, "commit")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}
