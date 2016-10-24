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

package ddl_test

import (
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	_ "github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
	db *sql.DB

	store      kv.Storage
	schemaName string

	s     tidb.Session
	lease time.Duration
}

func (s *testDBSuite) SetUpSuite(c *C) {
	var err error

	s.schemaName = "test_db"

	uri := "memory://test"
	s.store, err = tidb.NewStore(uri)
	c.Assert(err, IsNil)

	localstore.MockRemoteStore = true
	s.db, err = sql.Open("tidb", fmt.Sprintf("%s/%s", uri, s.schemaName))
	c.Assert(err, IsNil)

	s.s, err = tidb.CreateSession(s.store)
	c.Assert(err, IsNil)

	s.mustExec(c, "create table t1 (c1 int, c2 int, c3 int, primary key(c1))")
	s.mustExec(c, "create table t2 (c1 int, c2 int, c3 int)")

	// set proper schema lease
	s.lease = 500 * time.Millisecond
	ctx := s.s.(context.Context)
	sessionctx.GetDomain(ctx).SetLease(s.lease)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	localstore.MockRemoteStore = false

	s.db.Close()
	s.s.Close()
}

func (s *testDBSuite) TestIndex(c *C) {
	defer testleak.AfterTest(c)()
	s.testAddIndex(c)
	s.testDropIndex(c)
	s.testAddUniqueIndexRollback(c)
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	ctx := s.s.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := domain.MustReload()
	c.Assert(err, IsNil)
	tbl, err := domain.InfoSchema().TableByName(model.NewCIStr(s.schemaName), model.NewCIStr(name))
	c.Assert(err, IsNil)
	return tbl
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := tidb.CreateSession(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute("use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(sql)
	done <- errors.Trace(err)
}

func (s *testDBSuite) testAddUniqueIndexRollback(c *C) {
	// t1 (c1 int, c2 int, c3 int, primary key(c1))
	s.mustExec(c, "delete from t1")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	defaultBatchSize := 1024
	base := defaultBatchSize * 2
	count := base
	// add some rows
	for i := 0; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}
	// add some duplicate rows
	for i := count - 10; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i+10, i, i)
	}

	done := make(chan error, 1)
	go backgroundExec(s.store, "create unique index c3_index on t1 (c3)", done)

	times := 0
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[kv:1062]Duplicate for key c3_index", Commentf("err:%v", err))
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				s.mustExec(c, "delete from t1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := s.testGetTable(c, "t1")
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	// delete duplicate rows, then add index
	for i := base - 10; i < base; i++ {
		s.mustExec(c, "delete from t1 where c1 = ?", i+10)
	}
	sessionExec(c, s.store, "create index c3_index on t1 (c3)")
}

func (s *testDBSuite) testAddIndex(c *C) {
	done := make(chan struct{}, 1)

	num := 100
	// first add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	go func() {
		sessionExec(c, s.store, "create index c3_index on t1 (c3)")
		done <- struct{}{}
	}()

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			step := 10
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				deletedKeys[n] = struct{}{}
				s.mustExec(c, "delete from t1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := 0; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}

	// test index key
	for _, key := range keys {
		rows := s.mustQuery(c, "select c1 from t1 where c3 = ?", key)
		matchRows(c, rows, [][]interface{}{{key}})
	}

	// test delete key not in index
	for key := range deletedKeys {
		rows := s.mustQuery(c, "select c1 from t1 where c3 = ?", key)
		matchRows(c, rows, nil)
	}

	// test index range
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys) - 3)
		rows := s.mustQuery(c, "select c1 from t1 where c3 >= ? limit 3", keys[index])
		matchRows(c, rows, [][]interface{}{{keys[index]}, {keys[index+1]}, {keys[index+2]}})
	}

	// TODO: support explain in future.
	//rows := s.mustQuery(c, "explain select c1 from t1 where c3 >= 100")

	//ay := dumpRows(c, rows)
	//c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)

	// get all row handles
	ctx := s.s.(context.Context)
	t := s.testGetTable(c, "t1")
	handles := make(map[int64]struct{})
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		handles[h] = struct{}{}
		return true, nil
	})
	c.Assert(err, IsNil)

	// check in index
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			nidx = tidx
			break
		}
	}
	// Make sure there is index with name c3_index.
	c.Assert(nidx, NotNil)
	c.Assert(nidx.Meta().ID, Greater, int64(0))
	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	defer ctx.RollbackTxn()

	it, err := nidx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		_, ok := handles[h]
		c.Assert(ok, IsTrue)
		delete(handles, h)
	}

	c.Assert(handles, HasLen, 0)
}

func (s *testDBSuite) testDropIndex(c *C) {
	done := make(chan struct{}, 1)

	s.mustExec(c, "delete from t1")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}
	t := s.testGetTable(c, "t1")
	var c3idx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			c3idx = tidx
			break
		}
	}
	c.Assert(c3idx, NotNil)

	go func() {
		sessionExec(c, s.store, "drop index c3_index on t1")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			step := 10
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				s.mustExec(c, "update t1 set c2 = 1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := s.mustQuery(c, "explain select c1 from t1 where c3 >= 0")

	ay := dumpRows(c, rows)
	c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsFalse)

	// check in index, must no index in kv
	ctx := s.s.(context.Context)

	handles := make(map[int64]struct{})

	t = s.testGetTable(c, "t1")
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			nidx = tidx
			break
		}
	}
	// Make sure there is no index with name c3_index
	c.Assert(nidx, IsNil)
	idx := tables.NewIndex(t.Meta(), c3idx.Meta())
	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	defer ctx.RollbackTxn()

	it, err := idx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		handles[h] = struct{}{}
	}

	c.Assert(handles, HasLen, 0)
}

func (s *testDBSuite) showColumns(c *C, tableName string) [][]interface{} {
	rows := s.mustQuery(c, fmt.Sprintf("show columns from %s", tableName))
	values := dumpRows(c, rows)
	return values
}

func (s *testDBSuite) TestColumn(c *C) {
	defer testleak.AfterTest(c)()
	s.testAddColumn(c)
	s.testDropColumn(c)
}

func sessionExec(c *C, s kv.Storage, sql string) {
	se, err := tidb.CreateSession(s)
	c.Assert(err, IsNil)
	_, err = se.Execute("use test_db")
	c.Assert(err, IsNil)
	rs, err := se.Execute(sql)
	c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	c.Assert(rs, IsNil)
	se.Close()
}

func (s *testDBSuite) testAddColumn(c *C) {
	done := make(chan struct{}, 1)

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	go func() {
		sessionExec(c, s.store, "alter table t2 add column c4 int default -1")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				s.mustExec(c, "delete from t2 where c1 = ?", n)

				_, err := s.db.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4)
				}
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := s.mustQuery(c, "select count(c4) from t2")
	values := dumpRows(c, rows)
	c.Assert(values, HasLen, 1)
	c.Assert(values[0], HasLen, 1)
	count, ok := values[0][0].(int64)
	c.Assert(ok, IsTrue)
	c.Assert(count, Greater, int64(0))

	rows = s.mustQuery(c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	for i := num; i < num+step; i++ {
		rows := s.mustQuery(c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}

	ctx := s.s.(context.Context)
	t := s.testGetTable(c, "t2")
	i := 0
	j := 0
	defer ctx.RollbackTxn()
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		i++
		// c4 must be -1 or > 0
		v, err1 := data[3].ToInt64()
		c.Assert(err1, IsNil)
		if v == -1 {
			j++
		} else {
			c.Assert(v, Greater, int64(0))
		}
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int(count))
	c.Assert(i, LessEqual, num+step)
	c.Assert(j, Equals, int(count)-step)
}

func (s *testDBSuite) testDropColumn(c *C) {
	done := make(chan struct{}, 1)

	s.mustExec(c, "delete from t2")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	// get c4 column id
	ctx := s.s.(context.Context)

	go func() {
		sessionExec(c, s.store, "alter table t2 drop column c4")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				_, err := s.db.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4)
				}
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	rows := s.mustQuery(c, "select count(*) from t2")
	values := dumpRows(c, rows)
	c.Assert(values, HasLen, 1)
	c.Assert(values[0], HasLen, 1)
	count, ok := values[0][0].(int64)
	c.Assert(ok, IsTrue)
	c.Assert(count, Greater, int64(0))

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	ctx.CommitTxn()
}

func (s *testDBSuite) mustExec(c *C, query string, args ...interface{}) sql.Result {
	r, err := s.db.Exec(query, args...)
	c.Assert(err, IsNil, Commentf("query %s, args %v", query, args))
	return r
}

func (s *testDBSuite) mustQuery(c *C, query string, args ...interface{}) *sql.Rows {
	r, err := s.db.Query(query, args...)
	c.Assert(err, IsNil, Commentf("query %s, args %v", query, args))
	return r
}

func dumpRows(c *C, rows *sql.Rows) [][]interface{} {
	cols, err := rows.Columns()
	c.Assert(err, IsNil)
	ay := make([][]interface{}, 0)
	for rows.Next() {
		v := make([]interface{}, len(cols))
		for i := range v {
			v[i] = new(interface{})
		}
		err = rows.Scan(v...)
		c.Assert(err, IsNil)

		for i := range v {
			v[i] = *(v[i].(*interface{}))
		}
		ay = append(ay, v)
	}

	rows.Close()
	c.Assert(rows.Err(), IsNil, Commentf("%v", ay))
	return ay
}

func matchRows(c *C, rows *sql.Rows, expected [][]interface{}) {
	ay := dumpRows(c, rows)
	c.Assert(len(ay), Equals, len(expected), Commentf("%v", expected))
	for i := range ay {
		match(c, ay[i], expected[i]...)
	}
}

func match(c *C, row []interface{}, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i])
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func (s *testDBSuite) TestUpdateMultipleTable(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://update_multiple_table")
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	ctx := tk.Se.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	is := domain.InfoSchema()
	db, ok := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	t1Tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1Info := t1Tbl.Meta()

	// Add a new column in write only state.
	newColumn := &model.ColumnInfo{
		ID:           100,
		Name:         model.NewCIStr("c3"),
		Offset:       2,
		DefaultValue: 9,
		FieldType:    *types.NewFieldType(mysql.TypeLonglong),
		State:        model.StateWriteOnly,
	}
	t1Info.Columns = append(t1Info.Columns, newColumn)

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = domain.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))

	newColumn.State = model.StatePublic

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = domain.Reload()
	c.Assert(err, IsNil)

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
}

func (s *testDBSuite) TestTruncateTable(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://truncate_table")
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table t")

	tk.MustExec("insert t values (3, 3), (4, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 3", "4 4"))

	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < 30; i++ {
		err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			it, err1 := txn.Seek(tablePrefix)
			if err1 != nil {
				return err1
			}
			if !it.Valid() {
				hasOldTableData = false
			} else {
				hasOldTableData = it.Key().HasPrefix(tablePrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldTableData {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	c.Assert(hasOldTableData, IsFalse)
}
