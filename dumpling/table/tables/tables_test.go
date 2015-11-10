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

package tables_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	se    tidb.Session
}

func (ts *testSuite) SetUpSuite(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Check(err, IsNil)
	ts.store = store
	ts.se, err = tidb.CreateSession(ts.store)
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestBasic(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.TableID(), Greater, int64(0))
	c.Assert(tb.TableName().L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(tb.FirstKey(), Not(Equals), "")
	c.Assert(tb.IndexPrefix(), Not(Equals), "")
	c.Assert(tb.KeyPrefix(), Not(Equals), "")
	c.Assert(tb.FindIndexByColName("b"), NotNil)

	autoid, err := tb.AllocAutoID()
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	rid, err := tb.AddRecord(ctx, []interface{}{1, "abc"})
	c.Assert(err, IsNil)
	c.Assert(rid, Greater, int64(0))
	row, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(row), Equals, 2)
	c.Assert(row[0].(int64), Equals, int64(1))

	_, err = tb.AddRecord(ctx, []interface{}{1, "aba"})
	c.Assert(err, NotNil)
	_, err = tb.AddRecord(ctx, []interface{}{2, "abc"})
	c.Assert(err, NotNil)

	c.Assert(tb.UpdateRecord(ctx, rid, []interface{}{1, "abc"}, []interface{}{1, "cba"}, map[int]bool{0: false, 1: true}), IsNil)

	tb.IterRecords(ctx, tb.FirstKey(), tb.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		return true, nil
	})

	c.Assert(tb.RemoveRecord(ctx, rid, []interface{}{1, "cba"}), IsNil)

	// Make sure there is index data in the storage.
	prefix := tb.IndexPrefix()
	cnt, err := countEntriesWithPrefix(ctx, prefix)
	c.Assert(err, IsNil)
	c.Assert(cnt, Greater, 0)
	c.Assert(tb.Truncate(ctx), IsNil)
	// Make sure index data is also removed after tb.Truncate().
	cnt, err = countEntriesWithPrefix(ctx, prefix)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 0)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)
}

func countEntriesWithPrefix(ctx context.Context, prefix string) (int, error) {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return 0, err
	}
	cnt := 0
	err = util.ScanMetaWithPrefix(txn, prefix, func(k, v []byte) bool {
		cnt++
		return true
	})
	return cnt, err
}

func (ts *testSuite) TestTypes(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c8 time, c9 timestamp not null default CURRENT_TIMESTAMP, c10 decimal)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	_, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values (1, 2, 3, 4, '5', '6', '7', '10:10:10', null, 1.4)")
	c.Assert(err, IsNil)
	rs, err := ts.se.Execute("select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err := rs[0].FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute("CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values (1, 2, 3, 4, 5, 6)")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute("select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err = rs[0].FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row[5], Equals, mysql.Bit{Value: 6, Width: 8})
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute("CREATE TABLE test.t (c1 enum('a', 'b', 'c'))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("insert test.t values ('a'), (2), ('c')")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute("select c1 + 1 from test.t where c1 = 1")
	c.Assert(err, IsNil)
	row, err = rs[0].FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row[0], DeepEquals, float64(2))
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestUniqueIndexMultipleNullEntries(c *C) {
	_, err := ts.se.Execute("CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.TableID(), Greater, int64(0))
	c.Assert(tb.TableName().L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(tb.FirstKey(), Not(Equals), "")
	c.Assert(tb.IndexPrefix(), Not(Equals), "")
	c.Assert(tb.KeyPrefix(), Not(Equals), "")
	c.Assert(tb.FindIndexByColName("b"), NotNil)

	autoid, err := tb.AllocAutoID()
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	_, err = tb.AddRecord(ctx, []interface{}{1, nil})
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ctx, []interface{}{2, nil})
	c.Assert(err, IsNil)
	_, err = ts.se.Execute("drop table test.t")
	c.Assert(err, IsNil)
}
