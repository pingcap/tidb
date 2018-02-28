// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func (s *testSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := atomic.LoadInt32(&executor.LookupTableTaskChannelSize)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, 1)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_index_lookup_size = '10'")
	tk.MustExec("use test")
	tk.MustExec("create table dist (id int primary key, c_idx int, c_col int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustExec("insert dist values " + strings.Join(values, ","))

	rs, err := tk.Exec("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	_, err = rs.Next(context.Background())
	c.Assert(err, IsNil)
	keyword := "pickAndExecTask"
	rs.Close()
	time.Sleep(time.Millisecond * 50)
	c.Check(checkGoroutineExists(keyword), IsFalse)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, originSize)
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *testSuite) TestCopClientSend(c *C) {
	c.Skip("not stable")
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table copclient (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert copclient values " + strings.Join(values, ","))

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("copclient"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	ctx := context.Background()
	// Send coprocessor request when the table split.
	rs, err := tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	defer rs.Close()
	row, err := rs.Next(ctx)
	c.Assert(err, IsNil)
	c.Assert(row.GetMyDecimal(0).String(), Equals, "499500")

	// Split one region.
	key := tablecodec.EncodeRowKeyWithHandle(tblID, 500)
	region, _ := s.cluster.GetRegionByKey([]byte(key))
	peerID := s.cluster.AllocID()
	s.cluster.Split(region.GetId(), s.cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rs, err = tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	row, err = rs.Next(ctx)
	c.Assert(err, IsNil)
	c.Assert(row.GetMyDecimal(0).String(), Equals, "499500")
	rs.Close()

	// Check there is no goroutine leak.
	rs, err = tk.Exec("select * from copclient order by id")
	c.Assert(err, IsNil)
	_, err = rs.Next(ctx)
	c.Assert(err, IsNil)
	rs.Close()
	keyword := "(*copIterator).work"
	c.Check(checkGoroutineExists(keyword), IsFalse)
}

func (s *testSuite) TestScanX(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	dom, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	se, err := tidb.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()

	_, err = se.Execute(context.Background(), "create database test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	db := model.NewCIStr("test_admin")
	dbInfo, ok := is.SchemaByName(db)
	c.Assert(ok, IsTrue)
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(db, tblName)
	c.Assert(err, IsNil)
	tbInfo := tbl.Meta()

	alloc := autoid.NewAllocator(s.store, dbInfo.ID)
	tb, err := tables.TableFromMeta(alloc, tbInfo)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, types.MakeDatums(1, 10, 11), false)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(context.Background()), IsNil)

	recordVal1 := types.MakeDatums(int64(1), int64(10), int64(11))
	recordVal2 := types.MakeDatums(int64(2), int64(20), int64(21))

	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, recordVal2, false)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(context.Background()), IsNil)

	ctx := se.(sessionctx.Context)
	s.testIndex(c, ctx, db.L, tb, tb.Indices()[0])
	r, err := se.Execute(context.Background(), "select c1 from t use index(c)")
	c.Assert(err, IsNil)
	c.Assert(r, HasLen, 1)
	goCtx := context.Background()
	row, err := r[0].Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	row, err = r[0].Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	row, err = r[0].Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	row, err = r[0].Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)

	c.Assert(s.ctx.NewTxn(), IsNil)
	err = tb.RemoveRecord(s.ctx, 1, recordVal1)
	c.Assert(err, IsNil)
	err = tb.RemoveRecord(s.ctx, 2, recordVal2)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(context.Background()), IsNil)
}

func (s *testSuite) testIndex(c *C, ctx sessionctx.Context, dbName string, tb table.Table, idx table.Index) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	// sc := &stmtctx.StatementContext{TimeZone: time.Local}
	// idxNames := []string{idx.Meta().Name.L}
	mockCtx := mock.NewContext()

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), 3)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func setColValue(c *C, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
}
