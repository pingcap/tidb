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

package ddl_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	parsertypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/tikv/client-go/v2/testutils"
)

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

var _ = Suite(&testDBSuite1{&testDBSuite{}})
var _ = Suite(&testDBSuite2{&testDBSuite{}})
var _ = Suite(&testDBSuite3{&testDBSuite{}})
var _ = Suite(&testDBSuite4{&testDBSuite{}})
var _ = Suite(&testDBSuite5{&testDBSuite{}})
var _ = SerialSuites(&testDBSuite6{&testDBSuite{}})
var _ = Suite(&testDBSuite7{&testDBSuite{}})
var _ = Suite(&testDBSuite8{&testDBSuite{}})
var _ = SerialSuites(&testSerialDBSuite{&testDBSuite{}})
var _ = SerialSuites(&testSerialDBSuite1{&testDBSuite{}})

const defaultBatchSize = 1024
const defaultReorgBatchSize = 256

type testDBSuite struct {
	cluster    testutils.Cluster
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	s          session.Session
	lease      time.Duration
	autoIDStep int64
	ctx        sessionctx.Context
}

func setUpSuite(s *testDBSuite, c *C) {
	var err error

	s.lease = 600 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	ddl.SetWaitTimeWhenErrorOccurred(0)

	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.s, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = s.s.(sessionctx.Context)

	_, err = s.s.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
	_, err = s.s.Execute(context.Background(), "set @@global.tidb_max_delta_schema_count= 4096")
	c.Assert(err, IsNil)
}

func tearDownSuite(s *testDBSuite, c *C) {
	_, err := s.s.Execute(context.Background(), "drop database if exists test_db")
	c.Assert(err, IsNil)
	s.s.Close()
	s.dom.Close()
	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testDBSuite) SetUpSuite(c *C) {
	setUpSuite(s, c)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	tearDownSuite(s, c)
}

type testDBSuite1 struct{ *testDBSuite }
type testDBSuite2 struct{ *testDBSuite }
type testDBSuite3 struct{ *testDBSuite }
type testDBSuite4 struct{ *testDBSuite }
type testDBSuite5 struct{ *testDBSuite }
type testDBSuite6 struct{ *testDBSuite }
type testDBSuite7 struct{ *testDBSuite }
type testDBSuite8 struct{ *testDBSuite }
type testSerialDBSuite struct{ *testDBSuite }
type testSerialDBSuite1 struct{ *testDBSuite }

func testAddIndexWithPK(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists test_add_index_with_pk")
	tk.MustExec("create table test_add_index_with_pk(a int not null, b int not null default '0', primary key(a))")
	tk.MustExec("insert into test_add_index_with_pk values(1, 2)")
	tk.MustExec("alter table test_add_index_with_pk add index idx (a)")
	tk.MustQuery("select a from test_add_index_with_pk").Check(testkit.Rows("1"))
	tk.MustExec("insert into test_add_index_with_pk values(2, 2)")
	tk.MustExec("alter table test_add_index_with_pk add index idx1 (a, b)")
	tk.MustQuery("select * from test_add_index_with_pk").Check(testkit.Rows("1 2", "2 2"))
	tk.MustExec("drop table if exists test_add_index_with_pk1")
	tk.MustExec("create table test_add_index_with_pk1(a int not null, b int not null default '0', c int, d int, primary key(c))")
	tk.MustExec("insert into test_add_index_with_pk1 values(1, 1, 1, 1)")
	tk.MustExec("alter table test_add_index_with_pk1 add index idx (c)")
	tk.MustExec("insert into test_add_index_with_pk1 values(2, 2, 2, 2)")
	tk.MustQuery("select * from test_add_index_with_pk1").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	tk.MustExec("drop table if exists test_add_index_with_pk2")
	tk.MustExec("create table test_add_index_with_pk2(a int not null, b int not null default '0', c int unsigned, d int, primary key(c))")
	tk.MustExec("insert into test_add_index_with_pk2 values(1, 1, 1, 1)")
	tk.MustExec("alter table test_add_index_with_pk2 add index idx (c)")
	tk.MustExec("insert into test_add_index_with_pk2 values(2, 2, 2, 2)")
	tk.MustQuery("select * from test_add_index_with_pk2").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, primary key(a, b));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("create index idx on t (a, b);")
}

func (s *testDBSuite7) TestAddIndexWithPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	testAddIndexWithPK(tk)
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	testAddIndexWithPK(tk)
}

func (s *testDBSuite5) TestAddIndexWithDupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	err1 := ddl.ErrDupKeyName.GenWithStack("index already exist %s", "idx")
	err2 := ddl.ErrDupKeyName.GenWithStack("index already exist %s; "+
		"a background job is trying to add the same index, "+
		"please check by `ADMIN SHOW DDL JOBS`", "idx")

	// When there is already an duplicate index, show error message.
	tk.MustExec("create table test_add_index_with_dup (a int, key idx (a))")
	_, err := tk.Exec("alter table test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err1).Error() == err.Error(), IsTrue)

	// When there is another session adding duplicate index with state other than
	// StatePublic, show explicit error message.
	t := s.testGetTable(c, "test_add_index_with_dup")
	indexInfo := t.Meta().FindIndexByName("idx")
	indexInfo.State = model.StateNone
	_, err = tk.Exec("alter table test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err2).Error() == err.Error(), IsTrue)

	tk.MustExec("drop table test_add_index_with_dup")
}

func (s *testDBSuite1) TestRenameIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("create table t (pk int primary key, c int default 1, c1 int default 1, unique key k1(c), key k2(c1))")

	// Test rename success
	tk.MustExec("alter table t rename index k1 to k3")
	tk.MustExec("admin check index t k3")

	// Test rename to the same name
	tk.MustExec("alter table t rename index k3 to k3")
	tk.MustExec("admin check index t k3")

	// Test rename on non-exists keys
	tk.MustGetErrCode("alter table t rename index x to x", errno.ErrKeyDoesNotExist)

	// Test rename on already-exists keys
	tk.MustGetErrCode("alter table t rename index k3 to k2", errno.ErrDupKeyName)

	tk.MustExec("alter table t rename index k2 to K2")
	tk.MustGetErrCode("alter table t rename key k3 to K2", errno.ErrDupKeyName)
}

func testGetTableByName(c *C, ctx sessionctx.Context, db, table string) table.Table {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func testGetSchemaByName(c *C, ctx sessionctx.Context, db string) *model.DBInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	dbInfo, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(db))
	c.Assert(ok, IsTrue)
	return dbInfo
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	ctx := s.s.(sessionctx.Context)
	return testGetTableByName(c, ctx, s.schemaName, name)
}

func (s *testDBSuite) testGetDB(c *C, dbName string) *model.DBInfo {
	ctx := s.s.(sessionctx.Context)
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	c.Assert(ok, IsTrue)
	return db
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := session.CreateSession4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute(context.Background(), "use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), sql)
	done <- errors.Trace(err)
}

// TestAddPrimaryKeyRollback1 is used to test scenarios that will roll back when a duplicate primary key is encountered.
func (s *testDBSuite8) TestAddPrimaryKeyRollback1(c *C) {
	hasNullValsInKey := false
	idxName := "PRIMARY"
	addIdxSQL := "alter table t1 add primary key c3_index (c3);"
	errMsg := "[kv:1062]Duplicate entry '" + strconv.Itoa(defaultBatchSize*2-10) + "' for key 'PRIMARY'"
	testAddIndexRollback(c, s.store, s.lease, idxName, addIdxSQL, errMsg, hasNullValsInKey)
}

// TestAddPrimaryKeyRollback2 is used to test scenarios that will roll back when a null primary key is encountered.
func (s *testDBSuite8) TestAddPrimaryKeyRollback2(c *C) {
	hasNullValsInKey := true
	idxName := "PRIMARY"
	addIdxSQL := "alter table t1 add primary key c3_index (c3);"
	errMsg := "[ddl:1138]Invalid use of NULL value"
	testAddIndexRollback(c, s.store, s.lease, idxName, addIdxSQL, errMsg, hasNullValsInKey)
}

func (s *testDBSuite2) TestAddUniqueIndexRollback(c *C) {
	hasNullValsInKey := false
	idxName := "c3_index"
	addIdxSQL := "create unique index c3_index on t1 (c3)"
	errMsg := "[kv:1062]Duplicate entry '" + strconv.Itoa(defaultBatchSize*2-10) + "' for key 'c3_index'"
	testAddIndexRollback(c, s.store, s.lease, idxName, addIdxSQL, errMsg, hasNullValsInKey)
}

func (s *testSerialDBSuite) TestWriteReorgForColumnTypeChangeOnAmendTxn(c *C) {
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = ON;")
	defer func() {
		tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = OFF;")
	}()

	d := s.dom.DDL()
	originalHook := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalHook)
	testInsertOnModifyColumn := func(sql string, startColState, commitColState model.SchemaState, retStrs []string, retErr error) {
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test_db")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
		tk.MustExec("insert into t1 values (20, 20, 20);")

		var checkErr error
		tk1 := testkit.NewTestKit(c, s.store)
		defer func() {
			if tk1.Se != nil {
				tk1.Se.Close()
			}
		}()
		hook := &ddl.TestDDLCallback{Do: s.dom}
		times := 0
		hook.OnJobUpdatedExported = func(job *model.Job) {
			if job.Type != model.ActionModifyColumn || checkErr != nil ||
				(job.SchemaState != startColState && job.SchemaState != commitColState) {
				return
			}

			if job.SchemaState == startColState {
				tk1.MustExec("use test_db")
				tk1.MustExec("begin pessimistic;")
				tk1.MustExec("insert into t1 values(101, 102, 103)")
				return
			}
			if times == 0 {
				_, checkErr = tk1.Exec("commit;")
			}
			times++
		}
		d.(ddl.DDLForTest).SetHook(hook)

		tk.MustExec(sql)
		if retErr == nil {
			c.Assert(checkErr, IsNil)
		} else {
			c.Assert(strings.Contains(checkErr.Error(), retErr.Error()), IsTrue)
		}
		tk.MustQuery("select * from t1;").Check(testkit.Rows(retStrs...))

		tk.MustExec("admin check table t1")
	}

	// Testing it needs reorg data.
	ddlStatement := "alter table t1 change column c2 cc smallint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)

	// Testing it needs not reorg data. This case only have two state: none, public.
	ddlStatement = "alter table t1 change column c2 cc bigint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20", "101 102 103"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, nil)
}

func (s *testSerialDBSuite) TestAddExpressionIndexRollback(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	tk.MustExec("insert into t1 values (20, 20, 20), (40, 40, 40), (80, 80, 80), (160, 160, 160);")

	var checkErr error
	tk1 := testkit.NewTestKit(c, s.store)
	_, checkErr = tk1.Exec("use test_db")

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var currJob *model.Job
	ctx := mock.NewContext()
	ctx.Store = s.store
	times := 0
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (6, 3, 3) on duplicate key update c1 = 10")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 7 where c2=6;")
			}
			if checkErr == nil {
				_, checkErr = tk1.Exec("delete from t1 where c1 = 40;")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2, 2, 2)")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 3 where c2 = 80")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (4, 4, 4)")
				if checkErr != nil {
					return
				}
				_, checkErr = tk1.Exec("update t1 set c1 = 5 where c2 = 80")
				if checkErr != nil {
					return
				}
				currJob = job
				times++
			}
		}
	}
	d.(ddl.DDLForTest).SetHook(hook)

	tk.MustGetErrMsg("alter table t1 add index expr_idx ((pow(c1, c2)));", "[ddl:8202]Cannot decode index value, because [types:1690]DOUBLE value is out of range in 'pow(160, 160)'")
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows("2 2 2", "4 4 4", "5 80 80", "10 3 3", "20 20 20", "160 160 160"))

	// Check whether the reorg information is cleaned up.
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	element, start, end, physicalID, err := m.GetDDLReorgHandle(currJob)
	c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)
	c.Assert(element, IsNil)
	c.Assert(start, IsNil)
	c.Assert(end, IsNil)
	c.Assert(physicalID, Equals, int64(0))
}

func (s *testSerialDBSuite) TestDropTableOnTiKVDiskFull(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table test_disk_full_drop_table(a int);")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull")
	tk.MustExec("drop table test_disk_full_drop_table;")
}

func batchInsert(tk *testkit.TestKit, tbl string, start, end int) {
	dml := fmt.Sprintf("insert into %s values", tbl)
	for i := start; i < end; i++ {
		dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
		if i != end-1 {
			dml += ","
		}
	}
	tk.MustExec(dml)
}

func testAddIndexRollback(c *C, store kv.Storage, lease time.Duration, idxName, addIdxSQL, errMsg string, hasNullValsInKey bool) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 2
	count := base
	// add some rows
	batchInsert(tk, "t1", 0, count)
	// add some null rows
	if hasNullValsInKey {
		for i := count - 10; i < count; i++ {
			tk.MustExec("insert into t1 values (?, ?, null)", i+10, i)
		}
	} else {
		// add some duplicate rows
		for i := count - 10; i < count; i++ {
			tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
		}
	}

	done := make(chan error, 1)
	go backgroundExec(store, addIdxSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, errMsg, Commentf("err:%v", err))
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				// (2048, 2038, 2038) and (2038, 2038, 2038)
				// Don't delete rows where c1 is 2048 or 2038, otherwise, the entry value in duplicated error message would change.
				if n == defaultBatchSize*2-10 || n == defaultBatchSize*2 {
					continue
				}
				tk.MustExec("delete from t1 where c1 = ?", n)
				tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	ctx := tk.Se.(sessionctx.Context)
	t := testGetTableByName(c, ctx, "test_db", "t1")
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, idxName), IsFalse)
	}

	// delete duplicated/null rows, then add index
	for i := base - 10; i < base; i++ {
		tk.MustExec("delete from t1 where c1 = ?", i+10)
	}
	sessionExec(c, store, addIdxSQL)
	tk.MustExec("drop table t1")
}

func (s *testDBSuite8) TestCancelAddPrimaryKey(c *C) {
	idxName := "primary"
	addIdxSQL := "alter table t1 add primary key idx_c2 (c2);"
	testCancelAddIndex(c, s.store, s.dom.DDL(), s.lease, idxName, addIdxSQL, "", s.dom)

	// Check the column's flag when the "add primary key" failed.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetTableByName(c, ctx, "test_db", "t1")
	col1Flag := t.Cols()[1].Flag
	c.Assert(!mysql.HasNotNullFlag(col1Flag) && !mysql.HasPreventNullInsertFlag(col1Flag) && mysql.HasUnsignedFlag(col1Flag), IsTrue)
	tk.MustExec("drop table t1")
}

func (s *testDBSuite7) TestCancelAddIndex(c *C) {
	idxName := "c3_index"
	addIdxSQL := "create unique index c3_index on t1 (c3)"
	testCancelAddIndex(c, s.store, s.dom.DDL(), s.lease, idxName, addIdxSQL, "", s.dom)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table t1")
}

func testCancelAddIndex(c *C, store kv.Storage, d ddl.DDL, lease time.Duration, idxName, addIdxSQL, sqlModeSQL string, dom *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int unsigned, c3 int, unique key(c1))")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	count := defaultBatchSize * 32
	start := 0
	// add some rows
	if len(sqlModeSQL) != 0 {
		// Insert some null values.
		tk.MustExec(sqlModeSQL)
		tk.MustExec("insert into t1 set c1 = ?", 0)
		tk.MustExec("insert into t1 set c2 = ?", 1)
		tk.MustExec("insert into t1 set c3 = ?", 2)
		start = 3
	}
	for i := start; i < count; i += defaultBatchSize {
		batchInsert(tk, "t1", i, i+defaultBatchSize)
	}

	var c3IdxInfo *model.IndexInfo
	hook := &ddl.TestDDLCallback{Do: dom}
	originBatchSize := tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	// Set batch size to lower try to slow down add-index reorganization, This if for hook to cancel this ddl job.
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 32")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_batch_size = %v", originBatchSize.Rows()[0][0]))
	// let hook.OnJobUpdatedExported has chance to cancel the job.
	// the hook.OnJobUpdatedExported is called when the job is updated, runReorgJob will wait ddl.ReorgWaitTimeout, then return the ddl.runDDLJob.
	// After that ddl call d.hook.OnJobUpdated(job), so that we can canceled the job in this test case.
	var checkErr error
	ctx := tk.Se.(sessionctx.Context)
	hook.OnJobUpdatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUpdatedExported(c, store, ctx, hook, idxName)
	originalHook := d.GetHook()
	jobIDExt := wrapJobIDExtCallback(hook)
	d.(ddl.DDLForTest).SetHook(jobIDExt)
	done := make(chan error, 1)
	go backgroundExec(store, addIdxSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustExec("delete from t1 where c1 = ?", n)
				tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}
	checkDelRangeAdded(tk, jobIDExt.jobID, c3IdxInfo.ID)
	d.(ddl.DDLForTest).SetHook(originalHook)
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testDBSuite4) TestCancelAddIndex1(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t")
	s.mustExec(tk, c, "create table t(c1 int, c2 int)")
	defer s.mustExec(tk, c, "drop table t;")

	for i := 0; i < 50; i++ {
		s.mustExec(tk, c, "insert into t values (?, ?)", i, i)
	}

	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}

			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")

	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	s.mustExec(tk, c, "alter table t add index idx_c2(c2)")
	s.mustExec(tk, c, "alter table t drop index idx_c2")
}

// TestCancelDropIndex tests cancel ddl job which type is drop primary key.
func (s *testDBSuite4) TestCancelDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxSQL := "alter table t add primary key idx_c2 (c2);"
	dropIdxSQL := "alter table t drop primary key;"
	testCancelDropIndex(c, s.store, s.dom.DDL(), idxName, addIdxSQL, dropIdxSQL, s.dom)
}

// TestCancelDropIndex tests cancel ddl job which type is drop index.
func (s *testDBSuite5) TestCancelDropIndex(c *C) {
	idxName := "idx_c2"
	addIdxSQL := "alter table t add index idx_c2 (c2);"
	dropIdxSQL := "alter table t drop index idx_c2;"
	testCancelDropIndex(c, s.store, s.dom.DDL(), idxName, addIdxSQL, dropIdxSQL, s.dom)
}

// testCancelDropIndex tests cancel ddl job which type is drop index.
func testCancelDropIndex(c *C, store kv.Storage, d ddl.DDL, idxName, addIdxSQL, dropIdxSQL string, dom *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	testCases := []struct {
		needAddIndex   bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		// model.JobStateNone means the jobs is canceled before the first run.
		// if we cancel successfully, we need to set needAddIndex to false in the next test case. Otherwise, set needAddIndex to true.
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: dom}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if (job.Type == model.ActionDropIndex || job.Type == model.ActionDropPrimaryKey) &&
			job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	ctx := tk.Se.(sessionctx.Context)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			tk.MustExec(addIdxSQL)
		}
		rs, err := tk.Exec(dropIdxSQL)
		if rs != nil {
			rs.Close()
		}
		t := testGetTableByName(c, ctx, "test_db", "t")
		indexInfo := t.Meta().FindIndexByName(idxName)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			c.Assert(indexInfo, NotNil)
			c.Assert(indexInfo.State, Equals, model.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfo, IsNil)
		}
	}
	d.(ddl.DDLForTest).SetHook(originalHook)
	tk.MustExec(addIdxSQL)
	tk.MustExec(dropIdxSQL)
}

// TestCancelTruncateTable tests cancel ddl job which type is truncate table.
func (s *testDBSuite5) TestCancelTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "create database if not exists test_truncate_table")
	s.mustExec(tk, c, "drop table if exists t")
	s.mustExec(tk, c, "create table t(c1 int, c2 int)")
	defer s.mustExec(tk, c, "drop table t;")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTable && job.State == model.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err := tk.Exec("truncate table t")
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
}

func (s *testDBSuite5) TestParallelDropSchemaAndDropTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "create database if not exists test_drop_schema_table")
	s.mustExec(tk, c, "use test_drop_schema_table")
	s.mustExec(tk, c, "create table t(c1 int, c2 int)")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	dbInfo := testGetSchemaByName(c, tk.Se, "test_drop_schema_table")
	done := false
	var wg sync.WaitGroup
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_drop_schema_table")
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.Type == model.ActionDropSchema && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteOnly && job.SchemaID == dbInfo.ID && done == false {
			wg.Add(1)
			done = true
			go func() {
				_, checkErr = tk2.Exec("drop table t")
				wg.Done()
			}()
			time.Sleep(5 * time.Millisecond)
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	s.mustExec(tk, c, "drop database test_drop_schema_table")
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	wg.Wait()
	c.Assert(done, IsTrue)
	c.Assert(checkErr, NotNil)
	// There are two possible assert result because:
	// 1: If drop-database is finished before drop-table being put into the ddl job queue, it will return "unknown table" error directly in the previous check.
	// 2: If drop-table has passed the previous check and been put into the ddl job queue, then drop-database finished, it will return schema change error.
	assertRes := checkErr.Error() == "[domain:8028]Information schema is changed during the execution of the"+
		" statement(for example, table definition may be updated by other DDL ran in parallel). "+
		"If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]" ||
		checkErr.Error() == "[schema:1051]Unknown table 'test_drop_schema_table.t'"

	c.Assert(assertRes, Equals, true)

	// Below behaviour is use to mock query `curl "http://$IP:10080/tiflash/replica"`
	fn := func(jobs []*model.Job) (bool, error) {
		return executor.GetDropOrTruncateTableInfoFromJobs(jobs, 0, s.dom, func(job *model.Job, info *model.TableInfo) (bool, error) {
			return false, nil
		})
	}
	err := tk.Se.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	err = admin.IterHistoryDDLJobs(txn, fn)
	c.Assert(err, IsNil)
}

// TestCancelRenameIndex tests cancel ddl job which type is rename index.
func (s *testDBSuite1) TestCancelRenameIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "create database if not exists test_rename_index")
	s.mustExec(tk, c, "drop table if exists t")
	s.mustExec(tk, c, "create table t(c1 int, c2 int)")
	defer s.mustExec(tk, c, "drop table t;")
	for i := 0; i < 100; i++ {
		s.mustExec(tk, c, "insert into t values (?, ?)", i, i)
	}
	s.mustExec(tk, c, "alter table t add index idx_c2(c2)")
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRenameIndex && job.State == model.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := tk.Exec("alter table t rename index idx_c2 to idx_c3")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c3"), IsFalse)
	}
	s.mustExec(tk, c, "alter table t rename index idx_c2 to idx_c3")
}

// TestCancelDropTable tests cancel ddl job which type is drop table.
func (s *testDBSuite2) TestCancelDropTableAndSchema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testCases := []struct {
		needAddTableOrDB bool
		action           model.ActionType
		jobState         model.JobState
		JobSchemaState   model.SchemaState
		cancelSucc       bool
	}{
		// Check drop table.
		// model.JobStateNone means the jobs is canceled before the first run.
		{true, model.ActionDropTable, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropTable, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropTable, model.JobStateRunning, model.StateDeleteOnly, false},

		// Check drop database.
		{true, model.ActionDropSchema, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropSchema, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropSchema, model.JobStateRunning, model.StateDeleteOnly, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var jobID int64
	testCase := &testCases[0]
	s.mustExec(tk, c, "create database if not exists test_drop_db")
	dbInfo := s.testGetDB(c, "test_drop_db")

	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState && job.SchemaID == dbInfo.ID {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err error
	sql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddTableOrDB {
			s.mustExec(tk, c, "create database if not exists test_drop_db")
			s.mustExec(tk, c, "use test_drop_db")
			s.mustExec(tk, c, "create table if not exists t(c1 int, c2 int)")
		}

		dbInfo = s.testGetDB(c, "test_drop_db")

		if testCase.action == model.ActionDropTable {
			sql = "drop table t;"
		} else if testCase.action == model.ActionDropSchema {
			sql = "drop database test_drop_db;"
		}

		_, err = tk.Exec(sql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			s.mustExec(tk, c, "insert into t values (?, ?)", i, i)
		} else {
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			_, err = tk.Exec("insert into t values (?, ?)", i, i)
			c.Assert(err, NotNil)
		}
	}
}

func (s *testDBSuite3) TestAddAnonymousIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	s.mustExec(tk, c, "create table t_anonymous_index (c1 int, c2 int, C3 int)")
	s.mustExec(tk, c, "alter table t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	_, err := tk.Exec("alter table t_anonymous_index drop index")
	c.Assert(err, NotNil)
	// The index name is c1 when adding index (c1, c2).
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c1")
	t := s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 0)
	// for adding some indices that the first column name is c1
	s.mustExec(tk, c, "alter table t_anonymous_index add index (c1)")
	_, err = tk.Exec("alter table t_anonymous_index add index c1 (c2)")
	c.Assert(err, NotNil)
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 1)
	idx := t.Indices()[0].Meta().Name.L
	c.Assert(idx, Equals, "c1")
	// The MySQL will be a warning.
	s.mustExec(tk, c, "alter table t_anonymous_index add index c1_3 (c1)")
	s.mustExec(tk, c, "alter table t_anonymous_index add index (c1, c2, C3)")
	// The MySQL will be a warning.
	s.mustExec(tk, c, "alter table t_anonymous_index add index (c1)")
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 4)
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c1")
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c1_2")
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c1_3")
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c1_4")
	// for case insensitive
	s.mustExec(tk, c, "alter table t_anonymous_index add index (C3)")
	s.mustExec(tk, c, "alter table t_anonymous_index drop index c3")
	s.mustExec(tk, c, "alter table t_anonymous_index add index c3 (C3)")
	s.mustExec(tk, c, "alter table t_anonymous_index drop index C3")
	// for anonymous index with column name `primary`
	s.mustExec(tk, c, "create table t_primary (`primary` int, b int, key (`primary`))")
	t = s.testGetTable(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	s.mustExec(tk, c, "alter table t_primary add index (`primary`);")
	t = s.testGetTable(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustExec(tk, c, "alter table t_primary add primary key(b);")
	t = s.testGetTable(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	c.Assert(t.Indices()[2].Meta().Name.L, Equals, "primary")
	s.mustExec(tk, c, "create table t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	t = s.testGetTable(c, "t_primary_2")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustExec(tk, c, "create table t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	t = s.testGetTable(c, "t_primary_3")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
}

func (s *testDBSuite4) TestAlterLock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	s.mustExec(tk, c, "create table t_index_lock (c1 int, c2 int, C3 int)")
	s.mustExec(tk, c, "alter table t_index_lock add index (c1, c2), lock=none")
}

func (s *testDBSuite5) TestAddMultiColumnsIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (1, 1);")
	tk.MustExec("update tidb.test set b = b + 1 where a = 1;")
	tk.MustExec("insert into tidb.test values (2, 2);")
	// Test that the b value is nil.
	tk.MustExec("insert into tidb.test (a) values (3);")
	tk.MustExec("insert into tidb.test values (4, 4);")
	// Test that the b value is nil again.
	tk.MustExec("insert into tidb.test (a) values (5);")
	tk.MustExec("insert tidb.test values (6, 6);")
	tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	tk.MustExec("admin check table test")
}

func (s *testDBSuite6) TestAddMultiColumnsIndexClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_add_multi_col_index_clustered;")
	tk.MustExec("create database test_add_multi_col_index_clustered;")
	tk.MustExec("use test_add_multi_col_index_clustered;")

	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b varchar(10), c int, primary key (a, b));")
	tk.MustExec("insert into t values (1, '1', 1), (2, '2', NULL), (3, '3', 3);")
	tk.MustExec("create index idx on t (a, c);")

	tk.MustExec("admin check index t idx;")
	tk.MustExec("admin check table t;")

	tk.MustExec("insert into t values (5, '5', 5), (6, '6', NULL);")

	tk.MustExec("admin check index t idx;")
	tk.MustExec("admin check table t;")
}

func (s *testDBSuite6) TestAddPrimaryKey1(c *C) {
	testAddIndex(c, s.store, s.lease, testPlain,
		"create table test_add_index (c1 bigint, c2 bigint, c3 bigint, unique key(c1))", "primary")
}

func (s *testDBSuite2) TestAddPrimaryKey2(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func (s *testDBSuite3) TestAddPrimaryKey3(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by hash (c3) partitions 4;`, "primary")
}

func (s *testDBSuite4) TestAddPrimaryKey4(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range columns (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func (s *testDBSuite6) TestAddIndex1(c *C) {
	testAddIndex(c, s.store, s.lease, testPlain,
		"create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))", "")
}

func (s *testSerialDBSuite1) TestAddIndex1WithShardRowID(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition|testShardRowID,
		"create table test_add_index (c1 bigint, c2 bigint, c3 bigint) SHARD_ROW_ID_BITS = 4 pre_split_regions = 4;", "")
}

func (s *testDBSuite2) TestAddIndex2(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testSerialDBSuite) TestAddIndex2WithShardRowID(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testDBSuite3) TestAddIndex3(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by hash (c1) partitions 4;`, "")
}

func (s *testSerialDBSuite1) TestAddIndex3WithShardRowID(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by hash (c1) partitions 4;`, "")
}

func (s *testDBSuite8) TestAddIndex4(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range columns (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testSerialDBSuite) TestAddIndex4WithShardRowID(c *C) {
	testAddIndex(c, s.store, s.lease, testPartition|testShardRowID,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint)
				  SHARD_ROW_ID_BITS = 4 pre_split_regions = 4
			      partition by range columns (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testDBSuite5) TestAddIndex5(c *C) {
	testAddIndex(c, s.store, s.lease, testClusteredIndex,
		`create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c2, c3))`, "")
}

type testAddIndexType uint8

const (
	testPlain          testAddIndexType = 1
	testPartition      testAddIndexType = 1 << 1
	testClusteredIndex testAddIndexType = 1 << 2
	testShardRowID     testAddIndexType = 1 << 3
)

func testAddIndex(c *C, store kv.Storage, lease time.Duration, tp testAddIndexType, createTableSQL, idxTp string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	isTestPartition := (testPartition & tp) > 0
	isTestShardRowID := (testShardRowID & tp) > 0
	if isTestShardRowID {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 1")
		defer func() {
			atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
			tk.MustExec("set global tidb_scatter_region = 0")
		}()
	}
	if isTestPartition {
		tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	} else if (testClusteredIndex & tp) > 0 {
		tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	}
	tk.MustExec("drop table if exists test_add_index")
	tk.MustExec(createTableSQL)

	done := make(chan error, 1)
	start := -10
	num := defaultBatchSize
	// first add some rows
	batchInsert(tk, "test_add_index", start, num)

	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	otherKeys := make([]int, 0, batchCnt*maxBatch)
	// Make sure there are no duplicate keys.
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		if isTestShardRowID {
			base = i % 4 << 61
		}
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", n, n, n)
			tk.MustExec(sql)
			otherKeys = append(otherKeys, n)
		}
	}
	// Encounter the value of math.MaxInt64 in middle of
	v := math.MaxInt64 - defaultBatchSize/2
	tk.MustExec(fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", v, v, v))
	otherKeys = append(otherKeys, v)

	addIdxSQL := fmt.Sprintf("alter table test_add_index add %s key c3_index(c3)", idxTp)
	testddlutil.SessionExecInGoroutine(store, addIdxSQL, done)

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num > defaultBatchSize*10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				deletedKeys[n] = struct{}{}
				sql := fmt.Sprintf("delete from test_add_index where c1 = %d", n)
				tk.MustExec(sql)
				sql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				tk.MustExec(sql)
			}
			num += step
		}
	}

	if isTestShardRowID {
		re := tk.MustQuery("show table test_add_index regions;")
		rows := re.Rows()
		c.Assert(len(rows), GreaterEqual, 16)
		tk.MustExec("admin check table test_add_index")
		return
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := start; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}
	keys = append(keys, otherKeys...)

	// test index key
	expectedRows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		expectedRows = append(expectedRows, []interface{}{key})
	}
	rows := tk.MustQuery(fmt.Sprintf("select c1 from test_add_index where c3 >= %d order by c1", start)).Rows()
	matchRows(c, rows, expectedRows)

	tk.MustExec("admin check table test_add_index")
	if isTestPartition {
		return
	}

	// TODO: Support explain in future.
	// rows := s.mustQuery(c, "explain select c1 from test_add_index where c3 >= 100")

	// ay := dumpRows(c, rows)
	// c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)

	// get all row handles
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetTableByName(c, ctx, "test_db", "test_add_index")
	handles := kv.NewHandleMap()
	err := tables.IterRecords(t, ctx, t.Cols(),
		func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			handles.Set(h, struct{}{})
			return true, nil
		})
	c.Assert(err, IsNil)

	// check in index
	var nidx table.Index
	idxName := "c3_index"
	if len(idxTp) != 0 {
		idxName = "primary"
	}
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			nidx = tidx
			break
		}
	}
	// Make sure there is index with name c3_index.
	c.Assert(nidx, NotNil)
	c.Assert(nidx.Meta().ID, Greater, int64(0))
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	c.Assert(ctx.NewTxn(context.Background()), IsNil)

	tk.MustExec("admin check table test_add_index")
	tk.MustExec("drop table test_add_index")
}

func (s *testDBSuite1) TestAddIndexWithSplitTable(c *C) {
	createSQL := "CREATE TABLE test_add_index(a bigint PRIMARY KEY AUTO_RANDOM(4), b varchar(255), c bigint)"
	stSQL := fmt.Sprintf("SPLIT TABLE test_add_index BETWEEN (%d) AND (%d) REGIONS 16;", math.MinInt64, math.MaxInt64)
	testAddIndexWithSplitTable(c, s.store, s.lease, createSQL, stSQL)
}

func (s *testSerialDBSuite) TestAddIndexWithShardRowID(c *C) {
	createSQL := "create table test_add_index(a bigint, b bigint, c bigint) SHARD_ROW_ID_BITS = 4 pre_split_regions = 4;"
	testAddIndexWithSplitTable(c, s.store, s.lease, createSQL, "")
}

func testAddIndexWithSplitTable(c *C, store kv.Storage, lease time.Duration, createSQL, splitTableSQL string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_add_index")
	hasAutoRadomField := len(splitTableSQL) > 0
	if !hasAutoRadomField {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 1")
		defer func() {
			atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
			tk.MustExec("set global tidb_scatter_region = 0")
		}()
	}
	tk.MustExec(createSQL)

	batchInsertRows := func(tk *testkit.TestKit, needVal bool, tbl string, start, end int) error {
		dml := fmt.Sprintf("insert into %s values", tbl)
		for i := start; i < end; i++ {
			if needVal {
				dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
			} else {
				dml += "()"
			}
			if i != end-1 {
				dml += ","
			}
		}
		_, err := tk.Exec(dml)
		return err
	}

	done := make(chan error, 1)
	start := -20
	num := defaultBatchSize
	// Add some discrete rows.
	goCnt := 10
	errCh := make(chan error, goCnt)
	for i := 0; i < goCnt; i++ {
		base := (i % 8) << 60
		go func(b int, eCh chan error) {
			tk1 := testkit.NewTestKit(c, store)
			tk1.MustExec("use test_db")
			eCh <- batchInsertRows(tk1, !hasAutoRadomField, "test_add_index", base+start, base+num)
		}(base, errCh)
	}
	for i := 0; i < goCnt; i++ {
		e := <-errCh
		c.Assert(e, IsNil)
	}

	if hasAutoRadomField {
		tk.MustQuery(splitTableSQL).Check(testkit.Rows("15 1"))
	}
	tk.MustQuery("select @@session.tidb_wait_split_region_finish;").Check(testkit.Rows("1"))
	re := tk.MustQuery("show table test_add_index regions;")
	rows := re.Rows()
	c.Assert(len(rows), Equals, 16)
	addIdxSQL := "alter table test_add_index add index idx(a)"
	testddlutil.SessionExecInGoroutine(store, addIdxSQL, done)

	ticker := time.NewTicker(lease / 5)
	defer ticker.Stop()
	num = 0
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num >= 1000 {
				break
			}
			step := 20
			// delete, insert and update some data
			for i := num; i < num+step; i++ {
				sql := fmt.Sprintf("delete from test_add_index where a = %d", i+1)
				tk.MustExec(sql)
				if hasAutoRadomField {
					sql = "insert into test_add_index values ()"
				} else {
					sql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				}
				tk.MustExec(sql)
				sql = fmt.Sprintf("update test_add_index set b = %d", i*10)
				tk.MustExec(sql)
			}
			num += step
		}
	}

	tk.MustExec("admin check table test_add_index")
}

// TestCancelAddTableAndDropTablePartition tests cancel ddl job which type is add/drop table partition.
func (s *testDBSuite1) TestCancelAddTableAndDropTablePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "create database if not exists test_partition_table")
	s.mustExec(tk, c, "use test_partition_table")
	s.mustExec(tk, c, "drop table if exists t_part")
	s.mustExec(tk, c, `create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	);`)
	defer s.mustExec(tk, c, "drop table t_part;")
	base := 10
	for i := 0; i < base; i++ {
		s.mustExec(tk, c, "insert into t_part values (?)", i)
	}

	testCases := []struct {
		action         model.ActionType
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		{model.ActionAddTablePartition, model.JobStateNone, model.StateNone, true},
		{model.ActionDropTablePartition, model.JobStateNone, model.StateNone, true},
		// Add table partition now can be cancelled in ReplicaOnly state.
		{model.ActionAddTablePartition, model.JobStateRunning, model.StateReplicaOnly, true},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	testCase := &testCases[0]
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	var err error
	sql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.action == model.ActionAddTablePartition {
			sql = `alter table t_part add partition (
				partition p2 values less than (30)
				);`
		} else if testCase.action == model.ActionDropTablePartition {
			sql = "alter table t_part drop partition p1;"
		}
		_, err = tk.Exec(sql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			s.mustExec(tk, c, "insert into t_part values (?)", i+base)

			ctx := s.s.(sessionctx.Context)
			is := domain.GetDomain(ctx).InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test_partition_table"), model.NewCIStr("t_part"))
			c.Assert(err, IsNil)
			partitionInfo := tbl.Meta().GetPartitionInfo()
			c.Assert(partitionInfo, NotNil)
			c.Assert(len(partitionInfo.AddingDefinitions), Equals, 0)
		} else {
			c.Assert(err, IsNil, Commentf("err:%v", err))
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			_, err = tk.Exec("insert into t_part values (?)", i)
			c.Assert(err, NotNil)
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
}

func (s *testDBSuite1) TestDropPrimaryKey(c *C) {
	idxName := "primary"
	createSQL := "create table test_drop_index (c1 int, c2 int, c3 int, unique key(c1), primary key(c3) nonclustered)"
	dropIdxSQL := "alter table test_drop_index drop primary key;"
	testDropIndex(c, s.store, s.lease, createSQL, dropIdxSQL, idxName)
}

func (s *testDBSuite2) TestDropIndex(c *C) {
	idxName := "c3_index"
	createSQL := "create table test_drop_index (c1 int, c2 int, c3 int, unique key(c1), key c3_index(c3))"
	dropIdxSQL := "alter table test_drop_index drop index c3_index;"
	testDropIndex(c, s.store, s.lease, createSQL, dropIdxSQL, idxName)
}

func testDropIndex(c *C, store kv.Storage, lease time.Duration, createSQL, dropIdxSQL, idxName string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_drop_index")
	tk.MustExec(createSQL)
	done := make(chan error, 1)
	tk.MustExec("delete from test_drop_index")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		tk.MustExec("insert into test_drop_index values (?, ?, ?)", i, i, i)
	}
	ctx := tk.Se.(sessionctx.Context)
	indexID := testGetIndexID(c, ctx, "test_db", "test_drop_index", idxName)
	jobIDExt, reset := setupJobIDExtCallback(ctx)
	defer reset()
	testddlutil.SessionExecInGoroutine(store, dropIdxSQL, done)

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("update test_drop_index set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into test_drop_index values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := tk.MustQuery("explain select c1 from test_drop_index where c3 >= 0")
	c.Assert(strings.Contains(fmt.Sprintf("%v", rows), idxName), IsFalse)

	checkDelRangeAdded(tk, jobIDExt.jobID, indexID)
	tk.MustExec("drop table test_drop_index")
}

// TestCancelDropColumn tests cancel ddl job which type is drop column.
func (s *testDBSuite3) TestCancelDropColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	s.mustExec(tk, c, "drop table if exists test_drop_column")
	s.mustExec(tk, c, "create table test_drop_column(c1 int, c2 int)")
	defer s.mustExec(tk, c, "drop table test_drop_column;")
	testCases := []struct {
		needAddColumn  bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionDropColumn && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}

	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err1 error
	for i := range testCases {
		var c3IdxID int64
		testCase = &testCases[i]
		if testCase.needAddColumn {
			s.mustExec(tk, c, "alter table test_drop_column add column c3 int")
			s.mustExec(tk, c, "alter table test_drop_column add index idx_c3(c3)")
			ctx := tk.Se.(sessionctx.Context)
			c3IdxID = testGetIndexID(c, ctx, s.schemaName, "test_drop_column", "idx_c3")
		}
		_, err1 = tk.Exec("alter table test_drop_column drop column c3")
		var col1 *table.Column
		var idx1 table.Index
		t := s.testGetTable(c, "test_drop_column")
		for _, col := range t.Cols() {
			if strings.EqualFold(col.Name.L, "c3") {
				col1 = col
				break
			}
		}
		for _, idx := range t.Indices() {
			if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
				idx1 = idx
				break
			}
		}
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(col1, NotNil)
			c.Assert(col1.Name.L, Equals, "c3")
			c.Assert(idx1, NotNil)
			c.Assert(idx1.Meta().Name.L, Equals, "idx_c3")
			c.Assert(err1.Error(), Equals, "[ddl:8214]Cancelled DDL job")
		} else {
			c.Assert(col1, IsNil)
			c.Assert(idx1, IsNil)
			c.Assert(err1, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			if c3IdxID != 0 {
				// Check index is deleted
				checkDelRangeAdded(tk, jobID, c3IdxID)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.mustExec(tk, c, "alter table test_drop_column add column c3 int")
	s.mustExec(tk, c, "alter table test_drop_column drop column c3")
}

// TestCancelDropColumns tests cancel ddl job which type is drop multi-columns.
func (s *testDBSuite3) TestCancelDropColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	s.mustExec(tk, c, "drop table if exists test_drop_column")
	s.mustExec(tk, c, "create table test_drop_column(c1 int, c2 int)")
	defer s.mustExec(tk, c, "drop table test_drop_column;")
	testCases := []struct {
		needAddColumn  bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionDropColumns && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}

	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err1 error
	for i := range testCases {
		var c3IdxID int64
		testCase = &testCases[i]
		if testCase.needAddColumn {
			s.mustExec(tk, c, "alter table test_drop_column add column c3 int, add column c4 int")
			s.mustExec(tk, c, "alter table test_drop_column add index idx_c3(c3)")
			ctx := tk.Se.(sessionctx.Context)
			c3IdxID = testGetIndexID(c, ctx, s.schemaName, "test_drop_column", "idx_c3")
		}
		_, err1 = tk.Exec("alter table test_drop_column drop column c3, drop column c4")
		t := s.testGetTable(c, "test_drop_column")
		col3 := table.FindCol(t.Cols(), "c3")
		col4 := table.FindCol(t.Cols(), "c4")
		var idx3 table.Index
		for _, idx := range t.Indices() {
			if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
				idx3 = idx
				break
			}
		}
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(col3, NotNil)
			c.Assert(col4, NotNil)
			c.Assert(idx3, NotNil)
			c.Assert(col3.Name.L, Equals, "c3")
			c.Assert(col4.Name.L, Equals, "c4")
			c.Assert(idx3.Meta().Name.L, Equals, "idx_c3")
			c.Assert(err1.Error(), Equals, "[ddl:8214]Cancelled DDL job")
		} else {
			c.Assert(col3, IsNil)
			c.Assert(col4, IsNil)
			c.Assert(idx3, IsNil)
			c.Assert(err1, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			if c3IdxID != 0 {
				// Check index is deleted
				checkDelRangeAdded(tk, jobID, c3IdxID)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.mustExec(tk, c, "alter table test_drop_column add column c3 int, add column c4 int")
	s.mustExec(tk, c, "alter table test_drop_column drop column c3, drop column c4")
}

func testGetIndexID(c *C, ctx sessionctx.Context, dbName, tblName, idxName string) int64 {
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	c.Assert(err, IsNil)

	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName {
			return idx.Meta().ID
		}
	}
	c.Fatalf("index %s not found(db: %s, tbl: %s)", idxName, dbName, tblName)
	return -1
}

type testDDLJobIDCallback struct {
	ddl.Callback
	jobID int64
}

func (t *testDDLJobIDCallback) OnJobUpdated(job *model.Job) {
	if t.jobID == 0 {
		t.jobID = job.ID
	}
	if t.Callback != nil {
		t.Callback.OnJobUpdated(job)
	}
}

func wrapJobIDExtCallback(oldCallback ddl.Callback) *testDDLJobIDCallback {
	return &testDDLJobIDCallback{
		Callback: oldCallback,
		jobID:    0,
	}
}

func setupJobIDExtCallback(ctx sessionctx.Context) (jobExt *testDDLJobIDCallback, tearDown func()) {
	dom := domain.GetDomain(ctx)
	originHook := dom.DDL().GetHook()
	jobIDExt := wrapJobIDExtCallback(originHook)
	dom.DDL().SetHook(jobIDExt)
	return jobIDExt, func() {
		dom.DDL().SetHook(originHook)
	}
}

func checkDelRangeAdded(tk *testkit.TestKit, jobID int64, elemID int64) {
	query := `select sum(cnt) from
	(select count(1) cnt from mysql.gc_delete_range where job_id = ? and element_id = ? union
	select count(1) cnt from mysql.gc_delete_range_done where job_id = ? and element_id = ?) as gdr;`
	tk.MustQuery(query, jobID, elemID, jobID, elemID).Check(testkit.Rows("1"))
}

func checkGlobalIndexCleanUpDone(c *C, ctx sessionctx.Context, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, pid int64) int {
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	defer func() {
		err := txn.Rollback()
		c.Assert(err, IsNil)
	}()

	cnt := 0
	prefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idxInfo.ID)
	it, err := txn.Iter(prefix, nil)
	c.Assert(err, IsNil)
	for it.Valid() {
		if !it.Key().HasPrefix(prefix) {
			break
		}
		segs := tablecodec.SplitIndexValue(it.Value())
		c.Assert(segs.PartitionID, NotNil)
		_, pi, err := codec.DecodeInt(segs.PartitionID)
		c.Assert(err, IsNil)
		c.Assert(pi, Not(Equals), pid)
		cnt++
		err = it.Next()
		c.Assert(err, IsNil)
	}
	return cnt
}

func (s *testDBSuite5) TestAlterPrimaryKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', d int as (a+b), e int as (a+1) stored, index idx(b))")
	defer tk.MustExec("drop table test_add_pk")

	// for generated columns
	tk.MustGetErrCode("alter table test_add_pk add primary key(d);", errno.ErrUnsupportedOnGeneratedColumn)
	// The primary key name is the same as the existing index name.
	tk.MustExec("alter table test_add_pk add primary key idx(e)")
	tk.MustExec("drop index `primary` on test_add_pk")

	// for describing table
	tk.MustExec("create table test_add_pk1(a int, index idx(a))")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),YES,MUL,<nil>,`))
	tk.MustExec("alter table test_add_pk1 add primary key idx(a)")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),NO,PRI,<nil>,`))
	tk.MustExec("alter table test_add_pk1 drop primary key")
	tk.MustQuery("desc test_add_pk1").Check(testutil.RowsWithSep(",", `a,int(11),NO,MUL,<nil>,`))
	tk.MustExec("create table test_add_pk2(a int, b int, index idx(a))")
	tk.MustExec("alter table test_add_pk2 add primary key idx(a, b)")
	tk.MustQuery("desc test_add_pk2").Check(testutil.RowsWithSep(",", ""+
		"a int(11) NO PRI <nil> ]\n"+
		"[b int(11) NO PRI <nil> "))
	tk.MustQuery("show create table test_add_pk2").Check(testutil.RowsWithSep("|", ""+
		"test_add_pk2 CREATE TABLE `test_add_pk2` (\n"+
		"  `a` int(11) NOT NULL,\n"+
		"  `b` int(11) NOT NULL,\n"+
		"  KEY `idx` (`a`),\n"+
		"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] NONCLUSTERED */\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table test_add_pk2 drop primary key")
	tk.MustQuery("desc test_add_pk2").Check(testutil.RowsWithSep(",", ""+
		"a int(11) NO MUL <nil> ]\n"+
		"[b int(11) NO  <nil> "))

	// Check if the primary key exists before checking the table's pkIsHandle.
	tk.MustGetErrCode("alter table test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)

	// for the limit of name
	validName := strings.Repeat("a", mysql.MaxIndexIdentifierLen)
	invalidName := strings.Repeat("b", mysql.MaxIndexIdentifierLen+1)
	tk.MustGetErrCode("alter table test_add_pk add primary key "+invalidName+"(a)", errno.ErrTooLongIdent)
	// for valid name
	tk.MustExec("alter table test_add_pk add primary key " + validName + "(a)")
	// for multiple primary key
	tk.MustGetErrCode("alter table test_add_pk add primary key (a)", errno.ErrMultiplePriKey)
	tk.MustExec("alter table test_add_pk drop primary key")
	// for not existing primary key
	tk.MustGetErrCode("alter table test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("drop index `primary` on test_add_pk", errno.ErrCantDropFieldOrKey)

	// for too many key parts specified
	tk.MustGetErrCode("alter table test_add_pk add primary key idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);",
		errno.ErrTooManyKeyParts)

	// for the limit of comment's length
	validComment := "'" + strings.Repeat("a", ddl.MaxCommentLength) + "'"
	invalidComment := "'" + strings.Repeat("b", ddl.MaxCommentLength+1) + "'"
	tk.MustGetErrCode("alter table test_add_pk add primary key(a) comment "+invalidComment, errno.ErrTooLongIndexComment)
	// for empty sql_mode
	r := tk.MustQuery("select @@sql_mode")
	sqlMode := r.Rows()[0][0].(string)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("alter table test_add_pk add primary key(a) comment " + invalidComment)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'PRIMARY' is too long (max = 1024)"))
	tk.MustExec("set @@sql_mode= '" + sqlMode + "'")
	tk.MustExec("alter table test_add_pk drop primary key")
	// for valid comment
	tk.MustExec("alter table test_add_pk add primary key(a, b, c) comment " + validComment)
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetTableByName(c, ctx, "test", "test_add_pk")
	col1Flag := t.Cols()[0].Flag
	col2Flag := t.Cols()[1].Flag
	col3Flag := t.Cols()[2].Flag
	c.Assert(mysql.HasNotNullFlag(col1Flag) && !mysql.HasPreventNullInsertFlag(col1Flag), IsTrue)
	c.Assert(mysql.HasNotNullFlag(col2Flag) && !mysql.HasPreventNullInsertFlag(col2Flag) && mysql.HasUnsignedFlag(col2Flag), IsTrue)
	c.Assert(mysql.HasNotNullFlag(col3Flag) && !mysql.HasPreventNullInsertFlag(col3Flag) && !mysql.HasNoDefaultValueFlag(col3Flag), IsTrue)
	tk.MustExec("alter table test_add_pk drop primary key")

	// for null values in primary key
	tk.MustExec("drop table test_add_pk")
	tk.MustExec("create table test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', index idx(b))")
	tk.MustExec("insert into test_add_pk set a = 0, b = 0, c = 0")
	tk.MustExec("insert into test_add_pk set a = 1")
	tk.MustGetErrCode("alter table test_add_pk add primary key (b)", errno.ErrInvalidUseOfNull)
	tk.MustExec("insert into test_add_pk set a = 2, b = 2")
	tk.MustGetErrCode("alter table test_add_pk add primary key (a, b)", errno.ErrInvalidUseOfNull)
	tk.MustExec("insert into test_add_pk set a = 3, c = 3")
	tk.MustGetErrCode("alter table test_add_pk add primary key (c, b, a)", errno.ErrInvalidUseOfNull)
}

func (s *testDBSuite4) TestAddIndexWithDupCols(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	err1 := infoschema.ErrColumnExists.GenWithStackByArgs("b")
	err2 := infoschema.ErrColumnExists.GenWithStackByArgs("B")

	tk.MustExec("create table test_add_index_with_dup (a int, b int)")
	_, err := tk.Exec("create index c on test_add_index_with_dup(b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("create index c on test_add_index_with_dup(b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("alter table test_add_index_with_dup add index c (b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.Exec("alter table test_add_index_with_dup add index c (b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	tk.MustExec("drop table test_add_index_with_dup")
}

// checkGlobalIndexRow reads one record from global index and check. Only support int handle.
func checkGlobalIndexRow(c *C, ctx sessionctx.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo,
	pid int64, idxVals []types.Datum, rowVals []types.Datum) {
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	sc := ctx.GetSessionVars().StmtCtx
	c.Assert(err, IsNil)

	tblColMap := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		tblColMap[col.ID] = &col.FieldType
	}

	// Check local index entry does not exist.
	localPrefix := tablecodec.EncodeTableIndexPrefix(pid, indexInfo.ID)
	it, err := txn.Iter(localPrefix, nil)
	c.Assert(err, IsNil)
	// no local index entry.
	c.Assert(it.Valid() && it.Key().HasPrefix(localPrefix), IsFalse)
	it.Close()

	// Check global index entry.
	encodedValue, err := codec.EncodeKey(sc, nil, idxVals...)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeIndexSeekKey(tblInfo.ID, indexInfo.ID, encodedValue)
	c.Assert(err, IsNil)
	value, err := txn.Get(context.Background(), key)
	c.Assert(err, IsNil)
	idxColInfos := tables.BuildRowcodecColInfoForIndexColumns(indexInfo, tblInfo)
	colVals, err := tablecodec.DecodeIndexKV(key, value, len(indexInfo.Columns), tablecodec.HandleDefault, idxColInfos)
	c.Assert(err, IsNil)
	c.Assert(colVals, HasLen, len(idxVals)+2)
	for i, val := range idxVals {
		_, d, err := codec.DecodeOne(colVals[i])
		c.Assert(err, IsNil)
		c.Assert(d, DeepEquals, val)
	}
	_, d, err := codec.DecodeOne(colVals[len(idxVals)+1]) // pid
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64(), Equals, pid)

	_, d, err = codec.DecodeOne(colVals[len(idxVals)]) // handle
	c.Assert(err, IsNil)
	h := kv.IntHandle(d.GetInt64())
	rowKey := tablecodec.EncodeRowKey(pid, h.Encoded())
	rowValue, err := txn.Get(context.Background(), rowKey)
	c.Assert(err, IsNil)
	rowValueDatums, err := tablecodec.DecodeRowToDatumMap(rowValue, tblColMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(rowValueDatums, NotNil)
	for i, val := range rowVals {
		c.Assert(rowValueDatums[tblInfo.Columns[i].ID], DeepEquals, val)
	}
}

func (s *testSerialDBSuite) TestAddGlobalIndex(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table test_t1 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("insert test_t1 values (1, 1)")
	tk.MustExec("alter table test_t1 add unique index p_a (a);")
	tk.MustExec("insert test_t1 values (2, 11)")
	t := s.testGetTable(c, "test_t1")
	tblInfo := t.Meta()
	indexInfo := tblInfo.FindIndexByName("p_a")
	c.Assert(indexInfo, NotNil)
	c.Assert(indexInfo.Global, IsTrue)

	ctx := s.s.(sessionctx.Context)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)

	// check row 1
	pid := tblInfo.Partition.Definitions[0].ID
	idxVals := []types.Datum{types.NewDatum(1)}
	rowVals := []types.Datum{types.NewDatum(1), types.NewDatum(1)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	// check row 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Datum{types.NewDatum(2)}
	rowVals = []types.Datum{types.NewDatum(2), types.NewDatum(11)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Test add global Primary Key index
	tk.MustExec("create table test_t2 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustExec("insert test_t2 values (1, 1)")
	tk.MustExec("alter table test_t2 add primary key (a) nonclustered;")
	tk.MustExec("insert test_t2 values (2, 11)")
	t = s.testGetTable(c, "test_t2")
	tblInfo = t.Meta()
	indexInfo = t.Meta().FindIndexByName("primary")
	c.Assert(indexInfo, NotNil)
	c.Assert(indexInfo.Global, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)

	// check row 1
	pid = tblInfo.Partition.Definitions[0].ID
	idxVals = []types.Datum{types.NewDatum(1)}
	rowVals = []types.Datum{types.NewDatum(1), types.NewDatum(1)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	// check row 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Datum{types.NewDatum(2)}
	rowVals = []types.Datum{types.NewDatum(2), types.NewDatum(11)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testDBSuite) showColumns(tk *testkit.TestKit, c *C, tableName string) [][]interface{} {
	return s.mustQuery(tk, c, fmt.Sprintf("show columns from %s", tableName))
}

func (s *testDBSuite5) TestCreateIndexType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	sql := `CREATE TABLE test_index (
		price int(5) DEFAULT '0' NOT NULL,
		area varchar(40) DEFAULT '' NOT NULL,
		type varchar(40) DEFAULT '' NOT NULL,
		transityes set('a','b'),
		shopsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		schoolsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		petsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		KEY price (price,area,type,transityes,shopsyes,schoolsyes,petsyes));`
	tk.MustExec(sql)
}

func (s *testDBSuite6) TestColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("create table t2 (c1 int, c2 int, c3 int)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	s.testAddColumn(tk, c)
	s.testDropColumn(tk, c)
	tk.MustExec("drop table t2")
}

func sessionExec(c *C, s kv.Storage, sql string) {
	se, err := session.CreateSession4Test(s)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db")
	c.Assert(err, IsNil)
	rs, err := se.Execute(context.Background(), sql)
	c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	c.Assert(rs, IsNil)
	se.Close()
}

func (s *testDBSuite) testAddColumn(tk *testkit.TestKit, c *C) {
	done := make(chan error, 1)

	num := defaultBatchSize + 10
	// add some rows
	batchInsert(tk, "t2", 0, num)

	testddlutil.SessionExecInGoroutine(s.store, "alter table t2 add column c4 int default -1", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("begin")
				tk.MustExec("delete from t2 where c1 = ?", n)
				tk.MustExec("commit")

				// Make sure that statement of insert and show use the same infoSchema.
				tk.MustExec("begin")
				_, err := tk.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(tk, c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		tk.MustExec("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := s.mustQuery(tk, c, "select count(c4) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))

	rows = s.mustQuery(tk, c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	for i := num; i < num+step; i++ {
		rows = s.mustQuery(tk, c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}

	ctx := s.s.(sessionctx.Context)
	t := s.testGetTable(c, "t2")
	i := 0
	j := 0
	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			err := txn.Rollback()
			c.Assert(err, IsNil)
		}
	}()
	err = tables.IterRecords(t, ctx, t.Cols(),
		func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			i++
			// c4 must be -1 or > 0
			v, err1 := data[3].ToInt64(ctx.GetSessionVars().StmtCtx)
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

	// for modifying columns after adding columns
	tk.MustExec("alter table t2 modify c4 int default 11")
	for i := num + step; i < num+step+10; i++ {
		s.mustExec(tk, c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}
	rows = s.mustQuery(tk, c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	// add timestamp type column
	s.mustExec(tk, c, "create table test_on_update_c (c1 int, c2 timestamp);")
	defer tk.MustExec("drop table test_on_update_c;")
	s.mustExec(tk, c, "alter table test_on_update_c add column c3 timestamp null default '2017-02-11' on update current_timestamp;")
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_on_update_c"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colC := tblInfo.Columns[2]
	c.Assert(colC.Tp, Equals, mysql.TypeTimestamp)
	hasNotNull := mysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// add datetime type column
	s.mustExec(tk, c, "create table test_on_update_d (c1 int, c2 datetime);")
	defer tk.MustExec("drop table test_on_update_d;")
	s.mustExec(tk, c, "alter table test_on_update_d add column c3 datetime on update current_timestamp;")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_on_update_d"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	c.Assert(colC.Tp, Equals, mysql.TypeDatetime)
	hasNotNull = mysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)

	// add year type column
	s.mustExec(tk, c, "create table test_on_update_e (c1 int);")
	defer tk.MustExec("drop table test_on_update_e;")
	s.mustExec(tk, c, "insert into test_on_update_e (c1) values (0);")
	s.mustExec(tk, c, "alter table test_on_update_e add column c2 year not null;")
	tk.MustQuery("select c2 from test_on_update_e").Check(testkit.Rows("0"))

	// test add unsupported constraint
	s.mustExec(tk, c, "create table t_add_unsupported_constraint (a int);")
	_, err = tk.Exec("ALTER TABLE t_add_unsupported_constraint ADD id int AUTO_INCREMENT;")
	c.Assert(err.Error(), Equals, "[ddl:8200]unsupported add column 'id' constraint AUTO_INCREMENT when altering 'test_db.t_add_unsupported_constraint'")
	_, err = tk.Exec("ALTER TABLE t_add_unsupported_constraint ADD id int KEY;")
	c.Assert(err.Error(), Equals, "[ddl:8200]unsupported add column 'id' constraint PRIMARY KEY when altering 'test_db.t_add_unsupported_constraint'")
	_, err = tk.Exec("ALTER TABLE t_add_unsupported_constraint ADD id int UNIQUE;")
	c.Assert(err.Error(), Equals, "[ddl:8200]unsupported add column 'id' constraint UNIQUE KEY when altering 'test_db.t_add_unsupported_constraint'")
}

func (s *testDBSuite) testDropColumn(tk *testkit.TestKit, c *C) {
	done := make(chan error, 1)
	s.mustExec(tk, c, "delete from t2")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(tk, c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	// get c4 column id
	testddlutil.SessionExecInGoroutine(s.store, "alter table t2 drop column c4", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				// Make sure that statement of insert and show use the same infoSchema.
				tk.MustExec("begin")
				_, err := tk.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// If executing is failed, the column number must be 4 now.
					values := s.showColumns(tk, c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		s.mustExec(tk, c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	rows := s.mustQuery(tk, c, "select count(*) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))
}

// TestDropColumn is for inserting value with a to-be-dropped column when do drop column.
// Column info from schema in build-insert-plan should be public only,
// otherwise they will not be consist with Table.Col(), then the server will panic.
func (s *testDBSuite6) TestDropColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database drop_col_db")
	tk.MustExec("use drop_col_db")
	num := 25
	multiDDL := make([]string, 0, num)
	sql := "create table t2 (c1 int, c2 int, c3 int, "
	for i := 4; i < 4+num; i++ {
		multiDDL = append(multiDDL, fmt.Sprintf("alter table t2 drop column c%d", i))

		if i != 3+num {
			sql += fmt.Sprintf("c%d int, ", i)
		} else {
			sql += fmt.Sprintf("c%d int)", i)
		}
	}
	tk.MustExec(sql)
	dmlDone := make(chan error, num)
	ddlDone := make(chan error, num)

	testddlutil.ExecMultiSQLInGoroutine(s.store, "drop_col_db", multiDDL, ddlDone)
	for i := 0; i < num; i++ {
		testddlutil.ExecMultiSQLInGoroutine(s.store, "drop_col_db", []string{"insert into t2 set c1 = 1, c2 = 1, c3 = 1, c4 = 1"}, dmlDone)
	}
	for i := 0; i < num; i++ {
		err := <-ddlDone
		c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	}

	// Test for drop partition table column.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int) partition by hash(a) partitions 4;")
	_, err := tk.Exec("alter table t1 drop column a")
	c.Assert(err, NotNil)
	// TODO: refine the error message to compatible with MySQL
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'expression'")

	tk.MustExec("drop database drop_col_db")
}

func (s *testDBSuite4) TestChangeColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	s.mustExec(tk, c, "create table t3 (a int default '0', b varchar(10), d int not null default '0')")
	s.mustExec(tk, c, "insert into t3 set b = 'a'")
	tk.MustQuery("select a from t3").Check(testkit.Rows("0"))
	s.mustExec(tk, c, "alter table t3 change a aa bigint")
	s.mustExec(tk, c, "insert into t3 set b = 'b'")
	tk.MustQuery("select aa from t3").Check(testkit.Rows("0", "<nil>"))
	// for no default flag
	s.mustExec(tk, c, "alter table t3 change d dd bigint not null")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colD := tblInfo.Columns[2]
	hasNoDefault := mysql.HasNoDefaultValueFlag(colD.Flag)
	c.Assert(hasNoDefault, IsTrue)
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	s.mustExec(tk, c, "alter table t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colB := tblInfo.Columns[1]
	c.Assert(colB.Comment, Equals, "my comment")
	hasNotNull := mysql.HasNotNullFlag(colB.Flag)
	c.Assert(hasNotNull, IsFalse)
	s.mustExec(tk, c, "insert into t3 set aa = 3, dd = 5")
	tk.MustQuery("select b from t3").Check(testkit.Rows("a", "b", "c"))
	// for timestamp
	s.mustExec(tk, c, "alter table t3 add column c timestamp not null")
	s.mustExec(tk, c, "alter table t3 change c c timestamp null default '2017-02-11' comment 'col c comment' on update current_timestamp")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[3]
	c.Assert(colC.Comment, Equals, "col c comment")
	hasNotNull = mysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// for enum
	s.mustExec(tk, c, "alter table t3 add column en enum('a', 'b', 'c') not null default 'a'")
	// https://github.com/pingcap/tidb/issues/23488
	// if there is a prefix index on the varchar column, then we can change it to text
	s.mustExec(tk, c, "drop table if exists t")
	s.mustExec(tk, c, "create table t (k char(10), v int, INDEX(k(7)));")
	s.mustExec(tk, c, "alter table t change column k k tinytext")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	// for failing tests
	sql := "alter table t3 change aa a bigint default ''"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "alter table t3 change a testx.t3.aa bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongDBName)
	sql = "alter table t3 change t.a aa bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	s.mustExec(tk, c, "create table t4 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("insert into t4(c2) values (null);")
	_, err = tk.Exec("alter table t4 change c1 a1 int not null;")
	c.Assert(err.Error(), Equals, "[ddl:1265]Data truncated for column 'a1' at row 1")
	sql = "alter table t4 change c2 a bigint not null;"
	tk.MustGetErrCode(sql, mysql.WarnDataTruncated)
	sql = "alter table t3 modify en enum('a', 'z', 'b', 'c') not null default 'a'"
	tk.MustExec(sql)
	// Rename to an existing column.
	s.mustExec(tk, c, "alter table t3 add column a bigint")
	sql = "alter table t3 change aa a bigint"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	// https://github.com/pingcap/tidb/issues/23488
	s.mustExec(tk, c, "drop table if exists t5")
	s.mustExec(tk, c, "create table t5 (k char(10) primary key, v int)")
	sql = "alter table t5 change column k k tinytext;"
	tk.MustGetErrCode(sql, mysql.ErrBlobKeyWithoutLength)
	tk.MustExec("drop table t5")

	s.mustExec(tk, c, "drop table if exists t5")
	s.mustExec(tk, c, "create table t5 (k char(10), v int, INDEX(k))")
	sql = "alter table t5 change column k k tinytext;"
	tk.MustGetErrCode(sql, mysql.ErrBlobKeyWithoutLength)
	tk.MustExec("drop table t5")

	tk.MustExec("drop table t3")
}

func (s *testDBSuite5) TestRenameColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	assertColNames := func(tableName string, colNames ...string) {
		cols := s.testGetTable(c, tableName).Cols()
		c.Assert(len(cols), Equals, len(colNames), Commentf("number of columns mismatch"))
		for i := range cols {
			c.Assert(cols[i].Name.L, Equals, strings.ToLower(colNames[i]))
		}
	}

	s.mustExec(tk, c, "create table test_rename_column (id int not null primary key auto_increment, col1 int)")
	s.mustExec(tk, c, "alter table test_rename_column rename column col1 to col1")
	assertColNames("test_rename_column", "id", "col1")
	s.mustExec(tk, c, "alter table test_rename_column rename column col1 to col2")
	assertColNames("test_rename_column", "id", "col2")

	// Test renaming non-exist columns.
	tk.MustGetErrCode("alter table test_rename_column rename column non_exist_col to col3", errno.ErrBadField)

	// Test renaming to an exist column.
	tk.MustGetErrCode("alter table test_rename_column rename column col2 to id", errno.ErrDupFieldName)

	// Test renaming the column with foreign key.
	tk.MustExec("drop table test_rename_column")
	tk.MustExec("create table test_rename_column_base (base int)")
	tk.MustExec("create table test_rename_column (col int, foreign key (col) references test_rename_column_base(base))")

	tk.MustGetErrCode("alter table test_rename_column rename column col to col1", errno.ErrFKIncompatibleColumns)

	tk.MustExec("drop table test_rename_column_base")

	// Test renaming generated columns.
	tk.MustExec("drop table test_rename_column")
	tk.MustExec("create table test_rename_column (id int, col1 int generated always as (id + 1))")

	s.mustExec(tk, c, "alter table test_rename_column rename column col1 to col2")
	assertColNames("test_rename_column", "id", "col2")
	s.mustExec(tk, c, "alter table test_rename_column rename column col2 to col1")
	assertColNames("test_rename_column", "id", "col1")
	tk.MustGetErrCode("alter table test_rename_column rename column id to id1", errno.ErrDependentByGeneratedColumn)

	// Test renaming view columns.
	tk.MustExec("drop table test_rename_column")
	s.mustExec(tk, c, "create table test_rename_column (id int, col1 int)")
	s.mustExec(tk, c, "create view test_rename_column_view as select * from test_rename_column")

	s.mustExec(tk, c, "alter table test_rename_column rename column col1 to col2")
	tk.MustGetErrCode("select * from test_rename_column_view", errno.ErrViewInvalid)

	s.mustExec(tk, c, "drop view test_rename_column_view")
	tk.MustExec("drop table test_rename_column")
}

func (s *testDBSuite7) TestSelectInViewFromAnotherDB(c *C) {
	_, _ = s.s.Execute(context.Background(), "create database test_db2")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int)")
	tk.MustExec("use test_db2")
	tk.MustExec("create sql security invoker view v as select * from " + s.schemaName + ".t")
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("select test_db2.v.a from test_db2.v")
}

func (s *testDBSuite) mustExec(tk *testkit.TestKit, c *C, query string, args ...interface{}) {
	tk.MustExec(query, args...)
}

func (s *testDBSuite) mustQuery(tk *testkit.TestKit, c *C, query string, args ...interface{}) [][]interface{} {
	r := tk.MustQuery(query, args...)
	return r.Rows()
}

func matchRows(c *C, rows [][]interface{}, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected), Commentf("got %v, expected %v", rows, expected))
	for i := range rows {
		match(c, rows[i], expected[i]...)
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

// TestCreateTableWithLike2 tests create table with like when refer table have non-public column/index.
func (s *testSerialDBSuite) TestCreateTableWithLike2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1,t2;")
	defer tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (a int, b int, c int, index idx1(c));")

	tbl1 := testGetTableByName(c, s.s, "test_db", "t1")
	doneCh := make(chan error, 2)
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var onceChecker sync.Map
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionAddColumn && job.Type != model.ActionDropColumn &&
			job.Type != model.ActionAddColumns && job.Type != model.ActionDropColumns &&
			job.Type != model.ActionAddIndex && job.Type != model.ActionDropIndex {
			return
		}
		if job.TableID != tbl1.Meta().ID {
			return
		}

		if job.SchemaState == model.StateDeleteOnly {
			if _, ok := onceChecker.Load(job.ID); ok {
				return
			}

			onceChecker.Store(job.ID, true)
			go backgroundExec(s.store, "create table t2 like t1", doneCh)
		}
	}
	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// create table when refer table add column
	tk.MustExec("alter table t1 add column d int")
	checkTbl2 := func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustExec("alter table t2 add column e int")
		t2Info := testGetTableByName(c, s.s, "test_db", "t2")
		c.Assert(len(t2Info.Meta().Columns), Equals, len(t2Info.Cols()))
	}
	checkTbl2()

	// create table when refer table drop column
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop column b;")
	checkTbl2()

	// create table when refer table add index
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 add index idx2(a);")
	checkTbl2 = func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustExec("alter table t2 add column e int")
		tbl2 := testGetTableByName(c, s.s, "test_db", "t2")
		c.Assert(len(tbl2.Meta().Columns), Equals, len(tbl2.Cols()))

		for i := 0; i < len(tbl2.Meta().Indices); i++ {
			c.Assert(tbl2.Meta().Indices[i].State, Equals, model.StatePublic)
		}
	}
	checkTbl2()

	// create table when refer table drop index.
	tk.MustExec("drop table t2;")
	tk.MustExec("alter table t1 drop index idx2;")
	checkTbl2()

	// Test for table has tiflash  replica.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetTableByName(c, s.s, "test_db", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustExec("create table t2 like t1")
	t2 := testGetTableByName(c, s.s, "test_db", "t2")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)
	// Test for not affecting the original table.
	t1 = testGetTableByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})
}

func (s *testSerialDBSuite) TestCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	cols := tbl.Cols()

	c.Assert(len(cols), Equals, 1)
	col := cols[0]
	c.Assert(col.Name.L, Equals, "a")
	d, ok := col.DefaultValue.(string)
	c.Assert(ok, IsTrue)
	c.Assert(d, Equals, "2.0")

	tk.MustExec("drop table t")

	tk.MustGetErrCode("CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", errno.ErrUnknownCharacterSet)

	tk.MustExec("CREATE TABLE `collateTest` (`a` int, `b` varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")
	expects := "collateTest CREATE TABLE `collateTest` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table collateTest").Check(testkit.Rows(expects))

	tk.MustGetErrCode("CREATE TABLE `collateTest2` (`a` int) CHARSET utf8 COLLATE utf8mb4_unicode_ci", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("CREATE TABLE `collateTest3` (`a` int) COLLATE utf8mb4_unicode_ci CHARSET utf8", errno.ErrConflictingDeclarations)

	tk.MustExec("CREATE TABLE `collateTest4` (`a` int) COLLATE utf8_uniCOde_ci")
	expects = "collateTest4 CREATE TABLE `collateTest4` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	tk.MustQuery("show create table collateTest4").Check(testkit.Rows(expects))

	tk.MustExec("create database test2 default charset utf8 collate utf8_general_ci")
	tk.MustExec("use test2")
	tk.MustExec("create table dbCollateTest (a varchar(10))")
	expects = "dbCollateTest CREATE TABLE `dbCollateTest` (\n  `a` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table dbCollateTest").Check(testkit.Rows(expects))

	// test for enum column
	tk.MustExec("use test")
	failSQL := "create table t_enum (a enum('e','e'));"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	tk = testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	failSQL = "create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_general_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('abc','Abc')) charset=utf8 collate=utf8_general_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_unicode_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	// test for set column
	failSQL = "create table t_enum (a set('e','e'));"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('e','E')) charset=utf8 collate=utf8_general_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('abc','Abc')) charset=utf8 collate=utf8_general_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	_, err = tk.Exec("create table t_enum (a enum('B','b')) charset=utf8 collate=utf8_general_ci;")
	c.Assert(err.Error(), Equals, "[types:1291]Column 'a' has duplicated value 'b' in ENUM")
	failSQL = "create table t_enum (a set('e','E')) charset=utf8 collate=utf8_unicode_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('ss','ß')) charset=utf8 collate=utf8_unicode_ci;"
	tk.MustGetErrCode(failSQL, errno.ErrDuplicatedValueInType)
	_, err = tk.Exec("create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;")
	c.Assert(err.Error(), Equals, "[types:1291]Column 'a' has duplicated value 'ß' in ENUM")

	// test for table option "union" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	failSQL = "CREATE TABLE z (a INT) ENGINE = MERGE UNION = (x, y);"
	tk.MustGetErrCode(failSQL, errno.ErrTableOptionUnionUnsupported)
	failSQL = "ALTER TABLE x UNION = (y);"
	tk.MustGetErrCode(failSQL, errno.ErrTableOptionUnionUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")

	// test for table option "insert method" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	failSQL = "CREATE TABLE z (a INT) ENGINE = MERGE INSERT_METHOD=LAST;"
	tk.MustGetErrCode(failSQL, errno.ErrTableOptionInsertMethodUnsupported)
	failSQL = "ALTER TABLE x INSERT_METHOD=LAST;"
	tk.MustGetErrCode(failSQL, errno.ErrTableOptionInsertMethodUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")
}

func (s *testSerialDBSuite) TestRepairTable(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, other_table, origin")

	// Test repair table when TiDB is not in repair mode.
	tk.MustExec("CREATE TABLE t (a int primary key nonclustered, b varchar(10));")
	_, err := tk.Exec("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: TiDB is not in REPAIR MODE")

	// Test repair table when the repaired list is empty.
	domainutil.RepairInfo.SetRepairMode(true)
	_, err = tk.Exec("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: repair list is empty")

	// Test repair table when it's database isn't in repairInfo.
	domainutil.RepairInfo.SetRepairTableList([]string{"test.other_table"})
	_, err = tk.Exec("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: database test is not in repair")

	// Test repair table when the table isn't in repairInfo.
	tk.MustExec("CREATE TABLE other_table (a int, b varchar(1), key using hash(b));")
	_, err = tk.Exec("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: table t is not in repair")

	// Test user can't access to the repaired table.
	_, err = tk.Exec("select * from other_table")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'test.other_table' doesn't exist")

	// Test create statement use the same name with what is in repaired.
	_, err = tk.Exec("CREATE TABLE other_table (a int);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1103]Incorrect table name 'other_table'%!(EXTRA string=this table is in repair)")

	// Test column lost in repair table.
	_, err = tk.Exec("admin repair table other_table CREATE TABLE other_table (a int, c char(1));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Column c has lost")

	// Test column type should be the same.
	_, err = tk.Exec("admin repair table other_table CREATE TABLE other_table (a bigint, b varchar(1), key using hash(b));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Column a type should be the same")

	// Test index lost in repair table.
	_, err = tk.Exec("admin repair table other_table CREATE TABLE other_table (a int unique);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Index a has lost")

	// Test index type should be the same.
	_, err = tk.Exec("admin repair table other_table CREATE TABLE other_table (a int, b varchar(2) unique)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Index b type should be the same")

	// Test sub create statement in repair statement with the same name.
	_, err = tk.Exec("admin repair table other_table CREATE TABLE other_table (a int);")
	c.Assert(err, IsNil)

	// Test whether repair table name is case sensitive.
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.other_table2"})
	tk.MustExec("CREATE TABLE otHer_tAblE2 (a int, b varchar(1));")
	_, err = tk.Exec("admin repair table otHer_tAblE2 CREATE TABLE otHeR_tAbLe (a int, b varchar(2));")
	c.Assert(err, IsNil)
	repairTable := testGetTableByName(c, s.s, "test", "otHeR_tAbLe")
	c.Assert(repairTable.Meta().Name.O, Equals, "otHeR_tAbLe")

	// Test memory and system database is not for repair.
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.xxx"})
	_, err = tk.Exec("admin repair table performance_schema.xxx CREATE TABLE yyy (a int);")
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: memory or system database is not for repair")

	// Test the repair detail.
	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Domain reload the tableInfo and add it into repairInfo.
	tk.MustExec("CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(10), c int);")
	// Repaired tableInfo has been filtered by `domain.InfoSchema()`, so get it in repairInfo.
	originTableInfo, _ := domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")

	hook := &ddl.TestDDLCallback{Do: s.dom}
	var repairErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionRepairTable {
			return
		}
		if job.TableID != originTableInfo.ID {
			repairErr = errors.New("table id should be the same")
			return
		}
		if job.SchemaState != model.StateNone {
			repairErr = errors.New("repair job state should be the none")
			return
		}
		// Test whether it's readable, when repaired table is still stateNone.
		tkInternal := testkit.NewTestKitWithInit(c, s.store)
		_, repairErr = tkInternal.Exec("select * from origin")
		// Repaired tableInfo has been filtered by `domain.InfoSchema()`, here will get an error cause user can't get access to it.
		if repairErr != nil && terror.ErrorEqual(repairErr, infoschema.ErrTableNotExists) {
			repairErr = nil
		}
	}
	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// Exec the repair statement to override the tableInfo.
	tk.MustExec("admin repair table origin CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(5), c int);")
	c.Assert(repairErr, IsNil)

	// Check the repaired tableInfo is exactly the same with old one in tableID, indexID, colID.
	// testGetTableByName will extract the Table from `domain.InfoSchema()` directly.
	repairTable = testGetTableByName(c, s.s, "test", "origin")
	c.Assert(repairTable.Meta().ID, Equals, originTableInfo.ID)
	c.Assert(len(repairTable.Meta().Columns), Equals, 3)
	c.Assert(repairTable.Meta().Columns[0].ID, Equals, originTableInfo.Columns[0].ID)
	c.Assert(repairTable.Meta().Columns[1].ID, Equals, originTableInfo.Columns[1].ID)
	c.Assert(repairTable.Meta().Columns[2].ID, Equals, originTableInfo.Columns[2].ID)
	c.Assert(len(repairTable.Meta().Indices), Equals, 1)
	c.Assert(repairTable.Meta().Indices[0].ID, Equals, originTableInfo.Columns[0].ID)
	c.Assert(repairTable.Meta().AutoIncID, Equals, originTableInfo.AutoIncID)

	c.Assert(repairTable.Meta().Columns[0].Tp, Equals, mysql.TypeLong)
	c.Assert(repairTable.Meta().Columns[1].Tp, Equals, mysql.TypeVarchar)
	c.Assert(repairTable.Meta().Columns[1].Flen, Equals, 5)
	c.Assert(repairTable.Meta().Columns[2].Tp, Equals, mysql.TypeLong)

	// Exec the show create table statement to make sure new tableInfo has been set.
	result := tk.MustQuery("show create table origin")
	c.Assert(result.Rows()[0][1], Equals, "CREATE TABLE `origin` (\n  `a` int(11) NOT NULL AUTO_INCREMENT,\n  `b` varchar(5) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

}

func turnRepairModeAndInit(on bool) {
	list := make([]string, 0)
	if on {
		list = append(list, "test.origin")
	}
	domainutil.RepairInfo.SetRepairMode(on)
	domainutil.RepairInfo.SetRepairTableList(list)
}

func (s *testSerialDBSuite) TestRepairTableWithPartition(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists origin")

	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Domain reload the tableInfo and add it into repairInfo.
	tk.MustExec("create table origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p70 values less than (70)," +
		"partition p90 values less than (90));")
	// Test for some old partition has lost.
	_, err := tk.Exec("admin repair table origin create table origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90)," +
		"partition p100 values less than (100));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Partition p100 has lost")

	// Test for some partition changed the condition.
	_, err = tk.Exec("admin repair table origin create table origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p20 values less than (25)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Partition p20 has lost")

	// Test for some partition changed the partition name.
	_, err = tk.Exec("admin repair table origin create table origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition pNew values less than (50)," +
		"partition p90 values less than (90));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Partition pnew has lost")

	originTableInfo, _ := domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")
	tk.MustExec("admin repair table origin create table origin_rename (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90));")
	repairTable := testGetTableByName(c, s.s, "test", "origin_rename")
	c.Assert(repairTable.Meta().ID, Equals, originTableInfo.ID)
	c.Assert(len(repairTable.Meta().Columns), Equals, 1)
	c.Assert(repairTable.Meta().Columns[0].ID, Equals, originTableInfo.Columns[0].ID)
	c.Assert(len(repairTable.Meta().Partition.Definitions), Equals, 4)
	c.Assert(repairTable.Meta().Partition.Definitions[0].ID, Equals, originTableInfo.Partition.Definitions[0].ID)
	c.Assert(repairTable.Meta().Partition.Definitions[1].ID, Equals, originTableInfo.Partition.Definitions[1].ID)
	c.Assert(repairTable.Meta().Partition.Definitions[2].ID, Equals, originTableInfo.Partition.Definitions[2].ID)
	c.Assert(repairTable.Meta().Partition.Definitions[3].ID, Equals, originTableInfo.Partition.Definitions[4].ID)

	// Test hash partition.
	tk.MustExec("drop table if exists origin")
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.origin"})
	tk.MustExec("create table origin (a varchar(1), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")

	// Test partition num in repair should be exactly same with old one, other wise will cause partition semantic problem.
	_, err = tk.Exec("admin repair table origin create table origin (a varchar(2), b int not null, c int, key idx(c)) partition by hash(b) partitions 20")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8215]Failed to repair table: Hash partition num should be the same")

	originTableInfo, _ = domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")
	tk.MustExec("admin repair table origin create table origin (a varchar(3), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	repairTable = testGetTableByName(c, s.s, "test", "origin")
	c.Assert(repairTable.Meta().ID, Equals, originTableInfo.ID)
	c.Assert(len(repairTable.Meta().Partition.Definitions), Equals, 30)
	c.Assert(repairTable.Meta().Partition.Definitions[0].ID, Equals, originTableInfo.Partition.Definitions[0].ID)
	c.Assert(repairTable.Meta().Partition.Definitions[1].ID, Equals, originTableInfo.Partition.Definitions[1].ID)
	c.Assert(repairTable.Meta().Partition.Definitions[29].ID, Equals, originTableInfo.Partition.Definitions[29].ID)
}

func (s *testDBSuite2) TestCreateTableWithSetCol(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t_set (a int, b set('e') default '');")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` set('e') DEFAULT ''\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('a', 'b', 'c', 'd') default 'a,c,c');")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('a','b','c','d') DEFAULT 'a,c'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// It's for failure cases.
	// The type of default value is string.
	tk.MustExec("drop table t_set")
	failedSQL := "create table t_set (a set('1', '4', '10') default '3');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_set (a set('1', '4', '10') default '1,4,11');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	// Success when the new collation is enabled.
	tk.MustExec("create table t_set (a set('1', '4', '10') default '1 ,4');")
	// The type of default value is int.
	failedSQL = "create table t_set (a set('1', '4', '10') default 0);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_set (a set('1', '4', '10') default 8);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)

	// The type of default value is int.
	// It's for successful cases
	tk.MustExec("drop table if exists t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 1);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 2);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 3);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 15);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4,10,21'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("insert into t_set value()")
	tk.MustQuery("select * from t_set").Check(testkit.Rows("1,4,10,21"))
}

func (s *testDBSuite2) TestCreateTableWithEnumCol(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	// It's for failure cases.
	// The type of default value is string.
	tk.MustExec("drop table if exists t_enum")
	failedSQL := "create table t_enum (a enum('1', '4', '10') default '3');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_enum (a enum('1', '4', '10') default '');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	// The type of default value is int.
	failedSQL = "create table t_enum (a enum('1', '4', '10') default 0);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_enum (a enum('1', '4', '10') default 8);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)

	// The type of default value is int.
	// It's for successful cases
	tk.MustExec("drop table if exists t_enum")
	tk.MustExec("create table t_enum (a enum('2', '3', '4') default 2);")
	ret := tk.MustQuery("show create table t_enum").Rows()[0][1]
	c.Assert(strings.Contains(ret.(string), "`a` enum('2','3','4') DEFAULT '3'"), IsTrue)
	tk.MustExec("drop table t_enum")
	tk.MustExec("create table t_enum (a enum('a', 'c', 'd') default 2);")
	ret = tk.MustQuery("show create table t_enum").Rows()[0][1]
	c.Assert(strings.Contains(ret.(string), "`a` enum('a','c','d') DEFAULT 'c'"), IsTrue)
	tk.MustExec("insert into t_enum value()")
	tk.MustQuery("select * from t_enum").Check(testkit.Rows("c"))
}

func (s *testDBSuite2) TestTableForeignKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int);")
	// test create table with foreign key.
	failSQL := "create table t2 (c int, foreign key (a) references t1(a));"
	tk.MustGetErrCode(failSQL, errno.ErrKeyColumnDoesNotExits)
	// test add foreign key.
	tk.MustExec("create table t3 (a int, b int);")
	failSQL = "alter table t1 add foreign key (c) REFERENCES t3(a);"
	tk.MustGetErrCode(failSQL, errno.ErrKeyColumnDoesNotExits)
	// test origin key not match error
	failSQL = "alter table t1 add foreign key (a) REFERENCES t3(a, b);"
	tk.MustGetErrCode(failSQL, errno.ErrWrongFkDef)
	// Test drop column with foreign key.
	tk.MustExec("create table t4 (c int,d int,foreign key (d) references t1 (b));")
	failSQL = "alter table t4 drop column d"
	tk.MustGetErrCode(failSQL, errno.ErrFkColumnCannotDrop)
	// Test change column with foreign key.
	failSQL = "alter table t4 change column d e bigint;"
	tk.MustGetErrCode(failSQL, errno.ErrFKIncompatibleColumns)
	// Test modify column with foreign key.
	failSQL = "alter table t4 modify column d bigint;"
	tk.MustGetErrCode(failSQL, errno.ErrFKIncompatibleColumns)
	tk.MustQuery("select count(*) from information_schema.KEY_COLUMN_USAGE;")
	tk.MustExec("alter table t4 drop foreign key d")
	tk.MustExec("alter table t4 modify column d bigint;")
	tk.MustExec("drop table if exists t1,t2,t3,t4;")
}

func (s *testDBSuite2) TestDuplicateForeignKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	// Foreign table.
	tk.MustExec("create table t(id int key)")
	// Create target table with duplicate fk.
	tk.MustGetErrCode("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`), CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))", mysql.ErrFkDupName)
	tk.MustGetErrCode("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`), CONSTRAINT `fk_aaA` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))", mysql.ErrFkDupName)

	tk.MustExec("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))")
	// Alter target table with duplicate fk.
	tk.MustGetErrCode("alter table t1 add CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`)", mysql.ErrFkDupName)
	tk.MustGetErrCode("alter table t1 add CONSTRAINT `fk_aAa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`)", mysql.ErrFkDupName)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
}

func (s *testDBSuite2) TestTemporaryTableForeignKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int);")
	tk.MustExec("drop table if exists t1_tmp;")
	tk.MustExec("create global temporary table t1_tmp (a int, b int) on commit delete rows;")
	tk.MustExec("create temporary table t2_tmp (a int, b int)")
	// test add foreign key.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b int);")
	failSQL := "alter table t1_tmp add foreign key (c) REFERENCES t2(a);"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	failSQL = "alter table t2_tmp add foreign key (c) REFERENCES t2(a);"
	tk.MustGetErrCode(failSQL, errno.ErrUnsupportedDDLOperation)
	// Test drop column with foreign key.
	failSQL = "create global temporary table t3 (c int,d int,foreign key (d) references t1 (b)) on commit delete rows;"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	failSQL = "create temporary table t4(c int,d int,foreign key (d) references t1 (b));"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	tk.MustExec("drop table if exists t1,t2,t3, t4,t1_tmp,t2_tmp;")
}

func (s *testDBSuite8) TestFKOnGeneratedColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// test add foreign key to generated column

	// foreign key constraint cannot be defined on a virtual generated column.
	tk.MustExec("create table t1 (a int primary key);")
	tk.MustGetErrCode("create table t2 (a int, b int as (a+1) virtual, foreign key (b) references t1(a));", errno.ErrCannotAddForeign)
	tk.MustExec("create table t2 (a int, b int generated always as (a+1) virtual);")
	tk.MustGetErrCode("alter table t2 add foreign key (b) references t1(a);", errno.ErrCannotAddForeign)
	tk.MustExec("drop table t1, t2;")

	// foreign key constraint can be defined on a stored generated column.
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored, foreign key (b) references t2(a));")
	tk.MustExec("create table t3 (a int, b int generated always as (a+1) stored);")
	tk.MustExec("alter table t3 add foreign key (b) references t2(a);")
	tk.MustExec("drop table t1, t2, t3;")

	// foreign key constraint can reference a stored generated column.
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored primary key);")
	tk.MustExec("create table t2 (a int, foreign key (a) references t1(b));")
	tk.MustExec("create table t3 (a int);")
	tk.MustExec("alter table t3 add foreign key (a) references t1(b);")
	tk.MustExec("drop table t1, t2, t3;")

	// rejected FK options on stored generated columns
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update cascade);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set default);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set default);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update cascade;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set default;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on delete set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on delete set default;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("drop table t1, t2;")
	// column name with uppercase characters
	tk.MustGetErrCode("create table t1 (A int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on update set null);", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustExec("create table t1 (A int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrWrongFKOptionForGeneratedColumn)
	tk.MustExec("drop table t1, t2;")

	// special case: TiDB error different from MySQL 8.0
	// MySQL: ERROR 3104 (HY000): Cannot define foreign key with ON UPDATE SET NULL clause on a generated column.
	// TiDB:  ERROR 1146 (42S02): Table 'test.t2' doesn't exist
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (b) references t2(a) on update set null;", errno.ErrNoSuchTable)
	tk.MustExec("drop table t1;")

	// allowed FK options on stored generated columns
	tk.MustExec("create table t1 (a int primary key, b char(5));")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on update restrict);")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on update no action);")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete restrict);")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete cascade);")
	tk.MustExec("create table t6 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete no action);")
	tk.MustExec("drop table t2,t3,t4,t5,t6;")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t2 add foreign key (b) references t1(a) on update restrict;")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t3 add foreign key (b) references t1(a) on update no action;")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t4 add foreign key (b) references t1(a) on delete restrict;")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t5 add foreign key (b) references t1(a) on delete cascade;")
	tk.MustExec("create table t6 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t6 add foreign key (b) references t1(a) on delete no action;")
	tk.MustExec("drop table t1,t2,t3,t4,t5,t6;")

	// rejected FK options on the base columns of a stored generated columns
	tk.MustExec("create table t2 (a int primary key);")
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on update set default);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set default);", errno.ErrCannotAddForeign)
	tk.MustExec("create table t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on update set default;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter table t1 add foreign key (a) references t2(a) on delete set default;", errno.ErrCannotAddForeign)
	tk.MustExec("drop table t1, t2;")

	// allowed FK options on the base columns of a stored generated columns
	tk.MustExec("create table t1 (a int primary key, b char(5));")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on update restrict);")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on update no action);")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete restrict);")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete no action);")
	tk.MustExec("drop table t2,t3,t4,t5")
	tk.MustExec("create table t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t2 add foreign key (a) references t1(a) on update restrict;")
	tk.MustExec("create table t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t3 add foreign key (a) references t1(a) on update no action;")
	tk.MustExec("create table t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t4 add foreign key (a) references t1(a) on delete restrict;")
	tk.MustExec("create table t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustExec("alter table t5 add foreign key (a) references t1(a) on delete no action;")
	tk.MustExec("drop table t1,t2,t3,t4,t5;")
}

func (s *testSerialDBSuite) TestTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table truncate_table (c1 int, c2 int)")
	tk.MustExec("insert truncate_table values (1, 1), (2, 2)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table truncate_table")

	tk.MustExec("insert truncate_table values (3, 3), (4, 4)")
	tk.MustQuery("select * from truncate_table").Check(testkit.Rows("3 3", "4 4"))

	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// Verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err = kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
			it, err1 := txn.Iter(tablePrefix, nil)
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
		time.Sleep(waitForCleanDataInterval)
	}
	c.Assert(hasOldTableData, IsFalse)

	// Test for truncate table should clear the tiflash available status.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetTableByName(c, s.s, "test", "t1")
	// Mock for table tiflash replica was available.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t1.Meta().ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustExec("truncate table t1")
	t2 := testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Test for truncate partition should clear the tiflash available status.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")
	t1 = testGetTableByName(c, s.s, "test", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustExec("alter table t1 truncate partition p0")
	t2 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})
	// Test for truncate twice.
	tk.MustExec("alter table t1 truncate partition p0")
	t2 = testGetTableByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})

}

func (s *testDBSuite4) TestRenameTable(c *C) {
	isAlterTable := false
	s.testRenameTable(c, "rename table %s to %s", isAlterTable)
}

func (s *testDBSuite5) TestAlterTableRenameTable(c *C) {
	isAlterTable := true
	s.testRenameTable(c, "alter table %s rename to %s", isAlterTable)
}

func (s *testDBSuite) testRenameTable(c *C, sql string, isAlterTable bool) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustGetErrCode("rename table tb1 to tb2;", errno.ErrNoSuchTable)
	// for different databases
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "test.t", "test1.t1"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustExec("use test")

	// Make sure t doesn't exist.
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("drop table t")

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "t1", "t2"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	tk.MustQuery("show tables").Check(testkit.Rows("t2"))

	// for failure case
	failSQL := fmt.Sprintf(sql, "test_not_exist.t", "test_not_exist.t")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test.test_not_exist", "test.test_not_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test_not_exist.t")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test1.t2", "test_not_exist.t")
	tk.MustGetErrCode(failSQL, errno.ErrErrorOnRename)

	tk.MustExec("use test1")
	tk.MustExec("create table if not exists t_exist (c1 int, c2 int)")
	failSQL = fmt.Sprintf(sql, "test1.t2", "test1.t_exist")
	tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test1.t_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	}
	failSQL = fmt.Sprintf(sql, "test_not_exist.t", "test1.t_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	}
	failSQL = fmt.Sprintf(sql, "test_not_exist.t", "test1.t_not_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}

	// for the same table name
	tk.MustExec("use test1")
	tk.MustExec("create table if not exists t (c1 int, c2 int)")
	tk.MustExec("create table if not exists t1 (c1 int, c2 int)")
	if isAlterTable {
		tk.MustExec(fmt.Sprintf(sql, "test1.t", "t"))
		tk.MustExec(fmt.Sprintf(sql, "test1.t1", "test1.T1"))
	} else {
		tk.MustGetErrCode(fmt.Sprintf(sql, "test1.t", "t"), errno.ErrTableExists)
		tk.MustGetErrCode(fmt.Sprintf(sql, "test1.t1", "test1.T1"), errno.ErrTableExists)
	}

	// Test rename table name too long.
	tk.MustGetErrCode("rename table test1.t1 to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)
	tk.MustGetErrCode("alter  table test1.t1 rename to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)

	tk.MustExec("drop database test1")
}

func (s *testDBSuite1) TestRenameMultiTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	sql := "rename table t1 to t3, t2 to t4"
	_, err := tk.Exec(sql)
	c.Assert(err, IsNil)

	tk.MustExec("drop table t3, t4")

	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("insert t2 values (1, 1), (2, 2)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	oldTblID1 := oldTblInfo1.Meta().ID
	oldTblInfo2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	oldTblID2 := oldTblInfo2.Meta().ID
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec("rename table test.t1 to test1.t1, test.t2 to test1.t2")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo1.Meta().ID, Equals, oldTblID1)
	newTblInfo2, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo2.Meta().ID, Equals, oldTblID2)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))

	// Make sure t1,t2 doesn't exist.
	isExist := is.TableExists(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	isExist = is.TableExists(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(isExist, IsFalse)

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec("rename table test1.t1 to test1.t3, test1.t2 to test1.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo1.Meta().ID, Equals, oldTblID1)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo2.Meta().ID, Equals, oldTblID2)
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(isExist, IsFalse)
	tk.MustQuery("show tables").Check(testkit.Rows("t3", "t4"))

	// for multi tables same database
	tk.MustExec("create table t5 (c1 int, c2 int)")
	tk.MustExec("insert t5 values (1, 1), (2, 2)")
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo3, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t5"))
	c.Assert(err, IsNil)
	oldTblID3 := oldTblInfo3.Meta().ID
	tk.MustExec("rename table test1.t3 to test1.t1, test1.t4 to test1.t2, test1.t5 to test1.t3")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo1.Meta().ID, Equals, oldTblID1)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo2.Meta().ID, Equals, oldTblID2)
	newTblInfo3, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo3.Meta().ID, Equals, oldTblID3)
	tk.MustQuery("show tables").Check(testkit.Rows("t1", "t2", "t3"))

	// for multi tables different databases
	tk.MustExec("use test")
	tk.MustExec("rename table test1.t1 to test.t2, test1.t2 to test.t3, test1.t3 to test.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo1.Meta().ID, Equals, oldTblID1)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo2.Meta().ID, Equals, oldTblID2)
	newTblInfo3, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo3.Meta().ID, Equals, oldTblID3)
	tk.MustQuery("show tables").Check(testkit.Rows("t2", "t3", "t4"))

	// for failure case
	failSQL := "rename table test_not_exist.t to test_not_exist.t, test_not_exist.t to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test.test_not_exist to test.test_not_exist, test.test_not_exist to test.test_not_exist"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test.t_not_exist to test_not_exist.t, test.t_not_exist to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test1.t2 to test_not_exist.t, test1.t2 to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)

	tk.MustExec("drop database test1")
	tk.MustExec("drop database test")
}

func (s *testDBSuite2) TestAddNotNullColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	// for different databases
	tk.MustExec("create table tnn (c1 int primary key auto_increment, c2 int)")
	tk.MustExec("insert tnn (c2) values (0)" + strings.Repeat(",(0)", 99))
	done := make(chan error, 1)
	testddlutil.SessionExecInGoroutine(s.store, "alter table tnn add column c3 int not null default 3", done)
	updateCnt := 0
out:
	for {
		select {
		case err := <-done:
			c.Assert(err, IsNil)
			break out
		default:
			// Close issue #14636
			// Because add column action is not amendable now, it causes an error when the schema is changed
			// in the process of an insert statement.
			_, err := tk.Exec("update tnn set c2 = c2 + 1 where c1 = 99")
			if err == nil {
				updateCnt++
			}
		}
	}
	expected := fmt.Sprintf("%d %d", updateCnt, 3)
	tk.MustQuery("select c2, c3 from tnn where c1 = 99").Check(testkit.Rows(expected))

	tk.MustExec("drop table tnn")
}

func (s *testDBSuite3) TestVirtualColumnDDL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_gv_ddl")
	tk.MustExec(`create global temporary table test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored) on commit delete rows;`)
	defer tk.MustExec("drop table if exists test_gv_ddl")
	is := tk.Se.(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_gv_ddl"))
	c.Assert(err, IsNil)
	testCases := []struct {
		generatedExprString string
		generatedStored     bool
	}{
		{"", false},
		{"`a` + 8", false},
		{"`b` + 2", true},
	}
	for i, column := range table.Meta().Columns {
		c.Assert(column.GeneratedExprString, Equals, testCases[i].generatedExprString)
		c.Assert(column.GeneratedStored, Equals, testCases[i].generatedStored)
	}
	result := tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))
	tk.MustExec("begin;")
	tk.MustExec("insert into test_gv_ddl values (1, default, default)")
	tk.MustQuery("select * from test_gv_ddl").Check(testkit.Rows("1 9 11"))
	_, err = tk.Exec("commit")
	c.Assert(err, IsNil)

	// for local temporary table
	tk.MustExec(`create temporary table test_local_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored);`)
	defer tk.MustExec("drop table if exists test_local_gv_ddl")
	is = tk.Se.(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_local_gv_ddl"))
	c.Assert(err, IsNil)
	for i, column := range table.Meta().Columns {
		c.Assert(column.GeneratedExprString, Equals, testCases[i].generatedExprString)
		c.Assert(column.GeneratedStored, Equals, testCases[i].generatedStored)
	}
	result = tk.MustQuery(`DESC test_local_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))
	tk.MustExec("begin;")
	tk.MustExec("insert into test_local_gv_ddl values (1, default, default)")
	tk.MustQuery("select * from test_local_gv_ddl").Check(testkit.Rows("1 9 11"))
	_, err = tk.Exec("commit")
	c.Assert(err, IsNil)
	tk.MustQuery("select * from test_local_gv_ddl").Check(testkit.Rows("1 9 11"))
}

func (s *testDBSuite3) TestGeneratedColumnDDL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// Check create table with virtual and stored generated columns.
	tk.MustExec(`CREATE TABLE test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored)`)

	// Check desc table with virtual and stored generated columns.
	result := tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	// Check show create table with virtual and stored generated columns.
	result = tk.MustQuery(`show create table test_gv_ddl`)
	result.Check(testkit.Rows(
		"test_gv_ddl CREATE TABLE `test_gv_ddl` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` + 8) VIRTUAL,\n  `c` int(11) GENERATED ALWAYS AS (`b` + 2) STORED\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Check generated expression with blanks.
	tk.MustExec("create table table_with_gen_col_blanks (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char)), c int as (a+100))")
	result = tk.MustQuery(`show create table table_with_gen_col_blanks`)
	result.Check(testkit.Rows("table_with_gen_col_blanks CREATE TABLE `table_with_gen_col_blanks` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated expression with charset latin1 ("latin1" != mysql.DefaultCharset).
	tk.MustExec("create table table_with_gen_col_latin1 (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char charset latin1)), c int as (a+100))")
	result = tk.MustQuery(`show create table table_with_gen_col_latin1`)
	result.Check(testkit.Rows("table_with_gen_col_latin1 CREATE TABLE `table_with_gen_col_latin1` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char charset latin1)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated expression with string (issue 9457).
	tk.MustExec("create table table_with_gen_col_string (first_name varchar(10), last_name varchar(10), full_name varchar(255) AS (CONCAT(first_name,' ',last_name)))")
	result = tk.MustQuery(`show create table table_with_gen_col_string`)
	result.Check(testkit.Rows("table_with_gen_col_string CREATE TABLE `table_with_gen_col_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`first_name`, _utf8mb4' ', `last_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("alter table table_with_gen_col_string modify column full_name varchar(255) GENERATED ALWAYS AS (CONCAT(last_name,' ' ,first_name) ) VIRTUAL")
	result = tk.MustQuery(`show create table table_with_gen_col_string`)
	result.Check(testkit.Rows("table_with_gen_col_string CREATE TABLE `table_with_gen_col_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`last_name`, _utf8mb4' ', `first_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test incorrect parameter count.
	tk.MustGetErrCode("create table test_gv_incorrect_pc(a double, b int as (lower(a, 2)))", errno.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("create table test_gv_incorrect_pc(a double, b int as (lower(a, 2)) stored)", errno.ErrWrongParamcountToNativeFct)

	genExprTests := []struct {
		stmt string
		err  int
	}{
		// Drop/rename columns dependent by other column.
		{`alter table test_gv_ddl drop column a`, errno.ErrDependentByGeneratedColumn},
		{`alter table test_gv_ddl change column a anew int`, errno.ErrBadField},

		// Modify/change stored status of generated columns.
		{`alter table test_gv_ddl modify column b bigint`, errno.ErrUnsupportedOnGeneratedColumn},
		{`alter table test_gv_ddl change column c cnew bigint as (a+100)`, errno.ErrUnsupportedOnGeneratedColumn},

		// Modify/change generated columns breaking prior.
		{`alter table test_gv_ddl modify column b int as (c+100)`, errno.ErrGeneratedColumnNonPrior},
		{`alter table test_gv_ddl change column b bnew int as (c+100)`, errno.ErrGeneratedColumnNonPrior},

		// Refer not exist columns in generation expression.
		{`create table test_gv_ddl_bad (a int, b int as (c+8))`, errno.ErrBadField},

		// Refer generated columns non prior.
		{`create table test_gv_ddl_bad (a int, b int as (c+1), c int as (a+1))`, errno.ErrGeneratedColumnNonPrior},

		// Virtual generated columns cannot be primary key.
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b) primary key)`, errno.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(c))`, errno.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(a, c))`, errno.ErrUnsupportedOnGeneratedColumn},

		// Add stored generated column through alter table.
		{`alter table test_gv_ddl add column d int as (b+2) stored`, errno.ErrUnsupportedOnGeneratedColumn},
		{`alter table test_gv_ddl modify column b int as (a + 8) stored`, errno.ErrUnsupportedOnGeneratedColumn},

		// Add generated column with incorrect parameter count.
		{`alter table test_gv_ddl add column z int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
		{`alter table test_gv_ddl add column z int as (lower(a, 2)) stored`, errno.ErrWrongParamcountToNativeFct},

		// Modify generated column with incorrect parameter count.
		{`alter table test_gv_ddl modify column b int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
		{`alter table test_gv_ddl change column b b int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
	}
	for _, tt := range genExprTests {
		tk.MustGetErrCode(tt.stmt, tt.err)
	}

	// Check alter table modify/change generated column.
	modStoredColErrMsg := "[ddl:3106]'modifying a stored column' is not supported for generated columns."
	_, err := tk.Exec(`alter table test_gv_ddl modify column c bigint as (b+200) stored`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredColErrMsg)

	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustExec(`alter table test_gv_ddl change column b b bigint as (a+100) virtual`)
	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustExec(`alter table test_gv_ddl change column c cnew bigint`)
	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `cnew bigint(20) YES  <nil> `))

	// Test generated column `\\`.
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(c0 TEXT AS ('\\\\'));")
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("\\"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(c0 TEXT AS ('a\\\\b\\\\c\\\\'))")
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("a\\b\\c\\"))
}

func (s *testDBSuite4) TestComment(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists ct, ct1")

	validComment := strings.Repeat("a", 1024)
	invalidComment := strings.Repeat("b", 1025)

	tk.MustExec("create table ct (c int, d int, e int, key (c) comment '" + validComment + "')")
	tk.MustExec("create index i on ct (d) comment '" + validComment + "'")
	tk.MustExec("alter table ct add key (e) comment '" + validComment + "'")

	tk.MustGetErrCode("create table ct1 (c int, key (c) comment '"+invalidComment+"')", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("create index i1 on ct (d) comment '"+invalidComment+"b"+"'", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("alter table ct add key (e) comment '"+invalidComment+"'", errno.ErrTooLongIndexComment)

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create table ct1 (c int, d int, e int, key (c) comment '" + invalidComment + "')")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	tk.MustExec("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	tk.MustExec("alter table ct1 add key (e) comment '" + invalidComment + "'")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))

	tk.MustExec("drop table if exists ct, ct1")
}

func (s *testSerialDBSuite) TestRebaseAutoID(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)

	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1"))
	tk.MustExec("alter table tidb.test auto_increment = 6000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1"))
	tk.MustExec("alter table tidb.test auto_increment = 5;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1"))

	// Current range for table test is [11000, 15999].
	// Though it does not have a tuple "a = 15999", its global next auto increment id should be 16000.
	// Anyway it is not compatible with MySQL.
	tk.MustExec("alter table tidb.test auto_increment = 12000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1", "16000 1"))

	tk.MustExec("create table tidb.test2 (a int);")
	tk.MustGetErrCode("alter table tidb.test2 add column b int auto_increment key, auto_increment=10;", errno.ErrUnsupportedDDLOperation)
}

func (s *testDBSuite5) TestCheckColumnDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists text_default_text;")
	tk.MustGetErrCode("create table text_default_text(c1 text not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_text(c1 text not null default 'scds');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_json;")
	tk.MustGetErrCode("create table text_default_json(c1 json not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_json(c1 json not null default 'dfew555');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_blob;")
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default 'scds54');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("set sql_mode='';")
	tk.MustExec("create table text_default_text(c1 text not null default '');")
	tk.MustQuery(`show create table text_default_text`).Check(testutil.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_text"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	tk.MustExec("create table text_default_blob(c1 blob not null default '');")
	tk.MustQuery(`show create table text_default_blob`).Check(testutil.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_blob"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	tk.MustExec("create table text_default_json(c1 json not null default '');")
	tk.MustQuery(`show create table text_default_json`).Check(testutil.RowsWithSep("|",
		"text_default_json CREATE TABLE `text_default_json` (\n"+
			"  `c1` json NOT NULL DEFAULT 'null'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_json"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, `null`)
}

func (s *testDBSuite1) TestCharacterSetInColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database varchar_test;")
	defer tk.MustExec("drop database varchar_test;")
	tk.MustExec("use varchar_test")
	tk.MustExec("create table t (c1 int, s1 varchar(10), s2 text)")
	tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name != 'utf8mb4'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name = 'utf8mb4'").Check(testkit.Rows("2"))

	tk.MustExec("create table t1(id int) charset=UTF8;")
	tk.MustExec("create table t2(id int) charset=BINARY;")
	tk.MustExec("create table t3(id int) charset=LATIN1;")
	tk.MustExec("create table t4(id int) charset=ASCII;")
	tk.MustExec("create table t5(id int) charset=UTF8MB4;")

	tk.MustExec("create table t11(id int) charset=utf8;")
	tk.MustExec("create table t12(id int) charset=binary;")
	tk.MustExec("create table t13(id int) charset=latin1;")
	tk.MustExec("create table t14(id int) charset=ascii;")
	tk.MustExec("create table t15(id int) charset=utf8mb4;")
}

func (s *testDBSuite2) TestAddNotNullColumnWhileInsertOnDupUpdate(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use " + s.schemaName)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use " + s.schemaName)
	closeCh := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	tk1.MustExec("create table nn (a int primary key, b int)")
	tk1.MustExec("insert nn values (1, 1)")
	var tk2Err error
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeCh:
				return
			default:
			}
			_, tk2Err = tk2.Exec("insert nn (a, b) values (1, 1) on duplicate key update a = 1, b = values(b) + 1")
			if tk2Err != nil {
				return
			}
		}
	}()
	tk1.MustExec("alter table nn add column c int not null default 3 after a")
	close(closeCh)
	wg.Wait()
	c.Assert(tk2Err, IsNil)
	tk1.MustQuery("select * from nn").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite3) TestColumnModifyingDefinition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test2;")
	tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("alter table test2 change c2 a int not null;")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test2"))
	c.Assert(err, IsNil)
	var c2 *table.Column
	for _, col := range t.Cols() {
		if col.Name.L == "a" {
			c2 = col
		}
	}
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsTrue)

	tk.MustExec("drop table if exists test2;")
	tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("insert into test2(c2) values (null);")
	_, err = tk.Exec("alter table test2 change c2 a int not null")
	c.Assert(err.Error(), Equals, "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustGetErrCode("alter table test2 change c1 a1 bigint not null;", mysql.WarnDataTruncated)
}

func (s *testDBSuite4) TestCheckTooBigFieldLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tr_01;")
	tk.MustExec("create table tr_01 (id int, name varchar(20000), purchased date )  default charset=utf8 collate=utf8_bin;")

	tk.MustExec("drop table if exists tr_02;")
	tk.MustExec("create table tr_02 (id int, name varchar(16000), purchased date )  default charset=utf8mb4 collate=utf8mb4_bin;")

	tk.MustExec("drop table if exists tr_03;")
	tk.MustExec("create table tr_03 (id int, name varchar(65534), purchased date ) default charset=latin1;")

	tk.MustExec("drop table if exists tr_04;")
	tk.MustExec("create table tr_04 (a varchar(20000) ) default charset utf8;")
	tk.MustGetErrCode("alter table tr_04 add column b varchar(20000) charset utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("alter table tr_04 convert to character set utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(30000), purchased date )  default charset=utf8 collate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(20000) charset utf8mb4, purchased date ) default charset=utf8 collate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create table tr (id int, name varchar(65536), purchased date ) default charset=latin1;", errno.ErrTooBigFieldlength)

	tk.MustExec("drop table if exists tr_05;")
	tk.MustExec("create table tr_05 (a varchar(16000) charset utf8);")
	tk.MustExec("alter table tr_05 modify column a varchar(16000) charset utf8;")
	tk.MustExec("alter table tr_05 modify column a varchar(16000) charset utf8mb4;")
}

func (s *testDBSuite5) TestCheckConvertToCharacter(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(10) charset binary);")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8mb4 collate utf8mb4_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset latin1 collate latin1_bin", errno.ErrUnsupportedDDLOperation)
	c.Assert(t.Cols()[0].Charset, Equals, "binary")
}

func (s *testDBSuite5) TestModifyColumnRollBack(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (c1 int, c2 int, c3 int default 1, index (c1));")

	var c2 *table.Column
	var checkErr error
	hook := &ddl.TestDDLCallback{Do: s.dom}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}

		t := s.testGetTable(c, "t1")
		for _, col := range t.Cols() {
			if col.Name.L == "c2" {
				c2 = col
			}
		}
		if mysql.HasPreventNullInsertFlag(c2.Flag) {
			tk.MustGetErrCode("insert into t1(c2) values (null);", errno.ErrBadNull)
		}

		hookCtx := mock.NewContext()
		hookCtx.Store = s.store
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}

		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DDL job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}

		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}

	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(s.store, "alter table t1 change c2 c2 bigint not null;", done)

	err := <-done
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
	s.mustExec(tk, c, "insert into t1(c2) values (null);")

	t := s.testGetTable(c, "t1")
	for _, col := range t.Cols() {
		if col.Name.L == "c2" {
			c2 = col
		}
	}
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsFalse)
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.mustExec(tk, c, "drop table t1")
}

func (s *testSerialDBSuite) TestModifyColumnReorgInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, index idx(c2), index idx1(c1, c2));")

	sql := "alter table t1 change c2 c2 mediumint;"
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 8
	// add some rows
	batchInsert(tk, "t1", 0, base)
	// Make sure the count of regions more than backfill workers.
	tk.MustQuery("split table t1 between (0) and (8192) regions 8;").Check(testkit.Rows("8 1"))

	tbl := s.testGetTable(c, "t1")
	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	// Check insert null before job first update.
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var checkErr error
	var currJob *model.Job
	var elements []*meta.Element
	ctx := mock.NewContext()
	ctx.Store = s.store
	times := 0
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID || checkErr != nil || job.SchemaState != model.StateWriteReorganization {
			return
		}
		if job.Type == model.ActionModifyColumn {
			if times == 0 {
				times++
				return
			}
			currJob = job
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			checkErr = job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			elements = ddl.BuildElements(changingCol, changingIdxs)
		}
		if job.Type == model.ActionAddIndex {
			if times == 1 {
				times++
				return
			}
			tbl := s.testGetTable(c, "t1")
			indexInfo := tbl.Meta().FindIndexByName("idx2")
			elements = []*meta.Element{{ID: indexInfo.ID, TypeKey: meta.IndexElementKey}}
		}
	}
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr", `return("cantDecodeRecordErr")`), IsNil)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err := tk.Exec(sql)
	c.Assert(err.Error(), Equals, "[ddl:8202]Cannot decode index value, because mock can't decode record error")
	c.Assert(checkErr, IsNil)
	// Check whether the reorg information is cleaned up when executing "modify column" failed.
	checkReorgHandle := func(gotElements, expectedElements []*meta.Element) {
		for i, e := range gotElements {
			c.Assert(e, DeepEquals, expectedElements[i])
		}
		err := ctx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		m := meta.NewMeta(txn)
		e, start, end, physicalID, err := m.GetDDLReorgHandle(currJob)
		c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)
		c.Assert(e, IsNil)
		c.Assert(start, IsNil)
		c.Assert(end, IsNil)
		c.Assert(physicalID, Equals, int64(0))
	}
	expectedEles := []*meta.Element{
		{ID: 4, TypeKey: meta.ColumnElementKey},
		{ID: 3, TypeKey: meta.IndexElementKey},
		{ID: 4, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedEles)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr"), IsNil)
	tk.MustExec("admin check table t1")

	// Check whether the reorg information is cleaned up when executing "modify column" successfully.
	// Test encountering a "notOwnerErr" error which caused the processing backfill job to exit halfway.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr", `return("modifyColumnNotOwnerErr")`), IsNil)
	tk.MustExec(sql)
	expectedEles = []*meta.Element{
		{ID: 5, TypeKey: meta.ColumnElementKey},
		{ID: 5, TypeKey: meta.IndexElementKey},
		{ID: 6, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedEles)
	tk.MustExec("admin check table t1")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr"), IsNil)

	// Test encountering a "notOwnerErr" error which caused the processing backfill job to exit halfway.
	// During the period, the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr", `return("addIdxNotOwnerErr")`), IsNil)
	tk.MustExec("alter table t1 add index idx2(c1)")
	expectedEles = []*meta.Element{
		{ID: 7, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedEles)
	tk.MustExec("admin check table t1")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockGetIndexRecordErr"), IsNil)
}

func (s *testSerialDBSuite) TestModifyColumnNullToNotNullWithChangingVal2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockInsertValueAfterCheckNull", `return("insert into test.tt values (NULL, NULL)")`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/ddl/mockInsertValueAfterCheckNull")
		c.Assert(err, IsNil)
	}()

	tk.MustExec("drop table if exists tt;")
	tk.MustExec(`create table tt (a bigint, b int, unique index idx(a));`)
	tk.MustExec("insert into tt values (1,1),(2,2),(3,3);")
	_, err := tk.Exec("alter table tt modify a int not null;")
	c.Assert(err.Error(), Equals, "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustExec("drop table tt")
}

func (s *testDBSuite1) TestModifyColumnNullToNotNull(c *C) {
	sql1 := "alter table t1 change c2 c2 int not null;"
	sql2 := "alter table t1 change c2 c2 int not null;"
	testModifyColumnNullToNotNull(c, s.testDBSuite, false, sql1, sql2)
}

func (s *testSerialDBSuite) TestModifyColumnNullToNotNullWithChangingVal(c *C) {
	sql1 := "alter table t1 change c2 c2 tinyint not null;"
	sql2 := "alter table t1 change c2 c2 tinyint not null;"
	testModifyColumnNullToNotNull(c, s.testDBSuite, true, sql1, sql2)
	c2 := getModifyColumn(c, s.s.(sessionctx.Context), s.schemaName, "t1", "c2", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeTiny)
}

func (s *testSerialDBSuite) TestModifyColumnBetweenStringTypes(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// varchar to varchar
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a varchar(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a varchar(5);")
	mvc := getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(mvc.FieldType.Flen, Equals, 5)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a varchar(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a varchar(100);")
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// char to char
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a char(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a char(5);")
	mc := getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(mc.FieldType.Flen, Equals, 5)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a char(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a char(100);")
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// binary to binary
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a binary(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustGetErrMsg("alter table tt change a a binary(5);", "[types:1265]Data truncated for column 'a', value is '111\x00\x00\x00\x00\x00\x00\x00'")
	mb := getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(mb.FieldType.Flen, Equals, 10)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00", "10000\x00\x00\x00\x00\x00"))
	tk.MustGetErrMsg("alter table tt change a a binary(4);", "[types:1265]Data truncated for column 'a', value is '111\x00\x00\x00\x00\x00\x00\x00'")
	tk.MustExec("alter table tt change a a binary(12);")
	tk.MustQuery("select * from tt").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00\x00\x00", "10000\x00\x00\x00\x00\x00\x00\x00"))
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("12", "12"))

	// varbinary to varbinary
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a varbinary(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a varbinary(5);")
	mvb := getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(mvb.FieldType.Flen, Equals, 5)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a varbinary(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a varbinary(12);")
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// varchar to char
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a varchar(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")

	tk.MustExec("alter table tt change a a char(10);")
	c2 := getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeString)
	c.Assert(c2.FieldType.Flen, Equals, 10)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a char(4);", "[types:1265]Data truncated for column 'a', value is '10000'")

	// char to text
	tk.MustExec("alter table tt change a a text;")
	c2 = getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeBlob)

	// text to set
	tk.MustGetErrMsg("alter table tt change a a set('111', '2222');", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a set('111', '10000');")
	c2 = getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeSet)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))

	// set to set
	tk.MustExec("alter table tt change a a set('10000', '111');")
	c2 = getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeSet)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))

	// set to enum
	tk.MustGetErrMsg("alter table tt change a a enum('111', '2222');", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a enum('111', '10000');")
	c2 = getModifyColumn(c, s.s.(sessionctx.Context), "test", "tt", "a", false)
	c.Assert(c2.FieldType.Tp, Equals, mysql.TypeEnum)
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustExec("alter table tt change a a enum('10000', '111');")
	tk.MustQuery("select * from tt where a = 1").Check(testkit.Rows("10000"))
	tk.MustQuery("select * from tt where a = 2").Check(testkit.Rows("111"))

	// no-strict mode
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustExec("alter table tt change a a enum('111', '2222');")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data truncated for column 'a', value is '10000'"))

	tk.MustExec("drop table tt;")
}

func getModifyColumn(c *C, ctx sessionctx.Context, db, tbl, colName string, allColumn bool) *table.Column {
	t := testGetTableByName(c, ctx, db, tbl)
	colName = strings.ToLower(colName)
	var cols []*table.Column
	if allColumn {
		cols = t.(*tables.TableCommon).Columns
	} else {
		cols = t.Cols()
	}
	for _, col := range cols {
		if col.Name.L == colName {
			return col
		}
	}
	return nil
}

func testModifyColumnNullToNotNull(c *C, s *testDBSuite, enableChangeColumnType bool, sql1, sql2 string) {
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (c1 int, c2 int);")

	tbl := s.testGetTable(c, "t1")
	getModifyColumn(c, s.s.(sessionctx.Context), s.schemaName, "t1", "c2", false)

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	// Check insert null before job first update.
	times := 0
	hook := &ddl.TestDDLCallback{Do: s.dom}
	tk.MustExec("delete from t1")
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if times == 0 {
			_, checkErr = tk2.Exec("insert into t1 values ();")
		}
		times++
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err := tk.Exec(sql1)
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	if enableChangeColumnType {
		c.Assert(err.Error(), Equals, "[ddl:1265]Data truncated for column 'c2' at row 1")
		// Check whether the reorg information is cleaned up.
	} else {
		c.Assert(err.Error(), Equals, "[ddl:1138]Invalid use of NULL value")
	}
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil>"))

	// Check insert error when column has PreventNullInsertFlag.
	tk.MustExec("delete from t1")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.State != model.JobStateRunning {
			return
		}
		// now c2 has PreventNullInsertFlag, an error is expected.
		_, checkErr = tk2.Exec("insert into t1 values ();")
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	tk.MustExec(sql2)
	c.Assert(checkErr.Error(), Equals, "[table:1048]Column 'c2' cannot be null")

	c2 := getModifyColumn(c, s.s.(sessionctx.Context), s.schemaName, "t1", "c2", false)
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsTrue)
	c.Assert(mysql.HasPreventNullInsertFlag(c2.Flag), IsFalse)
	_, err = tk.Exec("insert into t1 values ();")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[table:1364]Field 'c2' doesn't have a default value")
}

func (s *testDBSuite2) TestTransactionOnAddDropColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (a int, b int);")
	s.mustExec(tk, c, "create table t2 (a int, b int);")
	s.mustExec(tk, c, "insert into t2 values (2,0)")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set b=1 where a=1",
			"commit",
		},
		{
			"begin",
			"insert into t1 select a,b from t2",
			"update t1 set b=2 where a=2",
			"commit",
		},
	}

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly, model.StateWriteReorganization, model.StateDeleteOnly, model.StateDeleteReorganization:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				if _, checkErr = tk.Exec(sql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, sql: %s, job schema state: %s", checkErr.Error(), sql, job.SchemaState)
					return
				}
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null after a", done)
	err := <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
	s.mustExec(tk, c, "delete from t1")

	// test transaction on drop column.
	go backgroundExec(s.store, "alter table t1 drop column c", done)
	err = <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
}

func (s *testDBSuite3) TestIssue22307(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t")
	s.mustExec(tk, c, "create table t (a int, b int)")
	s.mustExec(tk, c, "insert into t values(1, 1);")

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var checkErr1, checkErr2 error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		_, checkErr1 = tk.Exec("update t set a = 3 where b = 1;")
		_, checkErr2 = tk.Exec("update t set a = 3 order by b;")
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t drop column b;", done)
	err := <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr1.Error(), Equals, "[planner:1054]Unknown column 'b' in 'where clause'")
	c.Assert(checkErr2.Error(), Equals, "[planner:1054]Unknown column 'b' in 'order clause'")
}

func (s *testDBSuite3) TestTransactionWithWriteOnlyColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (a int key);")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set a=2 where a=1",
			"commit",
		},
	}

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{Do: s.dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				if _, checkErr = tk.Exec(sql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, sql: %s, job schema state: %s", checkErr.Error(), sql, job.SchemaState)
					return
				}
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	s.mustExec(tk, c, "delete from t1")

	// test transaction on drop column.
	go backgroundExec(s.store, "alter table t1 drop column c", done)
	err = <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

func (s *testDBSuite4) TestAddColumn2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.mustExec(tk, c, "use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (a int key, b int);")
	defer s.mustExec(tk, c, "drop table if exists t1, t2")

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	var writeOnlyTable table.Table
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			writeOnlyTable, _ = s.dom.InfoSchema().TableByID(job.TableID)
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)

	s.mustExec(tk, c, "insert into t1 values (1,1,1)")
	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 1 1"))

	// mock for outdated tidb update record.
	c.Assert(writeOnlyTable, NotNil)
	ctx := context.Background()
	err = tk.Se.NewTxn(ctx)
	c.Assert(err, IsNil)
	oldRow, err := tables.RowWithCols(writeOnlyTable, tk.Se, kv.IntHandle(1), writeOnlyTable.WritableCols())
	c.Assert(err, IsNil)
	c.Assert(len(oldRow), Equals, 3)
	err = writeOnlyTable.RemoveRecord(tk.Se, kv.IntHandle(1), oldRow)
	c.Assert(err, IsNil)
	_, err = writeOnlyTable.AddRecord(tk.Se, types.MakeDatums(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()), table.IsUpdate)
	c.Assert(err, IsNil)
	tk.Se.StmtCommit()
	err = tk.Se.CommitTxn(ctx)
	c.Assert(err, IsNil)

	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 2 1"))

	// Test for _tidb_rowid
	var re *testkit.Result
	s.mustExec(tk, c, "create table t2 (a int);")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		// allow write _tidb_rowid first
		s.mustExec(tk, c, "set @@tidb_opt_write_row_id=1")
		s.mustExec(tk, c, "begin")
		s.mustExec(tk, c, "insert into t2 (a,_tidb_rowid) values (1,2);")
		re = tk.MustQuery(" select a,_tidb_rowid from t2;")
		s.mustExec(tk, c, "commit")

	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	go backgroundExec(s.store, "alter table t2 add column b int not null default 3", done)
	err = <-done
	c.Assert(err, IsNil)
	re.Check(testkit.Rows("1 2"))
	tk.MustQuery("select a,b,_tidb_rowid from t2").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite4) TestIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (a int key);")

	// ADD COLUMN
	sql := "alter table t1 add column b int"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	s.mustExec(tk, c, "alter table t1 add column if not exists b int")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1060|Duplicate column name 'b'"))

	// ADD INDEX
	sql = "alter table t1 add index idx_b (b)"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	s.mustExec(tk, c, "alter table t1 add index if not exists idx_b (b)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// CREATE INDEX
	sql = "create index idx_b on t1 (b)"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	s.mustExec(tk, c, "create index if not exists idx_b on t1 (b)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// ADD PARTITION
	s.mustExec(tk, c, "drop table if exists t2")
	s.mustExec(tk, c, "create table t2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 add partition (partition p2 values less than (30))"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrSameNamePartition)
	s.mustExec(tk, c, "alter table t2 add partition if not exists (partition p2 values less than (30))")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1517|Duplicate partition name p2"))
}

func (s *testDBSuite4) TestIfExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	s.mustExec(tk, c, "drop table if exists t1")
	s.mustExec(tk, c, "create table t1 (a int key, b int);")

	// DROP COLUMN
	sql := "alter table t1 drop column b"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	s.mustExec(tk, c, "alter table t1 drop column if exists b") // only `a` exists now
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|Can't DROP 'b'; check that column/key exists"))

	// CHANGE COLUMN
	sql = "alter table t1 change column b c int"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	s.mustExec(tk, c, "alter table t1 change column if exists b c int")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'b' in 't1'"))
	s.mustExec(tk, c, "alter table t1 change column if exists a c int") // only `c` exists now

	// MODIFY COLUMN
	sql = "alter table t1 modify column a bigint"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	s.mustExec(tk, c, "alter table t1 modify column if exists a bigint")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'a' in 't1'"))
	s.mustExec(tk, c, "alter table t1 modify column if exists c bigint") // only `c` exists now

	// DROP INDEX
	s.mustExec(tk, c, "alter table t1 add index idx_c (c)")
	sql = "alter table t1 drop index idx_c"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	s.mustExec(tk, c, "alter table t1 drop index if exists idx_c")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|index idx_c doesn't exist"))

	// DROP PARTITION
	s.mustExec(tk, c, "drop table if exists t2")
	s.mustExec(tk, c, "create table t2 (a int key) partition by range(a) (partition pNeg values less than (0), partition p0 values less than (10), partition p1 values less than (20))")
	sql = "alter table t2 drop partition p1"
	s.mustExec(tk, c, sql)
	tk.MustGetErrCode(sql, errno.ErrDropPartitionNonExistent)
	s.mustExec(tk, c, "alter table t2 drop partition if exists p1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1507|Error in list of partitions to DROP"))
}

func testAddIndexForGeneratedColumn(tk *testkit.TestKit, s *testDBSuite5, c *C) {
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(y year NOT NULL DEFAULT '2155')")
	defer s.mustExec(tk, c, "drop table t;")
	for i := 0; i < 50; i++ {
		s.mustExec(tk, c, "insert into t values (?)", i)
	}
	tk.MustExec("insert into t values()")
	tk.MustExec("ALTER TABLE t ADD COLUMN y1 year as (y + 2)")
	_, err := tk.Exec("ALTER TABLE t ADD INDEX idx_y(y1)")
	c.Assert(err, IsNil)

	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/pingcap/tidb/issues/12181
	// s.mustExec(c, "delete from t where y = 2155")
	// s.mustExec(c, "alter table t add index idx_y(y1)")
	// s.mustExec(c, "alter table t drop index idx_y")

	// Fix issue 9311.
	tk.MustExec("drop table if exists gcai_table")
	tk.MustExec("create table gcai_table (id int primary key);")
	tk.MustExec("insert into gcai_table values(1);")
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d date DEFAULT '9999-12-31';")
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d1 date as (DATE_SUB(d, INTERVAL 31 DAY));")
	tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx(d1);")
	tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30"))
	tk.MustQuery("select d1 from gcai_table use index(idx)").Check(testkit.Rows("9999-11-30"))
	tk.MustExec("admin check table gcai_table")
	// The column is PKIsHandle in generated column expression.
	tk.MustExec("ALTER TABLE gcai_table ADD COLUMN id1 int as (id+5);")
	tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx1(id1);")
	tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30 6"))
	tk.MustQuery("select id1 from gcai_table use index(idx1)").Check(testkit.Rows("6"))
	tk.MustExec("admin check table gcai_table")
}

func (s *testDBSuite5) TestAddIndexForGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testAddIndexForGeneratedColumn(tk, s, c)
}

func (s *testDBSuite5) TestModifyGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test;")
	tk.MustExec("use test")
	modIdxColErrMsg := "[ddl:3106]'modifying an indexed column' is not supported for generated columns."
	modStoredColErrMsg := "[ddl:3106]'modifying a stored column' is not supported for generated columns."

	// Modify column with single-col-index.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	_, err := tk.Exec("alter table t1 modify column b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with multi-col-index.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(a, b));")
	tk.MustExec("insert into t1 set a=1;")
	_, err = tk.Exec("alter table t1 modify column b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with stored status to a different expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	_, err = tk.Exec("alter table t1 modify column b int as (a+2) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredColErrMsg)

	// Modify column with stored status to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1) stored;")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1) stored;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column with index to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1);")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column from non-generated to stored generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int);")
	_, err = tk.Exec("alter table t1 modify column b bigint as (a+1) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredColErrMsg)

	// Modify column from stored generated to non-generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b int;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))
}

func (s *testDBSuite5) TestDefaultSQLFunction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test;")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2, t3, t4;")

	// For issue #13189
	// Use `DEFAULT()` in `INSERT` / `INSERT ON DUPLICATE KEY UPDATE` statement
	tk.MustExec("create table t1 (a int primary key, b int default 20, c int default 30, d int default 40);")
	tk.MustExec("SET @@time_zone = '+00:00'")
	defer tk.MustExec("SET @@time_zone = DEFAULT")
	tk.MustQuery("SELECT @@time_zone").Check(testkit.Rows("+00:00"))
	tk.MustExec("create table t2 (a int primary key, b timestamp DEFAULT CURRENT_TIMESTAMP, c timestamp DEFAULT '2000-01-01 00:00:00')")
	tk.MustExec("insert into t1 set a = 1, b = default(c);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40"))
	tk.MustExec("insert into t1 set a = 2, b = default(c), c = default(d), d = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 30 40 20"))
	tk.MustExec("insert into t1 values (2, 3, 4, 5) on duplicate key update b = default(d), c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 40 20 20"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = default(b) + default(c) - default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20 30 40"))
	tk.MustExec("set @@timestamp = 1321009871")
	defer tk.MustExec("set @@timestamp = DEFAULT")
	tk.MustQuery("SELECT NOW()").Check(testkit.Rows("2011-11-11 11:11:11"))
	tk.MustExec("insert into t2 set a = 1, b = default(c)")
	tk.MustExec("insert into t2 set a = 2, c = default(b)")
	tk.MustGetErrCode("insert into t2 set a = 3, b = default(a)", errno.ErrNoDefaultForField)
	tk.MustExec("insert into t2 set a = 4, b = default(b), c = default(c)")
	tk.MustExec("insert into t2 set a = 5, b = default, c = default")
	tk.MustExec("insert into t2 set a = 6")
	tk.MustQuery("select * from t2").Sort().Check(testkit.Rows(
		"1 2000-01-01 00:00:00 2000-01-01 00:00:00",
		"2 2011-11-11 11:11:11 2011-11-11 11:11:11",
		"4 2011-11-11 11:11:11 2000-01-01 00:00:00",
		"5 2011-11-11 11:11:11 2000-01-01 00:00:00",
		"6 2011-11-11 11:11:11 2000-01-01 00:00:00"))
	// Use `DEFAULT()` in `UPDATE` statement
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 value (1, 2, 3, 4);")
	tk.MustExec("update t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4"))
	tk.MustExec("insert into t1 value (2, 2, 3, 4);")
	tk.MustExec("update t1 set c = default(b), b = default(c) where a = 2;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4", "2 30 20 4"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = 10")
	tk.MustExec("update t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	tk.MustExec("set @@timestamp = 1671747742")
	tk.MustExec("update t2 set b = default(c) WHERE a = 6")
	tk.MustExec("update t2 set c = default(b) WHERE a = 5")
	tk.MustGetErrCode("update t2 set b = default(a) WHERE a = 4", errno.ErrNoDefaultForField)
	tk.MustExec("update t2 set b = default(b), c = default(c) WHERE a = 4")
	// Non existing row!
	tk.MustExec("update t2 set b = default(b), c = default(c) WHERE a = 3")
	tk.MustExec("update t2 set b = default, c = default WHERE a = 2")
	tk.MustExec("update t2 set b = default(b) WHERE a = 1")
	tk.MustQuery("select * from t2;").Sort().Check(testkit.Rows(
		"1 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"2 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"4 2022-12-22 22:22:22 2000-01-01 00:00:00",
		"5 2011-11-11 11:11:11 2022-12-22 22:22:22",
		"6 2000-01-01 00:00:00 2000-01-01 00:00:00"))
	// Use `DEFAULT()` in `REPLACE` statement
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 value (1, 2, 3, 4);")
	tk.MustExec("replace into t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40"))
	tk.MustExec("insert into t1 value (2, 2, 3, 4);")
	tk.MustExec("replace into t1 set a = 2, d = default(b), c = default(d);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40", "2 20 40 20"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a = 10, c = 3")
	tk.MustExec("replace into t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	tk.MustExec("replace into t1 set a = 20, d = default(c) + default(b)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40", "20 20 30 50"))

	// Use `DEFAULT()` in expression of generate columns, issue #12471
	tk.MustExec("DROP TABLE t2")
	tk.MustExec("create table t2(a int default 9, b int as (1 + default(a)));")
	tk.MustExec("insert into t2 values(1, default);")
	tk.MustExec("insert into t2 values(2, default(b))")
	tk.MustQuery("select * from t2").Sort().Check(testkit.Rows("1 10", "2 10"))

	// Use `DEFAULT()` with subquery, issue #13390
	tk.MustExec("create table t3(f1 int default 11);")
	tk.MustExec("insert into t3 value ();")
	tk.MustQuery("select default(f1) from (select * from t3) t1;").Check(testkit.Rows("11"))
	tk.MustQuery("select default(f1) from (select * from (select * from t3) t1 ) t1;").Check(testkit.Rows("11"))

	tk.MustExec("create table t4(a int default 4);")
	tk.MustExec("insert into t4 value (2);")
	tk.MustQuery("select default(c) from (select b as c from (select a as b from t4) t3) t2;").Check(testkit.Rows("4"))
	tk.MustGetErrCode("select default(a) from (select a from (select 1 as a) t4) t4;", errno.ErrNoDefaultForField)

	tk.MustExec("drop table t1, t2, t3, t4;")
}

func (s *testDBSuite4) TestIssue9100(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table employ (a int, b int) partition by range (b) (partition p0 values less than (1));")
	_, err := tk.Exec("alter table employ add unique index p_a (a);")
	c.Assert(err.Error(), Equals, "[ddl:1503]A UNIQUE INDEX must include all columns in the table's partitioning function")
	_, err = tk.Exec("alter table employ add primary key p_a (a);")
	c.Assert(err.Error(), Equals, "[ddl:1503]A PRIMARY must include all columns in the table's partitioning function")

	tk.MustExec("create table issue9100t1 (col1 int not null, col2 date not null, col3 int not null, unique key (col1, col2)) partition by range( col1 ) (partition p1 values less than (11))")
	tk.MustExec("alter table issue9100t1 add unique index p_col1 (col1)")
	tk.MustExec("alter table issue9100t1 add primary key p_col1 (col1)")

	tk.MustExec("create table issue9100t2 (col1 int not null, col2 date not null, col3 int not null, unique key (col1, col3)) partition by range( col1 + col3 ) (partition p1 values less than (11))")
	_, err = tk.Exec("alter table issue9100t2 add unique index p_col1 (col1)")
	c.Assert(err.Error(), Equals, "[ddl:1503]A UNIQUE INDEX must include all columns in the table's partitioning function")
	_, err = tk.Exec("alter table issue9100t2 add primary key p_col1 (col1)")
	c.Assert(err.Error(), Equals, "[ddl:1503]A PRIMARY must include all columns in the table's partitioning function")
}

func (s *testSerialDBSuite) TestProcessColumnFlags(c *C) {
	// check `processColumnFlags()`
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t(a year(4) comment 'xxx', b year, c bit)")
	defer s.mustExec(tk, c, "drop table t;")

	check := func(n string, f func(uint) bool) {
		t := testGetTableByName(c, tk.Se, "test_db", "t")
		for _, col := range t.Cols() {
			if strings.EqualFold(col.Name.L, n) {
				c.Assert(f(col.Flag), IsTrue)
				break
			}
		}
	}

	yearcheck := func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && mysql.HasZerofillFlag(f) && !mysql.HasBinaryFlag(f)
	}

	tk.MustExec("alter table t modify a year(4)")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) unsigned")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) zerofill")

	tk.MustExec("alter table t modify b year")
	check("b", yearcheck)

	tk.MustExec("alter table t modify c bit")
	check("c", func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && !mysql.HasBinaryFlag(f)
	})
}

func (s *testSerialDBSuite) TestModifyColumnCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t_mcc(a varchar(8) charset utf8, b varchar(8) charset utf8)")
	defer s.mustExec(tk, c, "drop table t_mcc;")

	result := tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("alter table t_mcc modify column a varchar(8);")
	t := s.testGetTable(c, "t_mcc")
	t.Meta().Version = model.TableInfoVersion0
	// When the table version is TableInfoVersion0, the following statement don't change "b" charset.
	// So the behavior is not compatible with MySQL.
	tk.MustExec("alter table t_mcc modify column b varchar(8);")
	result = tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

}

func (s *testDBSuite1) TestModifyColumnTime_TimeToYear(c *C) {
	outOfRangeCode := uint16(1264)
	tests := []testModifyColumnTimeCase{
		// time to year, it's reasonable to return current year and discard the time (even if MySQL may get data out of range error).
		{"time", `"30 20:00:12"`, "year", "", outOfRangeCode},
		{"time", `"30 20:00"`, "year", "", outOfRangeCode},
		{"time", `"30 20"`, "year", "", outOfRangeCode},
		{"time", `"20:00:12"`, "year", "", outOfRangeCode},
		{"time", `"20:00"`, "year", "", outOfRangeCode},
		{"time", `"12"`, "year", "2012", 0},
		{"time", `"200012"`, "year", "", outOfRangeCode},
		{"time", `200012`, "year", "", outOfRangeCode},
		{"time", `0012`, "year", "2012", 0},
		{"time", `12`, "year", "2012", 0},
		{"time", `"30 20:00:12.498"`, "year", "", outOfRangeCode},
		{"time", `"20:00:12.498"`, "year", "", outOfRangeCode},
		{"time", `"200012.498"`, "year", "", outOfRangeCode},
		{"time", `200012.498`, "year", "", outOfRangeCode},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimeToDate(c *C) {
	now := time.Now().UTC()
	now = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	timeToDate1 := now.Format("2006-01-02")
	timeToDate2 := now.AddDate(0, 0, 30).Format("2006-01-02")
	tests := []testModifyColumnTimeCase{
		// time to date
		{"time", `"30 20:00:12"`, "date", timeToDate2, 0},
		{"time", `"30 20:00"`, "date", timeToDate2, 0},
		{"time", `"30 20"`, "date", timeToDate2, 0},
		{"time", `"20:00:12"`, "date", timeToDate1, 0},
		{"time", `"20:00"`, "date", timeToDate1, 0},
		{"time", `"12"`, "date", timeToDate1, 0},
		{"time", `"200012"`, "date", timeToDate1, 0},
		{"time", `200012`, "date", timeToDate1, 0},
		{"time", `0012`, "date", timeToDate1, 0},
		{"time", `12`, "date", timeToDate1, 0},
		{"time", `"30 20:00:12.498"`, "date", timeToDate2, 0},
		{"time", `"20:00:12.498"`, "date", timeToDate1, 0},
		{"time", `"200012.498"`, "date", timeToDate1, 0},
		{"time", `200012.498`, "date", timeToDate1, 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimeToDatetime(c *C) {
	now := time.Now().UTC()
	now = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	timeToDatetime1 := now.Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime2 := now.Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	timeToDatetime3 := now.Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime4 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime5 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	tests := []testModifyColumnTimeCase{
		// time to datetime
		{"time", `"30 20:00:12"`, "datetime", timeToDatetime4, 0},
		{"time", `"30 20:00"`, "datetime", timeToDatetime5, 0},
		{"time", `"30 20"`, "datetime", timeToDatetime5, 0},
		{"time", `"20:00:12"`, "datetime", timeToDatetime1, 0},
		{"time", `"20:00"`, "datetime", timeToDatetime2, 0},
		{"time", `"12"`, "datetime", timeToDatetime3, 0},
		{"time", `"200012"`, "datetime", timeToDatetime1, 0},
		{"time", `200012`, "datetime", timeToDatetime1, 0},
		{"time", `0012`, "datetime", timeToDatetime3, 0},
		{"time", `12`, "datetime", timeToDatetime3, 0},
		{"time", `"30 20:00:12.498"`, "datetime", timeToDatetime4, 0},
		{"time", `"20:00:12.498"`, "datetime", timeToDatetime1, 0},
		{"time", `"200012.498"`, "datetime", timeToDatetime1, 0},
		{"time", `200012.498`, "datetime", timeToDatetime1, 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimeToTimestamp(c *C) {
	now := time.Now().UTC()
	now = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	timeToTimestamp1 := now.Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp2 := now.Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	timeToTimestamp3 := now.Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp4 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp5 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	tests := []testModifyColumnTimeCase{
		// time to timestamp
		{"time", `"30 20:00:12"`, "timestamp", timeToTimestamp4, 0},
		{"time", `"30 20:00"`, "timestamp", timeToTimestamp5, 0},
		{"time", `"30 20"`, "timestamp", timeToTimestamp5, 0},
		{"time", `"20:00:12"`, "timestamp", timeToTimestamp1, 0},
		{"time", `"20:00"`, "timestamp", timeToTimestamp2, 0},
		{"time", `"12"`, "timestamp", timeToTimestamp3, 0},
		{"time", `"200012"`, "timestamp", timeToTimestamp1, 0},
		{"time", `200012`, "timestamp", timeToTimestamp1, 0},
		{"time", `0012`, "timestamp", timeToTimestamp3, 0},
		{"time", `12`, "timestamp", timeToTimestamp3, 0},
		{"time", `"30 20:00:12.498"`, "timestamp", timeToTimestamp4, 0},
		{"time", `"20:00:12.498"`, "timestamp", timeToTimestamp1, 0},
		{"time", `"200012.498"`, "timestamp", timeToTimestamp1, 0},
		{"time", `200012.498`, "timestamp", timeToTimestamp1, 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite7) TestModifyColumnTime_DateToTime(c *C) {
	tests := []testModifyColumnTimeCase{
		// date to time
		{"date", `"2019-01-02"`, "time", "00:00:00", 0},
		{"date", `"19-01-02"`, "time", "00:00:00", 0},
		{"date", `"20190102"`, "time", "00:00:00", 0},
		{"date", `"190102"`, "time", "00:00:00", 0},
		{"date", `20190102`, "time", "00:00:00", 0},
		{"date", `190102`, "time", "00:00:00", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DateToYear(c *C) {
	tests := []testModifyColumnTimeCase{
		// date to year
		{"date", `"2019-01-02"`, "year", "2019", 0},
		{"date", `"19-01-02"`, "year", "2019", 0},
		{"date", `"20190102"`, "year", "2019", 0},
		{"date", `"190102"`, "year", "2019", 0},
		{"date", `20190102`, "year", "2019", 0},
		{"date", `190102`, "year", "2019", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DateToDatetime(c *C) {
	tests := []testModifyColumnTimeCase{
		// date to datetime
		{"date", `"2019-01-02"`, "datetime", "2019-01-02 00:00:00", 0},
		{"date", `"19-01-02"`, "datetime", "2019-01-02 00:00:00", 0},
		{"date", `"20190102"`, "datetime", "2019-01-02 00:00:00", 0},
		{"date", `"190102"`, "datetime", "2019-01-02 00:00:00", 0},
		{"date", `20190102`, "datetime", "2019-01-02 00:00:00", 0},
		{"date", `190102`, "datetime", "2019-01-02 00:00:00", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DateToTimestamp(c *C) {
	tests := []testModifyColumnTimeCase{
		// date to timestamp
		{"date", `"2019-01-02"`, "timestamp", "2019-01-02 00:00:00", 0},
		{"date", `"19-01-02"`, "timestamp", "2019-01-02 00:00:00", 0},
		{"date", `"20190102"`, "timestamp", "2019-01-02 00:00:00", 0},
		{"date", `"190102"`, "timestamp", "2019-01-02 00:00:00", 0},
		{"date", `20190102`, "timestamp", "2019-01-02 00:00:00", 0},
		{"date", `190102`, "timestamp", "2019-01-02 00:00:00", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimestampToYear(c *C) {
	tests := []testModifyColumnTimeCase{
		// timestamp to year
		{"timestamp", `"2006-01-02 15:04:05"`, "year", "2006", 0},
		{"timestamp", `"06-01-02 15:04:05"`, "year", "2006", 0},
		{"timestamp", `"20060102150405"`, "year", "2006", 0},
		{"timestamp", `"060102150405"`, "year", "2006", 0},
		{"timestamp", `20060102150405`, "year", "2006", 0},
		{"timestamp", `060102150405`, "year", "2006", 0},
		{"timestamp", `"2006-01-02 23:59:59.506"`, "year", "2006", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimestampToTime(c *C) {
	tests := []testModifyColumnTimeCase{
		// timestamp to time
		{"timestamp", `"2006-01-02 15:04:05"`, "time", "15:04:05", 0},
		{"timestamp", `"06-01-02 15:04:05"`, "time", "15:04:05", 0},
		{"timestamp", `"20060102150405"`, "time", "15:04:05", 0},
		{"timestamp", `"060102150405"`, "time", "15:04:05", 0},
		{"timestamp", `20060102150405`, "time", "15:04:05", 0},
		{"timestamp", `060102150405`, "time", "15:04:05", 0},
		{"timestamp", `"2006-01-02 23:59:59.506"`, "time", "00:00:00", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimestampToDate(c *C) {
	tests := []testModifyColumnTimeCase{
		// timestamp to date
		{"timestamp", `"2006-01-02 15:04:05"`, "date", "2006-01-02", 0},
		{"timestamp", `"06-01-02 15:04:05"`, "date", "2006-01-02", 0},
		{"timestamp", `"20060102150405"`, "date", "2006-01-02", 0},
		{"timestamp", `"060102150405"`, "date", "2006-01-02", 0},
		{"timestamp", `20060102150405`, "date", "2006-01-02", 0},
		{"timestamp", `060102150405`, "date", "2006-01-02", 0},
		{"timestamp", `"2006-01-02 23:59:59.506"`, "date", "2006-01-03", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_TimestampToDatetime(c *C) {
	tests := []testModifyColumnTimeCase{
		// timestamp to datetime
		{"timestamp", `"2006-01-02 15:04:05"`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `"06-01-02 15:04:05"`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `"20060102150405"`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `"060102150405"`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `20060102150405`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `060102150405`, "datetime", "2006-01-02 15:04:05", 0},
		{"timestamp", `"2006-01-02 23:59:59.506"`, "datetime", "2006-01-03 00:00:00", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DatetimeToYear(c *C) {
	tests := []testModifyColumnTimeCase{
		// datetime to year
		{"datetime", `"2006-01-02 15:04:05"`, "year", "2006", 0},
		{"datetime", `"06-01-02 15:04:05"`, "year", "2006", 0},
		{"datetime", `"20060102150405"`, "year", "2006", 0},
		{"datetime", `"060102150405"`, "year", "2006", 0},
		{"datetime", `20060102150405`, "year", "2006", 0},
		{"datetime", `060102150405`, "year", "2006", 0},
		{"datetime", `"2006-01-02 23:59:59.506"`, "year", "2006", 0},
		{"datetime", `"1000-01-02 23:59:59"`, "year", "", errno.ErrWarnDataOutOfRange},
		{"datetime", `"9999-01-02 23:59:59"`, "year", "", errno.ErrWarnDataOutOfRange},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DatetimeToTime(c *C) {
	tests := []testModifyColumnTimeCase{
		// datetime to time
		{"datetime", `"2006-01-02 15:04:05"`, "time", "15:04:05", 0},
		{"datetime", `"06-01-02 15:04:05"`, "time", "15:04:05", 0},
		{"datetime", `"20060102150405"`, "time", "15:04:05", 0},
		{"datetime", `"060102150405"`, "time", "15:04:05", 0},
		{"datetime", `20060102150405`, "time", "15:04:05", 0},
		{"datetime", `060102150405`, "time", "15:04:05", 0},
		{"datetime", `"2006-01-02 23:59:59.506"`, "time", "00:00:00", 0},
		{"datetime", `"1000-01-02 23:59:59"`, "time", "23:59:59", 0},
		{"datetime", `"9999-01-02 23:59:59"`, "time", "23:59:59", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DatetimeToDate(c *C) {
	tests := []testModifyColumnTimeCase{
		// datetime to date
		{"datetime", `"2006-01-02 15:04:05"`, "date", "2006-01-02", 0},
		{"datetime", `"06-01-02 15:04:05"`, "date", "2006-01-02", 0},
		{"datetime", `"20060102150405"`, "date", "2006-01-02", 0},
		{"datetime", `"060102150405"`, "date", "2006-01-02", 0},
		{"datetime", `20060102150405`, "date", "2006-01-02", 0},
		{"datetime", `060102150405`, "date", "2006-01-02", 0},
		{"datetime", `"2006-01-02 23:59:59.506"`, "date", "2006-01-03", 0},
		{"datetime", `"1000-01-02 23:59:59"`, "date", "1000-01-02", 0},
		{"datetime", `"9999-01-02 23:59:59"`, "date", "9999-01-02", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_DatetimeToTimestamp(c *C) {
	tests := []testModifyColumnTimeCase{
		// datetime to timestamp
		{"datetime", `"2006-01-02 15:04:05"`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `"06-01-02 15:04:05"`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `"20060102150405"`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `"060102150405"`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `20060102150405`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `060102150405`, "timestamp", "2006-01-02 15:04:05", 0},
		{"datetime", `"2006-01-02 23:59:59.506"`, "timestamp", "2006-01-03 00:00:00", 0},
		{"datetime", `"1971-01-02 23:59:59"`, "timestamp", "1971-01-02 23:59:59", 0},
		{"datetime", `"2009-01-02 23:59:59"`, "timestamp", "2009-01-02 23:59:59", 0},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_YearToTime(c *C) {
	tests := []testModifyColumnTimeCase{
		// year to time
		// failed cases are not handled by TiDB
		{"year", `"2019"`, "time", "00:20:19", 0},
		{"year", `2019`, "time", "00:20:19", 0},
		{"year", `"00"`, "time", "00:20:00", 0},
		{"year", `"69"`, "time", "", errno.ErrTruncatedWrongValue},
		{"year", `"70"`, "time", "", errno.ErrTruncatedWrongValue},
		{"year", `"99"`, "time", "", errno.ErrTruncatedWrongValue},
		{"year", `00`, "time", "00:00:00", 0},
		{"year", `69`, "time", "", errno.ErrTruncatedWrongValue},
		{"year", `70`, "time", "", errno.ErrTruncatedWrongValue},
		{"year", `99`, "time", "", errno.ErrTruncatedWrongValue},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_YearToDate(c *C) {
	tests := []testModifyColumnTimeCase{
		// year to date
		{"year", `"2019"`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `2019`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `"00"`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `"69"`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `"70"`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `"99"`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `00`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `69`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `70`, "date", "", errno.ErrTruncatedWrongValue},
		{"year", `99`, "date", "", errno.ErrTruncatedWrongValue},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_YearToDatetime(c *C) {
	tests := []testModifyColumnTimeCase{
		// year to datetime
		{"year", `"2019"`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `2019`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `"00"`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `"69"`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `"70"`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `"99"`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `00`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `69`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `70`, "datetime", "", errno.ErrTruncatedWrongValue},
		{"year", `99`, "datetime", "", errno.ErrTruncatedWrongValue},
	}
	testModifyColumnTime(c, s.store, tests)
}

func (s *testDBSuite1) TestModifyColumnTime_YearToTimestamp(c *C) {
	tests := []testModifyColumnTimeCase{
		// year to timestamp
		{"year", `"2019"`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `2019`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `"00"`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `"69"`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `"70"`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `"99"`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `00`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `69`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `70`, "timestamp", "", errno.ErrTruncatedWrongValue},
		{"year", `99`, "timestamp", "", errno.ErrTruncatedWrongValue},
	}
	testModifyColumnTime(c, s.store, tests)
}

type testModifyColumnTimeCase struct {
	from   string
	value  string
	to     string
	expect string
	err    uint16
}

func testModifyColumnTime(c *C, store kv.Storage, tests []testModifyColumnTimeCase) {
	limit := variable.GetDDLErrorCountLimit()

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 3")

	// Set time zone to UTC.
	originalTz := tk.Se.GetSessionVars().TimeZone
	tk.Se.GetSessionVars().TimeZone = time.UTC
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", limit))
		tk.Se.GetSessionVars().TimeZone = originalTz
	}()

	for _, t := range tests {
		tk.MustExec("drop table if exists t_mc")
		tk.MustExec(fmt.Sprintf("create table t_mc(a %s)", t.from))
		tk.MustExec(fmt.Sprintf(`insert into t_mc (a) values (%s)`, t.value))
		_, err := tk.Exec(fmt.Sprintf(`alter table t_mc modify a %s`, t.to))
		if t.err != 0 {
			c.Assert(err, NotNil, Commentf("%+v", t))
			c.Assert(err, ErrorMatches, fmt.Sprintf(".*[ddl:%d].*", t.err), Commentf("%+v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%+v", t))

		rs, err := tk.Exec("select a from t_mc")
		c.Assert(err, IsNil, Commentf("%+v", t))

		tk.ResultSetToResult(rs, Commentf("%+v", t)).Check(testkit.Rows(t.expect))
	}
}

func (s *testSerialDBSuite) TestSetTableFlashReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	s.mustExec(tk, c, "drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int)")
	defer s.mustExec(tk, c, "drop table t_flash;")

	t := s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	tk.MustExec("alter table t_flash set tiflash replica 0")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	// Test set tiflash replica for partition table.
	s.mustExec(tk, c, "drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int) partition by hash(a) partitions 3")
	tk.MustExec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	// Use table ID as physical ID, mock for partition feature was not enabled.
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t.Meta().ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(len(t.Meta().TiFlashReplica.AvailablePartitionIDs), Equals, 0)

	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, t.Meta().ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)

	// Mock for partition 0 replica was available.
	partition := t.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 3)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	// Mock for partition 0 replica become unavailable.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Mock for partition 0, 1,2 replica was available.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[2].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID, partition.Definitions[2].ID})

	// Mock for partition 1 replica was unavailable.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, partition.Definitions[1].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetTable(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[2].ID})

	// Test for update table replica with unknown table ID.
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, math.MaxInt64, false)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:1146]Table which ID = 9223372036854775807 does not exist.")

	// Test for FindTableByPartitionID.
	is := domain.GetDomain(tk.Se).InfoSchema()
	t, dbInfo, _ := is.FindTableByPartitionID(partition.Definitions[0].ID)
	c.Assert(t, NotNil)
	c.Assert(dbInfo, NotNil)
	c.Assert(t.Meta().Name.L, Equals, "t_flash")
	t, dbInfo, _ = is.FindTableByPartitionID(t.Meta().ID)
	c.Assert(t, IsNil)
	c.Assert(dbInfo, IsNil)
	err = failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
	c.Assert(err, IsNil)

	// Test for set replica count more than the tiflash store count.
	s.mustExec(tk, c, "drop table if exists t_flash;")
	tk.MustExec("create table t_flash(a int, b int)")
	_, err = tk.Exec("alter table t_flash set tiflash replica 2 location labels 'a','b';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "the tiflash replica count: 2 should be less than the total tiflash server count: 0")
}

func (s *testSerialDBSuite) TestForbitCacheTableForSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			_, err := tk.Exec(fmt.Sprintf("alter table `%s` cache", one))
			if db == "MySQL" {
				c.Assert(err.Error(), Equals, "[ddl:8200]ALTER table cache for tables in system database is currently unsupported")
			} else {
				c.Assert(err.Error(), Equals, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}

		}
		sysTables = sysTables[:0]
	}
}

func (s *testSerialDBSuite) TestSetTableFlashReplicaForSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			_, err := tk.Exec(fmt.Sprintf("alter table `%s` set tiflash replica 1", one))
			if db == "MySQL" {
				c.Assert(err.Error(), Equals, "[ddl:8200]ALTER table replica for tables in system database is currently unsupported")
			} else {
				c.Assert(err.Error(), Equals, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}

		}
		sysTables = sysTables[:0]
	}
}

func (s *testSerialDBSuite) TestSetTiFlashReplicaForTemporaryTable(c *C) {
	// test for tiflash replica
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists temp, temp2")
	tk.MustExec("drop table if exists temp")
	tk.MustExec("create global temporary table temp(id int) on commit delete rows")
	tk.MustExec("create temporary table temp2(id int)")
	tk.MustGetErrCode("alter table temp set tiflash replica 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("alter table temp2 set tiflash replica 1", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("drop table temp, temp2")

	tk.MustExec("drop table if exists normal")
	tk.MustExec("create table normal(id int)")
	defer tk.MustExec("drop table normal")
	tk.MustExec("alter table normal set tiflash replica 1")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='normal'").Check(testkit.Rows("1"))
	tk.MustExec("create global temporary table temp like normal on commit delete rows")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
	tk.MustExec("drop table temp")
	tk.MustExec("create temporary table temp like normal")
	tk.MustQuery("select REPLICA_COUNT from information_schema.tiflash_replica where table_schema='test' and table_name='temp'").Check(testkit.Rows())
}

func (s *testSerialDBSuite) TestAlterShardRowIDBits(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	// Test alter shard_row_id_bits
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 5")
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustExec("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	_, err := tk.Exec("alter table t1 SHARD_ROW_ID_BITS = 10;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID 72057594037932936 overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := testGetTableByName(c, tk.Se, "test", "t1")
		c.Assert(tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits, IsTrue)
		c.Assert(tbl.Meta().ShardRowIDBits == shardRowIDBits, IsTrue)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 10")
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	_, err = tk.Exec("insert into t1 set a=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]Failed to read auto-increment value from storage engine")
}

func (s *testSerialDBSuite) TestShardRowIDBitsOnTemporaryTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// for global temporary table
	tk.MustExec("drop table if exists shard_row_id_temporary")
	_, err := tk.Exec("create global temporary table shard_row_id_temporary (a int) shard_row_id_bits = 5 on commit delete rows;")
	c.Assert(err.Error(), Equals, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create global temporary table shard_row_id_temporary (a int) on commit delete rows;")
	defer tk.MustExec("drop table if exists shard_row_id_temporary")
	_, err = tk.Exec("alter table shard_row_id_temporary shard_row_id_bits = 4;")
	c.Assert(err.Error(), Equals, ddl.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	// for local temporary table
	tk.MustExec("drop table if exists local_shard_row_id_temporary")
	_, err = tk.Exec("create temporary table local_shard_row_id_temporary (a int) shard_row_id_bits = 5;")
	c.Assert(err.Error(), Equals, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error())
	tk.MustExec("create temporary table local_shard_row_id_temporary (a int);")
	defer tk.MustExec("drop table if exists local_shard_row_id_temporary")
	_, err = tk.Exec("alter table local_shard_row_id_temporary shard_row_id_bits = 4;")
	c.Assert(err.Error(), Equals, ddl.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE").Error())
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/124c7ab1d6f914637521fd4463a993aa73403513/mysql-test/t/lock.test
func (s *testDBSuite2) TestLock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	/* Testing of table locking */
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 (  `id` int(11) NOT NULL default '0', `id2` int(11) NOT NULL default '0', `id3` int(11) NOT NULL default '0', `dummy1` char(30) default NULL, PRIMARY KEY  (`id`,`id2`), KEY `index_id3` (`id3`))")
	tk.MustExec("insert into t1 (id,id2) values (1,1),(1,2),(1,3)")
	tk.MustExec("LOCK TABLE t1 WRITE")
	tk.MustExec("select dummy1,count(distinct id) from t1 group by dummy1")
	tk.MustExec("update t1 set id=-1 where id=1")
	tk.MustExec("LOCK TABLE t1 READ")
	_, err := tk.Exec("update t1 set id=1 where id=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite), IsTrue)
	tk.MustExec("unlock tables")
	tk.MustExec("update t1 set id=1 where id=-1")
	tk.MustExec("drop table t1")
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/mysql-test/t/tablelock.test
func (s *testDBSuite2) TestTableLock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")

	/* Test of lock tables */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustExec("lock tables t1 write")
	tk.MustExec("insert into t1 values(NULL)")
	tk.MustExec("unlock tables")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)

	tk.MustExec("lock tables t1 write")
	tk.MustExec("insert into t1 values(NULL)")
	tk.MustExec("unlock tables")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)

	tk.MustExec("drop table if exists t1")

	/* Test of locking and delete of files */
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t1,t2")

	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t2,t1")
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/mysql-test/t/lock_tables_lost_commit.test
func (s *testDBSuite2) TestTableLocksLostCommit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1(a INT)")
	tk.MustExec("LOCK TABLES t1 WRITE")
	tk.MustExec("INSERT INTO t1 VALUES(10)")

	_, err := tk2.Exec("SELECT * FROM t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	tk.Se.Close()

	tk2.MustExec("SELECT * FROM t1")
	tk2.MustExec("DROP TABLE t1")

	tk.MustExec("unlock tables")
}

// test write local lock
func (s *testDBSuite2) TestWriteLocal(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 ( n int auto_increment primary key)")

	// Test: allow read
	tk.MustExec("lock tables t1 write local")
	tk.MustExec("insert into t1 values(NULL)")
	tk2.MustQuery("select count(*) from t1")
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test: forbid write
	tk.MustExec("lock tables t1 write local")
	_, err := tk2.Exec("insert into t1 values(NULL)")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write local first
	tk.MustExec("lock tables t1 write local")
	_, err = tk2.Exec("lock tables t1 write local")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("lock tables t1 read")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write first
	tk.MustExec("lock tables t1 write")
	_, err = tk2.Exec("lock tables t1 write local")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock read first
	tk.MustExec("lock tables t1 read")
	_, err = tk2.Exec("lock tables t1 write local")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func (s *testSerialDBSuite) TestSkipSchemaChecker(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	// Test skip schema checker for ActionSetTiFlashReplica.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 set tiflash replica 2 location labels 'a','b';")
	tk.MustExec("commit")

	// Test skip schema checker for ActionUpdateTiFlashReplicaStatus.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tb := testGetTableByName(c, tk.Se, "test", "t1")
	err := domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("commit")

	// Test can't skip schema checker.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 add column b int;")
	_, err = tk.Exec("commit")
	c.Assert(terror.ErrorEqual(domain.ErrInfoSchemaChanged, err), IsTrue)
}

// See issue: https://github.com/pingcap/tidb/issues/29752
// Ref https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
func (s *testDBSuite2) TestRenameTableWithLocked(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database renamedb")
	tk.MustExec("create database renamedb2")
	tk.MustExec("use renamedb")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (a int);")

	tk.MustExec("LOCK TABLES t1 WRITE;")
	tk.MustGetErrCode("drop database renamedb2;", errno.ErrLockOrActiveTransaction)
	tk.MustExec("RENAME TABLE t1 TO t2;")
	tk.MustQuery("select * from renamedb.t2").Check(testkit.Rows())
	tk.MustExec("UNLOCK TABLES")
	tk.MustExec("RENAME TABLE t2 TO t1;")
	tk.MustQuery("select * from renamedb.t1").Check(testkit.Rows())

	tk.MustExec("LOCK TABLES t1 READ;")
	tk.MustGetErrCode("RENAME TABLE t1 TO t2;", errno.ErrTableNotLockedForWrite)
	tk.MustExec("UNLOCK TABLES")

	tk.MustExec("drop database renamedb")
}

func (s *testDBSuite2) TestLockTables(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")

	// Test lock 1 table.
	tk.MustExec("lock tables t1 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)
	tk.MustExec("lock tables t1 read")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockRead)
	tk.MustExec("lock tables t1 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)

	// Test lock multi tables.
	tk.MustExec("lock tables t1 write, t2 read")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockRead)
	tk.MustExec("lock tables t1 read, t2 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockRead)
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockWrite)
	tk.MustExec("lock tables t2 write")
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockWrite)
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)
	tk.MustExec("lock tables t1 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockNone)

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	// Test read lock.
	tk.MustExec("lock tables t1 read")
	tk.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	_, err := tk.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite), IsTrue)
	_, err = tk.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite), IsTrue)
	_, err = tk.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite), IsTrue)

	_, err = tk2.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk2.MustExec("lock tables t1 read")
	_, err = tk2.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite), IsTrue)

	// Test write lock.
	_, err = tk.Exec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk2.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	_, err = tk2.Exec("select * from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	// Test write local lock.
	tk.MustExec("lock tables t1 write local")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	tk2.MustQuery("select * from t1")
	_, err = tk2.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("lock tables t1 read")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	// Test none unique table.
	_, err = tk.Exec("lock tables t1 read, t1 write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrNonuniqTable), IsTrue)

	// Test lock table by other session in transaction and commit without retry.
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("set @@session.tidb_disable_txn_auto_retry=1")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustExec("lock tables t1 write")
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "previous statement: insert into t1 set a=1: [domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")

	// Test lock table by other session in transaction and commit with retry.
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("set @@session.tidb_disable_txn_auto_retry=0")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustExec("lock tables t1 write")
	_, err = tk.Exec("commit")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue, Commentf("err: %v\n", err))

	// Test for lock the same table multiple times.
	tk2.MustExec("lock tables t1 write")
	tk2.MustExec("lock tables t1 write, t2 read")

	// Test lock tables and drop tables
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t1")
	tk2.MustExec("create table t1 (a int)")
	tk.MustExec("lock tables t1 write, t2 read")

	// Test lock tables and drop database.
	tk.MustExec("unlock tables")
	tk.MustExec("create database test_lock")
	tk.MustExec("create table test_lock.t3 (a int)")
	tk.MustExec("lock tables t1 write, test_lock.t3 write")
	tk2.MustExec("create table t3 (a int)")
	tk.MustExec("lock tables t1 write, t3 write")
	tk.MustExec("drop table t3")

	// Test lock tables and truncate tables.
	tk.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 read")
	tk.MustExec("truncate table t1")
	tk.MustExec("insert into t1 set a=1")
	_, err = tk2.Exec("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	// Test for lock unsupported schema tables.
	_, err = tk2.Exec("lock tables performance_schema.global_status write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrAccessDenied), IsTrue)
	_, err = tk2.Exec("lock tables information_schema.tables write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrAccessDenied), IsTrue)
	_, err = tk2.Exec("lock tables mysql.db write")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrAccessDenied), IsTrue)

	// Test create table/view when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 read")
	_, err = tk.Exec("create table t3 (a int)")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLocked), IsTrue)
	_, err = tk.Exec("create view v1 as select * from t1;")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotLocked), IsTrue)

	// Test for locking view was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create view v1 as select * from t1;")
	_, err = tk.Exec("lock tables v1 read")
	c.Assert(terror.ErrorEqual(err, table.ErrUnsupportedOp), IsTrue)

	// Test for locking sequence was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create sequence seq")
	_, err = tk.Exec("lock tables seq read")
	c.Assert(terror.ErrorEqual(err, table.ErrUnsupportedOp), IsTrue)
	tk.MustExec("drop sequence seq")

	// Test for create/drop/alter database when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write")
	_, err = tk.Exec("drop database test")
	c.Assert(terror.ErrorEqual(err, table.ErrLockOrActiveTransaction), IsTrue)
	_, err = tk.Exec("create database test_lock")
	c.Assert(terror.ErrorEqual(err, table.ErrLockOrActiveTransaction), IsTrue)
	_, err = tk.Exec("alter database test charset='utf8mb4'")
	c.Assert(terror.ErrorEqual(err, table.ErrLockOrActiveTransaction), IsTrue)
	// Test alter/drop database when other session is holding the table locks of the database.
	tk2.MustExec("create database test_lock2")
	_, err = tk2.Exec("drop database test")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("alter database test charset='utf8mb4'")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	// Test for admin cleanup table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write, t2 write")
	_, err = tk2.Exec("lock tables t1 write, t2 read")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk2.MustExec("admin cleanup table lock t1,t2")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockNone)
	// cleanup unlocked table.
	tk2.MustExec("admin cleanup table lock t1,t2")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)
	checkTableLock(c, tk.Se, "test", "t2", model.TableLockNone)
	tk2.MustExec("lock tables t1 write, t2 read")
	checkTableLock(c, tk2.Se, "test", "t1", model.TableLockWrite)
	checkTableLock(c, tk2.Se, "test", "t2", model.TableLockRead)

	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func (s *testDBSuite2) TestTablesLockDelayClean(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")

	tk.MustExec("lock tables t1 write")
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DelayCleanTableLock = 100
	})
	var wg sync.WaitGroup
	wg.Add(1)
	var startTime time.Time
	go func() {
		startTime = time.Now()
		tk.Se.Close()
		wg.Done()
	}()
	time.Sleep(50 * time.Millisecond)
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockWrite)
	wg.Wait()
	c.Assert(time.Since(startTime).Seconds() > 0.1, IsTrue)
	checkTableLock(c, tk.Se, "test", "t1", model.TableLockNone)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DelayCleanTableLock = 0
	})
}

// TestConcurrentLockTables test concurrent lock/unlock tables.
func (s *testDBSuite4) TestConcurrentLockTables(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2.MustExec("use test")

	// Test concurrent lock tables read.
	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	})
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write.
	sql1 = "lock tables t1 write"
	sql2 = "lock tables t1 write"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, infoschema.ErrTableLocked), IsTrue)
	})
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write local.
	sql1 = "lock tables t1 write local"
	sql2 = "lock tables t1 write local"
	s.testParallelExecSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, infoschema.ErrTableLocked), IsTrue)
	})

	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func (s *testDBSuite4) TestLockTableReadOnly(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer func() {
		tk.MustExec("alter table t1 read write")
		tk.MustExec("alter table t2 read write")
		tk.MustExec("drop table if exists t1,t2")
	}()
	tk.MustExec("create table t1 (a int key, b int)")
	tk.MustExec("create table t2 (a int key)")

	tk.MustExec("alter table t1 read only")
	tk.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	_, err := tk.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)

	_, err = tk2.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("update t1 set a=1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	_, err = tk2.Exec("delete from t1")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk2.MustExec("alter table t1 read only")
	_, err = tk2.Exec("insert into t1 set a=1, b=2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("alter table t1 read write")

	tk.MustExec("lock tables t1 read")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("lock tables t1 write")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("lock tables t1 write local")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("unlock tables")

	tk.MustExec("alter table t1 read only")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked), IsTrue)
	c.Assert(terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked), IsTrue)
	tk.MustExec("admin cleanup table lock t1")
	tk2.MustExec("insert into t1 set a=1, b=2")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk2.MustExec("update t1 set b = 3")
	tk2.MustExec("alter table t1 read only")
	tk2.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 3"))
	tk.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk.MustExec("update t1 set b = 4")
	c.Assert(terror.ErrorEqual(tk.ExecToErr("commit"), domain.ErrInfoSchemaChanged), IsTrue)
	tk2.MustExec("alter table t1 read write")
}

type checkRet func(c *C, err1, err2 error)

func (s *testDBSuite4) testParallelExecSQL(c *C, sql1, sql2 string, se1, se2 session.Session, f checkRet) {
	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobRunBeforeExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err1 := admin.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)

	var wg util.WaitGroupWrapper
	var err1 error
	var err2 error
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err3 := admin.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Run(func() {
		_, err1 = se1.Execute(context.Background(), sql1)
	})
	wg.Run(func() {
		<-ch
		_, err2 = se2.Execute(context.Background(), sql2)
	})

	wg.Wait()
	f(c, err1, err2)
}

func checkTableLock(c *C, se session.Session, dbName, tableName string, lockTp model.TableLockType) {
	tb := testGetTableByName(c, se, dbName, tableName)
	dom := domain.GetDomain(se)
	err := dom.Reload()
	c.Assert(err, IsNil)
	if lockTp != model.TableLockNone {
		c.Assert(tb.Meta().Lock, NotNil)
		c.Assert(tb.Meta().Lock.Tp, Equals, lockTp)
		c.Assert(tb.Meta().Lock.State, Equals, model.TableLockStatePublic)
		c.Assert(len(tb.Meta().Lock.Sessions) == 1, IsTrue)
		c.Assert(tb.Meta().Lock.Sessions[0].ServerID, Equals, dom.DDL().GetID())
		c.Assert(tb.Meta().Lock.Sessions[0].SessionID, Equals, se.GetSessionVars().ConnectionID)
	} else {
		c.Assert(tb.Meta().Lock, IsNil)
	}
}

func (s *testDBSuite2) TestDDLWithInvalidTableInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	// Test create with invalid expression.
	_, err := tk.Exec(`CREATE TABLE t (
		c0 int(11) ,
  		c1 int(11),
    	c2 decimal(16,4) GENERATED ALWAYS AS ((case when (c0 = 0) then 0when (c0 > 0) then (c1 / c0) end))
	);`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 4 column 88 near \"then (c1 / c0) end))\n\t);\" ")

	tk.MustExec("create table t (a bigint, b int, c int generated always as (b+1)) partition by hash(a) partitions 4;")
	// Test drop partition column.
	_, err = tk.Exec("alter table t drop column a;")
	c.Assert(err, NotNil)
	// TODO: refine the error message to compatible with MySQL
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'expression'")
	// Test modify column with invalid expression.
	_, err = tk.Exec("alter table t modify column c int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 97 near \"then (b / a) end));\" ")
	// Test add column with invalid expression.
	_, err = tk.Exec("alter table t add column d int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 94 near \"then (b / a) end));\" ")
}

func (s *testDBSuite4) TestColumnCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists column_check")
	tk.MustExec("create table column_check (pk int primary key, a int check (a > 1))")
	defer tk.MustExec("drop table if exists column_check")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|CONSTRAINT CHECK is not supported"))
}

func (s *testDBSuite5) TestAlterCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists alter_check")
	tk.MustExec("create table alter_check (pk int primary key)")
	defer tk.MustExec("drop table if exists alter_check")
	tk.MustExec("alter table alter_check alter check crcn ENFORCED")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|ALTER CHECK is not supported"))
}

func (s *testDBSuite6) TestDropCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists drop_check")
	tk.MustExec("create table drop_check (pk int primary key)")
	defer tk.MustExec("drop table if exists drop_check")
	tk.MustExec("alter table drop_check drop check crcn")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|DROP CHECK is not supported"))
}

func (s *testDBSuite7) TestAddConstraintCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists add_constraint_check")
	tk.MustExec("create table add_constraint_check (pk int primary key, a int)")
	defer tk.MustExec("drop table if exists add_constraint_check")
	tk.MustExec("alter table add_constraint_check add constraint crn check (a > 1)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|ADD CONSTRAINT CHECK is not supported"))
}

func (s *testDBSuite7) TestCreateTableIngoreCheckConstraint(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("drop table if exists table_constraint_check")
	tk.MustExec("CREATE TABLE admin_user (enable bool, CHECK (enable IN (0, 1)));")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8231|CONSTRAINT CHECK is not supported"))
	tk.MustQuery("show create table admin_user").Check(testutil.RowsWithSep("|", ""+
		"admin_user CREATE TABLE `admin_user` (\n"+
		"  `enable` tinyint(1) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testDBSuite6) TestAlterOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use " + s.schemaName)
	tk.MustExec("create table ob (pk int primary key, c int default 1, c1 int default 1, KEY cl(c1))")

	// Test order by with primary key
	tk.MustExec("alter table ob order by c")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1105|ORDER BY ignored as there is a user-defined clustered index in the table 'ob'"))

	// Test order by with no primary key
	tk.MustExec("drop table if exists ob")
	tk.MustExec("create table ob (c int default 1, c1 int default 1, KEY cl(c1))")
	tk.MustExec("alter table ob order by c")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustExec("drop table if exists ob")
}

func (s *testSerialDBSuite) TestDDLJobErrorCount(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddl_error_table, new_ddl_error_table")
	tk.MustExec("create table ddl_error_table(a int)")
	is := s.dom.InfoSchema()
	schemaName := model.NewCIStr("test")
	tableName := model.NewCIStr("ddl_error_table")
	schema, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.TableByName(schemaName, tableName)
	c.Assert(err, IsNil)

	newTableName := model.NewCIStr("new_ddl_error_table")
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{schema.ID, newTableName},
	}

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockErrEntrySizeTooLarge"), IsNil)
	}()

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	ticker := time.NewTicker(s.lease)
	defer ticker.Stop()
	for range ticker.C {
		historyJob, err := getHistoryDDLJob(s.store, job.ID)
		c.Assert(err, IsNil)
		if historyJob == nil {
			continue
		}
		c.Assert(historyJob.ErrorCount, Equals, int64(1), Commentf("%v", historyJob))
		kv.ErrEntryTooLarge.Equal(historyJob.Error)
		break
	}
}

func (s *testDBSuite1) TestAlterTableWithValidation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec("create table t1 (c1 int, c2 int as (c1 + 1));")

	// Test for alter table with validation.
	tk.MustExec("alter table t1 with validation")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8200|ALTER TABLE WITH VALIDATION is currently unsupported"))

	// Test for alter table without validation.
	tk.MustExec("alter table t1 without validation")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|8200|ALTER TABLE WITHOUT VALIDATION is currently unsupported"))
}

func (s *testSerialDBSuite) TestCommitTxnWithIndexChange(c *C) {
	// Prepare work.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
	tk.MustExec("insert t1 values (1, 10, 100), (2, 20, 200)")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")

	// tkSQLs are the sql statements for the pessimistic transaction.
	// tk2DDL are the ddl statements executed before the pessimistic transaction.
	// idxDDL is the DDL statement executed between pessimistic transaction begin and commit.
	// failCommit means the pessimistic transaction commit should fail not.
	type caseUnit struct {
		tkSQLs     []string
		tk2DDL     []string
		idxDDL     string
		checkSQLs  []string
		rowsExps   [][]string
		failCommit bool
		stateEnd   model.SchemaState
	}

	cases := []caseUnit{
		// Test secondary index
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t2 values(11, 11, 11)"},
			[]string{"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2",
				"alter table t1 add index kk2(c2, c1)",
				"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2"},
			"alter table t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200", "3 30 300"},
				{"11 11 11"}},
			false,
			model.StateNone},
		// Test secondary index
		{[]string{"insert into t2 values(5, 50, 500)",
			"insert into t2 values(11, 11, 11)",
			"delete from t2 where c2 = 11",
			"update t2 set c2 = 110 where c1 = 11"},
			// "update t2 set c1 = 10 where c3 = 100"},
			[]string{"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2",
				"alter table t1 add index kk2(c2, c1)",
				"alter table t1 add index k2(c2)",
				"alter table t1 drop index k2"},
			"alter table t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11",
				"select * from t2 where c3 = 100"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200"},
				{},
				{"1 10 100"}},
			false,
			model.StateNone},
		// Test unique index
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 400)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 12, 11)"},
			[]string{"alter table t1 add unique index uk3(c3)",
				"alter table t1 drop index uk3",
				"alter table t2 add unique index ukc1c3(c1, c3)",
				"alter table t2 add unique index ukc3(c3)",
				"alter table t2 drop index ukc1c3",
				"alter table t2 drop index ukc3",
				"alter table t2 add index kc3(c3)"},
			"alter table t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 use index(uk3) where c3 = 400",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{"300 30"},
				{"400 40"},
				{"1 10 100", "2 20 200", "3 30 300", "4 40 400"},
				{"1 10 100", "2 20 200", "11 11 11", "12 12 11"}},
			false, model.StateNone},
		// Test unique index fail to commit, this case needs the new index could be inserted
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 300)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 11, 12)"},
			//[]string{"alter table t1 add unique index uk3(c3)", "alter table t1 drop index uk3"},
			[]string{},
			"alter table t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 where c1 = 4",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{},
				{},
				{"1 10 100", "2 20 200"},
				{"1 10 100", "2 20 200"}},
			true,
			model.StateWriteOnly},
	}
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))

	// Test add index state change
	do := s.dom.DDL()
	startStates := []model.SchemaState{model.StateNone, model.StateDeleteOnly}
	for _, startState := range startStates {
		endStatMap := session.ConstOpAddIndex[startState]
		var endStates []model.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			for _, curCase := range cases {
				if endState < curCase.stateEnd {
					break
				}
				tk2.MustExec("drop table if exists t1")
				tk2.MustExec("drop table if exists t2")
				tk2.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustExec("create table t2 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustExec("insert t1 values (1, 10, 100), (2, 20, 200)")
				tk2.MustExec("insert t2 values (1, 10, 100), (2, 20, 200)")
				tk2.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t2;").Check(testkit.Rows("1 10 100", "2 20 200"))

				for _, DDLSQL := range curCase.tk2DDL {
					tk2.MustExec(DDLSQL)
				}
				hook := &ddl.TestDDLCallback{}
				prepared := false
				committed := false
				hook.OnJobUpdatedExported = func(job *model.Job) {
					if job.SchemaState == startState {
						if !prepared {
							tk.MustExec("begin pessimistic")
							for _, tkSQL := range curCase.tkSQLs {
								tk.MustExec(tkSQL)
							}
							prepared = true
						}
					} else if job.SchemaState == endState {
						if !committed {
							if curCase.failCommit {
								_, err := tk.Exec("commit")
								c.Assert(err, NotNil)
							} else {
								tk.MustExec("commit")
							}
						}
						committed = true
					}
				}
				originalCallback := do.GetHook()
				do.(ddl.DDLForTest).SetHook(hook)
				tk2.MustExec(curCase.idxDDL)
				do.(ddl.DDLForTest).SetHook(originalCallback)
				tk2.MustExec("admin check table t1")
				for i, checkSQL := range curCase.checkSQLs {
					if len(curCase.rowsExps[i]) > 0 {
						tk2.MustQuery(checkSQL).Check(testkit.Rows(curCase.rowsExps[i]...))
					} else {
						tk2.MustQuery(checkSQL).Check(nil)
					}
				}
			}
		}
	}
	tk.MustExec("admin check table t1")
}

// TestAddIndexFailOnCaseWhenCanExit is used to close #19325.
func (s *testSerialDBSuite) TestAddIndexFailOnCaseWhenCanExit(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	originalVal := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", originalVal))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	_, err := tk.Exec("alter table t add index idx(b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]DDL job rollback, error msg: job.ErrCount:1, mock unknown type: ast.whenClause.")
	tk.MustExec("drop table if exists t")
}

func init() {
	// Make sure it will only be executed once.
	domain.SchemaOutOfDateRetryInterval.Store(50 * time.Millisecond)
	domain.SchemaOutOfDateRetryTimes.Store(50)
}

func (s *testSerialDBSuite) TestCreateTableWithIntegerLengthWaring(c *C) {
	// Inject the strict-integer-display-width variable in parser directly.
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() {
		parsertypes.TiDBStrictIntegerDisplayWidth = false
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a tinyint(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a smallint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a mediumint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a integer(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int1(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int2(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int3(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int4(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int8(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
}

func (s *testSerialDBSuite) TestColumnTypeChangeGenUniqueChangingName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	hook := &ddl.TestDDLCallback{}
	var checkErr error
	assertChangingColName := "_col$_c2_0"
	assertChangingIdxName := "_idx$_idx_0"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if changingCol.Name.L != assertChangingColName {
				checkErr = errors.New("changing column name is incorrect")
			} else if changingIdxs[0].Name.L != assertChangingIdxName {
				checkErr = errors.New("changing index name is incorrect")
			}
		}
	}
	d := s.dom.DDL()
	originHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	defer d.(ddl.DDLForTest).SetHook(originHook)

	tk.MustExec("create table if not exists t(c1 varchar(256), c2 bigint, `_col$_c2` varchar(10), unique _idx$_idx(c1), unique idx(c2));")
	tk.MustExec("alter table test.t change column c2 cC2 tinyint after `_col$_c2`")
	c.Assert(checkErr, IsNil)

	t := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(t.Meta().Columns), Equals, 3)
	c.Assert(t.Meta().Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Columns[0].Offset, Equals, 0)
	c.Assert(t.Meta().Columns[1].Name.O, Equals, "_col$_c2")
	c.Assert(t.Meta().Columns[1].Offset, Equals, 1)
	c.Assert(t.Meta().Columns[2].Name.O, Equals, "cC2")
	c.Assert(t.Meta().Columns[2].Offset, Equals, 2)

	c.Assert(len(t.Meta().Indices), Equals, 2)
	c.Assert(t.Meta().Indices[0].Name.O, Equals, "_idx$_idx")
	c.Assert(t.Meta().Indices[1].Name.O, Equals, "idx")

	c.Assert(len(t.Meta().Indices[0].Columns), Equals, 1)
	c.Assert(t.Meta().Indices[0].Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Indices[0].Columns[0].Offset, Equals, 0)

	c.Assert(len(t.Meta().Indices[1].Columns), Equals, 1)
	c.Assert(t.Meta().Indices[1].Columns[0].Name.O, Equals, "cC2")
	c.Assert(t.Meta().Indices[1].Columns[0].Offset, Equals, 2)

	assertChangingColName1 := "_col$__col$_c1_1"
	assertChangingColName2 := "_col$__col$__col$_c1_0_1"
	query1 := "alter table t modify column _col$_c1 tinyint"
	query2 := "alter table t modify column _col$__col$_c1_0 tinyint"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if (job.Query == query1 || job.Query == query2) && job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if job.Query == query1 && changingCol.Name.L != assertChangingColName1 {
				checkErr = errors.New("changing column name is incorrect")
			}
			if job.Query == query2 && changingCol.Name.L != assertChangingColName2 {
				checkErr = errors.New("changing column name is incorrect")
			}
		}
	}
	d.(ddl.DDLForTest).SetHook(hook)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table if not exists t(c1 bigint, _col$_c1 bigint, _col$__col$_c1_0 bigint, _col$__col$__col$_c1_0_0 bigint)")
	tk.MustExec("alter table t modify column c1 tinyint")
	tk.MustExec("alter table t modify column _col$_c1 tinyint")
	c.Assert(checkErr, IsNil)
	tk.MustExec("alter table t modify column _col$__col$_c1_0 tinyint")
	c.Assert(checkErr, IsNil)
	tk.MustExec("alter table t change column _col$__col$__col$_c1_0_0  _col$__col$__col$_c1_0_0 tinyint")

	t = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(t.Meta().Columns), Equals, 4)
	c.Assert(t.Meta().Columns[0].Name.O, Equals, "c1")
	c.Assert(t.Meta().Columns[0].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[0].Offset, Equals, 0)
	c.Assert(t.Meta().Columns[1].Name.O, Equals, "_col$_c1")
	c.Assert(t.Meta().Columns[1].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[1].Offset, Equals, 1)
	c.Assert(t.Meta().Columns[2].Name.O, Equals, "_col$__col$_c1_0")
	c.Assert(t.Meta().Columns[2].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[2].Offset, Equals, 2)
	c.Assert(t.Meta().Columns[3].Name.O, Equals, "_col$__col$__col$_c1_0_0")
	c.Assert(t.Meta().Columns[3].Tp, Equals, mysql.TypeTiny)
	c.Assert(t.Meta().Columns[3].Offset, Equals, 3)

	tk.MustExec("drop table if exists t")
}

func (s *testSerialDBSuite) TestModifyColumnTypeWithWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// Test normal warnings.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(5,2))")
	tk.MustExec("insert into t values(111.22),(111.22),(111.22),(111.22),(333.4)")
	// 111.22 will be truncated the fraction .22 as .2 with truncated warning for each row.
	tk.MustExec("alter table t modify column a decimal(4,1)")
	// there should 4 rows of warnings corresponding to the origin rows.
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '111.22'",
		"Warning 1292 Truncated incorrect DECIMAL value: '111.22'",
		"Warning 1292 Truncated incorrect DECIMAL value: '111.22'",
		"Warning 1292 Truncated incorrect DECIMAL value: '111.22'"))

	// Test the strict warnings is treated as errors under the strict mode.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(5,2))")
	tk.MustExec("insert into t values(111.22),(111.22),(111.22),(33.4)")
	// Since modify column a from decimal(5,2) to decimal(3,1), the first three rows with 111.22 will overflows the target types.
	_, err := tk.Exec("alter table t modify column a decimal(3,1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]DECIMAL value is out of range in '(3, 1)'")

	// Test the strict warnings is treated as warnings under the non-strict mode.
	tk.MustExec("set @@sql_mode=\"\"")
	tk.MustExec("alter table t modify column a decimal(3,1)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 DECIMAL value is out of range in '(3, 1)'",
		"Warning 1690 DECIMAL value is out of range in '(3, 1)'",
		"Warning 1690 DECIMAL value is out of range in '(3, 1)'"))
}

// TestModifyColumnTypeWhenInterception is to test modifying column type with warnings intercepted by
// reorg timeout, not owner error and so on.
func (s *testSerialDBSuite) TestModifyColumnTypeWhenInterception(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// Test normal warnings.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b decimal(4,2))")

	count := defaultBatchSize * 4
	// Add some rows.
	dml := "insert into t values"
	for i := 1; i <= count; i++ {
		dml += fmt.Sprintf("(%d, %f)", i, 11.22)
		if i != count {
			dml += ","
		}
	}
	tk.MustExec(dml)
	// Make the regions scale like: [1, 1024), [1024, 2048), [2048, 3072), [3072, 4096]
	tk.MustQuery("split table t between(0) and (4096) regions 4")

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{}
	var checkMiddleWarningCount bool
	var checkMiddleAddedCount bool
	// Since the `DefTiDBDDLReorgWorkerCount` is 4, every worker will be assigned with one region
	// for the first time. Here we mock the insert failure/reorg timeout in region [2048, 3072)
	// which will lead next handle be set to 2048 and partial warnings be stored into ddl job.
	// Since the existence of reorg batch size, only the last reorg batch [2816, 3072) of kv
	// range [2048, 3072) fail to commit, the rest of them all committed successfully. So the
	// addedCount and warnings count in the job are all equal to `4096 - reorg batch size`.
	// In the next round of this ddl job, the last reorg batch will be finished.
	var middleWarningsCount = int64(defaultBatchSize*4 - defaultReorgBatchSize)
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteReorganization || job.SnapshotVer != 0 {
			if len(job.ReorgMeta.WarningsCount) == len(job.ReorgMeta.Warnings) {
				for _, v := range job.ReorgMeta.WarningsCount {
					if v == middleWarningsCount {
						checkMiddleWarningCount = true
					}
				}
			}
			if job.RowCount == middleWarningsCount {
				checkMiddleAddedCount = true
			}
		}
	}
	originHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	defer d.(ddl.DDLForTest).SetHook(originHook)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/MockReorgTimeoutInOneRegion", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/MockReorgTimeoutInOneRegion"), IsNil)
	}()
	tk.MustExec("alter table t modify column b decimal(3,1)")
	c.Assert(checkMiddleWarningCount, Equals, true)
	c.Assert(checkMiddleAddedCount, Equals, true)
	res := tk.MustQuery("show warnings")
	c.Assert(len(res.Rows()), Equals, count)
}

func (s *testDBSuite4) TestGeneratedColumnWindowFunction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustGetErrCode("CREATE TABLE t (a INT , b INT as (ROW_NUMBER() OVER (ORDER BY a)))", errno.ErrWindowInvalidWindowFuncUse)
	tk.MustGetErrCode("CREATE TABLE t (a INT , index idx ((ROW_NUMBER() OVER (ORDER BY a))))", errno.ErrWindowInvalidWindowFuncUse)
}

func (s *testDBSuite4) TestAnonymousIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("create table t(bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb int, b int);")
	tk.MustExec("alter table t add index bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb(b);")
	tk.MustExec("alter table t add index (bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);")
	res := tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';")
	c.Assert(len(res.Rows()), Equals, 1)
	res = tk.MustQuery("show index from t where key_name='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb_2';")
	c.Assert(len(res.Rows()), Equals, 1)
}

func (s *testDBSuite4) TestUnsupportedAlterTableOption(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("create table t(a char(10) not null,b char(20)) shard_row_id_bits=6;")
	tk.MustGetErrCode("alter table t pre_split_regions=6;", errno.ErrUnsupportedDDLOperation)
}

func (s *testDBSuite4) TestCreateTableWithDecimalWithDoubleZero(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	checkType := func(db, table, field string) {
		ctx := tk.Se.(sessionctx.Context)
		is := domain.GetDomain(ctx).InfoSchema()
		tableInfo, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
		c.Assert(err, IsNil)
		tblInfo := tableInfo.Meta()
		for _, col := range tblInfo.Columns {
			if col.Name.L == field {
				c.Assert(col.Flen, Equals, 10)
			}
		}
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(d decimal(0, 0))")
	checkType("test", "tt", "d")

	tk.MustExec("drop table tt")
	tk.MustExec("create table tt(a int)")
	tk.MustExec("alter table tt add column d decimal(0, 0)")
	checkType("test", "tt", "d")

	/*
		Currently not support change column to decimal
		tk.MustExec("drop table tt")
		tk.MustExec("create table tt(d int)")
		tk.MustExec("alter table tt change column d d decimal(0, 0)")
		checkType("test", "tt", "d")
	*/
}

func (s *testDBSuite4) TestIssue22207(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("set @@session.tidb_enable_exchange_partition = 1;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1(id char(10)) partition by list columns(id) (partition p0 values in ('a'), partition p1 values in ('b'));")
	tk.MustExec("insert into t1 VALUES('a')")
	tk.MustExec("create table t2(id char(10));")
	tk.MustExec("ALTER TABLE t1 EXCHANGE PARTITION p0 WITH TABLE t2;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("a"))
	c.Assert(len(tk.MustQuery("select * from t1").Rows()), Equals, 0)

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1 (id int) partition by list (id) (partition p0 values in (1,2,3), partition p1 values in (4,5,6));")
	tk.MustExec("insert into t1 VALUES(1);")
	tk.MustExec("insert into t1 VALUES(2);")
	tk.MustExec("insert into t1 VALUES(3);")
	tk.MustExec("create table t2(id int);")
	tk.MustExec("ALTER TABLE t1 EXCHANGE PARTITION p0 WITH TABLE t2;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2", "3"))
	c.Assert(len(tk.MustQuery("select * from t1").Rows()), Equals, 0)
	tk.MustExec("set @@session.tidb_enable_exchange_partition = 0;")
}

func (s *testDBSuite5) TestIssue23473(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t_23473;")
	tk.MustExec("create table t_23473 (k int primary key, v int)")
	tk.MustExec("alter table t_23473 change column k k bigint")
	t := testGetTableByName(c, tk.Se.(sessionctx.Context), "test", "t_23473")
	col1Flag := t.Cols()[0].Flag
	c.Assert(mysql.HasNoDefaultValueFlag(col1Flag), IsTrue)
}

func (s *testSerialDBSuite) TestIssue22819(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test;")
	tk1.MustExec("drop table if exists t1;")
	defer func() {
		tk1.MustExec("drop table if exists t1;")
	}()

	tk1.MustExec("create table t1 (v int) partition by hash (v) partitions 2")
	tk1.MustExec("insert into t1 values (1)")

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test;")
	tk1.MustExec("begin")
	tk1.MustExec("update t1 set v = 2 where v = 1")

	tk2.MustExec("alter table t1 truncate partition p0")

	_, err := tk1.Exec("commit")
	c.Assert(err, ErrorMatches, ".*8028.*Information schema is changed during the execution of the statement.*")
}

func (s *testSerialSuite) TestTruncateAllPartitions(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test;")
	tk1.MustExec("drop table if exists partition_table;")
	defer func() {
		tk1.MustExec("drop table if exists partition_table;")
	}()
	tk1.MustExec("create table partition_table (v int) partition by hash (v) partitions 10;")
	tk1.MustExec("insert into partition_table values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);")
	tk1.MustExec("alter table partition_table truncate partition all;")
	tk1.MustQuery("select count(*) from partition_table;").Check(testkit.Rows("0"))
}

func (s *testSerialSuite) TestIssue23872(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists test_create_table;")
	defer tk.MustExec("drop table if exists test_create_table;")
	tk.MustExec("create table test_create_table(id smallint,id1 int, primary key (id));")
	rs, err := tk.Exec("select * from test_create_table;")
	c.Assert(err, IsNil)
	cols := rs.Fields()
	err = rs.Close()
	c.Assert(err, IsNil)
	expectFlag := uint16(mysql.NotNullFlag | mysql.PriKeyFlag | mysql.NoDefaultValueFlag)
	c.Assert(cols[0].Column.Flag, Equals, uint(expectFlag))
	tk.MustExec("create table t(a int default 1, primary key(a));")
	defer tk.MustExec("drop table if exists t;")
	rs1, err := tk.Exec("select * from t;")
	c.Assert(err, IsNil)
	cols1 := rs1.Fields()
	err = rs1.Close()
	c.Assert(err, IsNil)
	expectFlag1 := uint16(mysql.NotNullFlag | mysql.PriKeyFlag)
	c.Assert(cols1[0].Column.Flag, Equals, uint(expectFlag1))
}

// Close issue #23321.
// See https://github.com/pingcap/tidb/issues/23321
func (s *testSerialDBSuite) TestJsonUnmarshalErrWhenPanicInCancellingPath(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists test_add_index_after_add_col")
	tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0');")
	tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2);")
	tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0';")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit", `return(true)`), IsNil)
	defer c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit"), IsNil)

	_, err := tk.Exec("alter table test_add_index_after_add_col add unique index cc(c);")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '0' for key 'cc'")
}

// Close issue #24172.
// See https://github.com/pingcap/tidb/issues/24172
func (s *testSerialDBSuite) TestCancelJobWriteConflict(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int)")

	var cancelErr error
	var rs []sqlexec.RecordSet
	hook := &ddl.TestDDLCallback{}
	d := s.dom.DDL()
	originalHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	defer d.(ddl.DDLForTest).SetHook(originalHook)

	// Test when cancelling cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("no_retry")`), IsNil)
			defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn"), IsNil) }()
			rs, cancelErr = tk1.Se.Execute(context.Background(), stmt)
		}
	}
	tk.MustExec("alter table t add index (id)")
	c.Assert(cancelErr.Error(), Equals, "mock commit error")

	// Test when cancelling is retried only once and adding index is cancelled in the end.
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn", `return("retry_once")`), IsNil)
			defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/kv/mockCommitErrorInNewTxn"), IsNil) }()
			rs, cancelErr = tk1.Se.Execute(context.Background(), stmt)
		}
	}
	tk.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	c.Assert(cancelErr, IsNil)
	result := tk1.ResultSetToResultWithCtx(context.Background(), rs[0], Commentf("cancel ddl job fails"))
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}

// For Close issue #24288
// see https://github.com/pingcap/tidb/issues/24288
func (s *testDBSuite8) TestDdlMaxLimitOfIdentifier(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// create/drop database test
	longDbName := strings.Repeat("库", mysql.MaxDatabaseNameLength-1)
	tk.MustExec(fmt.Sprintf("create database %s", longDbName))
	defer func() {
		tk.MustExec(fmt.Sprintf("drop database %s", longDbName))
	}()
	tk.MustExec(fmt.Sprintf("use %s", longDbName))

	// create/drop table,index test
	longTblName := strings.Repeat("表", mysql.MaxTableNameLength-1)
	longColName := strings.Repeat("三", mysql.MaxColumnNameLength-1)
	longIdxName := strings.Repeat("索", mysql.MaxIndexIdentifierLen-1)
	tk.MustExec(fmt.Sprintf("create table %s(f1 int primary key,f2 int, %s varchar(50))", longTblName, longColName))
	tk.MustExec(fmt.Sprintf("create index %s on %s(%s)", longIdxName, longTblName, longColName))
	defer func() {
		tk.MustExec(fmt.Sprintf("drop index %s on %s", longIdxName, longTblName))
		tk.MustExec(fmt.Sprintf("drop table %s", longTblName))
	}()

	// alter table
	tk.MustExec(fmt.Sprintf("alter table %s change f2 %s int", longTblName, strings.Repeat("二", mysql.MaxColumnNameLength-1)))

}

func testDropIndexes(c *C, store kv.Storage, lease time.Duration, createSQL, dropIdxSQL string, idxNames []string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_drop_indexes")
	tk.MustExec(createSQL)
	done := make(chan error, 1)

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		tk.MustExec("insert into test_drop_indexes values (?, ?, ?)", i, i, i)
	}
	ctx := tk.Se.(sessionctx.Context)
	idxIDs := make([]int64, 0, 3)
	for _, idxName := range idxNames {
		idxIDs = append(idxIDs, testGetIndexID(c, ctx, "test_db", "test_drop_indexes", idxName))
	}
	jobIDExt, resetHook := setupJobIDExtCallback(ctx)
	defer resetHook()
	testddlutil.SessionExecInGoroutine(store, dropIdxSQL, done)

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("update test_drop_indexes set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into test_drop_indexes values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}
	for _, idxID := range idxIDs {
		checkDelRangeAdded(tk, jobIDExt.jobID, idxID)
	}
}

func testCancelDropIndexes(c *C, store kv.Storage, d ddl.DDL) {
	indexesName := []string{"idx_c1", "idx_c2"}
	addIdxesSQL := "alter table t add index idx_c1 (c1);alter table t add index idx_c2 (c2);"
	dropIdxesSQL := "alter table t drop index idx_c1;alter table t drop index idx_c2;"

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	testCases := []struct {
		needAddIndex   bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		// model.JobStateNone means the jobs is canceled before the first run.
		// if we cancel successfully, we need to set needAddIndex to false in the next test case. Otherwise, set needAddIndex to true.
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if (job.Type == model.ActionDropIndex || job.Type == model.ActionDropPrimaryKey) &&
			job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = store
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	ctx := tk.Se.(sessionctx.Context)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			tk.MustExec(addIdxesSQL)
		}
		rs, err := tk.Exec(dropIdxesSQL)
		if rs != nil {
			rs.Close()
		}
		t := testGetTableByName(c, ctx, "test_db", "t")

		var indexInfos []*model.IndexInfo
		for _, idxName := range indexesName {
			indexInfo := t.Meta().FindIndexByName(idxName)
			if indexInfo != nil {
				indexInfos = append(indexInfos, indexInfo)
			}
		}

		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			c.Assert(indexInfos, NotNil)
			c.Assert(indexInfos[0].State, Equals, model.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfos, IsNil)
		}
	}
	d.(ddl.DDLForTest).SetHook(originalHook)
	tk.MustExec(addIdxesSQL)
	tk.MustExec(dropIdxesSQL)
}

func testDropIndexesIfExists(c *C, store kv.Storage) {
	tk := testkit.NewTestKitWithInit(c, store)
	tk.MustExec("use test_db;")
	tk.MustExec("drop table if exists test_drop_indexes_if_exists;")
	tk.MustExec("create table test_drop_indexes_if_exists (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2));")

	// Drop different indexes.
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index i1, drop index i3;",
		"[ddl:1091]index i3 doesn't exist",
	)
	if _, err := tk.Exec("alter table test_drop_indexes_if_exists drop index i1, drop index if exists i3;"); true {
		c.Assert(err, IsNil)
	}
	tk.MustQuery("show warnings;").Check(
		testutil.RowsWithSep("|", "Warning|1091|index i3 doesn't exist"),
	)

	// Verify the impact of deletion order when dropping duplicate indexes.
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index i2, drop index i2;",
		"[ddl:1091]index i2 doesn't exist",
	)
	tk.MustGetErrMsg(
		"alter table test_drop_indexes_if_exists drop index if exists i2, drop index i2;",
		"[ddl:1091]index i2 doesn't exist",
	)
	if _, err := tk.Exec("alter table test_drop_indexes_if_exists drop index i2, drop index if exists i2;"); true {
		c.Assert(err, IsNil)
	}
	tk.MustQuery("show warnings;").Check(
		testutil.RowsWithSep("|", "Warning|1091|index i2 doesn't exist"),
	)
}

func testDropIndexesFromPartitionedTable(c *C, store kv.Storage) {
	tk := testkit.NewTestKitWithInit(c, store)
	tk.MustExec("use test_db;")
	tk.MustExec("drop table if exists test_drop_indexes_from_partitioned_table;")
	tk.MustExec(`
		create table test_drop_indexes_from_partitioned_table (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2))
		partition by range(id) (partition p0 values less than (6), partition p1 values less than maxvalue);
	`)
	for i := 0; i < 20; i++ {
		tk.MustExec("insert into test_drop_indexes_from_partitioned_table values (?, ?, ?)", i, i, i)
	}
	if _, err := tk.Exec("alter table test_drop_indexes_from_partitioned_table drop index i1, drop index if exists i2;"); true {
		c.Assert(err, IsNil)
	}
}

func (s *testDBSuite5) TestDropIndexes(c *C) {
	// drop multiple indexes
	createSQL := "create table test_drop_indexes (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2));"
	dropIdxSQL := "alter table test_drop_indexes drop index i1, drop index i2;"
	idxNames := []string{"i1", "i2"}
	testDropIndexes(c, s.store, s.lease, createSQL, dropIdxSQL, idxNames)

	createSQL = "create table test_drop_indexes (id int, c1 int, c2 int, primary key(id) nonclustered, unique key i1(c1), key i2(c2));"
	dropIdxSQL = "alter table test_drop_indexes drop primary key, drop index i1;"
	idxNames = []string{"primary", "i1"}
	testDropIndexes(c, s.store, s.lease, createSQL, dropIdxSQL, idxNames)

	createSQL = "create table test_drop_indexes (uuid varchar(32), c1 int, c2 int, primary key(uuid), unique key i1(c1), key i2(c2));"
	dropIdxSQL = "alter table test_drop_indexes drop primary key, drop index i1, drop index i2;"
	idxNames = []string{"primary", "i1", "i2"}
	testDropIndexes(c, s.store, s.lease, createSQL, dropIdxSQL, idxNames)

	testDropIndexesIfExists(c, s.store)
	testDropIndexesFromPartitionedTable(c, s.store)
	testCancelDropIndexes(c, s.store, s.dom.DDL())
}

// Close issue #24580.
// See https://github.com/pingcap/tidb/issues/24580
func (s *testDBSuite8) TestIssue24580(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(250) default null);")
	tk.MustExec("insert into t values();")

	_, err := tk.Exec("alter table t modify a char not null;")
	c.Assert(err.Error(), Equals, "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustExec("drop table if exists t")
}

// Close issue #27862 https://github.com/pingcap/tidb/issues/27862
// Ref: https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings
// text(100) in utf8mb4 charset needs max 400 byte length, thus tinytext is not enough.
func (s *testDBSuite8) TestCreateTextAdjustLen(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a text(100));")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` text DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t add b text(100);")
	tk.MustExec("alter table t add c text;")
	tk.MustExec("alter table t add d text(50);")
	tk.MustExec("alter table t change column a a text(50);")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` tinytext DEFAULT NULL,\n"+
		"  `b` text DEFAULT NULL,\n"+
		"  `c` text DEFAULT NULL,\n"+
		"  `d` tinytext DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Ref: https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
	// TINYBLOB, TINYTEXT	L + 1 bytes, where L < 2^8
	// BLOB, TEXT	L + 2 bytes, where L < 2^16
	// MEDIUMBLOB, MEDIUMTEXT	L + 3 bytes, where L < 2^24
	// LONGBLOB, LONGTEXT	L + 4 bytes, where L < 2^32
	tk.MustExec("alter table t change column d d text(100);")
	tk.MustExec("alter table t change column c c text(30000);")
	tk.MustExec("alter table t change column b b text(10000000);")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` tinytext DEFAULT NULL,\n"+
		"  `b` longtext DEFAULT NULL,\n"+
		"  `c` mediumtext DEFAULT NULL,\n"+
		"  `d` text DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table if exists t")
}

func (s *testDBSuite2) TestBatchCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tables_1")
	tk.MustExec("drop table if exists tables_2")
	tk.MustExec("drop table if exists tables_3")

	d := s.dom.DDL()
	infos := []*model.TableInfo{}
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_3"),
	})

	// correct name
	err := d.BatchCreateTableWithInfo(tk.Se, model.NewCIStr("test"), infos, ddl.OnExistError)
	c.Check(err, IsNil)

	tk.MustQuery("show tables like '%tables_%'").Check(testkit.Rows("tables_1", "tables_2", "tables_3"))
	job := tk.MustQuery("admin show ddl jobs").Rows()[0]
	c.Assert(job[1], Equals, "test")
	c.Assert(job[2], Equals, "tables_1,tables_2,tables_3")
	c.Assert(job[3], Equals, "create tables")
	c.Assert(job[4], Equals, "public")
	// FIXME: we must change column type to give multiple id
	// c.Assert(job[6], Matches, "[^,]+,[^,]+,[^,]+")

	// duplicated name
	infos[1].Name = model.NewCIStr("tables_1")
	err = d.BatchCreateTableWithInfo(tk.Se, model.NewCIStr("test"), infos, ddl.OnExistError)
	c.Check(terror.ErrorEqual(err, infoschema.ErrTableExists), IsTrue)

	newinfo := &model.TableInfo{
		Name: model.NewCIStr("tables_4"),
	}
	{
		colNum := 2
		cols := make([]*model.ColumnInfo, colNum)
		viewCols := make([]model.CIStr, colNum)
		var stmtBuffer bytes.Buffer
		stmtBuffer.WriteString("SELECT ")
		for i := range cols {
			col := &model.ColumnInfo{
				Name:   model.NewCIStr(fmt.Sprintf("c%d", i+1)),
				Offset: i,
				State:  model.StatePublic,
			}
			cols[i] = col
			viewCols[i] = col.Name
			stmtBuffer.WriteString(cols[i].Name.L + ",")
		}
		stmtBuffer.WriteString("1 FROM t")
		newinfo.Columns = cols
		newinfo.View = &model.ViewInfo{Cols: viewCols, Security: model.SecurityDefiner, Algorithm: model.AlgorithmMerge, SelectStmt: stmtBuffer.String(), CheckOption: model.CheckOptionCascaded, Definer: &auth.UserIdentity{CurrentUser: true}}
	}

	err = d.BatchCreateTableWithInfo(tk.Se, model.NewCIStr("test"), []*model.TableInfo{newinfo}, ddl.OnExistError)
	c.Check(err, IsNil)
}

func (s *testSerialDBSuite) TestAddGeneratedColumnAndInsert(c *C) {
	// For issue #31735.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, unique kye(a))")
	tk.MustExec("insert into t1 value (1), (10)")

	var checkErr error
	tk1 := testkit.NewTestKit(c, s.store)
	_, checkErr = tk1.Exec("use test_db")

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{Do: s.dom}
	ctx := mock.NewContext()
	ctx.Store = s.store
	times := 0
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (1) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (2)")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (3)")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (3) on duplicate key update a=a+1")
				if checkErr == nil {
					_, checkErr = tk1.Exec("replace into t1 values (4)")
				}
				times++
			}
		}
	}
	d.(ddl.DDLForTest).SetHook(hook)

	tk.MustExec("alter table t1 add column gc int as ((a+1))")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("4 5", "10 11"))
	c.Assert(checkErr, IsNil)
}

func (s *testDBSuite1) TestGetTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	testCases := []struct {
		tzSQL  string
		tzStr  string
		tzName string
		offset int
		err    string
	}{
		{"set time_zone = '+00:00'", "", "UTC", 0, ""},
		{"set time_zone = '-00:00'", "", "UTC", 0, ""},
		{"set time_zone = 'UTC'", "UTC", "UTC", 0, ""},
		{"set time_zone = '+05:00'", "", "UTC", 18000, ""},
		{"set time_zone = '-08:00'", "", "UTC", -28800, ""},
		{"set time_zone = '+08:00'", "", "UTC", 28800, ""},
		{"set time_zone = 'Asia/Shanghai'", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = 'SYSTEM'", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = DEFAULT", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = 'GMT'", "GMT", "GMT", 0, ""},
		{"set time_zone = 'GMT+1'", "GMT", "GMT", 0, "[variable:1298]Unknown or incorrect time zone: 'GMT+1'"},
		{"set time_zone = 'Etc/GMT+12'", "Etc/GMT+12", "Etc/GMT+12", 0, ""},
		{"set time_zone = 'Etc/GMT-12'", "Etc/GMT-12", "Etc/GMT-12", 0, ""},
		{"set time_zone = 'EST'", "EST", "EST", 0, ""},
		{"set time_zone = 'Australia/Lord_Howe'", "Australia/Lord_Howe", "Australia/Lord_Howe", 0, ""},
	}
	for _, tc := range testCases {
		err := tk.ExecToErr(tc.tzSQL)
		if err != nil {
			c.Assert(err.Error(), Equals, tc.err)
		} else {
			c.Assert(tc.err, Equals, "")
		}
		c.Assert(tk.Se.GetSessionVars().TimeZone.String(), Equals, tc.tzStr, Commentf("sql: %s", tc.tzSQL))
		tz, offset := ddlutil.GetTimeZone(tk.Se)
		c.Assert(tc.tzName, Equals, tz, Commentf("sql: %s, offset: %d", tc.tzSQL, offset))
		c.Assert(tc.offset, Equals, offset, Commentf("sql: %s", tc.tzSQL))
	}
}
