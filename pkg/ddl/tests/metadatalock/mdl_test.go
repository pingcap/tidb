// Copyright 2022 PingCAP, Inc.
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

package metadatalocktest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMDLBasicSelect(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLBasicInsert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (2);")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLBasicUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustExec("update t set a = 2;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLBasicDelete(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustExec("delete from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLBasicPointGet(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int, unique key(a));")
	tk.MustExec("insert into t values(1), (2), (3);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t where a = 1;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLBasicBatchPointGet(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int, unique key(a));")
	tk.MustExec("insert into t values(1), (2), (3);")

	tk.MustExec("begin")

	tk.MustQuery("select * from t where a in (12, 22);")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLAddForeignKey(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t1(id int key);")
	tk.MustExec("create table t2(id int key);")

	tk.MustExec("begin")
	tk.MustExec("insert into t2 values(1);")

	var wg sync.WaitGroup
	var ddlErr error
	wg.Add(1)
	var ts2 time.Time
	go func() {
		defer wg.Done()
		ddlErr = tkDDL.ExecToErr("alter table test.t2 add foreign key (id) references t1(id)")
		ts2 = time.Now()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Error(t, ddlErr)
	require.Equal(t, "[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`id`) REFERENCES `t1` (`id`))", ddlErr.Error())
	require.Less(t, ts1, ts2)
}

func TestMDLRRUpdateSchema(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	// Add a new column.
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t add column b int;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))

	// Add a new index.
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t add index idx(a);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
	tk.MustGetErrCode("select * from t use index(idx)", mysql.ErrKeyDoesNotExist)
	tk.MustExec("commit")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1 <nil>"))

	// Modify column(reorg).
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t modify column a char(10);")
	tk.MustGetErrCode("select * from t", mysql.ErrInfoSchemaChanged)
	tk.MustGetErrCode("select * from t", mysql.ErrInfoSchemaChanged)
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))

	// Modify column(non-reorg).
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t modify column a char(20);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
}

func TestMDLRCUpdateSchema(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("set @@transaction_isolation='READ-COMMITTED';")

	// Add a new column.
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t add column b int;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))

	// Add a new index.
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t add index idx(a);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1 <nil>"))

	// Modify column(reorg).
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t modify column a char(10);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))

	// Modify column(non-reorg).
	tk.MustExec("begin")
	tkDDL.MustExec("alter table test.t modify column a char(20);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil>"))
}

func TestMDLAutoCommitReadOnly(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	var wg sync.WaitGroup
	wg.Add(2)
	var ts2 time.Time
	var ts1 time.Time

	go func() {
		tk.MustQuery("select sleep(2) from t;")
		ts1 = time.Now()
		wg.Done()
	}()

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	wg.Wait()
	require.Greater(t, ts1, ts2)
}

func TestMDLAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	var wg sync.WaitGroup
	wg.Add(2)
	var ts2 time.Time
	var ts1 time.Time

	go func() {
		tk.MustExec("begin")
		tk.MustExec("analyze table t;")
		tk.MustQuery("select sleep(2);")
		tk.MustExec("commit")
		ts1 = time.Now()
		wg.Done()
	}()

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	wg.Wait()
	require.Greater(t, ts1, ts2)
}

func TestMDLAnalyzePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int) partition by range(a) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN MAXVALUE );")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	var wg sync.WaitGroup
	wg.Add(2)
	var ts2 time.Time
	var ts1 time.Time

	go func() {
		tk.MustExec("begin")
		tk.MustExec("analyze table t;")
		tk.MustExec("analyze table t partition p1;")
		tk.MustQuery("select sleep(2);")
		tk.MustExec("commit")
		ts1 = time.Now()
		wg.Done()
	}()

	go func() {
		tkDDL.MustExec("alter table test.t drop partition p2;")
		ts2 = time.Now()
		wg.Done()
	}()

	wg.Wait()
	require.Greater(t, ts1, ts2)
}

func TestMDLAutoCommitNonReadOnly(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	var wg sync.WaitGroup
	wg.Add(2)
	var ts2 time.Time
	var ts1 time.Time

	go func() {
		tk.MustExec("insert into t select sleep(2) from t;")
		ts1 = time.Now()
		wg.Done()
	}()

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLLocalTemporaryTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("use test")

	tk.MustExec("create temporary table t(a int);")
	tk.MustExec("insert into t values(1)")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	var ts1 time.Time

	tk.MustExec("begin")
	tk.MustExec("insert into t values (2)")

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)
	ts1 = time.Now()
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))
	tk.MustExec("commit")

	wg.Wait()
	require.Greater(t, ts1, ts2)
}

func TestMDLGlobalTemporaryTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create global temporary table t(a int) ON COMMIT DELETE ROWS;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	var ts1 time.Time

	tk.MustExec("begin")
	tk.MustExec("insert into t values (2)")

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)
	ts1 = time.Now()
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustExec("commit")

	wg.Wait()
	require.Greater(t, ts1, ts2)

	tk.MustExec("begin")

	tkDDL.MustExec("alter table test.t add column c int;")
	tk.MustExec("insert into t values (2, null, null)")

	tk.MustQuery("select * from t").Check(testkit.Rows("2 <nil> <nil>"))
	tk.MustExec("commit")
}

func TestMDLCacheTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("alter table t cache")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	var ts1 time.Time

	tk.MustExec("begin")
	tk.MustQuery("select * from t")
	tk.MustQuery("select * from t")

	go func() {
		tkDDL.MustExec("alter table test.t nocache;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)
	ts1 = time.Now()
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLStaleRead(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	time.Sleep(2 * time.Second)

	tk.MustExec("start transaction read only as of timestamp NOW() - INTERVAL 1 SECOND")
	tk.MustQuery("select * from t")

	tkDDL.MustExec("alter table test.t add column b int;")

	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("commit")
}

func TestMDLTiDBSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
    ON DUPLICATE KEY
    UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	var ts1 time.Time

	time.Sleep(2 * time.Second)

	tk.MustExec("begin")
	tk.MustExec("set @@tidb_snapshot = NOW() - INTERVAL 1 SECOND")
	tk.MustQuery("select * from t")

	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)
	ts1 = time.Now()
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	wg.Wait()
	require.Greater(t, ts1, ts2)
}

func TestMDLPartitionTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int) partition by hash(a) partitions 10;")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLPreparePlanBlockDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec(`prepare stmt_test_1 from 'select * from t where a >= ?';`)

	tk.MustExec("begin")
	tk.MustExec(`set @a = 1;`)
	tk.MustQuery(`execute stmt_test_1 using @a;`).Check(testkit.Rows("1", "2", "3", "4"))

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	tk.MustExec(`prepare stmt_test_1 from 'select * from t where a >= ?';`)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)

	tk.MustQuery(`execute stmt_test_1 using @a;`).Check(testkit.Rows("1 <nil>", "2 <nil>", "3 <nil>", "4 <nil>"))
}

func TestMDLPreparePlanCacheInvalid(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	tk.MustExec(`prepare stmt_test_1 from 'select * from t where a >= ?';`)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)

	tk.MustExec(`set @a = 1;`)
	tk.MustQuery(`execute stmt_test_1 using @a;`).Check(testkit.Rows("1 <nil>", "2 <nil>", "3 <nil>", "4 <nil>"))
}

func TestMDLPreparePlanCacheExecute(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("create table t2(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec(`prepare stmt_test_1 from 'update t set a = ? where a = ?';`)
	tk.MustExec(`set @a = 1, @b = 3;`)
	tk.MustExec(`execute stmt_test_1 using @a, @b;`)

	tk.MustExec("begin")

	ch := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-ch
		tkDDL.MustExec("alter table test.t add index idx(a);")
		wg.Done()
	}()

	tk.MustQuery("select * from t2")
	tk.MustExec(`set @a = 2, @b=4;`)
	tk.MustExec(`execute stmt_test_1 using @a, @b;`)
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	// The plan is from cache, the metadata lock should be added to block the DDL.
	ch <- struct{}{}

	time.Sleep(5 * time.Second)

	tk.MustExec("commit")

	wg.Wait()

	tk.MustExec("admin check table t")
}

func TestMDLPreparePlanCacheExecute2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("create table t2(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec(`prepare stmt_test_1 from 'select * from t where a = ?';`)
	tk.MustExec(`set @a = 1;`)
	tk.MustExec(`execute stmt_test_1 using @a;`)

	tk.MustExec("begin")
	tk.MustQuery("select * from t2")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tkDDL.MustExec("alter table test.t add index idx(a);")
		wg.Done()
	}()

	wg.Wait()

	tk.MustExec(`set @a = 2;`)
	tk.MustExec(`execute stmt_test_1 using @a;`)
	// The plan should not be from cache because the schema has changed.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("commit")

	tk.MustExec("admin check table t")
}

func TestMDLDisable2Enable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	conn3 := server.CreateMockConn(t, sv)
	tk3 := testkit.NewTestKitWithSession(t, store, conn3.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (10);")
	tk3.MustExec("use test")
	tk3.MustExec("begin")
	tk3.MustQuery("select * from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
		tkDDL.MustExec("alter table test.t add index idx(a);")
		wg.Done()
	}()

	wg.Wait()

	tk.MustGetErrCode("commit", mysql.ErrInfoSchemaChanged)
	tk3.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestMDLEnable2Disable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	conn3 := server.CreateMockConn(t, sv)
	tk3 := testkit.NewTestKitWithSession(t, store, conn3.Context().Session)
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3), (4);")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (10);")
	tk3.MustExec("use test")
	tk3.MustExec("begin")
	tk3.MustQuery("select * from t;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=0")
		tkDDL.MustExec("alter table test.t add index idx(a);")
		wg.Done()
	}()

	wg.Wait()

	tk.MustGetErrCode("commit", mysql.ErrInfoSchemaChanged)
	tk3.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestSwitchMDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustQuery("show global variables like 'tidb_enable_metadata_lock'").Check(testkit.Rows("tidb_enable_metadata_lock OFF"))

	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustQuery("show global variables like 'tidb_enable_metadata_lock'").Check(testkit.Rows("tidb_enable_metadata_lock ON"))

	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk.MustQuery("show global variables like 'tidb_enable_metadata_lock'").Check(testkit.Rows("tidb_enable_metadata_lock OFF"))
}

func TestMDLViewItself(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3);")
	tk.MustExec("create view v as select * from t")

	tk.MustExec("begin")
	tk.MustQuery("select * from v;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("drop view test.v;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLViewBaseTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3);")
	tk.MustExec("create view v as select * from t")

	tk.MustExec("begin")
	tk.MustQuery("select * from v;")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	wg.Wait()
	require.Less(t, ts1, ts2)
}

func TestMDLSavePoint(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustExec("savepoint s1")
	tk.MustQuery("select * from t;")
	tk.MustExec("rollback to s1")

	var wg sync.WaitGroup
	wg.Add(1)
	var ts2 time.Time
	go func() {
		tkDDL.MustExec("alter table test.t add column b int;")
		ts2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)

	ts1 := time.Now()
	tk.MustExec("commit")

	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))

	wg.Wait()
	require.Less(t, ts1, ts2)

	tk.MustExec("alter table t drop column b")
	tk.MustExec("begin")
	tk.MustExec("savepoint s2")
	tkDDL.MustExec("alter table test.t add column b int;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("rollback to s2")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <nil>"))
}

func TestMDLTableCreate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t;")
	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)

	tkDDL.MustExec("create table test.t1(a int);")

	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
}

func TestMDLTableDrop(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")

	tkDDL.MustExec("drop table test.t;")

	tk.MustGetErrCode("select * from t;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
}

func TestMDLDatabaseCreate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")

	tk.MustExec("begin")

	tkDDL.MustExec("create database test2;")
	tkDDL.MustExec("create table test2.t(a int);")

	tk.MustGetErrCode("use test2", mysql.ErrBadDB)
	tk.MustGetErrCode("select * from test2.t;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
}

func TestMDLDatabaseDrop(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")

	tkDDL.MustExec("drop database test;")

	tk.MustExec("use test;")
	tk.MustGetErrCode("select * from t;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
}

func TestMDLRenameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_metadata_lock=1")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")

	tkDDL.MustExec("rename table test.t to test.t1;")

	tk.MustGetErrCode("select * from t;", mysql.ErrNoSuchTable)
	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
	tk.MustExec("create database test2")
	tk.MustExec("begin")

	tkDDL.MustExec("rename table test.t1 to test2.t1;")

	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)
	tk.MustGetErrCode("select * from test2.t1;", mysql.ErrNoSuchTable)
	tk.MustExec("commit")
}

func TestMDLPrepareFail(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	_, _, _, err := tk.Session().PrepareStmt("select b from t")
	require.Error(t, err)

	tk2.MustExec("alter table test.t add column c int")
}

func TestMDLUpdateEtcdFail(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpdateMDLToETCDError", `3*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpdateMDLToETCDError"))
	}()

	tk.MustExec("alter table test.t add column c int")
}
