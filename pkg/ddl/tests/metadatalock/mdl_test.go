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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	if kerneltype.IsNextGen() {
		t.Skip("the test drives add-index backfill with the mock ingest backend, which nextgen does not support")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
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
	tk.MustExec(`execute stmt_test_1 using @a, @b;`) // can't reuse the prior plan created outside this txn.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec(`execute stmt_test_1 using @a, @b;`) // can't reuse the prior plan since this table becomes dirty.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec(`execute stmt_test_1 using @a, @b;`) // can't reuse the prior plan now.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	// The plan is from cache, the metadata lock should be added to block the DDL.
	ch <- struct{}{}

	time.Sleep(5 * time.Second)

	tk.MustExec("commit")

	wg.Wait()

	tk.MustExec("admin check table t")
}

func TestMDLPreparePlanCacheExecute2(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("the test drives add-index backfill with the mock ingest backend, which nextgen does not support")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
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

// TestMDLPreparePlanCacheExecuteInsert makes sure the insert statement handle the schema correctly in plan cache.
func TestMDLPreparePlanCacheExecuteInsert(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("the test drives add-index backfill with the mock ingest backend, which nextgen does not support")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int);")
	tk.MustExec("create table t2(a int);")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3), (4, 4);")

	tk.MustExec(`prepare insert_stmt from 'insert into t values (?, ?)'`)
	tk.MustExec(`set @a=4, @b=4;`)

	ch := make(chan struct{})

	first := true
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		switch job.SchemaState {
		case model.StateWriteReorganization:
			tbl, _ := dom.InfoSchema().TableByID(context.Background(), job.TableID)
			idx := tbl.Meta().FindIndexByName("idx")
			switch idx.BackfillState {
			case model.BackfillStateRunning:
				if first {
					// generate plan, cache it, and make some row change to make
					// sure backfill state 'merging' is not skipped.
					tk.MustExec(`begin`)
					tk.MustExec(`delete from t where a = 4;`)
					tk.MustExec(`execute insert_stmt using @a, @b;`)
					tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
					tk.MustExec(`commit`)

					tk.MustExec("begin")
					// Activate txn.
					tk.MustExec("select * from t2")
					first = false
					return
				}
			}
		}
	})

	ddl.MockDMLExecutionMerging = func() {
		tk.MustExec(`delete from t where a = 4;`)
		// we must generate a new plan here, because the schema has changed since
		// the last plan was generated.
		tk.MustExec(`execute insert_stmt using @a, @b;`)
		tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
		tk.MustExec("commit")
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionMerging", "1*return(true)->return(false)"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-ch
		tkDDL.MustExec("alter table test.t add index idx(a);")
		wg.Done()
	}()

	ch <- struct{}{}
	wg.Wait()

	tk.MustExec("admin check table t")
}

func TestMDLDisable2Enable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("MDL is always enabled and read only in nextgen")
	}
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
	if kerneltype.IsNextGen() {
		t.Skip("MDL is always enabled and read only in nextgen")
	}
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
	if kerneltype.IsNextGen() {
		t.Skip("MDL is always enabled and read only in nextgen")
	}
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

func TestSetMDLInNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only run in nextgen")
	}
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	require.ErrorContains(t, tk.ExecToErr("set global tidb_enable_metadata_lock=0"),
		"setting tidb_enable_metadata_lock is not supported in the next generation of TiDB")
	require.ErrorContains(t, tk.ExecToErr("set global tidb_enable_metadata_lock=1"),
		"setting tidb_enable_metadata_lock is not supported in the next generation of TiDB")
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
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t;")
	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)

	tkDDL.MustExec("create table test.t1(a int);")

	tk.MustGetErrCode("select * from t1;", mysql.ErrNoSuchTable)

	tk.MustExec("commit")
}

func TestMDLCreateMaterializedViewLogBlockByBaseTableTxn(t *testing.T) {
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
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tk.MustExec("set tidb_mview_enable = on")
	tkDDL.MustExec("set tidb_mview_enable = on")
	tk.MustExec("create table t(a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")

	mdlWaitCh := make(chan struct{})
	var mdlWaitOnce sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, schemaVer int64) {
		if job.Type == model.ActionCreateMaterializedViewLog && schemaVer != 0 {
			mdlWaitOnce.Do(func() { close(mdlWaitCh) })
		}
	})
	ddlDone := make(chan error, 1)
	go func() {
		ddlDone <- tkDDL.ExecToErr("create materialized view log on test.t (a)")
	}()

	select {
	case err := <-ddlDone:
		require.FailNowf(t, "create materialized view log should be blocked by running transaction", "ddl finished before waiting for schema sync, err=%v", err)
	case <-mdlWaitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG to enter schema sync")
	}
	tk.MustExec("commit")

	select {
	case err := <-ddlDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "create materialized view log should finish after transaction commit")
	}
}

func TestMDLCreateMaterializedViewLogNewTxnWriteMLog(t *testing.T) {
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
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tk.MustExec("set tidb_mview_enable = on")
	tkDDL.MustExec("set tidb_mview_enable = on")
	tk.MustExec("create table t(a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")

	mdlWaitCh := make(chan struct{})
	var mdlWaitOnce sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, schemaVer int64) {
		if job.Type == model.ActionCreateMaterializedViewLog && schemaVer != 0 {
			mdlWaitOnce.Do(func() { close(mdlWaitCh) })
		}
	})
	ddlDone := make(chan error, 1)
	go func() {
		ddlDone <- tkDDL.ExecToErr("create materialized view log on test.t (a)")
	}()

	select {
	case err := <-ddlDone:
		require.FailNowf(t, "create materialized view log should be blocked by running transaction", "ddl finished before waiting for schema sync, err=%v", err)
	case <-mdlWaitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG to enter schema sync")
	}
	tk.MustExec("commit")

	select {
	case err := <-ddlDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "create materialized view log should finish after transaction commit")
	}

	tk.MustExec("begin")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("commit")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a").Check(testkit.Rows("2 I 1"))

	tkDDL.MustExec("use test")
	for _, tc := range []struct {
		name     string
		write    string
		finish   string
		prepared bool
	}{
		{"add_column_insert", "insert into %s values (2, 20)", "commit", false},
		{"add_column_update", "update %s set b = 20 where id = 1", "commit", false},
		{"add_column_delete", "delete from %s where id = 1", "commit", false},
		{"add_column_rollback", "update %s set b = 20 where id = 1", "rollback", false},
		{"add_column_plan_cache", "", "commit", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
			tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
			tableName := "t_mlog_" + tc.name
			mlogName := "$mlog$" + tableName
			mvName := "mv_mlog_" + tc.name
			tk.MustExec(fmt.Sprintf("create table %s(id int primary key, b int not null)", tableName))
			tk.MustExec(fmt.Sprintf("insert into %s values (1, 10)", tableName))
			tk.MustExec(fmt.Sprintf("create materialized view log on %s (id)", tableName))
			if tc.prepared {
				tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
				tk.MustExec(fmt.Sprintf("prepare mlog_update_stmt from 'update %s set b = ? where id = 1'", tableName))
				tk.MustExec("begin pessimistic")
				tk.MustExec("set @b = 10")
				tk.MustExec("execute mlog_update_stmt using @b")
				tk.MustExec("execute mlog_update_stmt using @b")
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
				tk.MustExec("commit")
			}

			tk.MustExec("begin pessimistic")
			defer tk.MustExec("rollback")
			if tc.prepared {
				tk.MustExec("set @b = 20")
				tk.MustExec("execute mlog_update_stmt using @b")
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
			} else {
				tk.MustExec(fmt.Sprintf(tc.write, tableName))
			}

			ddlStarted := make(chan struct{})
			var once sync.Once
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
				if job.Type == model.ActionAddColumn && job.TableName == mlogName && job.SchemaState == model.StateDeleteOnly {
					once.Do(func() { close(ddlStarted) })
				}
			})
			ddlDone := make(chan error, 1)
			go func() {
				ddlDone <- tkDDL.ExecToErr(fmt.Sprintf("alter materialized view log on %s add column (b)", tableName))
			}()
			select {
			case <-ddlStarted:
			case <-time.After(10 * time.Second):
				require.FailNow(t, "MLog ADD COLUMN did not start")
			}
			select {
			case err := <-ddlDone:
				require.FailNowf(t, "MLog ADD COLUMN must wait for the base-table writer", "DDL finished before %s, err=%v", tc.finish, err)
			case <-time.After(200 * time.Millisecond):
			}
			tk.MustExec(tc.finish)
			select {
			case err := <-ddlDone:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				require.FailNow(t, "MLog ADD COLUMN did not finish after the writer ended")
			}

			// A new MV must include every old writer before it starts consuming
			// changes to the newly tracked column from the MLog.
			baseQuery := fmt.Sprintf("select id, sum(b), count(1) from %s group by id order by id", tableName)
			mvQuery := fmt.Sprintf("select id, s, cnt from %s order by id", mvName)
			tkDDL.MustExec(fmt.Sprintf("create materialized view %s (id, s, cnt) refresh fast as select id, sum(b), count(1) from %s group by id", mvName, tableName))
			require.Equal(t, tkDDL.MustQuery(baseQuery).Rows(), tkDDL.MustQuery(mvQuery).Rows())
			if tc.prepared {
				tk.MustExec("set @b = 30")
				tk.MustExec("execute mlog_update_stmt using @b")
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
			} else {
				tk.MustExec(fmt.Sprintf("update %s set b = b + 1", tableName))
			}
			tk.MustExec(fmt.Sprintf("insert into %s values (3, 30)", tableName))
			tkDDL.MustQuery(fmt.Sprintf("select b from `%s` where id = 3", mlogName)).Check(testkit.Rows("30"))
			tkDDL.MustExec(fmt.Sprintf("refresh materialized view %s fast", mvName))
			require.Equal(t, tkDDL.MustQuery(baseQuery).Rows(), tkDDL.MustQuery(mvQuery).Rows())
		})
	}

	t.Run("transaction_before_add_column", func(t *testing.T) {
		tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
		tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
		tk.MustExec("create table t_mlog_old_schema(id int primary key, b int not null)")
		tk.MustExec("insert into t_mlog_old_schema values (1, 10)")
		tk.MustExec("create materialized view log on t_mlog_old_schema (id)")
		tk.MustExec("begin pessimistic")
		defer tk.MustExec("rollback")
		// Reading the base table must not lock its MLog. The subsequent write
		// must still acquire the MLog dependency when the base MDL is held.
		tk.MustQuery("select b from t_mlog_old_schema").Check(testkit.Rows("10"))
		tkDDL.MustExec("alter materialized view log on t_mlog_old_schema add column (b)")
		tk.MustExec("update t_mlog_old_schema set b = 20 where id = 1")
		tk.MustExec("commit")
		tkDDL.MustQuery("select b, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_old_schema` order by `_MLOG$_OLD_NEW`").
			Check(testkit.Rows("10 -1", "20 1"))
	})

	t.Run("transaction_before_create_mlog", func(t *testing.T) {
		tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
		tkDDL := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
		tk.MustExec("create table t_mlog_new_schema(id int primary key, b int not null)")
		tk.MustExec("insert into t_mlog_new_schema values (1, 10)")
		tk.MustExec("begin pessimistic")
		defer tk.MustExec("rollback")
		tkDDL.MustExec("create materialized view log on t_mlog_new_schema (id, b)")
		tk.MustExec("update t_mlog_new_schema set b = 20 where id = 1")
		tk.MustExec("commit")
		tkDDL.MustQuery("select b, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_new_schema` order by `_MLOG$_OLD_NEW`").
			Check(testkit.Rows("10 -1", "20 1"))
	})
}

func TestMDLTableDrop(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tkDDL := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
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

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/schemaver/mockUpdateMDLToETCDError", `3*return(true)`)

	tk.MustExec("alter table test.t add column c int")
}

// TestMDLInternalSessionBlocksDDL verifies that a transaction on an internal session
// blocks DDL on both user tables and non-DDL system tables it writes. Internal sessions
// used to be skipped by the MDL census, which allowed DDL to change a table while such
// transactions were still writing it (see issue #71055).
func TestMDLInternalSessionBlocksDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	tkDDL.MustExec("use test")
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tkDDL.MustExec("create table test.t(a int)")
	tkDDL.MustExec("insert into test.t values (1)")
	tkDDL.MustExec("create table mysql.mdl_internal_session_sys_t(a int)")
	tkDDL.MustExec("insert into mysql.mdl_internal_session_sys_t values (1)")

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	// Internal SQL must carry a source type; TTL is the workload this scenario mimics.
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	defer func() {
		// Rollback is a no-op after a successful commit.
		rs, _ := se.ExecuteInternal(internalCtx, "rollback")
		if rs != nil {
			_ = rs.Close()
		}
		pool.Put(se)
	}()

	execInternal := func(sql string) {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		require.NoError(t, err)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
	}

	for _, tblName := range []string{"test.t", "mysql.mdl_internal_session_sys_t"} {
		// Get() registers the session to the session manager, and the explicit write
		// transaction makes it hold the MDL lock on both user and non-DDL mysql tables.
		execInternal("begin optimistic")
		execInternal(fmt.Sprintf("insert into %s values (2)", tblName))

		mdlWaitCh := make(chan struct{})
		var mdlWaitOnce sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, schemaVer int64) {
			if job.Type == model.ActionAddIndex && schemaVer != 0 {
				mdlWaitOnce.Do(func() { close(mdlWaitCh) })
			}
		})
		ddlDone := make(chan error, 1)
		go func() {
			ddlDone <- tkDDL.ExecToErr(fmt.Sprintf("alter table %s add index idx_a(a)", tblName))
		}()

		select {
		case err := <-ddlDone:
			require.FailNowf(t, "DDL should be blocked by the internal session transaction", "table=%s, err=%v", tblName, err)
		case <-mdlWaitCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for ALTER TABLE %s to enter schema sync", tblName)
		}
		select {
		case <-ddlDone:
			t.Fatalf("DDL on %s should not finish while the internal session transaction is running", tblName)
		case <-time.After(500 * time.Millisecond):
		}

		execInternal("commit")
		select {
		case err := <-ddlDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("DDL on %s should finish after the internal session transaction commits", tblName)
		}
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced")
		tkDDL.MustExec(fmt.Sprintf("admin check table %s", tblName))
	}
}

// TestMDLInternalSessionDoesNotBlockDDLSystemTableDDL verifies that internal sessions
// do not take MDL on DDL framework tables (`mysql.tidb_ddl_job`, `tidb_ddl_reorg`,
// `tidb_ddl_history`, `tidb_mdl_info`, `tidb_ddl_notifier`), avoiding circular blocking
// when upgrade DDL alters those tables.
func TestMDLInternalSessionDoesNotBlockDDLSystemTableDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	defer func() {
		rs, _ := se.ExecuteInternal(internalCtx, "rollback")
		if rs != nil {
			_ = rs.Close()
		}
		pool.Put(se)
	}()

	execInternal := func(sql string) {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		require.NoError(t, err)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
	}
	execInternal("begin optimistic")
	execInternal("delete from mysql.tidb_ddl_job where job_id = -1")
	execInternal("delete from mysql.tidb_ddl_reorg where job_id = -1")
	execInternal("delete from mysql.tidb_mdl_info where job_id = -1")
	execInternal("delete from mysql.tidb_ddl_notifier where ddl_job_id = -1")
	execInternal("insert into mysql.tidb_ddl_history(job_id, db_name, table_name) values (999991, 'test', 't')")

	ddlDone := make(chan error, 1)
	go func() {
		ddlDone <- tkDDL.ExecToErr("alter table mysql.tidb_ddl_history add column mdl_test_col int")
	}()
	select {
	case err := <-ddlDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DDL on a DDL framework system table should not be blocked by an internal session transaction")
	}
	// The DDL changed the table while the internal transaction was open, so commit would fail
	// the schema delta check; rolling back keeps this test focused on the non-blocking check.
	execInternal("rollback")

	// Reading a DDL framework table in an optimistic internal transaction must not trigger
	// the read-only MDL-skip retry suppression, so optimistic write conflicts still retry.
	tkDDL.MustExec("insert into mysql.tidb_ddl_history(job_id, db_name, table_name) values (999994, 'test', 'init')")
	execInternal("set @@tidb_retry_limit=2")
	execInternal("begin optimistic")
	execInternal("select db_name from mysql.tidb_ddl_history where job_id = 999994")
	tkDDL.MustExec("update mysql.tidb_ddl_history set table_name = 'concurrent' where job_id = 999994")
	execInternal("update mysql.tidb_ddl_history set db_name = 'retried' where job_id = 999994")
	execInternal("commit")
	tkDDL.MustQuery("select db_name, table_name from mysql.tidb_ddl_history where job_id = 999994").Check(testkit.Rows("retried concurrent"))
}

// TestMDLInternalSessionDDLSystemTableWriteValidatedAtCommit verifies that internal sessions
// do not take MDL on DDL framework tables, while a concurrent DDL still makes their commit
// fail via the schema delta check instead of letting a write with stale metadata commit.
func TestMDLInternalSessionDDLSystemTableWriteValidatedAtCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	defer func() {
		rs, _ := se.ExecuteInternal(internalCtx, "rollback")
		if rs != nil {
			_ = rs.Close()
		}
		pool.Put(se)
	}()
	execInternal := func(sql string) error {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		return err
	}

	require.NoError(t, execInternal("set @@tidb_retry_limit=0"))
	require.NoError(t, execInternal("set @@tidb_enable_1pc=1"))
	require.NoError(t, execInternal("set @@tidb_enable_async_commit=1"))
	require.NoError(t, execInternal("begin optimistic"))
	require.NoError(t, execInternal("insert into mysql.tidb_ddl_history(job_id, db_name, table_name) values (999992, 'test', 't')"))

	// No MDL is taken on DDL framework tables, so this DDL is not blocked by the internal session.
	tkDDL.MustExec("alter table mysql.tidb_ddl_history add column mdl_commit_col int")

	err = execInternal("commit")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Information schema is changed")
}

// TestMDLInternalSessionReadOnlyDoesNotBlockDDLAndValidatesWrite verifies that
// non-pessimistic read-only statements in internal explicit transactions do not take
// MDL on user or system tables (so DDL is not stalled), while validating those read
// tables via the schema delta check if the transaction later writes.
func TestMDLInternalSessionReadOnlyDoesNotBlockDDLAndValidatesWrite(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tkDDL.MustExec("use test")
	tkDDL.MustExec("create table t_ro(a int)")
	tkDDL.MustExec("insert into t_ro values (1)")
	tkDDL.MustExec("create table t_w(a int)")
	tkDDL.MustExec("create table t_rw(a int)")
	tkDDL.MustExec("create table mysql.mdl_sys_ro(a int)")
	tkDDL.MustExec("insert into mysql.mdl_sys_ro values (1)")

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
	defer func() {
		rs, _ := se.ExecuteInternal(internalCtx, "rollback")
		if rs != nil {
			_ = rs.Close()
		}
		pool.Put(se)
	}()
	execInternal := func(sql string) error {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		return err
	}

	// Reading t_ro and mysql.mdl_sys_ro in an optimistic internal transaction skips MDL,
	// so DDL on both tables completes immediately, while a subsequent write in the same
	// transaction is aborted at commit (even with retry_limit > 0, because transparent
	// write-only retry must not replay with stale caller-observed read results).
	require.NoError(t, execInternal("set @@tidb_retry_limit=2"))
	require.NoError(t, execInternal("begin optimistic"))
	require.NoError(t, execInternal("select * from test.t_ro"))
	require.NoError(t, execInternal("select * from mysql.mdl_sys_ro"))

	for _, alterSQL := range []string{
		"alter table mysql.mdl_sys_ro add column b int",
		"alter table test.t_ro add column b int",
	} {
		ddlDone := make(chan error, 1)
		go func() {
			ddlDone <- tkDDL.ExecToErr(alterSQL)
		}()
		select {
		case err := <-ddlDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("DDL (%s) should not be blocked by non-pessimistic internal read-only select", alterSQL)
		}
	}

	require.NoError(t, execInternal("insert into test.t_w values (1)"))
	err = execInternal("commit")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Information schema is changed")
	tkDDL.MustQuery("select count(*) from test.t_w").Check(testkit.Rows("0"))
	_ = execInternal("rollback")

	// Writing an MDL-locked user table t_rw (after an earlier DDL) and then reading t_rw
	// in the same optimistic internal transaction must not re-add t_rw to delta checks.
	require.NoError(t, execInternal("set @@tidb_retry_limit=0"))
	require.NoError(t, execInternal("begin optimistic"))
	tkDDL.MustExec("alter table test.t_rw add column b int")
	require.NoError(t, execInternal("insert into test.t_rw(a) values (1)"))
	require.NoError(t, execInternal("select * from test.t_rw"))
	require.NoError(t, execInternal("commit"))
	tkDDL.MustQuery("select a, b from test.t_rw").Check(testkit.Rows("1 <nil>"))
}

// TestMDLInternalSessionPessimisticSystemWriteDoesNotFailOnUserTableSchemaUpgrade
// verifies both internal pessimistic transaction patterns:
//  1. Writing an MDL-exempt DDL table (`mysql.tidb_ddl_history`) registers only that table
//     for commit-time delta validation, so a user table upgraded before first access and
//     then locked via MDL still commits cleanly.
//  2. Locking a non-DDL `mysql` table (`mysql.mdl_sys_meta`, which takes MDL) and then
//     writing a user table upgraded before first access also commits cleanly.
func TestMDLInternalSessionPessimisticSystemWriteDoesNotFailOnUserTableSchemaUpgrade(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tkDDL.MustExec("create table mysql.mdl_sys_meta(id int primary key, v int)")
	tkDDL.MustExec("insert into mysql.mdl_sys_meta values (1, 0)")
	tkDDL.MustExec("create table test.mdl_mv_target(id int primary key, v int)")

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	defer func() {
		rs, _ := se.ExecuteInternal(internalCtx, "rollback")
		if rs != nil {
			_ = rs.Close()
		}
		pool.Put(se)
	}()
	execInternal := func(sql string) error {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		return err
	}

	// Case 1: MDL-exempt DDL table write + MDL-locked user table write after user DDL.
	require.NoError(t, execInternal("set @@tidb_retry_limit=0"))
	require.NoError(t, execInternal("begin pessimistic"))
	require.NoError(t, execInternal("insert into mysql.tidb_ddl_history(job_id, db_name, table_name) values (999993, 'test', 'mdl_mv_target')"))

	tkDDL.MustExec("alter table test.mdl_mv_target add column extra int")

	require.NoError(t, execInternal("insert into test.mdl_mv_target(id, v) values (1, 10)"))
	require.NoError(t, execInternal("commit"))

	tkDDL.MustQuery("select id, v, extra from test.mdl_mv_target").Check(testkit.Rows("1 10 <nil>"))

	// Case 2: MDL-locked non-DDL mysql table + MDL-locked user table write after user DDL.
	require.NoError(t, execInternal("begin pessimistic"))
	require.NoError(t, execInternal("select v from mysql.mdl_sys_meta where id = 1 for update"))

	tkDDL.MustExec("alter table test.mdl_mv_target add column extra2 int")

	require.NoError(t, execInternal("insert into test.mdl_mv_target(id, v) values (2, 20)"))
	require.NoError(t, execInternal("update mysql.mdl_sys_meta set v = 1 where id = 1"))
	require.NoError(t, execInternal("commit"))

	tkDDL.MustQuery("select id, v, extra, extra2 from test.mdl_mv_target order by id").Check(testkit.Rows("1 10 <nil> <nil>", "2 20 <nil> <nil>"))
	tkDDL.MustQuery("select v from mysql.mdl_sys_meta where id = 1").Check(testkit.Rows("1"))
}

// TestMDLInternalSessionMViewRefreshBlocksDDL verifies that an active Materialized View
// refresh running in an internal pessimistic session holds MDL on the MV table through
// commit, blocking a concurrent ADD INDEX until refresh finishes, without breaking commit.
func TestMDLInternalSessionMViewRefreshBlocksDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	ddlConn := server.CreateMockConn(t, sv)
	tkDDL := testkit.NewTestKitWithSession(t, store, ddlConn.Context().Session)
	if kerneltype.IsClassic() {
		tkDDL.MustExec("set global tidb_enable_metadata_lock=1")
	}
	tkDDL.MustExec("set tidb_mview_enable = on")
	tkDDL.MustExec("use test")

	tkDDL.MustExec("create table src_mv(id int primary key, g int not null, v int not null)")
	tkDDL.MustExec("insert into src_mv values (1, 1, 10)")
	tkDDL.MustExec("create materialized view log on src_mv (id, g, v)")
	tkDDL.MustExec("create materialized view mv_mdl (g, s, cnt) refresh fast as select g, sum(v), count(*) from src_mv group by g")
	tkDDL.MustExec("insert into src_mv values (2, 1, 20), (3, 2, 30)")

	pool := dom.AdvancedSysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	defer pool.Put(se)
	execInternal := func(sql string) error {
		rs, err := se.ExecuteInternal(internalCtx, sql)
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		return err
	}
	require.NoError(t, execInternal("set tidb_mview_enable = on"))

	paused := make(chan struct{})
	resume := make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() {
		resumeOnce.Do(func() { close(resume) })
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", func() {
		pauseOnce.Do(func() { close(paused) })
		<-resume
	})

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- execInternal("refresh materialized view test.mv_mdl fast")
	}()
	refreshFinished := false
	defer func() {
		release()
		if !refreshFinished {
			select {
			case <-refreshDone:
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up refresh")
			}
		}
	}()

	select {
	case <-paused:
	case err := <-refreshDone:
		refreshFinished = true
		require.FailNowf(t, "refresh should have paused at failpoint", "err=%v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for refresh to pause after data changes")
	}

	mdlWaitCh := make(chan struct{})
	var mdlWaitOnce sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, schemaVer int64) {
		if job.Type == model.ActionAddIndex && schemaVer != 0 {
			mdlWaitOnce.Do(func() { close(mdlWaitCh) })
		}
	})

	ddlDone := make(chan error, 1)
	go func() {
		ddlDone <- tkDDL.ExecToErr("alter table test.mv_mdl add index idx_s(s)")
	}()
	ddlFinished := false
	defer func() {
		release()
		if !ddlFinished {
			select {
			case <-ddlDone:
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up add index")
			}
		}
	}()

	select {
	case err := <-ddlDone:
		ddlFinished = true
		require.FailNowf(t, "ADD INDEX should wait on MDL held by internal refresh session", "err=%v", err)
	case <-mdlWaitCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for ADD INDEX to enter schema sync")
	}
	select {
	case err := <-ddlDone:
		ddlFinished = true
		require.FailNowf(t, "ADD INDEX should stay blocked until refresh commits", "err=%v", err)
	case <-time.After(500 * time.Millisecond):
	}

	release()
	select {
	case err := <-refreshDone:
		refreshFinished = true
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for refresh commit")
	}
	select {
	case err := <-ddlDone:
		ddlFinished = true
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for ADD INDEX to finish after refresh commits")
	}

	tkDDL.MustExec("admin check table test.mv_mdl")
	tkDDL.MustQuery("select g, s, cnt from test.mv_mdl order by g").Check(testkit.Rows("1 30 2", "2 30 1"))
}
