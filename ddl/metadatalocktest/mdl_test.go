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

//go:build !featuretag

package metadatalocktest

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestMDLStealRead(t *testing.T) {
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
	wg.Add(1)
	var ts2 time.Time
	var ts1 time.Time

	time.Sleep(2 * time.Second)

	tk.MustExec("start transaction read only as of timestamp NOW() - INTERVAL 1 SECOND")
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateMDLToETCDError", `3*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateMDLToETCDError"))
	}()

	tk.MustExec("alter table test.t add column c int")
}

// Tests that require MDL.
// They are here, since they must run without 'featuretag' defined
func TestExchangePartitionStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "partSchemaVer"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec(`set @@global.tidb_enable_metadata_lock = ON`)
	defer tk.MustExec(`set @@global.tidb_enable_metadata_lock = DEFAULT`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use " + dbName)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use " + dbName)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use " + dbName)
	tk.MustExec(`create table t (a int primary key, b varchar(255), key (b))`)
	tk.MustExec(`create table tp (a int primary key, b varchar(255), key (b)) partition by range (a) (partition p0 values less than (1000000), partition p1M values less than (2000000))`)
	tk.MustExec(`insert into t values (1, "1")`)
	tk.MustExec(`insert into tp values (2, "2")`)
	tk.MustExec(`analyze table t,tp`)
	var wg sync.WaitGroup
	wg.Add(1)
	dumpChan := make(chan struct{})
	defer func() {
		close(dumpChan)
		wg.Wait()
	}()
	go testkit.DebugDumpOnTimeout(&wg, dumpChan, 20*time.Second)
	tk.MustExec("BEGIN")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from tp`).Check(testkit.Rows("2 2"))
	alterChan := make(chan error)
	go func() {
		// WITH VALIDATION is the default
		err := tk2.ExecToErr(`alter table tp exchange partition p0 with table t`)
		alterChan <- err
	}()
	waitFor := func(tableName, s string, pos int) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk4.MustQuery(`admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = '` + tableName + `' and job_type = 'exchange partition'`).Rows()
			if len(res) == 1 && res[0][pos] == s {
				logutil.BgLogger().Info("Got state", zap.String("State", s))
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Sleep 50ms to wait load InfoSchema finish, issue #46815.
		time.Sleep(50 * time.Millisecond)
	}
	waitFor("t", "write only", 4)
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into t values (4,"4")`)
	tk3.MustContainErrMsg(`insert into t values (1000004,"1000004")`, "[table:1748]Found a row not matching the given partition set")
	tk.MustExec(`insert into t values (5,"5")`)
	// This should fail the alter table!
	tk.MustExec(`insert into t values (1000005,"1000005")`)

	// MDL will block the alter to not continue until all clients
	// are in StateWriteOnly, which tk is blocking until it commits
	tk.MustExec(`COMMIT`)
	waitFor("t", "rollback done", 11)
	// MDL will block the alter from finish, tk is in 'rollbacked' schema version
	// but the alter is still waiting for tk3 to commit, before continuing
	tk.MustExec("BEGIN")
	tk.MustExec(`insert into t values (1000006,"1000006")`)
	tk.MustExec(`insert into t values (6,"6")`)
	tk3.MustExec(`insert into t values (7,"7")`)
	tk3.MustContainErrMsg(`insert into t values (1000007,"1000007")`,
		"[table:1748]Found a row not matching the given partition set")
	tk3.MustExec("COMMIT")
	require.ErrorContains(t, <-alterChan,
		"[ddl:1737]Found a row that does not match the partition")
	tk3.MustExec(`BEGIN`)
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(
		"1 1", "1000005 1000005", "1000006 1000006", "5 5", "6 6"))
	tk.MustQuery(`select * from tp`).Sort().Check(testkit.Rows("2 2"))
	tk3.MustQuery(`select * from t`).Sort().Check(testkit.Rows(
		"1 1", "1000005 1000005", "4 4", "5 5", "7 7"))
	tk3.MustQuery(`select * from tp`).Sort().Check(testkit.Rows("2 2"))
	tk.MustContainErrMsg(`insert into t values (7,"7")`,
		"[kv:1062]Duplicate entry '7' for key 't.PRIMARY'")
	tk.MustExec(`insert into t values (8,"8")`)
	tk.MustExec(`insert into t values (1000008,"1000008")`)
	tk.MustExec(`insert into tp values (9,"9")`)
	tk.MustExec(`insert into tp values (1000009,"1000009")`)
	tk3.MustExec(`insert into t values (10,"10")`)
	tk3.MustExec(`insert into t values (1000010,"1000010")`)

	tk3.MustExec(`COMMIT`)
	tk.MustQuery(`show create table tp`).Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (1000000),\n" +
		" PARTITION `p1M` VALUES LESS THAN (2000000))"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec(`commit`)
	tk.MustExec(`insert into t values (11,"11")`)
	tk.MustExec(`insert into t values (1000011,"1000011")`)
	tk.MustExec(`insert into tp values (12,"12")`)
	tk.MustExec(`insert into tp values (1000012,"1000012")`)
}

func TestExchangePartitionMultiTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	dbName := "ExchangeMultiTable"
	tk1.MustExec(`create schema ` + dbName)
	tk1.MustExec(`use ` + dbName)
	tk1.MustExec(`set global tidb_enable_metadata_lock = 'ON'`)
	tk1.MustExec(`CREATE TABLE t1 (a int)`)
	tk1.MustExec(`CREATE TABLE t2 (a int)`)
	tk1.MustExec(`CREATE TABLE tp (a int) partition by hash(a) partitions 3`)
	tk1.MustExec(`insert into t1 values (0)`)
	tk1.MustExec(`insert into t2 values (3)`)
	tk1.MustExec(`insert into tp values (6)`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use ` + dbName)
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec(`use ` + dbName)
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec(`use ` + dbName)
	waitFor := func(col int, tableName, s string) {
		for {
			tk4 := testkit.NewTestKit(t, store)
			tk4.MustExec(`use test`)
			sql := `admin show ddl jobs where db_name = '` + strings.ToLower(dbName) + `' and table_name = '` + tableName + `' and job_type = 'exchange partition'`
			res := tk4.MustQuery(sql).Rows()
			if len(res) == 1 && res[0][col] == s {
				break
			}
			logutil.BgLogger().Info("No match", zap.String("sql", sql), zap.String("s", s))
			sql = `admin show ddl jobs`
			res = tk4.MustQuery(sql).Rows()
			for _, row := range res {
				strs := make([]string, 0, len(row))
				for _, c := range row {
					strs = append(strs, c.(string))
				}
				logutil.BgLogger().Info("admin show ddl jobs", zap.Strings("row", strs))
			}
			time.Sleep(100 * time.Millisecond)
		}
		// Sleep 50ms to wait load InfoSchema finish, issue #46815.
		time.Sleep(50 * time.Millisecond)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	dumpChan := make(chan struct{})
	defer func() {
		close(dumpChan)
		wg.Wait()
	}()
	go testkit.DebugDumpOnTimeout(&wg, dumpChan, 20*time.Second)
	alterChan1 := make(chan error)
	alterChan2 := make(chan error)
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into t1 values (1)`)
	tk3.MustExec(`insert into t2 values (2)`)
	tk3.MustExec(`insert into tp values (3)`)
	go func() {
		alterChan1 <- tk1.ExecToErr(`alter table tp exchange partition p0 with table t1`)
	}()
	waitFor(11, "t1", "running")
	go func() {
		alterChan2 <- tk2.ExecToErr(`alter table tp exchange partition p0 with table t2`)
	}()
	waitFor(11, "t2", "queueing")
	tk3.MustExec(`rollback`)
	logutil.BgLogger().Info("rollback done")
	//require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/exchangePartitionAutoID"))
	require.NoError(t, <-alterChan1)
	logutil.BgLogger().Info("alter1 done")
	err := <-alterChan2
	logutil.BgLogger().Info("alter2 done")
	tk3.MustQuery(`select * from t1`).Check(testkit.Rows("6"))
	logutil.BgLogger().Info("select t1 done")
	tk3.MustQuery(`select * from t2`).Check(testkit.Rows("0"))
	logutil.BgLogger().Info("select t2 done")
	tk3.MustQuery(`select * from tp`).Check(testkit.Rows("3"))
	logutil.BgLogger().Info("select tp done")
	require.NoError(t, err)
}
