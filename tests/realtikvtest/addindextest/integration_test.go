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

package addindextest_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddIndexIngestMemoryUsage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	local.RunInTest = true

	tk.MustExec("create table t (a int, b int, c int);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	size := 100
	for i := 0; i < size; i++ {
		sb.WriteString(fmt.Sprintf("(%d, %d, %d)", i, i, i))
		if i != size-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(";")
	tk.MustExec(sb.String())
	require.Equal(t, int64(0), ingest.LitMemRoot.CurrentUsage())
	tk.MustExec("alter table t add index idx(a);")
	tk.MustExec("alter table t add unique index idx1(b);")
	tk.MustExec("admin check table t;")
	require.Equal(t, int64(0), ingest.LitMemRoot.CurrentUsage())
	require.NoError(t, local.LastAlloc.CheckRefCnt())
}

func TestAddIndexIngestLimitOneBackend(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use addindexlit;")
	tk2.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk2.MustExec("create table t2 (a int, b int);")
	tk2.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3);")

	// Mock there is a running ingest job.
	ingest.LitBackCtxMgr.Store(65535, &ingest.BackendContext{})
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		tk.MustExec("alter table t add index idx(a);")
		wg.Done()
	}()
	go func() {
		tk2.MustExec("alter table t2 add index idx_b(b);")
		wg.Done()
	}()
	wg.Wait()
	rows := tk.MustQuery("admin show ddl jobs 2;").Rows()
	require.Len(t, rows, 2)
	require.False(t, strings.Contains(rows[0][3].(string) /* job_type */, "ingest"))
	require.False(t, strings.Contains(rows[1][3].(string) /* job_type */, "ingest"))
	require.Equal(t, rows[0][7].(string) /* row_count */, "3")
	require.Equal(t, rows[1][7].(string) /* row_count */, "3")

	// Remove the running ingest job.
	ingest.LitBackCtxMgr.Delete(65535)
	tk.MustExec("alter table t add index idx_a(a);")
	rows = tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	require.True(t, strings.Contains(rows[0][3].(string) /* job_type */, "ingest"))
	require.Equal(t, rows[0][7].(string) /* row_count */, "3")
}

func TestAddIndexIngestWriterCountOnPartitionTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t (a int primary key) partition by hash(a) partitions 32;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 100; i++ {
		sb.WriteString(fmt.Sprintf("(%d)", i))
		if i != 99 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())
	tk.MustExec("alter table t add index idx(a);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestIngestMVIndexOnPartitionTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 4;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 256; i++ {
		sb.WriteString(fmt.Sprintf("(%d, '[%d, %d, %d]')", i, i+1, i+2, i+3))
		if i != 256-1 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())
	tk.MustExec("alter table t add index idx((cast(a as signed array)));")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	tk.MustExec("admin check table t")

	tk.MustExec("drop table t")
	tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 4;")
	tk.MustExec(sb.String())
	var wg sync.WaitGroup
	wg.Add(1)
	var addIndexDone atomic.Bool
	go func() {
		n := 10240
		internalTK := testkit.NewTestKit(t, store)
		internalTK.MustExec("use addindexlit;")

		for i := 0; i < 1024; i++ {
			internalTK.MustExec(fmt.Sprintf("insert into t values (%d, '[%d, %d, %d]')", n, n, n+1, n+2))
			internalTK.MustExec(fmt.Sprintf("delete from t where pk = %d", n-10))
			internalTK.MustExec(fmt.Sprintf("update t set a = '[%d, %d, %d]' where pk = %d", n-3, n-2, n+1000, n-5))
			n++
			if i > 256 && addIndexDone.Load() {
				break
			}
		}
		wg.Done()
	}()
	tk.MustExec("alter table t add index idx((cast(a as signed array)));")
	addIndexDone.Store(true)
	wg.Wait()
	tk.MustExec("admin check table t")
}

func TestAddIndexIngestAdjustBackfillWorker(t *testing.T) {
	if variable.DDLEnableDistributeReorg.Load() {
		t.Skip("dist reorg didn't support checkBackfillWorkerNum, skip this test")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("create table t (a int primary key);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 20; i++ {
		sb.WriteString(fmt.Sprintf("(%d000)", i))
		if i != 19 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())
	tk.MustQuery("split table t between (0) and (20000) regions 20;").Check(testkit.Rows("19 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	done := make(chan error, 1)
	atomic.StoreInt32(&ddl.TestCheckWorkerNumber, 1)
	testutil.SessionExecInGoroutine(store, "addindexlit", "alter table t add index idx(a);", done)
	checkNum := 0

	running := true
	cnt := [3]int{1, 2, 4}
	offset := 0
	for running {
		select {
		case err := <-done:
			require.NoError(t, err)
			running = false
		case wg := <-ddl.TestCheckWorkerNumCh:
			offset = (offset + 1) % 3
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_worker_cnt=%d", cnt[offset]))
			atomic.StoreInt32(&ddl.TestCheckWorkerNumber, int32(cnt[offset])/2+1)
			checkNum++
			wg.Done()
		}
	}

	require.Greater(t, checkNum, 5)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	tk.MustExec("admin check table t;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 4;")
}

func TestAddIndexIngestAdjustBackfillWorkerCountFail(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	ingest.ImporterRangeConcurrencyForTest = &atomic.Int32{}
	ingest.ImporterRangeConcurrencyForTest.Store(2)
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 20;")
	tk.MustExec("create table t (a int primary key);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 20; i++ {
		sb.WriteString(fmt.Sprintf("(%d000)", i))
		if i != 19 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())
	tk.MustQuery("split table t between (0) and (20000) regions 20;").Check(testkit.Rows("19 1"))
	tk.MustExec("alter table t add index idx(a);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 4;")
	ingest.ImporterRangeConcurrencyForTest = nil
}

func TestAddIndexIngestGeneratedColumns(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	assertLastNDDLUseIngest := func(n int) {
		tk.MustExec("admin check table t;")
		rows := tk.MustQuery(fmt.Sprintf("admin show ddl jobs %d;", n)).Rows()
		require.Len(t, rows, n)
		for i := 0; i < n; i++ {
			jobTp := rows[i][3].(string)
			require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
		}
	}
	tk.MustExec("create table t (a int, b int, c int as (b+10), d int as (b+c), primary key (a) clustered);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(c, a);")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("alter table t add index idx3(d);")
	tk.MustExec("alter table t add index idx4(d, c);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 11 12", "2 2 12 14", "3 3 13 16"))
	assertLastNDDLUseIngest(5)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c char(10) as (concat(b, 'x')), d int, e char(20) as (c));")
	tk.MustExec("insert into t (a, b, d) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(a, c);")
	tk.MustExec("alter table t add index idx2(c(7));")
	tk.MustExec("alter table t add index idx3(e(5));")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1x 1 1x", "2 2 2x 2 2x", "3 3 3x 3 3x"))
	assertLastNDDLUseIngest(4)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c tinyint, d int as (a + c), e bigint as (d - a), primary key(b, a) clustered);")
	tk.MustExec("insert into t (a, b, c) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(d);")
	tk.MustExec("alter table t add index idx1(b(2), d);")
	tk.MustExec("alter table t add index idx2(d, c);")
	tk.MustExec("alter table t add index idx3(e);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1 2 1", "2 2 2 4 2", "3 3 3 6 3"))
	assertLastNDDLUseIngest(4)
}

func TestAddIndexIngestEmptyTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec("create table t (a int);")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("alter table t add index idx(a);")

	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestAddIndexIngestRestoredData(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec(`
		CREATE TABLE tbl_5 (
		  col_21 time DEFAULT '04:48:17',
		  col_22 varchar(403) COLLATE utf8_unicode_ci DEFAULT NULL,
		  col_23 year(4) NOT NULL,
		  col_24 char(182) CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL,
		  col_25 set('Alice','Bob','Charlie','David') COLLATE utf8_unicode_ci DEFAULT NULL,
		  PRIMARY KEY (col_24(3)) /*T![clustered_index] CLUSTERED */,
		  KEY idx_10 (col_22)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
	`)
	tk.MustExec("INSERT INTO tbl_5 VALUES ('15:33:15','&U+x1',2007,'','Bob');")
	tk.MustExec("alter table tbl_5 add unique key idx_13 ( col_23 );")
	tk.MustExec("admin check table tbl_5;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestAddIndexIngestPanicOnCopRead(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockCopSenderPanic", "return(true)"))
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
	tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustExec("alter table t add index idx(b);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockCopSenderPanic"))
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][3].(string)
	// Fallback to txn-merge process.
	require.True(t, strings.Contains(jobTp, "txn-merge"), jobTp)
}

func TestAddIndexIngestUniqueKey(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t (a int primary key, b int);")
	tk.MustExec("insert into t values (1, 1), (10000, 1);")
	tk.MustExec("split table t by (5000);")
	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a varchar(255) primary key, b int);")
	tk.MustExec("insert into t values ('a', 1), ('z', 1);")
	tk.MustExec("split table t by ('m');")
	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a varchar(255) primary key, b int, c char(5));")
	tk.MustExec("insert into t values ('a', 1, 'c1'), ('d', 2, 'c1'), ('x', 1, 'c2'), ('z', 1, 'c1');")
	tk.MustExec("split table t by ('m');")
	tk.MustGetErrMsg("alter table t add unique index idx(b, c);", "[kv:1062]Duplicate entry '1-c1' for key 't.idx'")
}

func TestAddIndexIngestCancel(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	defHook := dom.DDL().GetHook()
	customHook := newTestCallBack(t, dom)
	cancelled := false
	customHook.OnJobRunBeforeExported = func(job *model.Job) {
		if cancelled {
			return
		}
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			idx := testutil.FindIdxInfo(dom, "addindexlit", "t", "idx")
			if idx == nil {
				return
			}
			if idx.BackfillState == model.BackfillStateRunning {
				tk2 := testkit.NewTestKit(t, store)
				rs, err := tk2.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
				assert.NoError(t, err)
				assert.NoError(t, rs.Close())
				cancelled = true
			}
		}
	}
	dom.DDL().SetHook(customHook)
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrCancelledDDLJob)
	require.True(t, cancelled)
	dom.DDL().SetHook(defHook)
	require.Empty(t, ingest.LitBackCtxMgr.Keys())
}

func TestAddIndexSplitTableRanges(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 8; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (80000) regions 7;").Check(testkit.Rows("6 1"))

	ddl.SetBackfillTaskChanSizeForTest(4)
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
	ddl.SetBackfillTaskChanSizeForTest(7)
	tk.MustExec("alter table t add index idx_2(b);")
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 8; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t by (10000),(20000),(30000),(40000),(50000),(60000);").Check(testkit.Rows("6 1"))
	ddl.SetBackfillTaskChanSizeForTest(4)
	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
	ddl.SetBackfillTaskChanSizeForTest(1024)
}

type testCallback struct {
	ddl.Callback
	OnJobRunBeforeExported func(job *model.Job)
}

func newTestCallBack(t *testing.T, dom *domain.Domain) *testCallback {
	defHookFactory, err := ddl.GetCustomizedHook("default_hook")
	require.NoError(t, err)
	return &testCallback{
		Callback: defHookFactory(dom),
	}
}

func (c *testCallback) OnJobRunBefore(job *model.Job) {
	if c.OnJobRunBeforeExported != nil {
		c.OnJobRunBeforeExported(job)
	}
}
