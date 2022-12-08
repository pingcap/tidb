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
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexIngestMemoryUsage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

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

func TestAddIndexIngestAdjustBackfillWorker(t *testing.T) {
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
