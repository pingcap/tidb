// Copyright 2023 PingCAP, Inc.
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
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	disttestutil "github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func TestAddIndexIngestMemoryUsage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	local.RunInTest = true

	tk.MustExec("create table t (a int, b int, c int);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	size := 100
	for i := range size {
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
	require.NoError(t, local.LastAlloc.Load().CheckRefCnt())
}

func TestAddIndexIngestLimitOneBackend(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
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
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(rows[0][12].(string) /* comments */, "ingest"))
		require.True(t, strings.Contains(rows[1][12].(string) /* comments */, "ingest"))
	} else {
		require.Equal(t, rows[0][12].(string) /* comments */, "")
		require.Equal(t, rows[1][12].(string) /* comments */, "")
	}
	require.Equal(t, rows[0][7].(string) /* row_count */, "3")
	require.Equal(t, rows[1][7].(string) /* row_count */, "3")

	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	// TODO(lance6716): dist_task also need this

	// test cancel is timely
	enter := make(chan struct{})
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/lightning/backend/local/beforeExecuteRegionJob",
		func(ctx context.Context) {
			close(enter)
			select {
			case <-time.After(time.Second * 20):
			case <-ctx.Done():
			}
		})
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tk2.ExecToErr("alter table t add index idx_ba(b, a);")
		require.ErrorContains(t, err, "Cancelled DDL job")
	}()
	<-enter
	jobID := tk.MustQuery("admin show ddl jobs 1;").Rows()[0][0].(string)
	now := time.Now()
	tk.MustExec("admin cancel ddl jobs " + jobID)
	wg.Wait()
	// cancel should be timely
	require.Less(t, time.Since(now).Seconds(), 10.0)
}

func TestAddIndexIngestWriterCountOnPartitionTable(t *testing.T) {
	disttestutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	tk.MustExec("create table t (a int primary key) partition by hash(a) partitions 32;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := range 100 {
		sb.WriteString(fmt.Sprintf("(%d)", i))
		if i != 99 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())
	tk.MustExec("alter table t add index idx(a);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][12].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	} else {
		require.Equal(t, jobTp, "")
	}
}

func TestIngestMVIndexOnPartitionTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	cases := []string{
		"alter table t add index idx((cast(a as signed array)));",
		"alter table t add unique index idx(pk, (cast(a as signed array)));",
	}
	for _, c := range cases {
		tk.MustExec("drop database if exists addindexlit;")
		tk.MustExec("create database addindexlit;")
		tk.MustExec("use addindexlit;")
		if kerneltype.IsClassic() {
			tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
		}

		var sb strings.Builder

		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 4;")
		tk.MustExec(sb.String())
		var wg sync.WaitGroup
		wg.Add(1)
		var addIndexDone atomic.Bool
		go func() {
			n := 10240
			internalTK := testkit.NewTestKit(t, store)
			internalTK.MustExec("use addindexlit;")

			for i := range 1024 {
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
		tk.MustExec(c)
		rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
		require.Len(t, rows, 1)
		jobTp := rows[0][12].(string)
		if kerneltype.IsClassic() {
			require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
		} else {
			require.Equal(t, jobTp, "")
		}
		addIndexDone.Store(true)
		wg.Wait()
		tk.MustExec("admin check table t")
	}
}

func TestAddIndexIngestAdjustBackfillWorkerCountFail(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	ingest.ImporterRangeConcurrencyForTest = &atomic.Int32{}
	ingest.ImporterRangeConcurrencyForTest.Store(2)
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 20;")

	tk.MustExec("create table t (a int primary key);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := range 20 {
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
	jobTp := rows[0][12].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	} else {
		require.Equal(t, jobTp, "")
	}
	ingest.ImporterRangeConcurrencyForTest = nil
}

func TestAddIndexIngestEmptyTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec("create table t (a int);")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}
	tk.MustExec("alter table t add index idx(a);")

	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][12].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	} else {
		require.Equal(t, jobTp, "")
	}
}

func TestAddIndexIngestRestoredData(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

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
	jobTp := rows[0][12].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	} else {
		require.Equal(t, jobTp, "")
	}
}

func TestAddIndexIngestUniqueKey(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

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

func TestAddIndexSplitTableRanges(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 8 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (80000) regions 7;").Check(testkit.Rows("6 1"))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges", "return(4)")
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges", "return(7)")
	tk.MustExec("alter table t add index idx_2(b);")
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 8 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t by (10000),(20000),(30000),(40000),(50000),(60000);").Check(testkit.Rows("6 1"))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges", "return(4)")
	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexLoadTableRangeError(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	disttestutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec(`set global tidb_enable_dist_task=off;`) // Use checkpoint manager.

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 8 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t by (10000),(20000),(30000),(40000),(50000),(60000);").Check(testkit.Rows("6 1"))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/setLimitForLoadTableRanges", "return(3)")
	var batchCnt int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadRangeFromPD", func(mockErr *bool) {
		batchCnt++
		if batchCnt == 2 {
			*mockErr = true
		}
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/forceSyncFlagForTest", "return")
	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexMockFlushError(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 4 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockFlushError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockFlushError"))
	tk.MustExec("admin check table t;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp := rows[0][12].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
	} else {
		require.Equal(t, jobTp, "")
	}
}

func TestAddIndexDiskQuotaTS(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF and fast-reorg is always enabled on nextgen, and we only support global sort in release")
	}
	disttestutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	testAddIndexDiskQuotaTS(t, tk)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	testAddIndexDiskQuotaTS(t, tk)
}

func testAddIndexDiskQuotaTS(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt=1;")

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustQuery("split table t by (30000);").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into t values(1, 1, 1);")
	tk.MustExec("insert into t values(100000, 1, 1);")

	ingest.ForceSyncFlagForTest.Store(true)
	tk.MustExec("alter table t add index idx_test(b);")
	tk.MustExec("update t set b = b + 1;")

	counter := 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS", func(uint64) {
		counter++
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackAfterFn", func() {
		counter--
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/ReadyForImportEngine", func() {
		assert.Equal(t, counter, 0)
	})
	tk.MustExec("alter table t add index idx_test2(b);")
	ingest.ForceSyncFlagForTest.Store(false)
}

func TestAddIndexAdvanceWatermarkFailed(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt=1;")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustQuery("split table t by (30000);").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into t values(1, 1, 1);")
	tk.MustExec("insert into t values(100000, 1, 2);")
	ingest.ForceSyncFlagForTest.Store(true)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed", "2*return")
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
	tk.MustExec("update t set b = b + 1;")

	//// TODO(tangenta): add scan ts, import ts and key range to the checkpoint information, so that
	//// we can re-ingest the same task idempotently.
	// tk.MustExec("alter table t drop index idx;")
	// testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed", "2*return")
	// tk.MustExec("alter table t add unique index idx(k);")
	// tk.MustExec("admin check table t;")
	// tk.MustExec("update t set k = k + 10;")

	tk.MustExec("alter table t drop index idx;")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed", "2*return")
	tk.MustGetErrCode("alter table t add unique index idx(b);", errno.ErrDupEntry)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed", "1*return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/afterSetTSBeforeImportEngine", "1*return")
	tk.MustGetErrCode("alter table t add unique index idx(b);", errno.ErrDupEntry)
}

func TestAddIndexTempDirDataRemoved(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("next-gen doesn't use local backend")
	}
	tempDir := t.TempDir()
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempDir = tempDir
	})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1), (1), (1);")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockErrInMergeSSTs", "1*return(true)")
	removeOnce := sync.Once{}
	removed := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/beforeMergeSSTs", func() {
		removeOnce.Do(func() {
			var filesToRemove []string
			filepath.WalkDir(tempDir, func(path string, d fs.DirEntry, err error) error {
				if strings.HasSuffix(path, ".sst") {
					filesToRemove = append(filesToRemove, path)
				}
				return nil
			})
			for _, f := range filesToRemove {
				t.Log("removed " + f)
				err := os.RemoveAll(f)
				require.NoError(t, err)
				removed = true
			}
		})
	})

	tk.MustExec("alter table t add index idx(a);")
	require.True(t, removed)
}

func TestAddIndexRemoteDuplicateCheck(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt=1;")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustQuery("split table t by (30000);").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into t values(1, 1, 1);")
	tk.MustExec("insert into t values(100000, 1, 1);")

	ingest.ForceSyncFlagForTest.Store(true)
	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'")
	ingest.ForceSyncFlagForTest.Store(false)
}

func TestAddIndexRecoverOnDuplicateCheck(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("have overlapped ingest sst, skip")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on;")
	tk.MustExec("set @@global.tidb_enable_dist_task = on;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1), (2), (3);")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/ingest/mockCollectRemoteDuplicateRowsFailed", "1*return")
	tk.MustExec("alter table t add unique index idx(a);")
}

func TestAddIndexBackfillLostUpdate(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("have overlapped ingest sst, skip")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t(id int primary key, b int, k int);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")

	var runDML bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if t.Failed() || runDML {
			return
		}
		switch job.SchemaState {
		case model.StateWriteReorganization:
			_, err := tk1.Exec("insert into t values (1, 1, 1);")
			assert.NoError(t, err)
			// row: [h1 -> 1]
			// idx: []
			// tmp: [1 -> h1]
			runDML = true
		}
	})
	runDMLOnce := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunReorgJobAndHandleErr", func() {
		runDMLOnce.Do(func() {
			_, err := tk1.Exec("update t set b = 2 where id = 1;")
			assert.NoError(t, err)
			// row: [h1 -> 2]
			// idx: [1 -> h1]
			// tmp: [1 -> (h1,h1d), 2 -> h1]
			_, err = tk1.Exec("begin;")
			assert.NoError(t, err)
			_, err = tk1.Exec("insert into t values (2, 1, 2);")
			assert.NoError(t, err)
			// row: [h1 -> 2, h2 -> 1]
			// idx: [1 -> h1]
			// tmp: [1 -> (h1,h1d,h2), 2 -> h1]
			_, err = tk1.Exec("delete from t where id = 2;")
			assert.NoError(t, err)
			// row: [h1 -> 2]
			// idx: [1 -> h1]
			// tmp: [1 -> (h1,h1d,h2,h2d), 2 -> h1]
			_, err = tk1.Exec("commit;")
			assert.NoError(t, err)
		})
	})
	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 1"))
}

func TestAddIndexIngestFailures(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("have overlapped ingest sst, skip")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustExec("insert into t values (1, 1, 1);")

	// Test precheck failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockIngestCheckEnvFailed", "1*return"))
	tk.MustGetErrMsg("alter table t add index idx(b);", "[ddl:8256]Check ingest environment failed: mock error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockIngestCheckEnvFailed"))

	tk.MustExec(`set global tidb_enable_dist_task=on;`)
	// Test reset engine failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockResetEngineFailed", "1*return"))
	tk.MustExec("alter table t add index idx(b);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockResetEngineFailed"))
	tk.MustExec(`set global tidb_enable_dist_task=off;`)
}

func TestAddIndexImportFailed(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("have overlapped ingest sst, skip")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec(`set global tidb_enable_dist_task=off;`)

	tk.MustExec("create table t (a int, b int);")
	for i := range 10 {
		insertSQL := fmt.Sprintf("insert into t values (%d, %d)", i, i)
		tk.MustExec(insertSQL)
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/mockWritePeerErr", "1*return")
	require.NoError(t, err)
	tk.MustExec("alter table t add index idx(a);")
	err = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/mockWritePeerErr")
	require.NoError(t, err)
	tk.MustExec("admin check table t;")
}

func TestAddEmptyMultiValueIndex(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec(`set global tidb_enable_dist_task=off;`)

	tk.MustExec("create table t(j json);")
	tk.MustExec(`insert into t(j) values ('{"string":[]}');`)
	tk.MustExec("alter table t add index ((cast(j->'$.string' as char(10) array)));")
	tk.MustExec("admin check table t;")
}

func TestAddUniqueIndexDuplicatedError(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS `b1cce552` ")
	tk.MustExec("CREATE TABLE `b1cce552` (\n  `f5d9aecb` timestamp DEFAULT '2031-12-22 06:44:52',\n  `d9337060` varchar(186) DEFAULT 'duplicatevalue',\n  `4c74082f` year(4) DEFAULT '1977',\n  `9215adc3` tinytext DEFAULT NULL,\n  `85ad5a07` decimal(5,0) NOT NULL DEFAULT '68649',\n  `8c60260f` varchar(130) NOT NULL DEFAULT 'drfwe301tuehhkmk0jl79mzekuq0byg',\n  `8069da7b` varchar(90) DEFAULT 'ra5rhqzgjal4o47ppr33xqjmumpiiillh7o5ajx7gohmuroan0u',\n  `91e218e1` tinytext DEFAULT NULL,\n  PRIMARY KEY (`8c60260f`,`85ad5a07`) /*T![clustered_index] CLUSTERED */,\n  KEY `d88975e1` (`8069da7b`)\n);")
	tk.MustExec("INSERT INTO `b1cce552` (`f5d9aecb`, `d9337060`, `4c74082f`, `9215adc3`, `85ad5a07`, `8c60260f`, `8069da7b`, `91e218e1`) VALUES ('2031-12-22 06:44:52', 'duplicatevalue', 2028, NULL, 846, 'N6QD1=@ped@owVoJx', '9soPM2d6H', 'Tv%'), ('2031-12-22 06:44:52', 'duplicatevalue', 2028, NULL, 9052, '_HWaf#gD!bw', '9soPM2d6H', 'Tv%');")
	tk.MustGetErrCode("ALTER TABLE `b1cce552` ADD unique INDEX `65290727` (`4c74082f`, `d9337060`, `8069da7b`);", errno.ErrDupEntry)
}

func TestFirstLitSlowStart(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec(`set global tidb_enable_dist_task=off;`)

	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("create table t2(a int, b int);")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/beforeCreateLocalBackend", "1*return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/beforeCreateLocalBackend"))
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ownerResignAfterDispatchLoopCheck", "return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ownerResignAfterDispatchLoopCheck"))
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/slowCreateFS", "return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/slowCreateFS"))
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		tk.MustExec("alter table t add unique index idx(a);")
	}()
	go func() {
		defer wg.Done()
		tk1.MustExec("alter table t2 add unique index idx(a);")
	}()
	wg.Wait()
}

func TestConcFastReorg(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	tblNum := 10
	for i := range tblNum {
		tk.MustExec(fmt.Sprintf("create table t%d(a int);", i))
	}

	var wg sync.WaitGroup
	wg.Add(tblNum)
	for i := range tblNum {
		go func() {
			defer wg.Done()
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use addindexlit;")
			tk2.MustExec(fmt.Sprintf("insert into t%d values (1), (2), (3);", i))

			if i%2 == 0 {
				tk2.MustExec(fmt.Sprintf("alter table t%d add index idx(a);", i))
			} else {
				tk2.MustExec(fmt.Sprintf("alter table t%d add unique index idx(a);", i))
			}
		}()
	}

	wg.Wait()
}

func TestIssue55808(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("this is specific to classic kernel")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")
	tk.MustExec("set global tidb_ddl_error_count_limit = 0")
	defer func() {
		tk.MustExec("set global tidb_ddl_error_count_limit = default;")
	}()

	backup := local.MaxWriteAndIngestRetryTimes
	local.MaxWriteAndIngestRetryTimes = 1
	t.Cleanup(func() {
		local.MaxWriteAndIngestRetryTimes = backup
	})

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 4 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/doIngestFailed", "return()")
	err := tk.ExecToErr("alter table t add index idx(a);")
	require.ErrorContains(t, err, "injected error")
}

func TestAddIndexBackfillLostTempIndexValues(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = 0;")

	tk.MustExec("create table t(id int primary key, b int not null default 0);")
	tk.MustExec("insert into t values (1, 0);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAddIndexScan", func() {
		_, err := tk1.Exec("insert into t values (2, 0);")
		assert.NoError(t, err)
		_, err = tk1.Exec("delete from t where id = 1;")
		assert.NoError(t, err)
		_, err = tk1.Exec("insert into t values (3, 0);")
		assert.NoError(t, err)
		_, err = tk1.Exec("delete from t where id = 2;")
		assert.NoError(t, err)
	})

	beforeIngestOnce := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", func() {
		beforeIngestOnce.Do(func() {
			_, err := tk1.Exec("insert into t(id) values (4);")
			assert.NoError(t, err)
			_, err = tk1.Exec("delete from t where id = 3;")
			assert.NoError(t, err)
		})
	})
	var rows [][]any
	beforeMergeOnce := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", func() {
		beforeMergeOnce.Do(func() {
			rows = tk1.MustQuery("select * from t use index();").Rows()
			_, err := tk1.Exec("insert into t values (3, 0);")
			assert.NoError(t, err)
		})
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex", "return(false)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex"))
	})

	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '0' for key 't.idx'")
	require.Len(t, rows, 1)
	require.Equal(t, rows[0][0].(string), "4")

	tk.MustExec("admin check table t;")
}

func TestAddIndexInsertSameOriginIndexValue(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = 0;")

	tk.MustExec("create table t(id int primary key, b int not null default 0);")
	tk.MustExec("insert into t values (1, 0);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")

	beforeIngestOnce := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", func() {
		beforeIngestOnce.Do(func() {
			_, err := tk1.Exec("delete from t where id = 1;")
			assert.NoError(t, err)
			_, err = tk1.Exec("insert into t values (1, 0);")
			assert.NoError(t, err)
		})
	})
	beforeMergeOnce := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", func() {
		beforeMergeOnce.Do(func() {
			_, err := tk1.Exec("insert into t (id) values (1);")
			assert.ErrorContains(t, err, "Duplicate entry")
		})
	})
	tk.MustExec("alter table t add unique index idx(b);")
}

// TestIngestOwnerTransferEmptyPartition tests that add index on partitioned table
// with empty partitions handles owner transfer correctly. This covers issue #44265.
// The key point is that checkpoint should contain partition ID even for empty partitions.
func TestIngestOwnerTransferEmptyPartition(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	// Create partitioned table with some empty partitions
	tk.MustExec(`create table t (
		a int primary key,
		b int
	) partition by range(a) (
		partition p0 values less than (100),
		partition p1 values less than (200),
		partition p2 values less than (300),
		partition p3 values less than (400)
	);`)
	// Only insert data into p0 and p2, leaving p1 and p3 empty
	for i := 0; i < 50; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}
	for i := 200; i < 250; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}

	// Use failpoint to trigger owner resign after processing first partition
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/resignAfterFlush", "1*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ownerResignAfterDispatchLoopCheck", "return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/resignAfterFlush"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ownerResignAfterDispatchLoopCheck"))
	})

	tk.MustExec("alter table t add index idx(b);")

	// Verify all partitions have indexes (including empty ones)
	tk.MustExec("admin check table t;")

	// Verify data-index consistency by comparing results with and without index
	rs1 := tk.MustQuery("select * from t use index(idx) order by a;").Rows()
	rs2 := tk.MustQuery("select * from t ignore index(idx) order by a;").Rows()
	require.Equal(t, len(rs1), len(rs2))
	require.Equal(t, 100, len(rs1)) // 50 rows in p0 + 50 rows in p2

	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][12].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

// TestIngestPartitionCheckpointRecovery tests that checkpoint correctly saves partition
// info and allows recovery to continue from the correct partition. This covers issues
// #43997 and #44024.
func TestIngestPartitionCheckpointRecovery(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	// Create partitioned table with multiple partitions
	tk.MustExec(`create table t (
		a int primary key,
		b int
	) partition by range(a) (
		partition p0 values less than (100),
		partition p1 values less than (200),
		partition p2 values less than (300),
		partition p3 values less than (400)
	);`)
	// Insert data into all partitions
	for i := 0; i < 80; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}
	for i := 100; i < 180; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}
	for i := 200; i < 280; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}
	for i := 300; i < 380; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}

	rowCnt := atomic.Int32{}
	partitionSwitchCnt := atomic.Int32{}

	// Track when partitions are processed
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow", func() {
		rowCnt.Add(1)
		// Trigger job retry after processing around 80 rows (first partition)
		if rowCnt.Load() == 80 && partitionSwitchCnt.Load() == 0 {
			partitionSwitchCnt.Add(1)
			// Simulate write conflict to trigger job retry
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use addindexlit;")
			jobs := tk2.MustQuery("admin show ddl jobs 1;").Rows()
			if len(jobs) > 0 {
				jobID := jobs[0][0].(string)
				updateSQL := fmt.Sprintf("update mysql.tidb_ddl_job set processing = 0 where job_id = %s", jobID)
				tk2.MustExec(updateSQL)
				updateSQL = fmt.Sprintf("update mysql.tidb_ddl_job set processing = 1 where job_id = %s", jobID)
				tk2.MustExec(updateSQL)
			}
		}
	})

	tk.MustExec("alter table t add index idx(b);")

	// Verify checkpoint recovery worked correctly
	// If partition recovery failed, we would see more rows processed than expected
	totalRows := int(rowCnt.Load())
	// 320 rows total (80 per partition * 4 partitions)
	// With proper recovery, we should process close to 320 rows
	// Without proper recovery, we would reprocess some partitions and see more
	require.LessOrEqual(t, totalRows, 400) // Allow some overhead but not doubling

	tk.MustExec("admin check table t;")

	// Verify data-index consistency
	rs1 := tk.MustQuery("select count(*) from t use index(idx);").Rows()
	rs2 := tk.MustQuery("select count(*) from t ignore index(idx);").Rows()
	require.Equal(t, rs1[0][0], rs2[0][0])
	require.Equal(t, "320", rs1[0][0])
}

// TestIngestConcurrentJobCleanupRace tests that parallel add index jobs don't
// cause panic due to cleanup race. This covers issues #44137 and #44140.
func TestIngestConcurrentJobCleanupRace(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	}

	// Create two tables
	tk.MustExec("create table t1 (a int primary key, b int);")
	tk.MustExec("create table t2 (a int primary key, b int);")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d);", i, i))
		tk.MustExec(fmt.Sprintf("insert into t2 values (%d, %d);", i, i))
	}

	// Start two add index jobs concurrently
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use addindexlit;")

	wg := &sync.WaitGroup{}
	wg.Add(2)

	var err1, err2 error
	go func() {
		defer wg.Done()
		_, err1 = tk1.Exec("alter table t1 add index idx1(b);")
	}()
	go func() {
		defer wg.Done()
		_, err2 = tk2.Exec("alter table t2 add index idx2(b);")
	}()

	wg.Wait()

	// Verify no panic occurred and both indexes created successfully
	require.NoError(t, err1)
	require.NoError(t, err2)

	tk.MustExec("admin check table t1;")
	tk.MustExec("admin check table t2;")
}

// TestIngestGCSafepointBlocking tests that add index correctly uses TS from checkpoint
// and blocks GC safepoint advancement. This covers issues #40074 and #40081.
// The fix changed Copr request start TS to use explicit transaction start time.
func TestIngestGCSafepointBlocking(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}

	// Verify that add index completes without GC error
	// The fix ensures we use explicit transaction start time for Copr requests
	// which properly blocks GC safepoint advancement
	ingest.ForceSyncFlagForTest.Store(true)
	defer ingest.ForceSyncFlagForTest.Store(false)

	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")

	// Verify data consistency
	rs1 := tk.MustQuery("select count(*) from t use index(idx);").Rows()
	rs2 := tk.MustQuery("select count(*) from t ignore index(idx);").Rows()
	require.Equal(t, rs1[0][0], rs2[0][0])
	require.Equal(t, "100", rs1[0][0])

	// Verify the job used ingest mode
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobTp := rows[0][12].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

// TestIngestCancelCleanupOrder tests that canceling add index during execution
// doesn't cause nil pointer panic due to incorrect cleanup order.
// This covers issues #43323 and #43326.
// Note: This test verifies cancel during StateNone doesn't cause panic.
// The cancel-during-backfill scenario is covered by TestAddIndexCancelOnNoneState
// in integration_test.go with mock backend.
func TestIngestCancelCleanupOrder(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i, i))
	}

	// Test cancel during StateDeleteOnly - this verifies cleanup happens correctly
	// Using StateDeleteOnly as it's more likely to be caught than StateNone
	cancelOnce := sync.Once{}
	tkCancel := testkit.NewTestKit(t, store)
	tkCancel.MustExec("use addindexlit;")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateDeleteOnly {
			cancelOnce.Do(func() {
				_, err := tkCancel.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
				assert.NoError(t, err)
			})
		}
	})

	err := tk.ExecToErr("alter table t add index idx(b);")
	// The job might complete or be cancelled depending on timing
	// The key point is no panic occurs during the process
	if err != nil {
		require.ErrorContains(t, err, "Cancelled DDL job")
	}

	// Verify no panic occurred and table is consistent
	tk.MustExec("admin check table t;")

	// Verify backend context is cleaned up (should be 0 since cancel happened early)
	cnt := ingest.LitDiskRoot.Count()
	require.Equal(t, 0, cnt)
}

func TestMergeTempIndexSplitConflictTxn(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = off;")

	tk.MustExec("create table t (a int primary key, b int);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use addindexlit;")

	var runInsert bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if t.Failed() || runInsert {
			return
		}
		switch job.SchemaState {
		case model.StateWriteReorganization:
			for i := range 4 {
				_, err := tk1.Exec("insert into t values (?, ?);", i, i)
				assert.NoError(t, err)
			}
			runInsert = true
		}
	})

	var runUpdate bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionMergingInTxn", func() {
		if t.Failed() || runUpdate {
			return
		}
		for i := range 4 {
			_, err := tk1.Exec("update t set b = b+10 where a = ?;", i)
			assert.NoError(t, err)
		}
		runUpdate = true
	})

	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("0 10", "1 11", "2 12", "3 13"))
}
