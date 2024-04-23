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

package ingest_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddIndexIngestGeneratedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	assertLastNDDLUseIngest := func(n int) {
		tk.MustExec("admin check table t;")
		rows := tk.MustQuery(fmt.Sprintf("admin show ddl jobs %d;", n)).Rows()
		require.Len(t, rows, n)
		for i := 0; i < n; i++ {
			//nolint: forcetypeassert
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

func TestIngestError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set global tidb_enable_dist_task = 0")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 4; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderError"))
	tk.MustExec("admin check table t;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 4; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError"))
	tk.MustExec("admin check table t;")
	rows = tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp = rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestAddIndexIngestPanic(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("set global tidb_enable_dist_task = 0")

	// Mock panic on coprocessor request sender.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderPanic", "return(true)"))
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
	tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderPanic"))

	// Mock panic on local engine writer.
	tk.MustExec("drop table t;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic", "return"))
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
	tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic"))
}

func TestAddIndexIngestCancel(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

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
			idx := testutil.FindIdxInfo(dom, "test", "t", "idx")
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
	ok, err := ingest.LitBackCtxMgr.CheckMoreTasksAvailable(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
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

func TestIngestPartitionRowCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec(`create table t (a int, b int, c int as (b+10), d int as (b+c),
		primary key (a) clustered) partition by range (a) (
    		partition p0 values less than (1),
    		partition p1 values less than (2),
    		partition p2 values less than MAXVALUE);`)
	tk.MustExec("insert into t (a, b) values (0, 0), (1, 1), (2, 2);")
	tk.MustExec("alter table t add index idx(d);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	//nolint: forcetypeassert
	rowCount := rows[0][7].(string)
	require.Equal(t, "3", rowCount)
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestClientError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("CREATE TABLE t1 (f1 json);")
	tk.MustExec(`insert into t1(f1) values (cast("null" as json));`)
	tk.MustGetErrCode("create index i1 on t1((cast(f1 as unsigned array)));", errno.ErrInvalidJSONValueForFuncIndex)
}

func TestAddIndexCancelOnNoneState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkCancel := testkit.NewTestKit(t, store)
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("use test")
	tk.MustExec(`create table t (c1 int, c2 int, c3 int)`)
	tk.MustExec("insert into t values(1, 1, 1);")

	hook := &callback.TestDDLCallback{Do: dom}
	first := true
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateNone && first {
			_, err := tkCancel.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			assert.NoError(t, err)
			first = false
		}
	}
	dom.DDL().SetHook(hook.Clone())
	tk.MustGetErrCode("alter table t add index idx1(c1)", errno.ErrCancelledDDLJob)
	available, err := ingest.LitBackCtxMgr.CheckMoreTasksAvailable(context.Background())
	require.NoError(t, err)
	require.True(t, available)
}

func TestAddIndexIngestTimezone(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("SET time_zone = '-06:00';")
	tk.MustExec("create table t (`src` varchar(48),`t` timestamp,`timezone` varchar(100));")
	tk.MustExec("insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30','-6:00');")
	// Test Daylight time.
	tk.MustExec("insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','-6:00');")
	tk.MustExec("alter table t add index idx(t);")
	tk.MustExec("admin check table t;")

	tk.MustExec("alter table t drop index idx;")
	tk.MustExec("SET time_zone = 'Asia/Shanghai';")
	tk.MustExec("insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30', '+8:00');")
	tk.MustExec("insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','+8:00');")
	tk.MustExec("alter table t add index idx(t);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestMultiSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values(1, 1), (2, 2);")
	tk.MustExec("alter table t add index idx(a), add index idx_2(b);")
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t drop index idx, drop index idx_2;")
	tk.MustExec(`alter table t
		add unique index idx(a),
		add unique index idx_2(b, a),
		add unique index idx_3(b);`)
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table t;")
	tk.MustExec(`create table t (a int, b int, c int as (b+10), d int as (b+c),
		primary key (a) clustered) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than MAXVALUE);`)
	for i := 0; i < 30; i++ {
		insertSQL := fmt.Sprintf("insert into t (a, b) values (%d, %d);", i, i)
		tk.MustExec(insertSQL)
	}
	tk.MustExec("alter table t add index idx_a(a), add index idx_ab(a, b), add index idx_d(d);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexDuplicateMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer ingesttestutil.InjectMockBackendMgr(t, store)()

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustExec("insert into t values (1, 1, 1);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	var runDML bool
	var errDML error

	ingest.MockExecAfterWriteRow = func() {
		if runDML {
			return
		}
		_, errDML = tk1.Exec("insert into t values (2, 1, 2);")
		runDML = true
	}

	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'")

	require.NoError(t, errDML)
	require.True(t, runDML)
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1", "2 1 2"))
}

func TestMultiSchemaAddIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendMgr(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	for _, createTableSQL := range []string{
		"create table t (a int, b int);",
		"create table t (a int, b int) PARTITION BY HASH (`a`) PARTITIONS 4;",
	} {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createTableSQL)
		tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")

		first := true
		var tk2Err error
		ingest.MockExecAfterWriteRow = func() {
			if !first {
				return
			}
			_, tk2Err = tk2.Exec("insert into t values (4, 4), (5, 5);")
			first = false
		}
		tk.MustExec("alter table t add index idx1(a), add index idx2(b);")
		require.False(t, first)
		require.NoError(t, tk2Err)
		tk.MustExec("admin check table t;")
	}
}
