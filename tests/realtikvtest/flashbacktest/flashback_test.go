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

package flashbacktest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	tikvutil "github.com/tikv/client-go/v2/util"
)

// MockGC is used to make GC work in the test environment.
func MockGC(tk *testkit.TestKit) (string, string, string, func()) {
	originGC := ddlutil.IsEmulatorGCEnable()
	resetGC := func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddlutil.EmulatorGCDisable()
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(tikvutil.GCTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(tikvutil.GCTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	return timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC
}

func TestFlashback(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, index i(a))")
		tk.MustExec("insert t values (1), (2), (3)")

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		tk.MustExec("insert t values (4), (5), (6)")
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select max(a) from t").Rows()[0][0], "3")
		require.Equal(t, tk.MustQuery("select max(a) from t use index(i)").Rows()[0][0], "3")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestPrepareFlashbackFailed(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, index i(a))")
		tk.MustExec("insert t values (1), (2), (3)")

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockPrepareMeetsEpochNotMatch", `return(true)`))

		tk.MustExec("insert t values (4), (5), (6)")
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select max(a) from t").Rows()[0][0], "3")
		require.Equal(t, tk.MustQuery("select max(a) from t use index(i)").Rows()[0][0], "3")

		jobMeta := tk.MustQuery("select job_meta from mysql.tidb_ddl_history order by job_id desc limit 1").Rows()[0][0].(string)
		job := model.Job{}
		require.NoError(t, job.Decode([]byte(jobMeta)))
		require.Equal(t, job.ErrorCount, int64(0))

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockPrepareMeetsEpochNotMatch"))
	}
}

func TestFlashbackAddDropIndex(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, index i(a))")
		tk.MustExec("insert t values (1), (2), (3)")
		prevGCCount := tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0]

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("alter table t add index k(a)")
		require.Equal(t, tk.MustQuery("select max(a) from t use index(k)").Rows()[0][0], "3")
		tk.MustExec("alter table t drop index i")
		tk.MustGetErrCode("select max(a) from t use index(i)", errno.ErrKeyDoesNotExist)
		require.Greater(t, tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0], prevGCCount)

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		tk.MustExec("insert t values (4), (5), (6)")
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select max(a) from t use index(i)").Rows()[0][0], "3")
		tk.MustGetErrCode("select max(a) from t use index(k)", errno.ErrKeyDoesNotExist)
		require.Equal(t, tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0], prevGCCount)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackAddDropModifyColumn(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, index i(a))")
		tk.MustExec("insert t values (1, 1), (2, 2), (3, 3)")

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("alter table t add column c int")
		tk.MustExec("alter table t drop column b")
		tk.MustExec("alter table t modify column a tinyint")
		require.Equal(t, tk.MustQuery("show create table t").Rows()[0][1], "CREATE TABLE `t` (\n"+
			"  `a` tinyint(4) DEFAULT NULL,\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  KEY `i` (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		tk.MustExec("insert t values (4, 4), (5, 5), (6, 6)")
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("show create table t").Rows()[0][1], "CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL,\n"+
			"  KEY `i` (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
		require.Equal(t, tk.MustQuery("select max(b) from t").Rows()[0][0], "3")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackBasicRenameDropCreateTable(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t, t1, t2, t3")
		tk.MustExec("create table t(a int, index i(a))")
		tk.MustExec("insert t values (1), (2), (3)")
		tk.MustExec("create table t1(a int, index i(a))")
		tk.MustExec("insert t1 values (4), (5), (6)")
		prevGCCount := tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0]

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("rename table t to t3")
		tk.MustExec("drop table t1")
		tk.MustExec("create table t2(a int, index i(a))")
		tk.MustExec("insert t2 values (7), (8), (9)")

		require.Equal(t, tk.MustQuery("select max(a) from t3").Rows()[0][0], "3")
		require.Equal(t, tk.MustQuery("select max(a) from t2").Rows()[0][0], "9")

		require.Greater(t, tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0], prevGCCount)

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select max(a) from t").Rows()[0][0], "3")
		tk.MustExec("admin check table t1")
		require.Equal(t, tk.MustQuery("select max(a) from t1").Rows()[0][0], "6")
		require.Equal(t, tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0], prevGCCount)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackCreateDropTableWithData(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create table t(a int)")

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("insert into t values (1)")
		tk.MustExec("drop table t")
		tk.MustExec("create table t(b int)")
		tk.MustExec("insert into t(b) values (1)")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select count(a) from t").Rows()[0][0], "0")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackCreateDropSchema(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create table t(a int, index k(a))")
		tk.MustExec("insert into t values (1),(2)")

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("drop schema test")
		tk.MustExec("create schema test1")
		tk.MustExec("create schema test2")
		tk.MustExec("use test1")
		tk.MustGetErrCode("use test", errno.ErrBadDB)
		tk.MustExec("use test2")
		tk.MustExec("drop schema test2")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
		tk.MustExec("admin check table test.t")
		res := tk.MustQuery("select max(a) from test.t").Rows()
		require.Equal(t, res[0][0], "2")
		tk.MustGetErrCode("use test1", errno.ErrBadDB)
		tk.MustGetErrCode("use test2", errno.ErrBadDB)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackAutoID(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create table t(a int auto_increment, primary key(a)) auto_id_cache 100")
		tk.MustExec("insert into t values (),()")

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("insert into t values (),()")
		res := tk.MustQuery("select max(a) from test.t").Rows()
		require.Equal(t, res[0][0], "4")
		tk.MustExec("drop table t")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		res = tk.MustQuery("select max(a) from t").Rows()
		require.Equal(t, res[0][0], "2")
		tk.MustExec("insert into t values ()")
		res = tk.MustQuery("select max(a) from t").Rows()
		require.Equal(t, res[0][0], "101")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackSequence(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create sequence seq cache 100")
		res := tk.MustQuery("select nextval(seq)").Rows()
		require.Equal(t, res[0][0], "1")

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		res = tk.MustQuery("select nextval(seq)").Rows()
		require.Equal(t, res[0][0], "2")
		tk.MustExec("drop sequence seq")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		// flashback schema and skip cached values
		res = tk.MustQuery("select nextval(seq)").Rows()
		require.Equal(t, res[0][0], "101")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
		tk.MustExec("drop sequence seq")
	}
}

func TestFlashbackPartitionTable(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create table t(a int) partition by range(`a`) " +
			"(partition `a_1` values less than (25), " +
			"partition `a_2` values less than (75), " +
			"partition `a_3` values less than (200))")

		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("alter table t drop partition `a_3`")
		tk.MustExec("alter table t add partition (partition `a_3` values less than (300))")
		res := tk.MustQuery("select max(a) from t").Rows()
		require.Equal(t, res[0][0], "74")
		tk.MustExec("drop table t")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustExec("admin check table t")
		res = tk.MustQuery("select max(a), min(a), count(*) from t").Rows()
		require.Equal(t, res[0][0], "99")
		require.Equal(t, res[0][1], "0")
		require.Equal(t, res[0][2], "100")
		tk.MustExec("insert into t values (100), (-1)")
		res = tk.MustQuery("select max(a), min(a), count(*) from t").Rows()
		require.Equal(t, res[0][0], "100")
		require.Equal(t, res[0][1], "-1")
		require.Equal(t, res[0][2], "102")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackTmpTable(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("create temporary table t(a int)")

		// test flashback tmp table data
		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("insert into t values (1), (2), (3)")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		res := tk.MustQuery("select max(a) from t").Rows()
		require.Equal(t, res[0][0], "3")

		// test flashback tmp table schema
		time.Sleep(1 * time.Second)
		ts, err = tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		tk.MustExec("drop table t")

		injectSafeTS = oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

		tk.MustGetErrCode("select * from t", errno.ErrNoSuchTable)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
}

func TestFlashbackInProcessErrorMsg(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)

		originHook := dom.DDL().GetHook()

		tk := testkit.NewTestKit(t, store)
		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int)")

		time.Sleep(1 * time.Second)
		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		// do some ddl and dml
		tk.MustExec("alter table t add index k(a)")
		tk.MustExec("insert into t values (1), (2), (3)")

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		hook := newTestCallBack(t, dom)
		hook.OnJobRunBeforeExported = func(job *model.Job) {
			if job.Type == model.ActionFlashbackCluster && job.SchemaState == model.StateWriteReorganization {
				txn, err := store.Begin()
				assert.NoError(t, err)
				_, err = meta.NewMeta(txn).ListDatabases()
				errorMsg := err.Error()
				assert.Contains(t, errorMsg, "is in flashback progress, FlashbackStartTS is ")
				slices := strings.Split(errorMsg, "is in flashback progress, FlashbackStartTS is ")
				assert.Equal(t, len(slices), 2)
				assert.NotEqual(t, slices[1], "0")
				txn.Rollback()
			}
		}
		dom.DDL().SetHook(hook)
		tk.Exec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
		dom.DDL().SetHook(originHook)

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
	}
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
