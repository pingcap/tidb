// Copyright 2018 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
)

func TestInsertOnDuplicateKeyWithBinlog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	failpoint.Enable("github.com/pingcap/tidb/pkg/table/tblsession/forceWriteBinlog", "return")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/table/tblsession/forceWriteBinlog")
	testInsertOnDuplicateKey(t, tk)
}

func testInsertOnDuplicateKey(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 1"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 300"))

	tk.MustExec(`insert into t1 values(1, 1) on duplicate key update b1 = 400;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	tk.MustGetErrMsg(`insert into t1 select * from t2 on duplicate key update c = t2.b;`,
		`[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	tk.MustGetErrMsg(`insert into t1 select * from t2 on duplicate key update a = b;`,
		`[planner:1052]Column 'b' in field list is ambiguous`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	tk.MustGetErrMsg(`insert into t1 select * from t2 on duplicate key update c = b;`,
		`[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustGetErrMsg(`insert into t1 select * from t2 on duplicate key update a1 = values(b2);`,
		`[planner:1054]Unknown column 'b2' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));`)
	tk.MustExec(`insert into t (val, k1, k2) values (3, 1, 2);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 4`))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 double, k2 double, v double, primary key(k1, k2));`)
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 2, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	require.Equal(t, uint64(5), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 1, 2);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	require.Equal(t, uint64(4), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 3  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 2`, `3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int, c int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 2, 1, 2);`)
	tk.MustExec(`insert into t1 values (3, 3, 2, 2);`)
	tk.MustExec(`insert into t1 values (4, 4, 2, 2);`)
	tk.MustExec(`create table t2(a int primary key, b int, c int, unique(b), unique(c));`)
	tk.MustExec(`insert into t2 select a, b, c from t1 order by id on duplicate key update b=t2.b, c=t2.c;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 1 1`, `3 2 2`))

	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	require.Equal(t, uint64(5), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 5  Duplicates: 0  Warnings: 0")
	tk.MustExec(`insert into t1 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b=b+10`)
	require.Equal(t, uint64(7), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")

	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(x int primary key)")
	tk.MustExec("create table b(x int, y int)")
	tk.MustExec("insert into a values(1)")
	tk.MustExec("insert into b values(1, 2)")
	tk.MustExec("insert into a select x from b ON DUPLICATE KEY UPDATE a.x=b.y")
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.Rows("2"))

	// Test issue 28078.
	// Use different types of columns so that there's likely to be error if the types mismatches.
	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(id int, a1 timestamp, a2 varchar(10), a3 float, unique(id))")
	tk.MustExec("create table b(id int, b1 time, b2 varchar(10), b3 int)")
	tk.MustExec("insert into a values (1, '2022-01-04 07:02:04', 'a', 1.1), (2, '2022-01-04 07:02:05', 'b', 2.2)")
	tk.MustExec("insert into b values (2, '12:34:56', 'c', 10), (3, '01:23:45', 'd', 20)")
	tk.MustExec("insert into a (id) select id from b on duplicate key update a.a2 = b.b2, a.a3 = 3.3")
	require.Equal(t, uint64(3), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>"))
	tk.MustExec("insert into a (id) select 4 from b where b3 = 20 on duplicate key update a.a3 = b.b3")
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>",
		"4/<nil>/<nil>/<nil>"))
	tk.MustExec("insert into a (a2, a3) select 'x', 1.2 from b on duplicate key update a.a2 = b.b3")
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>",
		"4/<nil>/<nil>/<nil>",
		"<nil>/<nil>/x/1.2",
		"<nil>/<nil>/x/1.2"))

	// Test issue 56829
	tk.MustExec(`
		CREATE TABLE cache (
			cache_key varchar(512) NOT NULL,
			updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			expired_at datetime GENERATED ALWAYS AS (if(expires > 0, date_add(updated_at, interval expires second), date_add(updated_at, interval 99 year))) VIRTUAL,
			expires int(11),
			PRIMARY KEY (cache_key) /*T![clustered_index] CLUSTERED */,
			KEY idx_c_on_expired_at (expired_at)
		)`)
	tk.MustExec("INSERT INTO cache(cache_key, expires) VALUES ('2001-01-01 11:11:11', 60) ON DUPLICATE KEY UPDATE expires = expires + 1")
	tk.MustExec("select sleep(1)")
	tk.MustExec("INSERT INTO cache(cache_key, expires) VALUES ('2001-01-01 11:11:11', 60) ON DUPLICATE KEY UPDATE expires = expires + 1")
	tk.MustExec("admin check table cache")
	rs1 := tk.MustQuery("select cache_key, expired_at from cache use index() order by cache_key")
	rs2 := tk.MustQuery("select cache_key, expired_at from cache use index(idx_c_on_expired_at) order by cache_key")
	require.True(t, rs1.Equal(rs2.Rows()))

	// reproduce insert on duplicate key update bug under new row format.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	tk.MustQuery(`select * from t1 use index(primary)`).Check(testkit.Rows(`1.0000`))
}

func TestAllocateContinuousRowID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int, key I_a(a));`)
	var wg util.WaitGroupWrapper
	for i := range 5 {
		idx := i
		wg.Run(func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for j := range 10 {
				k := strconv.Itoa(idx*100 + j)
				sql := "insert into t1(a,b) values (" + k + ", 2)"
				for range 20 {
					sql += ",(" + k + ",2)"
				}
				tk.MustExec(sql)
				q := "select _tidb_rowid from t1 where a=" + k
				rows := tk.MustQuery(q).Rows()
				require.Equal(t, 21, len(rows))
				last := 0
				for _, r := range rows {
					require.Equal(t, 1, len(r))
					v, err := strconv.Atoi(r[0].(string))
					require.Equal(t, nil, err)
					if last > 0 {
						require.Equal(t, v, last+1)
					}
					last = v
				}
			}
		})
	}
	wg.Wait()
}

func TestAutoRandomID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs := tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a')`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random(15), name char(10))`)
	overflowVal := 1 << (64 - 5)
	errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, overflowVal, 1<<(64-16)-1)
	tk.MustContainErrMsg(fmt.Sprintf("alter table ar auto_random_base = %d", overflowVal), errMsg)
}

func TestMultiAutoRandomID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null),(null),(null)`)
	rs := tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0),(0),(0)`)
	rs = tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a'),('a'),('a')`)
	rs = tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}

func TestAutoRandomIDAllowZero(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	rs := tk.MustQuery(`select @@session.sql_mode`)
	sqlMode := rs.Rows()[0][0].(string)
	tk.MustExec(fmt.Sprintf(`set session sql_mode="%s,%s"`, sqlMode, "NO_AUTO_VALUE_ON_ZERO"))

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, 0, firstValue)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}
func TestInsertRuntimeStat(t *testing.T) {
	stats := &executor.InsertRuntimeStat{
		BasicRuntimeStats:    &execdetails.BasicRuntimeStats{},
		SnapshotRuntimeStats: nil,
		CheckInsertTime:      2 * time.Second,
		Prefetch:             1 * time.Second,
	}
	stats.BasicRuntimeStats.Record(5*time.Second, 1)
	require.Equal(t, "prepare: 3s, check_insert: {total_time: 2s, mem_insert_time: 1s, prefetch: 1s}", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	newStats := stats.Clone()
	newStats.(*executor.InsertRuntimeStat).BasicRuntimeStats.Record(5*time.Second, 1)
	stats.Merge(newStats)
	require.Equal(t, "prepare: 6s, check_insert: {total_time: 4s, mem_insert_time: 2s, prefetch: 2s}", stats.String())
	stats.FKCheckTime = time.Second
	require.Equal(t, "prepare: 6s, check_insert: {total_time: 4s, mem_insert_time: 2s, prefetch: 2s, fk_check: 1s}", stats.String())
}

func TestDuplicateEntryMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	for _, enable := range []vardef.ClusteredIndexDefMode{vardef.ClusteredIndexDefModeOn, vardef.ClusteredIndexDefModeOff, vardef.ClusteredIndexDefModeIntOnly} {
		tk.Session().GetSessionVars().EnableClusteredIndex = enable
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int, b char(10), unique key(b)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t value (34, '12Ak');")
		tk.MustGetErrMsg("insert into t value (34, '12Ak');", "[kv:1062]Duplicate entry '12Ak' for key 't.b'")

		tk.MustExec("begin optimistic;")
		tk.MustExec("insert into t value (34, '12ak');")
		tk.MustExec("delete from t where b = '12ak';")
		tk.MustGetErrMsg("commit;", "previous statement: delete from t where b = '12ak';: [kv:1062]Duplicate entry '12ak' for key 't.b'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime primary key);")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.PRIMARY'")

		tk.MustExec("begin optimistic;")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustExec("delete from t where a = '2020-01-01';")
		tk.MustGetErrMsg("commit;", "previous statement: delete from t where a = '2020-01-01';: [kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int primary key );")
		tk.MustExec("insert into t value (1);")
		tk.MustGetErrMsg("insert into t value (1);", "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime unique);")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.a'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime, b int, c varchar(10), primary key (a, b, c)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t values ('2020-01-01', 1, 'aSDd');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01', 1, 'ASDD');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 't.PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime, b int, c varchar(10), unique key (a, b, c)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t values ('2020-01-01', 1, 'aSDd');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01', 1, 'ASDD');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 't.a'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a char(10) collate utf8mb4_unicode_ci, b char(20) collate utf8mb4_general_ci, c int(11), primary key (a, b, c), unique key (a));")
		tk.MustExec("insert ignore into t values ('$', 'C', 10);")
		tk.MustExec("insert ignore into t values ('$', 'C', 10);")
		tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1062|Duplicate entry '$-C-10' for key 't.PRIMARY'"))

		tk.MustExec("begin pessimistic;")
		tk.MustExec("insert into t values ('a7', 'a', 10);")
		tk.MustGetErrMsg("insert into t values ('a7', 'a', 10);", "[kv:1062]Duplicate entry 'a7-a-10' for key 't.PRIMARY'")
		tk.MustExec("rollback;")

		// Test for large unsigned integer handle.
		// See https://github.com/pingcap/tidb/issues/12420.
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a bigint unsigned primary key);")
		tk.MustExec("insert into t values(18446744073709551615);")
		tk.MustGetErrMsg("insert into t values(18446744073709551615);", "[kv:1062]Duplicate entry '18446744073709551615' for key 't.PRIMARY'")
	}
}

func TestGlobalTempTableParallel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists temp_test")
	tk.MustExec("create global temporary table temp_test(id int primary key auto_increment) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_test")

	threads := 8
	loops := 1
	var wg util.WaitGroupWrapper

	insertFunc := func() {
		newTk := testkit.NewTestKit(t, store)
		newTk.MustExec("use test")
		newTk.MustExec("begin")
		for range loops {
			newTk.MustExec("insert temp_test value(0)")
			newTk.MustExec("insert temp_test value(0), (0)")
		}
		maxID := strconv.Itoa(loops * 3)
		newTk.MustQuery("select max(id) from temp_test").Check(testkit.Rows(maxID))
		newTk.MustExec("commit")
	}

	for range threads {
		wg.Run(insertFunc)
	}
	wg.Wait()
}

func TestInsertLockUnchangedKeys(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	for _, shouldLock := range []bool{false} {
		for _, tt := range []struct {
			name          string
			ddl           string
			dml           string
			isClusteredPK bool
		}{
			{
				"replace-pk",
				"create table t (c int primary key clustered)",
				"replace into t values (1)",
				true,
			},
			{
				"replace-uk",
				"create table t (c int unique key)",
				"replace into t values (1)",
				false,
			},
			{
				"insert-ignore-pk",
				"create table t (c int primary key clustered)",
				"insert ignore into t values (1)",
				true,
			},
			{
				"insert-ignore-uk",
				"create table t (c int unique key)",
				"insert ignore into t values (1)",
				false,
			},
			{
				"insert-update-pk",
				"create table t (c int primary key clustered)",
				"insert into t values (1) on duplicate key update c = values(c)",
				true,
			},
			{
				"insert-update-uk",
				"create table t (c int unique key)",
				"insert into t values (1) on duplicate key update c = values(c)",
				false,
			},
		} {
			t.Run(
				tt.name+"-"+strconv.FormatBool(shouldLock), func(t *testing.T) {
					tk1.MustExec(fmt.Sprintf("set @@tidb_lock_unchanged_keys = %v", shouldLock))
					tk1.MustExec("drop table if exists t")
					tk1.MustExec(tt.ddl)
					tk1.MustExec("insert into t values (1)")
					tk1.MustExec("begin")
					tk1.MustExec(tt.dml)
					errCh := make(chan error)
					go func() {
						_, err := tk2.Exec("insert into t values (1)")
						errCh <- err
					}()
					select {
					case <-errCh:
						if shouldLock {
							require.Failf(t, "txn2 is not blocked by %q", tt.dml)
						}
						close(errCh)
					case <-time.After(200 * time.Millisecond):
						if !shouldLock && !tt.isClusteredPK {
							require.Failf(t, "txn2 is blocked by %q", tt.dml)
						}
					}
					tk1.MustExec("commit")
					<-errCh
					tk1.MustQuery("select * from t").Check(testkit.Rows("1"))
				},
			)
		}
	}
}

func TestMySQLInsertID(t *testing.T) {
	// mysql_insert_id() differs from LAST_INSERT_ID()
	// See https://github.com/pingcap/tidb/issues/55965
	// mysql_insert_id() is got from tk.Session().LastInsertID()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists tb")
	tk.MustExec("create table tb(pk int primary key auto_increment, a int, b int, unique(a))")
	defer tk.MustExec("drop table if exists tb")

	tk.MustExec("insert into tb (a, b) values (1, 1) on duplicate key update b = values(b)")
	require.Equal(t, tk.Session().LastInsertID(), uint64(1))

	tk.MustExec("insert into tb (a, b) values (2, 2) on duplicate key update b = values(b)")
	require.Equal(t, tk.Session().LastInsertID(), uint64(2))

	// If there is an AUTO_INCREMENT column in the table and there were some explicit successfully
	// inserted values or some updated values, return the last of the inserted or updated values.
	// Ref https://dev.mysql.com/doc/c-api/5.7/en/mysql-insert-id.html#:~:text=When%20called%20after%20an%20INSERT%20...%20ON,of%20the%20inserted%20or%20updated%20values
	tk.MustExec("insert into tb (a, b) values (1, 2) on duplicate key update b = values(b)")
	require.Equal(t, tk.Session().LastInsertID(), uint64(1))
	tk.MustQuery("select LAST_INSERT_ID()").Check(testkit.Rows("2"))

	tk.MustQuery("select * from tb").Sort().Check(testkit.Rows("1 1 2", "2 2 2"))

	// When the new row and the old row are exactly the same (no inserted or updated values), mysql_insert_id() is 0
	tk.MustExec("insert into tb (a, b) values (1, 2) on duplicate key update b = 2")
	require.Equal(t, tk.Session().LastInsertID(), uint64(0))
	tk.MustQuery("select LAST_INSERT_ID()").Check(testkit.Rows("2"))

	// When the value of auto increment column is assigned explicitly, LAST_INSERT_ID() is unchanged.
	// mysql_insert_id() is set to the explicit assigned value.
	tk.MustExec("insert into tb values (6, 6, 6)")
	require.Equal(t, tk.Session().LastInsertID(), uint64(6))
	tk.MustQuery("select LAST_INSERT_ID()").Check(testkit.Rows("2"))

	// Update statement touches neigher mysql_insert_id() nor LAST_INSERT_ID()
	tk.MustExec("update tb set b = 7, pk = pk + 1 where b = 6")
	require.Equal(t, tk.Session().LastInsertID(), uint64(0))
	tk.MustQuery("select LAST_INSERT_ID()").Check(testkit.Rows("2"))

	// How to distinguish LAST_INSERT_ID() and mysql_insert_id()?
	// In a word, LAST_INSERT_ID() is always get from auto allocated value, while mysql_insert_id() can be
	// auto allocated or explicited specified.

	// Another scenario mentioned by @lcwangcao
	// What's the behaviour when transaction conflict involved?
	tk.MustExec("truncate table tb")
	tk.MustExec("insert into tb (a, b) values (1, 1), (2, 2)")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update tb set b = 2 where a = 1")
	go func() {
		time.Sleep(100 * time.Millisecond)
		tk1.MustExec("commit")
	}()
	// The first time this will update one row.
	// Then transaction conflict and retry, in the second time it modify nothing.
	tk.MustExec("insert into tb(a, b) values(1,2) on duplicate key update b = 2;")
	require.Equal(t, tk.Session().LastInsertID(), uint64(0))
}

func TestInsertNullInNonStrictMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int primary key, col1 varchar(10) not null default '')")
	tk.MustExec("create table t2 (id int primary key, col1 varchar(10))")
	tk.MustExec("insert into t2 values (1, null)")
	tk.MustExec("insert ignore into t1 values(5, null)")

	tk.MustExec("set session sql_mode = ''")

	err := tk.ExecToErr("insert into t1 values(1, null)")
	require.EqualError(t, err, table.ErrColumnCantNull.GenWithStackByArgs("col1").Error())

	err = tk.ExecToErr("insert into t1 set id = 1, col1 = null")
	require.EqualError(t, err, table.ErrColumnCantNull.GenWithStackByArgs("col1").Error())

	err = tk.ExecToErr("insert t1 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = null")
	require.EqualError(t, err, table.ErrColumnCantNull.GenWithStackByArgs("col1").Error())

	tk.MustExec("insert into t1 select * from t2")
	tk.MustExec("insert into t1 values(2, null), (3, 3), (4, 4)")
	tk.MustExec("update t1 set col1 = null where id = 3")
	tk.MustExec("insert ignore t1 VALUES (4, 4) ON DUPLICATE KEY UPDATE col1 = null")
	tk.MustQuery("select * from t1").Check(testkit.RowsWithSep("|", "1|", "2|", "3|", "4|", "5|"))
}

func TestInsertLargeRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ver := tk.MustQuery("select tidb_version()").Rows()[0][0].(string)
	if !strings.Contains(ver, "Store: unistore") {
		t.Skipf("Only support 'Store: unistore'\n%s", ver)
	}
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, b longtext)")
	tk.MustExec("set tidb_txn_entry_size_limit = 1<<23")
	// the unistore arena blocksize is 8MB (8388608 bytes), so Unistore cannot handle larger rows than that!
	// since a row cannot span multiple arena blocks.
	tk.MustContainErrMsg("insert into t values (1, REPEAT('t',8388493))", "unistore lock entry too big")
}

func TestInsertDuplicateToGeneratedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`
		CREATE TABLE tmv (
  			J1 json,
			J2 json GENERATED ALWAYS AS (j1) VIRTUAL,
  			UNIQUE KEY i1 ((cast(j1 as signed array))),
  			KEY i2 ((cast(j2 as signed array))))`)
	tk.MustExec("insert into tmv set j1 = '[1]'")
	tk.MustExec("insert ignore into tmv set j1 = '[1]' on duplicate key update j1 = '[2]'")
	for _, enabled := range []bool{false, true} {
		tk.MustExec(fmt.Sprintf("set @@tidb_enable_fast_table_check = %v", enabled))
		tk.MustExec("admin check table tmv;")
	}

	tk.MustExec(`
		CREATE TABLE ttime (
			id int primary key,
			t1 datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			t2 datetime GENERATED ALWAYS AS (date_add(t1, interval 1 day)) VIRTUAL,
			t3 datetime GENERATED ALWAYS AS (date_add(t2, interval 1 day)) VIRTUAL,
			t4 datetime GENERATED ALWAYS AS (date_sub(t2, interval 10 MINUTE)) VIRTUAL,
			UNIQUE KEY i1 (t1),
			UNIQUE KEY i2 (t2),
			UNIQUE KEY i3 (id, t3),
			KEY i4 (t4))`)
	tk.MustExec(`insert into ttime set id = 1, t1 = "2011-12-20 17:15:50"`)
	tk.MustExec("insert into ttime set id = 1 on duplicate key update id = 2")
	for _, enabled := range []bool{false, true} {
		tk.MustExec(fmt.Sprintf("set @@tidb_enable_fast_table_check = %v", enabled))
		tk.MustExec("admin check table ttime;")
	}
}

func TestInsertNullIntoNotNullGenerated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t3(
		id int primary key,
		c1 varchar(16) default null,
		c2 varchar(16) GENERATED ALWAYS AS (concat(c1, c1)) VIRTUAL NOT NULL,
		KEY idx (c2)
	)`)
	tk.ExecToErr(`insert into t3 set id = 2, c1 = null`)
	tk.MustExec(`insert into t3(id, c1) values(1, "aaaa")`)
	tk.MustExec(`insert ignore into t3 set id = 1, c1 = "bbbb" on duplicate key update id = 2, c1 = null`)

	// The following behavior is strange, but it is compatible with MySQL.
	// update is failed because c2 is null
	tk.ExecToErr(`insert into t3 set id = 2, c1 = "cccc" on duplicate key update c1 = "dddd"`)

	// But this row can't be deleted by c2 is null
	tk.MustExec(`delete from t3 where c2 is null`)
	rs := tk.MustQuery(`select * from t3`).Rows()
	require.Len(t, rs, 1)

	// We need to delete it by c2 = ""
	tk.MustExec(`delete from t3 where c2 = ""`)
	tk.MustQuery(`select * from t3`).Check(testkit.Rows())
}

func TestXXX(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (id int primary key, v int) active_active='on' softdelete retention 7 day")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("update t set _tidb_origin_ts = 1 where id = 2")
	tk.MustExec("update t set _tidb_softdelete_time = now() where id = 3")

	// mock CDC operation
	tk.MustExec("set @@tidb_cdc_write_source = 1")
	tk.MustExec("set @@tidb_translate_softdelete_sql = 0")
	// 1 duplicate and overwrite
	// 2 duplicate and skip
	// 3 softdeleted, no duplicate
	tk.MustExec(`insert into t (id, v, _tidb_origin_ts, _tidb_softdelete_time) values (1, 11, 666, NULL), (2, 22, 666, NULL), (3, 33, 666, NULL)
		on duplicate key update v = IF((@cond := (IFNULL(_tidb_origin_ts, _tidb_commit_ts) <= VALUES(_tidb_origin_ts))), VALUES(v), v),
		_tidb_softdelete_time = IF(@cond, VALUES(_tidb_softdelete_time), _tidb_softdelete_time),
		_tidb_origin_ts = IF(@cond, VALUES(_tidb_origin_ts), _tidb_origin_ts)`)
	tk.MustQuery("select id, v, _tidb_origin_ts from t where _tidb_softdelete_time is null").Check(testkit.Rows("1 1 <nil>", "2 22 666", "3 33 666"))
	tk.MustQuery("select @@tidb_active_active_sync_stats").Check(testkit.Rows("{\"conflict_skip_rows\": 1}"))
}
