// Copyright 2026 PingCAP, Inc.
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

package ttlworker_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func queryInt64(t *testing.T, tk *testkit.TestKit, sql string) int64 {
	gotStr := tk.MustQuery(sql).Rows()[0][0].(string)
	got, err := strconv.ParseInt(gotStr, 10, 64)
	require.NoError(t, err)
	return got
}

func requireEventually(t *testing.T, store kv.Storage, message string, cond func(tk *testkit.TestKit) bool) {
	require.Eventuallyf(t, func() bool {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		return cond(tk)
	}, 30*time.Second, 50*time.Millisecond, "%s", message)
}

func expireSoftDeletedRows(tk *testkit.TestKit, table string) {
	tk.MustExec("set @@tidb_translate_softdelete_sql=0")
	tk.MustExec(fmt.Sprintf(
		"update `%s` set _tidb_softdelete_time = _tidb_softdelete_time - interval 1 month where _tidb_softdelete_time is not null",
		table,
	))
	tk.MustExec("set @@tidb_translate_softdelete_sql=1")
}

func getDeletedRowsSumByJobType(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, table, jobType string) int64 {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr(table))
	require.NoError(t, err)
	parentTableID := tbl.Meta().ID
	return queryInt64(t, tk, fmt.Sprintf(
		"select ifnull(sum(deleted_rows),0) from mysql.tidb_ttl_job_history where job_type='%s' and status='finished' and parent_table_id=%d",
		jobType, parentTableID,
	))
}

func expireCreatedAtRows(tk *testkit.TestKit, table, where string) {
	tk.MustExec(fmt.Sprintf("update `%s` set created_at = created_at - interval 1 month where %s", table, where))
}

func TestSoftDeleteCleanupJobIntegration(t *testing.T) {
	defer boostJobScheduleForTest(t)()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_softdelete_job_enable='ON'")
	tk.MustExec("set global tidb_ttl_job_enable='ON'")
	tk.MustExec("set global tidb_ttl_job_schedule_window_start_time='00:00'")
	tk.MustExec("set global tidb_ttl_job_schedule_window_end_time='23:59'")
	tk.MustExec("set global tidb_ttl_running_tasks = 256")

	t.Run("non-partitioned deletes expired tombstone", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd")
		tk.MustExec("create table sd(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON'")
		sdDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd", "softdelete")
		tk.MustExec("insert into sd values (1, 10), (2, 20), (3, 30)")
		tk.MustExec("delete from sd where a in (1, 2)")
		expireSoftDeletedRows(tk, "sd")

		requireEventually(t, store, "eventually verify sd cleanup and deleted_rows", func(tk *testkit.TestKit) bool {
			return queryInt64(t, tk, "select count(*) from sd where a=1") == 0 &&
				queryInt64(t, tk, "select count(*) from sd where a=2") == 0 &&
				queryInt64(t, tk, "select count(*) from sd where a=3") == 1 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd", "softdelete") >= sdDeletedRowsBase+2
		})
	})

	t.Run("partitioned deletes expired tombstone", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_p")
		tk.MustExec("create table sd_p(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON' partition by range(a) (partition p0 values less than (10), partition p1 values less than (100))")
		sdPDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_p", "softdelete")
		tk.MustExec("insert into sd_p values (1, 10), (20, 200)")
		tk.MustExec("delete from sd_p where a in (1, 20)")
		expireSoftDeletedRows(tk, "sd_p")

		requireEventually(t, store, "eventually verify sd_p cleanup and deleted_rows", func(tk *testkit.TestKit) bool {
			return queryInt64(t, tk, "select count(*) from sd_p where a=20") == 0 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_p", "softdelete") >= sdPDeletedRowsBase+2
		})
	})

	t.Run("expired row can not be recovered after cleanup", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_recover_fail")
		tk.MustExec("create table sd_recover_fail(a int primary key, v int) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m'")
		sdRecoverFailDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_recover_fail", "softdelete")
		tk.MustExec("insert into sd_recover_fail values (1, 10), (2, 20)")
		tk.MustExec("delete from sd_recover_fail where a=1")
		expireSoftDeletedRows(tk, "sd_recover_fail")

		requireEventually(t, store, "eventually verify sd_recover_fail cleaned and not visible", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			cleaned := queryInt64(t, tk, "select count(*) from sd_recover_fail where a=1") == 0
			tk.MustExec("set @@tidb_translate_softdelete_sql=1")
			visibleAlive := queryInt64(t, tk, "select count(*) from sd_recover_fail where a=2") == 1
			return cleaned &&
				visibleAlive &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_recover_fail", "softdelete") >= sdRecoverFailDeletedRowsBase+1
		})

		tk.MustExec("recover values from sd_recover_fail where a=1")
		tk.MustQuery("select count(*) from sd_recover_fail where a=1").Check(testkit.Rows("0"))
	})

	t.Run("recover succeeds within retention window", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_recover_ok")
		tk.MustExec("create table sd_recover_ok(a int primary key, v int) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m'")
		tk.MustExec("insert into sd_recover_ok values (1, 10), (2, 20)")
		tk.MustExec("delete from sd_recover_ok where a=1")
		tk.MustQuery("select count(*) from sd_recover_ok where a=1").Check(testkit.Rows("0"))
		tk.MustExec("recover values from sd_recover_ok where a=1")
		tk.MustQuery("select count(*) from sd_recover_ok where a=1").Check(testkit.Rows("1"))
	})

	t.Run("table softdelete_job_enable off prevents cleanup", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_job_table_off")
		tk.MustExec("create table sd_job_table_off(a int primary key, v int) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m'")
		sdJobTableOffDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_job_table_off", "softdelete")
		tk.MustExec("insert into sd_job_table_off values (1, 10), (2, 20)")
		tk.MustExec("delete from sd_job_table_off where a=1")
		tk.MustExec("alter table sd_job_table_off softdelete_job_enable='OFF'")
		expireSoftDeletedRows(tk, "sd_job_table_off")

		requireEventually(t, store, "eventually verify table-off keeps tombstone and no deleted_rows", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			return queryInt64(t, tk, "select count(*) from sd_job_table_off where a=1 and _tidb_softdelete_time is not null") == 1 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_job_table_off", "softdelete") == sdJobTableOffDeletedRowsBase
		})

		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustQuery("select count(*) from sd_job_table_off where a=1").Check(testkit.Rows("0"))
		tk.MustExec("recover values from sd_job_table_off where a=1")
		tk.MustQuery("select count(*) from sd_job_table_off where a=1").Check(testkit.Rows("1"))
	})

	t.Run("global tidb_softdelete_job_enable off prevents cleanup", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_job_global_off")
		tk.MustExec("create table sd_job_global_off(a int primary key, v int) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m'")
		sdJobGlobalOffDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_job_global_off", "softdelete")
		tk.MustExec("insert into sd_job_global_off values (1, 10), (2, 20)")
		tk.MustExec("delete from sd_job_global_off where a=1")
		tk.MustExec("set global tidb_softdelete_job_enable='OFF'")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_softdelete_job_enable='ON'")
		})
		expireSoftDeletedRows(tk, "sd_job_global_off")

		requireEventually(t, store, "eventually verify global-off keeps tombstone and no deleted_rows", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			return queryInt64(t, tk, "select count(*) from sd_job_global_off where a=1 and _tidb_softdelete_time is not null") == 1 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_job_global_off", "softdelete") == sdJobGlobalOffDeletedRowsBase
		})

		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustQuery("select count(*) from sd_job_global_off where a=1").Check(testkit.Rows("0"))
		tk.MustExec("recover values from sd_job_global_off where a=1")
		tk.MustQuery("select count(*) from sd_job_global_off where a=1").Check(testkit.Rows("1"))
	})

	t.Run("tidb_translate_softdelete_sql off does not affect cleanup job", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_translate_off")
		tk.MustExec("create table sd_translate_off(a int primary key, v int) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m'")
		sdTranslateOffDeletedRowsBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_translate_off", "softdelete")
		tk.MustExec("insert into sd_translate_off values (1, 10), (2, 20)")
		tk.MustExec("delete from sd_translate_off where a=1")
		tk.MustExec("set global tidb_translate_softdelete_sql='OFF'")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_translate_softdelete_sql='ON'")
		})
		expireSoftDeletedRows(tk, "sd_translate_off")

		requireEventually(t, store, "eventually verify translate-off still cleans and updates deleted_rows", func(tk *testkit.TestKit) bool {
			return queryInt64(t, tk, "select count(*) from sd_translate_off where a=1") == 0 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_translate_off", "softdelete") >= sdTranslateOffDeletedRowsBase+1
		})

		tk.MustExec("recover values from sd_translate_off where a=1")
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustQuery("select count(*) from sd_translate_off where a=1").Check(testkit.Rows("0"))
	})

	t.Run("softdelete retention shorter than ttl", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_ttl_soft_first")
		tk.MustExec("create table sd_ttl_soft_first(a int primary key, v int, created_at datetime) softdelete retention 5 minute softdelete_job_enable='ON' softdelete_job_interval='1m' ttl = `created_at` + interval 10 minute ttl_job_interval='1m'")
		tk.MustExec("insert into sd_ttl_soft_first values (1, 10, now())")
		phase1SoftDeleteBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_soft_first", "softdelete")
		phase1TTLBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_soft_first", "ttl")

		tk.MustExec("delete from sd_ttl_soft_first where a=1")
		expireSoftDeletedRows(tk, "sd_ttl_soft_first")
		requireEventually(t, store, "eventually verify retention shorter than ttl: softdelete hard-deletes row 1", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			return queryInt64(t, tk, "select count(*) from sd_ttl_soft_first where a=1") == 0 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_soft_first", "softdelete") >= phase1SoftDeleteBase+1 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_soft_first", "ttl") == phase1TTLBase
		})

		expireCreatedAtRows(tk, "sd_ttl_soft_first", "a=1")
		requireEventually(t, store, "eventually verify ttl sees empty target after softdelete removed row 1", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			return queryInt64(t, tk, "select count(*) from sd_ttl_soft_first where a=1") == 0 &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_soft_first", "ttl") == phase1TTLBase
		})
	})

	t.Run("ttl shorter than softdelete retention", func(t *testing.T) {
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("drop table if exists sd_ttl_ttl_first")
		tk.MustExec("create table sd_ttl_ttl_first(a int primary key, v int, created_at datetime) softdelete retention 10 minute softdelete_job_enable='OFF' softdelete_job_interval='1m' ttl = `created_at` + interval 5 minute ttl_job_interval='1m'")
		tk.MustExec("insert into sd_ttl_ttl_first values (2, 20, now())")
		phase2SoftDeleteBase := getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_ttl_first", "softdelete")

		expireCreatedAtRows(tk, "sd_ttl_ttl_first", "a=2")
		requireEventually(t, store, "eventually verify ttl shorter than retention: live row 2 turns into tombstone before hard delete", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			row2IsTombstone := queryInt64(t, tk, "select count(*) from sd_ttl_ttl_first where a=2 and _tidb_softdelete_time is not null") == 1
			tk.MustExec("set @@tidb_translate_softdelete_sql=1")
			row2Invisible := queryInt64(t, tk, "select count(*) from sd_ttl_ttl_first where a=2") == 0
			return row2IsTombstone &&
				row2Invisible &&
				getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_ttl_first", "softdelete") == phase2SoftDeleteBase
		})

		expireSoftDeletedRows(tk, "sd_ttl_ttl_first")
		tk.MustExec("alter table sd_ttl_ttl_first softdelete_job_enable='ON'")
		requireEventually(t, store, "eventually verify softdelete hard-deletes row 2 after ttl soft-deletes it", func(tk *testkit.TestKit) bool {
			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			rawCount := queryInt64(t, tk, "select count(*) from sd_ttl_ttl_first where a=2")
			softdeleteDeletedRows := getDeletedRowsSumByJobType(t, tk, dom, "sd_ttl_ttl_first", "softdelete")
			return rawCount == 0 && softdeleteDeletedRows >= phase2SoftDeleteBase+1
		})
	})
}
