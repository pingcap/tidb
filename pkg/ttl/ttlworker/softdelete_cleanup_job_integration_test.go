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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func parseEventuallyExpectation(t *testing.T, expected string) (minOK bool, minValue int) {
	if !strings.HasPrefix(expected, ">=") {
		return false, 0
	}
	minv, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(expected, ">=")))
	require.NoError(t, err)
	return true, minv
}

func TestSoftDeleteCleanupJobIntegration(t *testing.T) {
	defer boostJobScheduleForTest(t)()
	defer overwriteJobInterval(t)()

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_softdelete_job_enable='ON'")
	tk.MustExec("set global tidb_ttl_job_enable='ON'")
	tk.MustExec("set global tidb_ttl_job_schedule_window_start_time='00:00'")
	tk.MustExec("set global tidb_ttl_job_schedule_window_end_time='23:59'")

	cases := []struct {
		name       string
		tblName    string
		setupSQLs  []string
		verifySQLs map[string]string
	}{
		{
			name:    "non-partitioned deletes expired tombstone",
			tblName: "sd",
			setupSQLs: []string{
				"create table sd(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON'",
				"insert into sd values (1, 10), (2, 20), (3, 30)",
				"delete from sd where a in (1, 2)",
			},
			verifySQLs: map[string]string{
				"select count(*) from sd where a=2":                                           "0",
				"select count(*) from sd where a=3":                                           "1",
				"select count(*) from mysql.tidb_softdelete_table_status":                     ">= 1",
				"select count(*) from mysql.tidb_ttl_job_history where job_type='softdelete'": ">= 1",
			},
		},
		{
			name:    "partitioned deletes expired tombstone",
			tblName: "sd_p",
			setupSQLs: []string{
				"create table sd_p(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON' partition by range(a) (partition p0 values less than (10), partition p1 values less than (100))",
				"insert into sd_p values (1, 10), (20, 200)",
				"delete from sd_p where a in (1, 20)",
			},
			verifySQLs: map[string]string{
				"select count(*) from sd_p where a=20":                                        "0",
				"select count(*) from mysql.tidb_softdelete_table_status":                     ">= 2",
				"select count(*) from mysql.tidb_ttl_job_history where job_type='softdelete'": ">= 1",
			},
		},
		{
			name:    "does not delete not expired",
			tblName: "sd_safe",
			setupSQLs: []string{
				"create table sd_safe(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON'",
				"insert into sd_safe values (1, 10)",
				"delete from sd_safe where a=1",
			},
			verifySQLs: map[string]string{
				"select count(*) from mysql.tidb_softdelete_table_status":                     ">= 1",
				"select count(*) from mysql.tidb_ttl_job_history where job_type='softdelete'": ">= 1",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Keep tidb_translate_softdelete_sql enabled for DML rewriting, but disable it when verifying tombstone rows
			// in user SQL (otherwise the rewritten SELECT will hide soft-deleted rows).
			tk.MustExec("set @@tidb_translate_softdelete_sql=1")

			tk.MustExec("drop table if exists " + c.tblName)
			for _, sql := range c.setupSQLs {
				tk.MustExec(sql)
			}

			tk.MustExec("set @@tidb_translate_softdelete_sql=0")
			tk.MustExec(fmt.Sprintf("update `%s` set _tidb_softdelete_time = _tidb_softdelete_time - interval 1 month where _tidb_softdelete_time is not null", c.tblName))
			tk.MustExec("set @@tidb_translate_softdelete_sql=1")

			// testify's Eventually runs the condition function in a separate goroutine.
			// Don't share the same session/TestKit with other statements in this test.
			verifyTK := testkit.NewTestKit(t, store)
			verifyTK.MustExec("use test")
			verifyTK.MustExec("set @@tidb_translate_softdelete_sql=1")

			for q, want := range c.verifySQLs {
				sql := q
				expected := want
				minOK, minv := parseEventuallyExpectation(t, expected)
				require.Eventuallyf(t, func() bool {
					gotStr := verifyTK.MustQuery(sql).Rows()[0][0].(string)
					if minOK {
						got, err := strconv.Atoi(gotStr)
						require.NoError(t, err)
						return got >= minv
					}
					return gotStr == expected
				}, 15*time.Second, 50*time.Millisecond, "eventually verify: %s", sql)
			}
		})
	}
}
