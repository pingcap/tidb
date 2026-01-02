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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func checkCountAtLeast(t *testing.T, tk *testkit.TestKit, sql string, minx int) {
	rows := tk.MustQuery(sql).Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	got, err := strconv.Atoi(rows[0][0].(string))
	require.NoError(t, err)
	require.GreaterOrEqual(t, got, minx, "sql: %s", sql)
}

func TestSoftDeleteCleanupJobIntegration(t *testing.T) {
	defer boostJobScheduleForTest(t)()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	seFactory := sessionFactory(t, dom)

	waitAndStopTTLManager(t, dom)

	se, closeSe := seFactory()
	defer closeSe()

	var leader atomic.Bool
	m := ttlworker.NewJobManager("manager-1", dom.AdvancedSysSessionPool(), store, nil, func() bool { return leader.Load() })
	leader.Store(true)
	m.Start()
	defer func() {
		m.TaskManager().ResizeWorkersToZero(t)
		m.Stop()
		require.NoError(t, m.WaitStopped(context.Background(), time.Minute))
	}()
	m.TaskManager().ResizeWorkersWithSysVar()
	require.NoError(t, m.InfoSchemaCache().Update(se))

	ctx := context.Background()
	tk.MustExec("use test")
	tk.MustExec("set global tidb_softdelete_job_enable='ON'")
	tk.MustExec("set @@tidb_translate_softdelete_sql=ON")

	cases := []struct {
		name             string
		tblName          string
		createTableSQL   string
		setupSQLs        []string
		submitPartitions bool
		waitKey          int
		wantWaitValue    string
		verifySQLs       map[string]string
		minStatusRows    int
		minHistoryRows   int
	}{
		{
			name:           "non-partitioned deletes expired tombstone",
			tblName:        "sd",
			createTableSQL: "create table sd(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON'",
			setupSQLs: []string{
				"insert into sd values (1, 10), (2, 20), (3, 30)",
				"delete from sd where a in (1, 2)",
				"update sd set _tidb_softdelete_time = date_sub(utc_timestamp(6), interval 8 day) where a = 1",
			},
			submitPartitions: false,
			waitKey:          1,
			wantWaitValue:    "0",
			verifySQLs: map[string]string{
				"select count(*) from sd where a=2": "1",
				"select count(*) from sd where a=3": "1",
			},
			minStatusRows:  1,
			minHistoryRows: 1,
		},
		{
			name:           "partitioned deletes expired tombstone",
			tblName:        "sd_p",
			createTableSQL: "create table sd_p(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON' partition by range(a) (partition p0 values less than (10), partition p1 values less than (100))",
			setupSQLs: []string{
				"insert into sd_p values (1, 10), (20, 200)",
				"delete from sd_p where a in (1, 20)",
				"update sd_p set _tidb_softdelete_time = date_sub(utc_timestamp(6), interval 8 day) where a = 1",
			},
			submitPartitions: true,
			waitKey:          1,
			wantWaitValue:    "0",
			verifySQLs: map[string]string{
				"select count(*) from sd_p where a=20": "1",
			},
			minStatusRows:  2,
			minHistoryRows: 1,
		},
		{
			name:           "does not delete not expired",
			tblName:        "sd_safe",
			createTableSQL: "create table sd_safe(a int primary key, v int) softdelete retention 7 day softdelete_job_enable='ON'",
			setupSQLs: []string{
				"insert into sd_safe values (1, 10)",
				"delete from sd_safe where a=1",
			},
			submitPartitions: false,
			waitKey:          1,
			wantWaitValue:    "1",
			verifySQLs:       map[string]string{},
			minStatusRows:    1,
			minHistoryRows:   1,
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tk.MustExec("drop table if exists " + c.tblName)
			tk.MustExec(c.createTableSQL)
			for _, sql := range c.setupSQLs {
				tk.MustExec(sql)
			}
			tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(c.tblName))
			require.NoError(t, err)

			require.NoError(t, m.InfoSchemaCache().Update(se))

			physicalIDs := []int64{tbl.Meta().ID}
			if c.submitPartitions {
				physicalIDs = physicalIDs[:0]
				for _, def := range tbl.Meta().Partition.Definitions {
					physicalIDs = append(physicalIDs, def.ID)
				}
			}

			jobIDs := make([]string, 0, len(physicalIDs))
			for j, physicalID := range physicalIDs {
				jobID := fmt.Sprintf("sd_case_%s_%d_%d", c.tblName, i, j)
				jobIDs = append(jobIDs, jobID)
				require.NoError(t, m.SubmitSoftDeleteJob(se, tbl.Meta().ID, physicalID, jobID))
			}

			waitSQL := fmt.Sprintf("select count(*) from %s where a=%d", c.tblName, c.waitKey)
			require.Eventually(t, func() bool {
				return tk.MustQuery(waitSQL).Rows()[0][0] == c.wantWaitValue
			}, 15*time.Second, 50*time.Millisecond)

			for q, want := range c.verifySQLs {
				tk.MustQuery(q).Check(testkit.Rows(want))
			}

			checkCountAtLeast(t, tk, "select count(*) from mysql.tidb_softdelete_table_status", c.minStatusRows)
			checkCountAtLeast(t, tk, "select count(*) from mysql.tidb_ttl_job_history where job_type='softdelete'", c.minHistoryRows)
			for _, jobID := range jobIDs {
				tk.MustQuery("select count(*) from mysql.tidb_ttl_job_history where job_id='" + jobID + "'").Check(testkit.Rows("1"))
			}
		})
	}
}
