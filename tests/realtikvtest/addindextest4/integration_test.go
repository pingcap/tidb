// Copyright 2024 PingCAP, Inc.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func TestMultiSchemaChangeTwoIndexes(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg=on;")
	tk.MustExec("set @@global.tidb_enable_dist_task=on;")

	createTables := []string{
		"create table t (id int, b int, c int, primary key(id) clustered);",
		`create table t (id int, b int, c int, primary key(id) clustered) partition by range(id) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN MAXVALUE
		)`,
	}
	createIndexes := []string{
		"alter table t add unique index b(b), add index c(c);",
		"alter table t add unique index b(b) global, add index c(c) global;",
	}

	for i := range createTables {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createTables[i])
		tk.MustExec("insert into t values (1,1,1)")

		runDMLBeforeScan := false
		runDMLBeforeMerge := false

		var hexKey string
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAddIndexScan", func() {
			if runDMLBeforeScan {
				return
			}
			runDMLBeforeScan = true

			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test;")
			rows := tk1.MustQuery("select tidb_encode_index_key('test', 't', 'b', 1, null);").Rows()
			hexKey = rows[0][0].(string)
			_, err := tk1.Exec("delete from t where id = 1;")
			assert.NoError(t, err)
			_, err = tk1.Exec("insert into t values (2,1,1);")
			assert.NoError(t, err)
			rs := tk1.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after first insertion", rs[0][0].(string))
			_, err = tk1.Exec("delete from t where id = 2;")
			assert.NoError(t, err)
			rs = tk1.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after second insertion", rs[0][0].(string))
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeBackfillMerge", func() {
			if runDMLBeforeMerge {
				return
			}
			runDMLBeforeMerge = true
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test;")
			_, err := tk1.Exec("insert into t values (3, 1, 1);")
			assert.NoError(t, err)
			rs := tk1.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after third insertion", rs[0][0].(string))
		})

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex", "return(false)")

		tk.MustExec(createIndexes[i])
		tk.MustExec("admin check table t;")
	}
}

func TestFixAdminAlterDDLJobs(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t (a int);")
	tk1.MustExec("insert into t values (1);")
	tk1.MustExec("set @@global.tidb_enable_dist_task=off;")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(100)")

	testCases := []struct {
		stuckFp          string
		checkReorgMetaFp string
		sql              string
		setVars          string
		revertVars       string
	}{
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockIndexIngestWorkerFault",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgConcurrency",
			sql:              "alter table t add index idx_a(a)",
		},
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockUpdateColumnWorkerStuck",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
			sql:              "alter table t modify a varchar(30)",
		},
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockAddIndexTxnWorkerStuck",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
			sql:              "alter table t add index idx(a)",
			setVars:          "set @@global.tidb_ddl_enable_fast_reorg=off",
			revertVars:       "set @@global.tidb_ddl_enable_fast_reorg=on",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.stuckFp, func(t *testing.T) {
			if tc.setVars != "" {
				tk1.MustExec(tc.setVars)
			}

			ch := make(chan struct{})
			testfailpoint.EnableCall(t, tc.stuckFp, func() {
				<-ch
			})
			var wg util.WaitGroupWrapper
			wg.Run(func() {
				tk1.MustExec(tc.sql)
			})
			var (
				realWorkerCnt     atomic.Int64
				realBatchSize     atomic.Int64
				realMaxWriteSpeed atomic.Int64
			)
			testfailpoint.EnableCall(t, tc.checkReorgMetaFp, func(j *model.Job) {
				realWorkerCnt.Store(int64(j.ReorgMeta.GetConcurrency()))
				realBatchSize.Store(int64(j.ReorgMeta.GetBatchSize()))
				realMaxWriteSpeed.Store(int64(j.ReorgMeta.GetMaxWriteSpeed()))
			})

			jobID := ""
			tk2 := testkit.NewTestKit(t, store)
			for {
				row := tk2.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
				if len(row) == 1 {
					jobID = row[0][0].(string)
					break
				}
			}
			workerCnt := int64(7)
			batchSize := int64(89)
			maxWriteSpeed := int64(1011)
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = %d", jobID, workerCnt))
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s batch_size = %d", jobID, batchSize))
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s max_write_speed = %d", jobID, maxWriteSpeed))
			require.Eventually(t, func() bool {
				return realWorkerCnt.Load() == workerCnt && realBatchSize.Load() == batchSize && realMaxWriteSpeed.Load() == maxWriteSpeed
			}, 30*time.Second, time.Millisecond*100)
			close(ch)
			wg.Wait()
			if tc.revertVars != "" {
				tk1.MustExec(tc.revertVars)
			}
		})
	}
	tk1.MustExec("set @@global.tidb_enable_dist_task = on;")
	tk1.MustExec("drop table t;")
}
