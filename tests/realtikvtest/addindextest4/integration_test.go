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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

func TestAdminAlterDDLJobAdjustConfigLocalIngest(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t (a int);")
	tk1.MustExec("insert into t values (1);")
	tk1.MustExec("set @@global.tidb_enable_dist_task=off;")
	failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockStuckIndexIngestWorker", "return(false)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(100)")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk1.MustExec("alter table t add index idx_a(a);")
	}()
	realWorkerCnt := 0
	realBatchSize := 0
	realMaxWriteSpeed := 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkReorgConcurrency", func(j *model.Job) {
		realWorkerCnt = j.ReorgMeta.GetConcurrency()
		realBatchSize = j.ReorgMeta.GetBatchSize()
		realMaxWriteSpeed = j.ReorgMeta.GetMaxWriteSpeed()
		fmt.Println("checkReorgConcurrency", realWorkerCnt)
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
	workerCnt := 7
	batchSize := 89
	maxWriteSpeed := 1011
	tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = %d", jobID, workerCnt))
	tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s batch_size = %d", jobID, batchSize))
	tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s max_write_speed = %d", jobID, maxWriteSpeed))
	require.Eventually(t, func() bool {
		return realWorkerCnt == workerCnt && realBatchSize == batchSize && realMaxWriteSpeed == maxWriteSpeed
	}, time.Second*5, time.Millisecond*100)
	failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockStuckIndexIngestWorker", "return(true)")
	wg.Wait()
}
