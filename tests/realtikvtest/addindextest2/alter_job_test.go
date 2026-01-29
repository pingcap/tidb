// Copyright 2025 PingCAP, Inc.
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

package addindextest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterThreadRightAfterJobFinish(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task=0;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task=1;")
	})
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3);")
	var updated bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkJobCancelled", func(job *model.Job) {
		if !updated && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			updated = true
			tk2 := testkit.NewTestKit(t, store)
			rs, err := tk2.Exec(fmt.Sprintf("admin alter ddl jobs %d thread = 1", job.ID))
			if rs != nil {
				closeErr := rs.Close()
				if err == nil {
					err = closeErr
				}
			}
			assert.NoError(t, err)
		}
	})
	var pipeClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(*operator.AsyncPipeline) {
		pipeClosed.Store(true)
		time.Sleep(5 * time.Second)
	})
	var onUpdateJobParam bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onUpdateJobParam", func() {
		if !onUpdateJobParam {
			onUpdateJobParam = true
			for !pipeClosed.Load() {
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
	tk.MustExec("alter table t add index idx(c2)")
	require.True(t, updated)
	require.True(t, pipeClosed.Load())
}

func TestAlterJobOnDXF(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("resource params are calculated automatically on nextgen for add-index, we don't support alter them")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(16)`)
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_enable_dist_task=1;`)
	}
	tk.MustExec("create table t1(a bigint auto_random primary key);")
	for range 16 {
		tk.MustExec("insert into t1 values (), (), (), ()")
	}
	tk.MustExec("split table t1 between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 32")
	tk.MustExec(`set @@global.tidb_cloud_storage_uri = ""`)
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 16")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 0")
		})
	}
	var pipeClosed bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(pipe *operator.AsyncPipeline) {
		pipeClosed = true
		reader, writer := pipe.GetReaderAndWriter()
		readerPoolSize, writerPoolSize := reader.GetWorkerPoolSize(), writer.GetWorkerPoolSize()
		assert.Truef(t,
			(readerPoolSize == 4 && writerPoolSize == 6) || // avgRowSize not available
				(readerPoolSize == 4 && writerPoolSize == 8) || // small avgRowSize
				(readerPoolSize == 8 && writerPoolSize == 8), // medium avgRowSize
			"unexpected pipeline worker pool size: reader=%d writer=%d",
			readerPoolSize,
			writerPoolSize,
		)
	})
	var finishedSubtasks int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", func(be *local.Backend) {
		finishedSubtasks++
		assert.EqualValues(t, 1024, be.GetWriteSpeedLimit())
	})
	var modified atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterDetectAndHandleParamModify", func(_ proto.Step) {
		modified.Store(true)
	})
	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(reorgMeta *model.DDLReorgMeta) {
		once.Do(func() {
			execSQL := func(tk *testkit.TestKit, sql string, args ...any) error {
				rs, err := tk.Exec(sql, args...)
				if rs != nil {
					closeErr := rs.Close()
					if err == nil {
						err = closeErr
					}
				}
				return err
			}
			queryRows := func(tk *testkit.TestKit, sql string, args ...any) ([][]string, error) {
				rs, err := tk.Exec(sql, args...)
				if err != nil {
					if rs != nil {
						_ = rs.Close()
					}
					return nil, err
				}
				return session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
			}

			tk1 := testkit.NewTestKit(t, store)
			rows, err := queryRows(tk1, "select job_id from mysql.tidb_ddl_job")
			if !assert.NoError(t, err) {
				return
			}
			if !assert.Len(t, rows, 1) {
				return
			}
			if !assert.NoError(t, execSQL(tk1, fmt.Sprintf("admin alter ddl jobs %s thread = 8, batch_size = 256, max_write_speed=1024", rows[0][0]))) {
				return
			}
			if !assert.Eventually(t, func() bool {
				return modified.Load()
			}, 20*time.Second, 100*time.Millisecond) {
				return
			}
			assert.Equal(t, 256, reorgMeta.GetBatchSize())
		})
	})
	tk.MustExec("alter table t1 add index idx(a);")
	require.True(t, pipeClosed)
	require.EqualValues(t, 1, finishedSubtasks)
	require.True(t, modified.Load())
	tk.MustExec("admin check index t1 idx;")
}
