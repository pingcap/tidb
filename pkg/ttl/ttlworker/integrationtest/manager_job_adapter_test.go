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

package ttlworker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestManagerJobAdapterCanSubmitJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.AdvancedSysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	// Stop TTLJobManager to avoid unnecessary job schedule and make test stable.
	dom.TTLJobManager().Stop()
	require.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), time.Minute))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.False(t, adapter.CanSubmitJob(9999, 9999))

	tk.MustExec("create table t1(t timestamp)")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	tk.MustExec("create table ttl1(t timestamp) TTL=`t`+interval 1 DAY")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl1"))
	require.NoError(t, err)
	require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	tk.MustExec("create table ttl2(t timestamp) TTL=`t`+interval 1 DAY TTL_ENABLE='OFF'")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl2"))
	require.NoError(t, err)
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))

	tk.MustExec("create table ttlp1(a int, t timestamp) TTL=`t`+interval 1 DAY PARTITION BY RANGE (a) (" +
		"PARTITION p0 VALUES LESS THAN (10)," +
		"PARTITION p1 VALUES LESS THAN (100)" +
		")")
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttlp1"))
	require.NoError(t, err)
	for _, def := range tbl.Meta().Partition.Definitions {
		require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, def.ID))
	}

	tk.MustExec("set @@global.tidb_ttl_running_tasks=8")
	defer tk.MustExec("set @@global.tidb_ttl_running_tasks=-1")
	for i := 1; i <= 16; i++ {
		jobID := strconv.Itoa(i)
		sql, args, err := cache.InsertIntoTTLTask(tk.Session().GetSessionVars().Location(), jobID, int64(1000+i), i, nil, nil, time.Now(), time.Now())
		require.NoError(t, err)
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTTL)
		_, err = tk.Session().ExecuteInternal(ctx, sql, args...)
		require.NoError(t, err)

		if i <= 4 {
			tk.MustExec("update mysql.tidb_ttl_task set status='running' where job_id=?", jobID)
		}
		if i > 7 {
			tk.MustExec("update mysql.tidb_ttl_task set status='finished' where job_id=?", jobID)
		}
	}
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl1"))
	require.NoError(t, err)
	require.True(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))
	tk.MustExec("update mysql.tidb_ttl_task set status='running' where job_id='8'")
	require.False(t, adapter.CanSubmitJob(tbl.Meta().ID, tbl.Meta().ID))
}

func TestManagerJobAdapterSubmitJob(t *testing.T) {
	ch := make(chan *ttlworker.SubmitTTLManagerJobRequest)
	adapter := ttlworker.NewManagerJobAdapter(nil, nil, ch)

	var reqPointer atomic.Pointer[ttlworker.SubmitTTLManagerJobRequest]
	responseRequest := func(err error) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()
		select {
		case <-ctx.Done():
			require.FailNow(t, "timeout")
		case req, ok := <-ch:
			require.True(t, ok)
			reqPointer.Store(req)
			select {
			case req.RespCh <- err:
			default:
				require.FailNow(t, "blocked")
			}
		}
	}

	go responseRequest(nil)
	job, err := adapter.SubmitJob(context.TODO(), 1, 2, "req1", time.Now())
	require.NoError(t, err)
	require.Equal(t, "req1", job.RequestID)
	require.False(t, job.Finished)
	require.Nil(t, job.Summary)
	req := reqPointer.Load()
	require.NotNil(t, req)
	require.Equal(t, int64(1), req.TableID)
	require.Equal(t, int64(2), req.PhysicalID)
	require.Equal(t, "req1", req.RequestID)

	go responseRequest(errors.New("mockErr"))
	job, err = adapter.SubmitJob(context.TODO(), 1, 2, "req1", time.Now())
	require.EqualError(t, err, "mockErr")
	require.Nil(t, job)

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	job, err = adapter.SubmitJob(ctx, 1, 2, "req1", time.Now())
	require.Same(t, err, ctx.Err())
	require.Nil(t, job)

	ch = make(chan *ttlworker.SubmitTTLManagerJobRequest, 1)
	adapter = ttlworker.NewManagerJobAdapter(nil, nil, ch)
	ctx, cancel = context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	job, err = adapter.SubmitJob(ctx, 1, 2, "req1", time.Now())
	require.EqualError(t, err, ctx.Err().Error())
	require.Nil(t, job)
}

func TestManagerJobAdapterGetJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.AdvancedSysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	summary := ttlworker.TTLSummary{
		TotalRows:         1000,
		SuccessRows:       998,
		ErrorRows:         2,
		TotalScanTask:     10,
		ScheduledScanTask: 9,
		FinishedScanTask:  8,
		ScanTaskErr:       "err1",
	}

	summaryText, err := json.Marshal(summary)
	require.NoError(t, err)

	insertJob := func(tableID, physicalID int64, jobID string, status cache.JobStatus) {
		tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb_ttl_job_history (
				job_id,
				table_id,
				parent_table_id,
				table_schema,
				table_name,
				partition_name,
				create_time,
				finish_time,
				ttl_expire,
				summary_text,
				expired_rows,
				deleted_rows,
				error_delete_rows,
				status
			)
		VALUES
			(
			 	'%s', %d, %d, 'test', '%s', '', now() - interval 1 MINUTE, now(), now() - interval 1 DAY,
			 	'%s', %d, %d, %d, '%s'
		)`,
			jobID, physicalID, tableID, "t1", summaryText, summary.TotalRows, summary.SuccessRows, summary.ErrorRows, status,
		))
	}

	job, err := adapter.GetJob(context.TODO(), 1, 2, "req1")
	require.NoError(t, err)
	require.Nil(t, job)

	insertJob(2, 2, "req1", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	insertJob(1, 3, "req1", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	insertJob(1, 2, "req2", cache.JobStatusFinished)
	require.NoError(t, err)
	require.Nil(t, job)
	tk.MustExec("delete from mysql.tidb_ttl_job_history")

	statusList := []cache.JobStatus{
		cache.JobStatusWaiting,
		cache.JobStatusRunning,
		cache.JobStatusCancelling,
		cache.JobStatusCancelled,
		cache.JobStatusTimeout,
		cache.JobStatusFinished,
	}
	for _, status := range statusList {
		insertJob(1, 2, "req1", status)
		job, err = adapter.GetJob(context.TODO(), 1, 2, "req1")
		require.NoError(t, err, status)
		require.NotNil(t, job, status)
		require.Equal(t, "req1", job.RequestID, status)
		switch status {
		case cache.JobStatusTimeout, cache.JobStatusFinished, cache.JobStatusCancelled:
			require.True(t, job.Finished, status)
		default:
			require.False(t, job.Finished, status)
		}
		require.Equal(t, summary, *job.Summary, status)
		tk.MustExec("delete from mysql.tidb_ttl_job_history")
	}
}

func TestManagerJobAdapterNow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	pool := wrapPoolForTest(dom.AdvancedSysSessionPool())
	defer pool.AssertNoSessionInUse(t)
	adapter := ttlworker.NewManagerJobAdapter(store, pool, nil)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.time_zone ='Europe/Berlin'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")

	now, err := adapter.Now()
	require.NoError(t, err)
	localNow := time.Now()

	require.Equal(t, "Europe/Berlin", now.Location().String())
	require.InDelta(t, now.Unix(), localNow.Unix(), 10)
}
