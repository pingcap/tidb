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

package ddl_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/cmp"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

func TestGetDDLJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	sess := testkit.NewTestKit(t, store).Session()
	_, err := sess.Execute(context.Background(), "begin")
	require.NoError(t, err)

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	cnt := 10
	jobs := make([]*model.Job, cnt)
	var currJobs2 []*model.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err := addDDLJobs(sess, txn, jobs[i])
		require.NoError(t, err)

		currJobs, err := ddl.GetAllDDLJobs(sess, meta.NewMeta(txn))
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = ddl.IterAllDDLJobs(sess, txn, func(jobs []*model.Job) (b bool, e error) {
			for _, job := range jobs {
				if job.NotStarted() {
					currJobs2 = append(currJobs2, job)
				} else {
					return true, nil
				}
			}
			return false, nil
		})
		require.NoError(t, err)
		require.Len(t, currJobs2, i+1)
	}

	currJobs, err := ddl.GetAllDDLJobs(sess, meta.NewMeta(txn))
	require.NoError(t, err)

	for i, job := range jobs {
		require.Equal(t, currJobs[i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}
	require.Equal(t, currJobs2, currJobs)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
}

func TestGetDDLJobsIsSort(t *testing.T) {
	store := testkit.CreateMockStore(t)

	sess := testkit.NewTestKit(t, store).Session()
	_, err := sess.Execute(context.Background(), "begin")
	require.NoError(t, err)

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	enQueueDDLJobs(t, sess, txn, model.ActionAddIndex, 5, 10)

	currJobs, err := ddl.GetAllDDLJobs(sess, meta.NewMeta(txn))
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := slices.IsSortedFunc(currJobs, func(i, j *model.Job) int {
		return cmp.Compare(i.ID, j.ID)
	})
	require.True(t, isSort)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
}

func TestGetHistoryDDLJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// delete the internal DDL record.
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(ctx context.Context, txn kv.Transaction) error {
		return meta.NewMeta(txn).ClearAllHistoryJob()
	})
	require.NoError(t, err)
	testkit.NewTestKit(t, store).MustExec("delete from mysql.tidb_ddl_history")

	tk := testkit.NewTestKit(t, store)
	sess := tk.Session()
	tk.MustExec("begin")

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 11
	jobs := make([]*model.Job, cnt)
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = ddl.AddHistoryDDLJobForTest(sess, m, jobs[i], true)
		require.NoError(t, err)

		historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, ddl.DefNumHistoryJobs)
		require.NoError(t, err)

		if i+1 > ddl.MaxHistoryJobs {
			require.Len(t, historyJobs, ddl.MaxHistoryJobs)
		} else {
			require.Len(t, historyJobs, i+1)
		}
	}

	delta := cnt - ddl.MaxHistoryJobs
	historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, ddl.DefNumHistoryJobs)
	require.NoError(t, err)
	require.Len(t, historyJobs, ddl.MaxHistoryJobs)

	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		require.Equal(t, jobs[delta+l-i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}

	var historyJobs2 []*model.Job
	err = ddl.IterHistoryDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
		for _, job := range jobs {
			historyJobs2 = append(historyJobs2, job)
			if len(historyJobs2) == ddl.DefNumHistoryJobs {
				return true, nil
			}
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, historyJobs, historyJobs2)

	tk.MustExec("rollback")
}

func TestIsJobRollbackable(t *testing.T) {
	cases := []struct {
		tp     model.ActionType
		state  model.SchemaState
		result bool
	}{
		{model.ActionDropIndex, model.StateNone, true},
		{model.ActionDropIndex, model.StateDeleteOnly, false},
		{model.ActionDropSchema, model.StateDeleteOnly, false},
		{model.ActionDropColumn, model.StateDeleteOnly, false},
	}
	job := &model.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := job.IsRollbackable()
		require.Equal(t, ca.result, re)
	}
}

func enQueueDDLJobs(t *testing.T, sess session.Session, txn kv.Transaction, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := addDDLJobs(sess, txn, job)
		require.NoError(t, err)
	}
}

func TestCreateViewConcurrently(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int);")
	tk.MustExec("create view v as select * from t;")
	tk.MustExec("set global tidb_enable_metadata_lock = 1;")
	tk.MustExec("set global tidb_enable_concurrent_ddl = 1;")
	var (
		counterErr error
		counter    int
	)
	ddl.OnCreateViewForTest = func(job *model.Job) {
		counter++
		if counter > 1 {
			counterErr = fmt.Errorf("create view job should not run concurrently")
			return
		}
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/onDDLCreateView", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/onDDLCreateView"))
	}()

	ddl.AfterDelivery2WorkerForTest = func(job *model.Job) {
		if job.Type == model.ActionCreateView {
			counter--
		}
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/afterDelivery2Worker", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/afterDelivery2Worker"))
	}()

	var eg errgroup.Group
	for i := 0; i < 5; i++ {
		eg.Go(func() error {
			newTk := testkit.NewTestKit(t, store)
			_, err := newTk.Exec("use test")
			if err != nil {
				return err
			}
			_, err = newTk.Exec("create or replace view v as select * from t;")
			return err
		})
	}
	err := eg.Wait()
	require.NoError(t, err)
	require.NoError(t, counterErr)
}

func TestCreateDropCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk.MustExec("set global tidb_enable_concurrent_ddl = 1;")
	tk.MustExec("set global tidb_enable_metadata_lock = 1;")

	tk.MustExec("create table t (a int);")

	wg := sync.WaitGroup{}
	var createErr error
	var fpErr error
	var createTable bool

	originHook := dom.DDL().GetHook()
	onJobUpdated := func(job *model.Job) {
		if job.Type == model.ActionDropTable && job.SchemaState == model.StateWriteOnly && !createTable {
			fpErr = failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockOwnerCheckAllVersionSlow", fmt.Sprintf("return(%d)", job.ID))
			wg.Add(1)
			go func() {
				_, createErr = tk1.Exec("create table t (b int);")
				wg.Done()
			}()
			createTable = true
		}
	}
	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobUpdatedExported.Store(&onJobUpdated)
	dom.DDL().SetHook(hook)
	tk.MustExec("drop table t;")
	dom.DDL().SetHook(originHook)

	wg.Wait()
	require.NoError(t, createErr)
	require.NoError(t, fpErr)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockOwnerCheckAllVersionSlow"))

	rs := tk.MustQuery("admin show ddl jobs 3;").Rows()
	create1JobID := rs[0][0].(string)
	dropJobID := rs[1][0].(string)
	create0JobID := rs[2][0].(string)
	jobRecordSet, err := tk.Exec("select job_meta from mysql.tidb_ddl_history where job_id in (?, ?, ?);",
		create1JobID, dropJobID, create0JobID)
	require.NoError(t, err)

	var finishTSs []uint64
	req := jobRecordSet.NewChunk(nil)
	err = jobRecordSet.Next(context.Background(), req)
	require.Greater(t, req.NumRows(), 0)
	require.NoError(t, err)
	iter := chunk.NewIterator4Chunk(req.CopyConstruct())
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		jobMeta := row.GetBytes(0)
		job := model.Job{}
		err = job.Decode(jobMeta)
		require.NoError(t, err)
		finishTSs = append(finishTSs, job.BinlogInfo.FinishedTS)
	}
	create1TS, dropTS, create0TS := finishTSs[0], finishTSs[1], finishTSs[2]
	require.Less(t, create0TS, dropTS, "first create should finish before drop")
	require.Less(t, dropTS, create1TS, "second create should finish after drop")
}

func TestFix56930(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t; create table posts (id int auto_increment primary key, title varchar(500) character set utf8, subtitle varchar(500) character set utf8, unique key(title, subtitle));")
	tk.MustGetErrMsg("alter table posts convert to character set utf8mb4;", "[ddl:1071]Specified key was too long; max key length is 3072 bytes")
	tk.MustExec("drop table if exists t; create table t(a varchar(1000) character set utf8, primary key(a));")
	tk.MustGetErrMsg("alter table t convert to character set utf8mb4;", "[ddl:1071]Specified key was too long; max key length is 3072 bytes")
	tk.MustExec("drop table if exists t; create table t(a varchar(1000) character set utf8, key(a));")
	tk.MustGetErrMsg("alter table t convert to character set utf8mb4;", "[ddl:1071]Specified key was too long; max key length is 3072 bytes")
}
