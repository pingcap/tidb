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
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
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

		currJobs, err := ddl.GetAllDDLJobs(sess)
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = ddl.IterAllDDLJobs(sess, txn, func(jobs []*model.Job) (b bool, e error) {
			for _, job := range jobs {
				if !job.NotStarted() {
					return true, nil
				}
				currJobs2 = append(currJobs2, job)
			}
			return false, nil
		})
		require.NoError(t, err)
		require.Len(t, currJobs2, i+1)
	}

	currJobs, err := ddl.GetAllDDLJobs(sess)
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

	currJobs, err := ddl.GetAllDDLJobs(sess)
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := slices.IsSortedFunc(currJobs, func(i, j *model.Job) int {
		return cmp.Compare(i.ID, j.ID)
	})
	require.True(t, isSort)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
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

func enQueueDDLJobs(t *testing.T, sess sessiontypes.Session, txn kv.Transaction, jobType model.ActionType, start, end int) {
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

func TestCreateDropCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

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
	hook := &callback.TestDDLCallback{}
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
