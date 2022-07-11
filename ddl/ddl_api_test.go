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
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestGetDDLJobs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
		err := addDDLJobs(txn, jobs[i])
		require.NoError(t, err)

		currJobs, err := ddl.GetAllDDLJobs(meta.NewMeta(txn))
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = ddl.IterAllDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
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

	currJobs, err := ddl.GetAllDDLJobs(meta.NewMeta(txn))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	sess := testkit.NewTestKit(t, store).Session()
	_, err := sess.Execute(context.Background(), "begin")
	require.NoError(t, err)

	txn, err := sess.Txn(true)
	require.NoError(t, err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, txn, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, txn, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	enQueueDDLJobs(t, txn, model.ActionAddIndex, 5, 10)

	currJobs, err := ddl.GetAllDDLJobs(meta.NewMeta(txn))
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := slices.IsSortedFunc(currJobs, func(i, j *model.Job) bool {
		return i.ID <= j.ID
	})
	require.True(t, isSort)

	_, err = sess.Execute(context.Background(), "rollback")
	require.NoError(t, err)
}

func TestGetHistoryDDLJobs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	// delete the internal DDL record.
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(ctx context.Context, txn kv.Transaction) error {
		return meta.NewMeta(txn).ClearAllHistoryJob()
	})

	require.NoError(t, err)

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
		err = ddl.AddHistoryDDLJob(m, jobs[i], true)
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

func enQueueDDLJobs(t *testing.T, txn kv.Transaction, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := addDDLJobs(txn, job)
		require.NoError(t, err)
	}
}
