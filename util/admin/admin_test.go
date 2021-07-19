// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package admin_test

import (
	"testing"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	. "github.com/pingcap/tidb/util/admin"
	"github.com/stretchr/testify/require"
)

func TestGetDDLInfo(t *testing.T) {
	t.Parallel()

	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)

	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
		RowCount: 0,
	}
	job1 := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionAddIndex,
		RowCount: 0,
	}

	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)

	info, err := GetDDLInfo(txn)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 1)
	require.Equal(t, job, info.Jobs[0])
	require.Nil(t, info.ReorgHandle)

	// two jobs
	m = meta.NewMeta(txn, meta.AddIndexJobListKey)
	err = m.EnQueueDDLJob(job1)
	require.NoError(t, err)

	info, err = GetDDLInfo(txn)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 2)
	require.Equal(t, job, info.Jobs[0])
	require.Equal(t, job1, info.Jobs[1])
	require.Nil(t, info.ReorgHandle)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetDDLJobs(t *testing.T) {
	t.Parallel()

	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 10
	jobs := make([]*model.Job, cnt)
	var currJobs2 []*model.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = m.EnQueueDDLJob(jobs[i])
		require.NoError(t, err)

		currJobs, err := GetDDLJobs(txn)
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = IterAllDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
			for _, job := range jobs {
				if job.State == model.JobStateNone {
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

	currJobs, err := GetDDLJobs(txn)
	require.NoError(t, err)

	for i, job := range jobs {
		require.Equal(t, currJobs[i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}
	require.Equal(t, currJobs2, currJobs)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetDDLJobsIsSort(t *testing.T) {
	t.Parallel()

	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	m := meta.NewMeta(txn)
	enQueueDDLJobs(t, m, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, m, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	m = meta.NewMeta(txn, meta.AddIndexJobListKey)
	enQueueDDLJobs(t, m, model.ActionAddIndex, 5, 10)

	currJobs, err := GetDDLJobs(txn)
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := isJobsSorted(currJobs)
	require.True(t, isSort)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestCancelJobs(t *testing.T) {
	t.Parallel()

	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 10
	ids := make([]int64, cnt)
	for i := 0; i < cnt; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		if i == 0 {
			job.State = model.JobStateDone
		}
		if i == 1 {
			job.State = model.JobStateCancelled
		}
		ids[i] = int64(i)
		err = m.EnQueueDDLJob(job)
		require.NoError(t, err)
	}

	errs, err := CancelJobs(txn, ids)
	require.NoError(t, err)
	for i, err := range errs {
		if i == 0 {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
	}

	errs, err = CancelJobs(txn, []int64{})
	require.NoError(t, err)
	require.Nil(t, errs)

	errs, err = CancelJobs(txn, []int64{-1})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, ".*DDL Job:-1 not found", errs[0].Error())

	// test cancel finish job.
	job := &model.Job{
		ID:       100,
		SchemaID: 1,
		Type:     model.ActionCreateTable,
		State:    model.JobStateDone,
	}
	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)
	errs, err = CancelJobs(txn, []int64{100})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, ".*This job:100 is finished, so can't be cancelled", errs[0].Error())

	// test can't cancelable job.
	job.Type = model.ActionDropIndex
	job.SchemaState = model.StateWriteOnly
	job.State = model.JobStateRunning
	job.ID = 101
	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)
	errs, err = CancelJobs(txn, []int64{101})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, ".*This job:101 is almost finished, can't be cancelled now", errs[0].Error())

	// When both types of jobs exist in the DDL queue,
	// we first cancel the job with a larger ID.
	job = &model.Job{
		ID:       1000,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddIndex,
	}
	job1 := &model.Job{
		ID:       1001,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddColumn,
	}
	job2 := &model.Job{
		ID:       1002,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddIndex,
	}
	job3 := &model.Job{
		ID:       1003,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionRepairTable,
	}
	require.NoError(t, m.EnQueueDDLJob(job, meta.AddIndexJobListKey))
	require.NoError(t, m.EnQueueDDLJob(job1))
	require.NoError(t, m.EnQueueDDLJob(job2, meta.AddIndexJobListKey))
	require.NoError(t, m.EnQueueDDLJob(job3))

	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID, job3.ID})
	require.NoError(t, err)
	for _, err := range errs {
		require.NoError(t, err)
	}

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetHistoryDDLJobs(t *testing.T) {
	t.Parallel()

	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
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
		err = m.AddHistoryDDLJob(jobs[i], true)
		require.NoError(t, err)

		historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
		require.NoError(t, err)

		if i+1 > MaxHistoryJobs {
			require.Len(t, historyJobs, MaxHistoryJobs)
		} else {
			require.Len(t, historyJobs, i+1)
		}
	}

	delta := cnt - MaxHistoryJobs
	historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
	require.NoError(t, err)
	require.Len(t, historyJobs, MaxHistoryJobs)

	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		require.Equal(t, jobs[delta+l-i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}

	var historyJobs2 []*model.Job
	err = IterHistoryDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
		for _, job := range jobs {
			historyJobs2 = append(historyJobs2, job)
			if len(historyJobs2) == DefNumHistoryJobs {
				return true, nil
			}
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, historyJobs, historyJobs2)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestIsJobRollbackable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		tp     model.ActionType
		state  model.SchemaState
		result bool
	}{
		{model.ActionDropIndex, model.StateNone, true},
		{model.ActionDropIndex, model.StateDeleteOnly, false},
		{model.ActionDropSchema, model.StateDeleteOnly, false},
		{model.ActionDropColumn, model.StateDeleteOnly, false},
		{model.ActionDropColumns, model.StateDeleteOnly, false},
	}
	job := &model.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := IsJobRollbackable(job)
		require.Equal(t, ca.result, re)
	}
}

func TestError(t *testing.T) {
	t.Parallel()

	kvErrs := []*terror.Error{
		ErrDataInConsistent,
		ErrDDLJobNotFound,
		ErrCancelFinishedDDLJob,
		ErrCannotCancelDDLJob,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.NotEqual(t, mysql.ErrUnknown, code)
		require.Equal(t, uint16(err.Code()), code)
	}
}

func newMockStore(t *testing.T) (store kv.Storage, clean func()) {
	var err error
	store, err = mockstore.NewMockStore()
	require.NoError(t, err)

	clean = func() {
		err = store.Close()
		require.NoError(t, err)
	}

	return
}

func isJobsSorted(jobs []*model.Job) bool {
	if len(jobs) <= 1 {
		return true
	}
	for i := 1; i < len(jobs); i++ {
		if jobs[i].ID <= jobs[i-1].ID {
			return false
		}
	}
	return true
}

func enQueueDDLJobs(t *testing.T, m *meta.Meta, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := m.EnQueueDDLJob(job)
		require.NoError(t, err)
	}
}
