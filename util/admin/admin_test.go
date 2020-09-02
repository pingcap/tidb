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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	. "github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	ctx   *mock.Context
}

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
}

func (s *testSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func (s *testSuite) TestGetDDLInfo(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

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
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	info, err := GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Jobs, HasLen, 1)
	c.Assert(info.Jobs[0], DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, nil)
	// Two jobs.
	t = meta.NewMeta(txn, meta.AddIndexJobListKey)
	err = t.EnQueueDDLJob(job1)
	c.Assert(err, IsNil)
	info, err = GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Jobs, HasLen, 2)
	c.Assert(info.Jobs[0], DeepEquals, job)
	c.Assert(info.Jobs[1], DeepEquals, job1)
	c.Assert(info.ReorgHandle, Equals, nil)
	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDDLJobs(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	cnt := 10
	jobs := make([]*model.Job, cnt)
	var currJobs2 []*model.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = t.EnQueueDDLJob(jobs[i])
		c.Assert(err, IsNil)
		currJobs, err1 := GetDDLJobs(txn)
		c.Assert(err1, IsNil)
		c.Assert(currJobs, HasLen, i+1)
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
		c.Assert(err, IsNil)
		c.Assert(currJobs2, HasLen, i+1)
	}

	currJobs, err := GetDDLJobs(txn)
	c.Assert(err, IsNil)
	for i, job := range jobs {
		c.Assert(job.ID, Equals, currJobs[i].ID)
		c.Assert(job.SchemaID, Equals, int64(1))
		c.Assert(job.Type, Equals, model.ActionCreateTable)
	}
	c.Assert(currJobs, DeepEquals, currJobs2)

	err = txn.Rollback()
	c.Assert(err, IsNil)
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

func enQueueDDLJobs(c *C, t *meta.Meta, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := t.EnQueueDDLJob(job)
		c.Assert(err, IsNil)
	}
}

func (s *testSuite) TestGetDDLJobsIsSort(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	// insert 5 drop table jobs to DefaultJobListKey queue
	t := meta.NewMeta(txn)
	enQueueDDLJobs(c, t, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(c, t, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	t = meta.NewMeta(txn, meta.AddIndexJobListKey)
	enQueueDDLJobs(c, t, model.ActionAddIndex, 5, 10)

	currJobs, err := GetDDLJobs(txn)
	c.Assert(err, IsNil)
	c.Assert(currJobs, HasLen, 15)

	isSort := isJobsSorted(currJobs)
	c.Assert(isSort, Equals, true)

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestCancelJobs(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
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
		err = t.EnQueueDDLJob(job)
		c.Assert(err, IsNil)
	}

	errs, err := CancelJobs(txn, ids)
	c.Assert(err, IsNil)
	for i, err := range errs {
		if i == 0 {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
	}

	errs, err = CancelJobs(txn, []int64{})
	c.Assert(err, IsNil)
	c.Assert(errs, IsNil)
	errs, err = CancelJobs(txn, []int64{-1})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*DDL Job:-1 not found")

	// test cancel finish job.
	job := &model.Job{
		ID:       100,
		SchemaID: 1,
		Type:     model.ActionCreateTable,
		State:    model.JobStateDone,
	}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{100})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*This job:100 is finished, so can't be cancelled")

	// test can't cancelable job.
	job.Type = model.ActionDropIndex
	job.SchemaState = model.StateWriteOnly
	job.State = model.JobStateRunning
	job.ID = 101
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{101})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*This job:101 is almost finished, can't be cancelled now")

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
	err = t.EnQueueDDLJob(job, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	err = t.EnQueueDDLJob(job1)
	c.Assert(err, IsNil)
	err = t.EnQueueDDLJob(job2, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	err = t.EnQueueDDLJob(job3)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID, job3.ID})
	c.Assert(err, IsNil)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetHistoryDDLJobs(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	cnt := 11
	jobs := make([]*model.Job, cnt)
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = t.AddHistoryDDLJob(jobs[i], true)
		c.Assert(err, IsNil)
		historyJobs, err1 := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
		c.Assert(err1, IsNil)
		if i+1 > MaxHistoryJobs {
			c.Assert(historyJobs, HasLen, MaxHistoryJobs)
		} else {
			c.Assert(historyJobs, HasLen, i+1)
		}
	}

	delta := cnt - MaxHistoryJobs
	historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, HasLen, MaxHistoryJobs)
	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		c.Assert(job.ID, Equals, jobs[delta+l-i].ID)
		c.Assert(job.SchemaID, Equals, int64(1))
		c.Assert(job.Type, Equals, model.ActionCreateTable)
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
	c.Assert(err, IsNil)
	c.Assert(historyJobs2, DeepEquals, historyJobs)

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestIsJobRollbackable(c *C) {
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
		c.Assert(re == ca.result, IsTrue)
	}
}

func (s *testSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		ErrDataInConsistent,
		ErrDDLJobNotFound,
		ErrCancelFinishedDDLJob,
		ErrCannotCancelDDLJob,
	}
	for _, err := range kvErrs {
		code := err.ToSQLError().Code
		c.Assert(code != mysql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
