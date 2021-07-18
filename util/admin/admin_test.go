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
	"fmt"
	"testing"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	. "github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/stretchr/testify/suite"
)

func TestAdminTestSuite(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	suite.Run(t, new(testSuite))
}

type testSuite struct {
	suite.Suite
	store kv.Storage
	ctx   *mock.Context
}

func (s *testSuite) SetupSuite() {
	testleak.BeforeTest()
	var err error
	s.store, err = mockstore.NewMockStore()
	s.Require().Nil(err)
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
}

func (s *testSuite) TearDownSuite() {
	err := s.store.Close()
	s.Require().Nil(err)
}

func (s *testSuite) TestGetDDLInfo() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
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
	s.Require().Nil(err)
	info, err := GetDDLInfo(txn)
	s.Require().Nil(err)
	s.Assert().Len(info.Jobs, 1)
	s.Assert().Equal(info.Jobs[0], job)
	s.Require().Nil(err)
	// Two jobs.
	t = meta.NewMeta(txn, meta.AddIndexJobListKey)
	err = t.EnQueueDDLJob(job1)
	s.Require().Nil(err)
	info, err = GetDDLInfo(txn)
	s.Require().Nil(err)
	s.Assert().Len(info.Jobs, 2)
	s.Assert().Equal(info.Jobs[0], job)
	s.Assert().Equal(info.Jobs[1], job1)
	s.Require().Nil(info.ReorgHandle)
	err = txn.Rollback()
	s.Require().Nil(err)
}

func (s *testSuite) TestGetDDLJobs() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
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
		s.Require().Nil(err)
		currJobs, err1 := GetDDLJobs(txn)
		s.Require().Nil(err1)
		s.Assert().Len(currJobs, i+1)
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
		s.Require().Nil(err)
		s.Assert().Len(currJobs2, i+1)
	}

	currJobs, err := GetDDLJobs(txn)
	s.Require().Nil(err)
	for i, job := range jobs {
		s.Assert().Equal(job.ID, currJobs[i].ID)
		s.Assert().Equal(job.SchemaID, int64(1))
		s.Assert().Equal(job.Type, model.ActionCreateTable)
	}
	s.Assert().Equal(currJobs, currJobs2)

	err = txn.Rollback()
	s.Require().Nil(err)
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

func enQueueDDLJobs(s *testSuite, t *meta.Meta, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := t.EnQueueDDLJob(job)
		s.Require().Nil(err)
	}
}

func (s *testSuite) TestGetDDLJobsIsSort() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	t := meta.NewMeta(txn)
	enQueueDDLJobs(s, t, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(s, t, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	t = meta.NewMeta(txn, meta.AddIndexJobListKey)
	enQueueDDLJobs(s, t, model.ActionAddIndex, 5, 10)

	currJobs, err := GetDDLJobs(txn)
	s.Require().Nil(err)
	s.Assert().Len(currJobs, 15)

	isSort := isJobsSorted(currJobs)
	s.Assert().Equal(isSort, true)

	err = txn.Rollback()
	s.Require().Nil(err)
}

func (s *testSuite) TestCancelJobs() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
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
		s.Require().Nil(err)
	}

	errs, err := CancelJobs(txn, ids)
	s.Require().Nil(err)
	for i, err := range errs {
		if i == 0 {
			s.Require().NotNil(err)
			continue
		}
		s.Require().Nil(err)
	}

	errs, err = CancelJobs(txn, []int64{})
	s.Require().Nil(err)
	s.Require().Nil(errs)
	errs, err = CancelJobs(txn, []int64{-1})
	s.Require().Nil(err)
	s.Require().NotNil(errs[0])
	s.Assert().Regexp(".*DDL Job:-1 not found", errs[0].Error())

	// test cancel finish job.
	job := &model.Job{
		ID:       100,
		SchemaID: 1,
		Type:     model.ActionCreateTable,
		State:    model.JobStateDone,
	}
	err = t.EnQueueDDLJob(job)
	s.Require().Nil(err)
	errs, err = CancelJobs(txn, []int64{100})
	s.Require().Nil(err)
	s.Require().NotNil(errs[0])
	s.Assert().Regexp(".*This job:100 is finished, so can't be cancelled", errs[0].Error())

	// test can't cancelable job.
	job.Type = model.ActionDropIndex
	job.SchemaState = model.StateWriteOnly
	job.State = model.JobStateRunning
	job.ID = 101
	err = t.EnQueueDDLJob(job)
	s.Require().Nil(err)
	errs, err = CancelJobs(txn, []int64{101})
	s.Require().Nil(err)
	s.Require().NotNil(errs[0])
	s.Assert().Regexp(".*This job:101 is almost finished, can't be cancelled now", errs[0].Error())

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
	s.Require().Nil(err)
	err = t.EnQueueDDLJob(job1)
	s.Require().Nil(err)
	err = t.EnQueueDDLJob(job2, meta.AddIndexJobListKey)
	s.Require().Nil(err)
	err = t.EnQueueDDLJob(job3)
	s.Require().Nil(err)
	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID, job3.ID})
	s.Require().Nil(err)
	for _, err := range errs {
		s.Require().Nil(err)
	}

	err = txn.Rollback()
	s.Require().Nil(err)
}

func (s *testSuite) TestGetHistoryDDLJobs() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
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
		s.Require().Nil(err)
		historyJobs, err1 := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
		s.Require().Nil(err1)
		if i+1 > MaxHistoryJobs {
			s.Assert().Len(historyJobs, MaxHistoryJobs)

		} else {
			s.Assert().Len(historyJobs, i+1)
		}
	}

	delta := cnt - MaxHistoryJobs
	historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
	s.Require().Nil(err)
	s.Assert().Len(historyJobs, MaxHistoryJobs)
	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		s.Assert().Equal(job.ID, jobs[delta+l-i].ID)
		s.Assert().Equal(job.SchemaID, int64(1))
		s.Assert().Equal(job.Type, model.ActionCreateTable)
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
	s.Require().Nil(err)
	s.Assert().Equal(historyJobs2, historyJobs)

	err = txn.Rollback()
	s.Require().Nil(err)
}

func (s *testSuite) TestIsJobRollbackable() {
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
		s.Assert().True(re == ca.result)
	}
}

func (s *testSuite) TestError() {
	kvErrs := []*terror.Error{
		ErrDataInConsistent,
		ErrDDLJobNotFound,
		ErrCancelFinishedDDLJob,
		ErrCannotCancelDDLJob,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		s.Assert().Truef(code != mysql.ErrUnknown && code == uint16(err.Code()), fmt.Sprintf("err: %v", err))
	}
}
