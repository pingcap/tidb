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
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	. "github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
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
	s.store, err = mockstore.NewMockTikvStore()
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
	c.Assert(info.ReorgHandle, Equals, int64(0))
	// Two jobs.
	t = meta.NewMeta(txn, meta.AddIndexJobListKey)
	err = t.EnQueueDDLJob(job1)
	c.Assert(err, IsNil)
	info, err = GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Jobs, HasLen, 2)
	c.Assert(info.Jobs[0], DeepEquals, job)
	c.Assert(info.Jobs[1], DeepEquals, job1)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDDLJobs(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	cnt := 10
	jobs := make([]*model.Job, cnt)
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
	}

	currJobs, err := GetDDLJobs(txn)
	c.Assert(err, IsNil)
	for i, job := range jobs {
		c.Assert(job.ID, Equals, currJobs[i].ID)
		c.Assert(job.SchemaID, Equals, int64(1))
		c.Assert(job.Type, Equals, model.ActionCreateTable)
	}

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
	c.Assert(errs[0].Error(), Equals, "[admin:4]DDL Job:-1 not found")

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
	c.Assert(errs[0].Error(), Equals, "[admin:5]This job:100 is finished, so can't be cancelled")

	// test can't cancelable job.
	job.Type = model.ActionDropIndex
	job.SchemaState = model.StateDeleteOnly
	job.State = model.JobStateRunning
	job.ID = 101
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{101})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Equals, "[admin:6]This job:101 is almost finished, can't be cancelled now")

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
	err = t.EnQueueDDLJob(job, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	err = t.EnQueueDDLJob(job1)
	c.Assert(err, IsNil)
	err = t.EnQueueDDLJob(job2, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID})
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
		err = t.AddHistoryDDLJob(jobs[i])
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

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestScan(c *C) {
	dom, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		se.Close()
	}()

	_, err = se.Execute(context.Background(), "create database test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	db := model.NewCIStr("test_admin")
	dbInfo, ok := is.SchemaByName(db)
	c.Assert(ok, IsTrue)
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(db, tblName)
	c.Assert(err, IsNil)
	tbInfo := tbl.Meta()

	alloc := autoid.NewAllocator(s.store, dbInfo.ID, false)
	tb, err := tables.TableFromMeta(alloc, tbInfo)
	c.Assert(err, IsNil)
	indices := tb.Indices()
	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	_, err = tb.AddRecord(s.ctx, types.MakeDatums(1, 10, 11))
	c.Assert(err, IsNil)
	txn, err := s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	record1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(1), int64(10), int64(11))}
	record2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(2), int64(20), int64(21))}
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	records, _, err := ScanSnapshotTableRecord(se, s.store, ver, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})

	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	_, err = tb.AddRecord(s.ctx, record2.Values)
	c.Assert(err, IsNil)
	txn, err = s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)

	records, nextHandle, err := ScanTableRecord(se, txn, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})
	records, nextHandle, err = ScanTableRecord(se, txn, tb, nextHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record2})
	startHandle := nextHandle
	records, nextHandle, err = ScanTableRecord(se, txn, tb, startHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
	c.Assert(nextHandle, Equals, startHandle)

	idxRow1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(10))}
	idxRow2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(20))}
	kvIndex := tables.NewIndex(tb.Meta().ID, tb.Meta(), indices[0].Meta())
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	idxRows, _, err := ScanIndexData(sc, txn, kvIndex, idxRow1.Values, 2)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow1, idxRow2})
	idxRows, nextVals, err := ScanIndexData(sc, txn, kvIndex, idxRow1.Values, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow1})
	idxRows, nextVals, err = ScanIndexData(sc, txn, kvIndex, nextVals, 1)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow2})
	idxRows, nextVals, err = ScanIndexData(sc, txn, kvIndex, nextVals, 1)
	c.Assert(idxRows, IsNil)
	c.Assert(nextVals, DeepEquals, types.MakeDatums(nil))
	c.Assert(err, IsNil)

	s.testTableData(c, tb, []*RecordData{record1, record2})

	ctx := se.(sessionctx.Context)
	s.testIndex(c, ctx, db.L, tb, tb.Indices()[0])

	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	err = tb.RemoveRecord(s.ctx, 1, record1.Values)
	c.Assert(err, IsNil)
	err = tb.RemoveRecord(s.ctx, 2, record2.Values)
	c.Assert(err, IsNil)
	txn, err = s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func newDiffRetError(prefix string, ra, rb *RecordData) string {
	if rb == nil {
		return fmt.Sprintf("[admin:1]%s:%#v != record:%#v", prefix, ra, nil)
	} else if ra == nil {
		return fmt.Sprintf("[admin:1]%s:%#v != record:%#v", prefix, nil, rb)
	} else {
		return fmt.Sprintf("[admin:1]%s:%#v != record:%#v", prefix, ra, rb)
	}
}

func (s *testSuite) testTableData(c *C, tb table.Table, rs []*RecordData) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	err = CompareTableRecord(s.ctx, txn, tb, rs, true)
	c.Assert(err, IsNil)

	records := []*RecordData{
		{Handle: rs[0].Handle},
		{Handle: rs[1].Handle},
	}
	err = CompareTableRecord(s.ctx, txn, tb, records, false)
	c.Assert(err, IsNil)

	record := &RecordData{Handle: rs[1].Handle, Values: types.MakeDatums(int64(30))}
	err = CompareTableRecord(s.ctx, txn, tb, []*RecordData{rs[0], record}, true)
	c.Assert(err, NotNil)
	diffMsg := newDiffRetError("data", record, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	record.Handle = 3
	err = CompareTableRecord(s.ctx, txn, tb, []*RecordData{rs[0], record, rs[1]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(s.ctx, txn, tb, []*RecordData{rs[0], rs[1], record}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(s.ctx, txn, tb, []*RecordData{rs[0]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(s.ctx, txn, tb, nil, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[0])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	errRs := append(rs, &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(3))})
	err = CompareTableRecord(s.ctx, txn, tb, errRs, false)
	c.Assert(err.Error(), DeepEquals, "[admin:2]handle:1 is repeated in data")
}

func (s *testSuite) testIndex(c *C, ctx sessionctx.Context, dbName string, tb table.Table, idx table.Index) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	err = CompareIndexData(ctx, txn, tb, idx, nil)
	c.Assert(err, IsNil)

	idxNames := []string{idx.Meta().Name.L}
	_, _, err = CheckIndicesCount(ctx, dbName, tb.Meta().Name.L, idxNames)
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), 3, table.WithAssertion(txn))
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(ctx, txn, tb, idx, nil)
	c.Assert(err, NotNil)
	record1 := &RecordData{Handle: int64(3), Values: types.MakeDatums(int64(30))}
	diffMsg := newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	_, _, err = CheckIndicesCount(ctx, dbName, tb.Meta().Name.L, idxNames)
	c.Assert(err, IsNil)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40), (3, 31)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(40)), 4, table.WithAssertion(txn))
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(31)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(ctx, txn, tb, idx, nil)
	c.Assert(err, NotNil)
	record2 := &RecordData{Handle: int64(3), Values: types.MakeDatums(int64(31))}
	diffMsg = newDiffRetError("index", record1, record2)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40), (5, 30)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 5))
	setColValue(c, txn, key, types.NewDatum(int64(30)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CheckRecordAndIndex(ctx, txn, tb, idx, nil)
	c.Assert(err, NotNil)
	record2 = &RecordData{Handle: int64(5), Values: types.MakeDatums(int64(30))}
	diffMsg = newDiffRetError("index", record1, record2)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 5))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(30)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(ctx, txn, tb, idx, nil)
	c.Assert(err, NotNil)
	record1 = &RecordData{Handle: int64(4), Values: types.MakeDatums(int64(40))}
	diffMsg = newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	_, _, err = CheckIndicesCount(ctx, dbName, tb.Meta().Name.L, idxNames)
	c.Assert(err.Error(), Equals, "table count 3 != index(c) count 4")

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(40)), 4, nil)
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(ctx, txn, tb, idx, nil)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("index", nil, record1)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	_, _, err = CheckIndicesCount(ctx, dbName, tb.Meta().Name.L, idxNames)
	c.Assert(err.Error(), Equals, "table count 4 != index(c) count 3")
}

func setColValue(c *C, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
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
	}
	job := &model.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := IsJobRollbackable(job)
		c.Assert(re == ca.result, IsTrue)
	}
}
