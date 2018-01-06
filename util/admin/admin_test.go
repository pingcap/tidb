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

package admin

import (
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store  kv.Storage
	ctx    *mock.Context
	dbInfo *model.DBInfo
	tbInfo *model.TableInfo
}

func (s *testSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = tikv.NewMockTikvStore()
	c.Assert(err, IsNil)

	s.ctx = mock.NewContext()
	s.ctx.Store = s.store

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	s.dbInfo = &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = t.CreateDatabase(s.dbInfo)
	c.Assert(err, IsNil)
	pkFieldType := types.NewFieldType(mysql.TypeLong)
	pkFieldType.Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	pkCol := &model.ColumnInfo{
		Name:      model.NewCIStr("pk"),
		ID:        1,
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *pkFieldType,
	}
	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c"),
		ID:           2,
		Offset:       1,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	col1 := &model.ColumnInfo{
		Name:         model.NewCIStr("c1"),
		ID:           3,
		Offset:       2,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	idx := &model.IndexInfo{
		Name:   model.NewCIStr("c"),
		ID:     4,
		Unique: true,
		Columns: []*model.IndexColumn{{
			Name:   model.NewCIStr("c"),
			Offset: 1,
			Length: 255,
		}},
		State: model.StatePublic,
	}
	s.tbInfo = &model.TableInfo{
		ID:         5,
		Name:       model.NewCIStr("t"),
		State:      model.StatePublic,
		Columns:    []*model.ColumnInfo{pkCol, col, col1},
		Indices:    []*model.IndexInfo{idx},
		PKIsHandle: true,
	}
	err = t.CreateTable(s.dbInfo.ID, s.tbInfo)
	c.Assert(err, IsNil)

	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownSuite(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)

	err = t.DropTable(s.dbInfo.ID, s.tbInfo.ID, true)
	c.Assert(err, IsNil)
	err = t.DropDatabase(s.dbInfo.ID)
	c.Assert(err, IsNil)
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDDLInfo(c *C) {
	defer testleak.AfterTest(c)()
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
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	info, err := GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Job, DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDDLJobs(c *C) {
	defer testleak.AfterTest(c)()

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

func (s *testSuite) TestCancelJobs(c *C) {
	defer testleak.AfterTest(c)()

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

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetHistoryDDLJobs(c *C) {
	defer testleak.AfterTest(c)()

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
		historyJobs, err1 := GetHistoryDDLJobs(txn)
		c.Assert(err1, IsNil)
		if i+1 > maxHistoryJobs {
			c.Assert(historyJobs, HasLen, maxHistoryJobs)
		} else {
			c.Assert(historyJobs, HasLen, i+1)
		}
	}

	delta := cnt - maxHistoryJobs
	historyJobs, err := GetHistoryDDLJobs(txn)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, HasLen, maxHistoryJobs)
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
	defer testleak.AfterTest(c)()
	alloc := autoid.NewAllocator(s.store, s.dbInfo.ID)
	tb, err := tables.TableFromMeta(alloc, s.tbInfo)
	c.Assert(err, IsNil)
	indices := tb.Indices()
	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, types.MakeDatums(1, 10, 11), false)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(goctx.Background()), IsNil)

	record1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(1), int64(10), int64(11))}
	record2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(2), int64(20), int64(21))}
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	records, _, err := ScanSnapshotTableRecord(s.store, ver, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})

	c.Assert(s.ctx.NewTxn(), IsNil)
	_, err = tb.AddRecord(s.ctx, record2.Values, false)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(goctx.Background()), IsNil)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	records, nextHandle, err := ScanTableRecord(txn, tb, int64(1), 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record1})
	records, nextHandle, err = ScanTableRecord(txn, tb, nextHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, DeepEquals, []*RecordData{record2})
	startHandle := nextHandle
	records, nextHandle, err = ScanTableRecord(txn, tb, startHandle, 1)
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
	c.Assert(nextHandle, Equals, startHandle)

	idxRow1 := &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(10))}
	idxRow2 := &RecordData{Handle: int64(2), Values: types.MakeDatums(int64(20))}
	kvIndex := tables.NewIndex(tb.Meta(), indices[0].Meta())
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	idxRows, nextVals, err := ScanIndexData(sc, txn, kvIndex, idxRow1.Values, 2)
	c.Assert(err, IsNil)
	c.Assert(idxRows, DeepEquals, []*RecordData{idxRow1, idxRow2})
	idxRows, nextVals, err = ScanIndexData(sc, txn, kvIndex, idxRow1.Values, 1)
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

	s.testIndex(c, tb, tb.Indices()[0])

	c.Assert(s.ctx.NewTxn(), IsNil)
	err = tb.RemoveRecord(s.ctx, 1, record1.Values)
	c.Assert(err, IsNil)
	err = tb.RemoveRecord(s.ctx, 2, record2.Values)
	c.Assert(err, IsNil)
	c.Assert(s.ctx.Txn().Commit(goctx.Background()), IsNil)
}

func newDiffRetError(prefix string, ra, rb *RecordData) string {
	return fmt.Sprintf("[admin:1]%s:%v != record:%v", prefix, ra, rb)
}

func (s *testSuite) testTableData(c *C, tb table.Table, rs []*RecordData) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	err = CompareTableRecord(txn, tb, rs, true)
	c.Assert(err, IsNil)

	cnt, err := GetTableRecordsCount(txn, tb, 0)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(len(rs)))

	records := []*RecordData{
		{Handle: rs[0].Handle},
		{Handle: rs[1].Handle},
	}
	err = CompareTableRecord(txn, tb, records, false)
	c.Assert(err, IsNil)

	record := &RecordData{Handle: rs[1].Handle, Values: types.MakeDatums(int64(30))}
	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], record}, true)
	c.Assert(err, NotNil)
	diffMsg := newDiffRetError("data", record, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	record.Handle = 3
	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], record, rs[1]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, []*RecordData{rs[0], rs[1], record}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", record, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, []*RecordData{rs[0]}, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[1])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	err = CompareTableRecord(txn, tb, nil, true)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("data", nil, rs[0])
	c.Assert(err.Error(), DeepEquals, diffMsg)

	errRs := append(rs, &RecordData{Handle: int64(1), Values: types.MakeDatums(int64(3))})
	err = CompareTableRecord(txn, tb, errRs, false)
	c.Assert(err.Error(), DeepEquals, "[admin:2]handle:1 is repeated in data")
}

func (s *testSuite) testIndex(c *C, tb table.Table, idx table.Index) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	err = CompareIndexData(sc, txn, tb, idx)
	c.Assert(err, IsNil)

	cnt, err := GetIndexRecordsCount(sc, txn, idx, nil)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))

	mockCtx := mock.NewContext()
	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), 3)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(sc, txn, tb, idx)
	c.Assert(err, NotNil)
	record1 := &RecordData{Handle: int64(3), Values: types.MakeDatums(int64(30))}
	diffMsg := newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40), (3, 31)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(40)), 4)
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(31)))
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(sc, txn, tb, idx)
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
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = checkRecordAndIndex(sc, txn, tb, idx)
	c.Assert(err, NotNil)
	record2 = &RecordData{Handle: int64(5), Values: types.MakeDatums(int64(30))}
	diffMsg = newDiffRetError("index", record1, record2)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	txn.Delete(key)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 3))
	setColValue(c, txn, key, types.NewDatum(int64(30)))
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(sc, txn, tb, idx)
	c.Assert(err, NotNil)
	record1 = &RecordData{Handle: int64(4), Values: types.MakeDatums(int64(40))}
	diffMsg = newDiffRetError("index", record1, nil)
	c.Assert(err.Error(), DeepEquals, diffMsg)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(40)), 4)
	c.Assert(err, IsNil)
	key = tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = CompareIndexData(sc, txn, tb, idx)
	c.Assert(err, NotNil)
	diffMsg = newDiffRetError("index", nil, record1)
	c.Assert(err.Error(), DeepEquals, diffMsg)
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
