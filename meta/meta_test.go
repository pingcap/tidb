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

package meta_test

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
	. "github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	CommonHandleSuite
}

func TestMeta(t *testing.T) {
	t.Parallel()
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)

	defer func() {
		err := store.Close()
		require.Nil(t, err)
	}()

	txn, err := store.Begin()
	require.Nil(t, err)

	mt := meta.NewMeta(txn)

	n, err := mt.GenGlobalID()
	require.Nil(t, err)
	require.Equal(t, int64(1), n)

	n, err = mt.GetGlobalID()
	require.Nil(t, err)
	require.Equal(t, int64(1), n)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := mt.GenGlobalIDs(3)
		require.Nil(t, err)
		anyMatch(t, ids, []int64{2, 3, 4}, []int64{6, 7, 8})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := mt.GenGlobalIDs(4)
		require.Nil(t, err)
		anyMatch(t, ids, []int64{5, 6, 7, 8}, []int64{2, 3, 4, 5})
	}()
	wg.Wait()

	n, err = mt.GetSchemaVersion()
	require.Nil(t, err)
	require.Equal(t, int64(0), n)

	n, err = mt.GenSchemaVersion()
	require.Nil(t, err)
	require.Equal(t, int64(1), n)

	n, err = mt.GetSchemaVersion()
	require.Nil(t, err)
	require.Equal(t, int64(1), n)

	dbInfo := &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = mt.CreateDatabase(dbInfo)
	require.Nil(t, err)

	err = mt.CreateDatabase(dbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrDBExists.Equal(err))

	v, err := mt.GetDatabase(1)
	require.Nil(t, err)
	require.Equal(t, dbInfo, v)

	dbInfo.Name = model.NewCIStr("aa")
	err = mt.UpdateDatabase(dbInfo)
	require.Nil(t, err)

	v, err = mt.GetDatabase(1)
	require.Nil(t, err)
	require.Equal(t, dbInfo, v)

	dbs, err := mt.ListDatabases()
	require.Nil(t, err)
	require.Equal(t, []*model.DBInfo{dbInfo}, dbs)

	tbInfo := &model.TableInfo{
		ID:   1,
		Name: model.NewCIStr("t"),
	}
	err = mt.CreateTableOrView(1, tbInfo)
	require.Nil(t, err)

	n, err = mt.GenAutoTableID(1, 1, 10)
	require.Nil(t, err)
	require.Equal(t, int64(10), n)

	n, err = mt.GetAutoTableID(1, 1)
	require.Nil(t, err)
	require.Equal(t, int64(10), n)

	err = mt.CreateTableOrView(1, tbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrTableExists.Equal(err))

	tbInfo.Name = model.NewCIStr("tt")
	err = mt.UpdateTable(1, tbInfo)
	require.Nil(t, err)

	table, err := mt.GetTable(1, 1)
	require.Nil(t, err)
	require.Equal(t, tbInfo, table)

	table, err = mt.GetTable(1, 2)
	require.Nil(t, err)
	require.Nil(t, table)

	tbInfo2 := &model.TableInfo{
		ID:   2,
		Name: model.NewCIStr("bb"),
	}
	err = mt.CreateTableOrView(1, tbInfo2)
	require.Nil(t, err)

	tables, err := mt.ListTables(1)
	require.Nil(t, err)
	require.Equal(t, []*model.TableInfo{tbInfo, tbInfo2}, tables)
	// Generate an auto id.
	n, err = mt.GenAutoTableID(1, 2, 10)
	require.Nil(t, err)
	require.Equal(t, int64(10), n)
	// Make sure the auto id key-value entry is there.
	n, err = mt.GetAutoTableID(1, 2)
	require.Nil(t, err)
	require.Equal(t, int64(10), n)

	err = mt.DropTableOrView(1, tbInfo2.ID, true)
	require.Nil(t, err)
	// Make sure auto id key-value entry is gone.
	n, err = mt.GetAutoTableID(1, 2)
	require.Nil(t, err)
	require.Equal(t, int64(0), n)

	tables, err = mt.ListTables(1)
	require.Nil(t, err)
	require.Equal(t, []*model.TableInfo{tbInfo}, tables)

	// Test case for drop a table without delete auto id key-value entry.
	tid := int64(100)
	tbInfo100 := &model.TableInfo{
		ID:   tid,
		Name: model.NewCIStr("t_rename"),
	}
	// Create table.
	err = mt.CreateTableOrView(1, tbInfo100)
	require.Nil(t, err)
	// Update auto ID.
	currentDBID := int64(1)
	n, err = mt.GenAutoTableID(currentDBID, tid, 10)
	require.Nil(t, err)
	require.Equal(t, int64(10), n)
	// Fail to update auto ID.
	// The table ID doesn't exist.
	nonExistentID := int64(1234)
	_, err = mt.GenAutoTableID(currentDBID, nonExistentID, 10)
	require.NotNil(t, err)
	require.True(t, meta.ErrTableNotExists.Equal(err))
	// Fail to update auto ID.
	// The current database ID doesn't exist.
	currentDBID = nonExistentID
	_, err = mt.GenAutoTableID(currentDBID, tid, 10)
	require.NotNil(t, err)
	require.True(t, meta.ErrDBNotExists.Equal(err))
	// Test case for CreateTableAndSetAutoID.
	tbInfo3 := &model.TableInfo{
		ID:   3,
		Name: model.NewCIStr("tbl3"),
	}
	err = mt.CreateTableAndSetAutoID(1, tbInfo3, 123, 0)
	require.Nil(t, err)
	id, err := mt.GetAutoTableID(1, tbInfo3.ID)
	require.Nil(t, err)
	require.Equal(t, int64(123), id)
	// Test case for GenAutoTableIDKeyValue.
	key, val := mt.GenAutoTableIDKeyValue(1, tbInfo3.ID, 1234)
	require.Equal(t, []byte(strconv.FormatInt(1234, 10)), val)
	require.Equal(t, []byte{0x6d, 0x44, 0x42, 0x3a, 0x31, 0x0, 0x0, 0x0, 0x0, 0xfb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x0, 0x0, 0x0, 0xfc}, key)

	err = mt.DropDatabase(1)
	require.Nil(t, err)
	err = mt.DropDatabase(currentDBID)
	require.Nil(t, err)

	dbs, err = mt.ListDatabases()
	require.Nil(t, err)
	require.Len(t, dbs, 0)

	bootstrapVer, err := mt.GetBootstrapVersion()
	require.Nil(t, err)
	require.Equal(t, int64(0), bootstrapVer)

	err = mt.FinishBootstrap(int64(1))
	require.Nil(t, err)

	bootstrapVer, err = mt.GetBootstrapVersion()
	require.Nil(t, err)
	require.Equal(t, int64(1), bootstrapVer)

	// Test case for meta.FinishBootstrap with a version.
	err = mt.FinishBootstrap(int64(10))
	require.Nil(t, err)
	bootstrapVer, err = mt.GetBootstrapVersion()
	require.Nil(t, err)

	require.Equal(t, int64(10), bootstrapVer)

	// Test case for SchemaDiff.
	schemaDiff := &model.SchemaDiff{
		Version:    100,
		SchemaID:   1,
		Type:       model.ActionTruncateTable,
		TableID:    2,
		OldTableID: 3,
	}
	err = mt.SetSchemaDiff(schemaDiff)
	require.Nil(t, err)
	readDiff, err := mt.GetSchemaDiff(schemaDiff.Version)
	require.Nil(t, err)
	require.Equal(t, schemaDiff, readDiff)

	err = txn.Commit(context.Background())
	require.Nil(t, err)

	// Test for DDLJobHistoryKey.
	key = meta.DDLJobHistoryKey(mt, 888)
	require.Equal(t, []byte{0x6d, 0x44, 0x44, 0x4c, 0x4a, 0x6f, 0x62, 0x48, 0x69, 0xff, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x78, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7}, key)
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer func() {
		err := store.Close()
		require.Nil(t, err)
	}()

	txn, _ := store.Begin()
	m := meta.NewMeta(txn)
	_, err = m.GenGlobalID()
	require.Nil(t, err)
	n, _ := m.GetGlobalID()
	require.Equal(t, int64(1), n)
	err = txn.Commit(context.Background())
	require.Nil(t, err)

	ver1, _ := store.CurrentVersion(kv.GlobalTxnScope)
	time.Sleep(time.Millisecond)
	txn, _ = store.Begin()
	m = meta.NewMeta(txn)
	_, err = m.GenGlobalID()
	require.Nil(t, err)
	n, _ = m.GetGlobalID()
	require.Equal(t, int64(2), n)
	err = txn.Commit(context.Background())
	require.Nil(t, err)

	snapshot := store.GetSnapshot(ver1)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	n, _ = snapMeta.GetGlobalID()
	require.Equal(t, int64(1), n)
	_, err = snapMeta.GenGlobalID()
	require.NotNil(t, err)
	require.Equal(t, "[structure:8220]write on snapshot", err.Error())
}

func TestElement(t *testing.T) {
	t.Parallel()
	checkElement := func(key []byte, resErr error) {
		e := &meta.Element{ID: 123, TypeKey: key}
		eBytes := e.EncodeElement()
		resE, err := meta.DecodeElement(eBytes)
		if resErr == nil {
			require.Equal(t, resErr, err)
			require.Equal(t, resE, e)
		} else {
			require.Equal(t, resErr.Error(), err.Error())
		}
	}
	key := []byte("_col")
	checkElement(key, errors.Errorf(`invalid encoded element key prefix "_col\x00"`))
	checkElement(meta.IndexElementKey, nil)
	checkElement(meta.ColumnElementKey, nil)
	key = []byte("inexistent")
	checkElement(key, errors.Errorf("invalid encoded element key prefix %q", key[:5]))

	_, err := meta.DecodeElement([]byte("_col"))
	require.Equal(t, `invalid encoded element "_col" length 4`, err.Error())
	_, err = meta.DecodeElement(meta.ColumnElementKey)
	require.Equal(t, `invalid encoded element "_col_" length 5`, err.Error())
}

func (s *testSuite) TestDDL(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	t := meta.NewMeta(txn)

	job := &model.Job{ID: 1}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	n, err := t.DDLJobQueueLen()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	v, err := t.GetDDLJobByIdx(0)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)
	v, err = t.GetDDLJobByIdx(1)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
	job.ID = 2
	err = t.UpdateDDLJob(0, job, true)
	c.Assert(err, IsNil)

	element := &meta.Element{ID: 123, TypeKey: meta.IndexElementKey}
	// There are 3 meta key relate to index reorganization:
	// start_handle, end_handle and physical_table_id.
	// Only start_handle is initialized.
	err = t.UpdateDDLReorgStartHandle(job, element, kv.IntHandle(1).Encoded())
	c.Assert(err, IsNil)

	// Since physical_table_id is uninitialized, we simulate older TiDB version that doesn't store them.
	// In this case GetDDLReorgHandle always return maxInt64 as end_handle.
	e, i, j, k, err := t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(e, DeepEquals, element)
	c.Assert(i, DeepEquals, kv.Key(kv.IntHandle(1).Encoded()))
	c.Assert(j, DeepEquals, kv.Key(kv.IntHandle(math.MaxInt64).Encoded()))
	c.Assert(k, Equals, int64(0))

	startHandle := s.NewHandle().Int(1).Common("abc", 1222, "string")
	endHandle := s.NewHandle().Int(2).Common("dddd", 1222, "string")
	element = &meta.Element{ID: 222, TypeKey: meta.ColumnElementKey}
	err = t.UpdateDDLReorgHandle(job, startHandle.Encoded(), endHandle.Encoded(), 3, element)
	c.Assert(err, IsNil)
	element1 := &meta.Element{ID: 223, TypeKey: meta.IndexElementKey}
	err = t.UpdateDDLReorgHandle(job, startHandle.Encoded(), endHandle.Encoded(), 3, element1)
	c.Assert(err, IsNil)

	e, i, j, k, err = t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(e, DeepEquals, element1)
	c.Assert(i, DeepEquals, kv.Key(startHandle.Encoded()))
	c.Assert(j, DeepEquals, kv.Key(endHandle.Encoded()))
	c.Assert(k, Equals, int64(3))

	err = t.RemoveDDLReorgHandle(job, []*meta.Element{element, element1})
	c.Assert(err, IsNil)
	e, i, j, k, err = t.GetDDLReorgHandle(job)
	c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)
	c.Assert(e, IsNil)
	c.Assert(i, IsNil)
	c.Assert(j, IsNil)
	c.Assert(k, Equals, int64(0))

	// new TiDB binary running on old TiDB DDL reorg data.
	e, i, j, k, err = t.GetDDLReorgHandle(job)
	c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)
	c.Assert(e, IsNil)
	c.Assert(i, IsNil)
	c.Assert(j, IsNil)
	c.Assert(k, Equals, int64(0))

	// Test GetDDLReorgHandle failed.
	_, _, _, _, err = t.GetDDLReorgHandle(job)
	c.Assert(meta.ErrDDLReorgElementNotExist.Equal(err), IsTrue)

	v, err = t.DeQueueDDLJob()
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	err = t.AddHistoryDDLJob(job, true)
	c.Assert(err, IsNil)
	v, err = t.GetHistoryDDLJob(2)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	// Add multiple history jobs.
	arg := "test arg"
	historyJob1 := &model.Job{ID: 1234}
	historyJob1.Args = append(job.Args, arg)
	err = t.AddHistoryDDLJob(historyJob1, true)
	c.Assert(err, IsNil)
	historyJob2 := &model.Job{ID: 123}
	historyJob2.Args = append(job.Args, arg)
	err = t.AddHistoryDDLJob(historyJob2, false)
	c.Assert(err, IsNil)
	all, err := t.GetAllHistoryDDLJobs()
	c.Assert(err, IsNil)
	var lastID int64
	for _, job := range all {
		c.Assert(job.ID, Greater, lastID)
		lastID = job.ID
		arg1 := ""
		err := job.DecodeArgs(&arg1)
		c.Assert(err, IsNil)
		if job.ID == historyJob1.ID {
			c.Assert(*(job.Args[0].(*string)), Equals, historyJob1.Args[0])
		} else {
			c.Assert(job.Args, HasLen, 0)
		}
	}

	// Test for get last N history ddl jobs.
	historyJobs, err := t.GetLastNHistoryDDLJobs(2)
	c.Assert(err, IsNil)
	c.Assert(len(historyJobs), Equals, 2)
	c.Assert(historyJobs[0].ID == 1234, IsTrue)
	c.Assert(historyJobs[1].ID == 123, IsTrue)

	// Test GetAllDDLJobsInQueue.
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	job1 := &model.Job{ID: 2}
	err = t.EnQueueDDLJob(job1)
	c.Assert(err, IsNil)
	jobs, err := t.GetAllDDLJobsInQueue()
	c.Assert(err, IsNil)
	expectJobs := []*model.Job{job, job1}
	c.Assert(jobs, DeepEquals, expectJobs)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Test for add index job.
	txn1, err := store.Begin()
	c.Assert(err, IsNil)

	m := meta.NewMeta(txn1, meta.AddIndexJobListKey)
	err = m.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	job.ID = 123
	err = m.UpdateDDLJob(0, job, true, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	v, err = m.GetDDLJobByIdx(0, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)
	l, err := m.DDLJobQueueLen(meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))
	jobs, err = m.GetAllDDLJobsInQueue(meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	expectJobs = []*model.Job{job}
	c.Assert(jobs, DeepEquals, expectJobs)

	err = txn1.Commit(context.Background())
	c.Assert(err, IsNil)

	s.RerunWithCommonHandleEnabled(c, s.TestDDL)
}

func BenchmarkGenGlobalIDs(b *testing.B) {
	store, err := mockstore.NewMockStore()
	require.Nil(b, err)
	defer func() {
		err := store.Close()
		require.Nil(b, err)
	}()

	txn, err := store.Begin()
	require.Nil(b, err)
	defer func() {
		err := txn.Rollback()
		require.Nil(b, err)
	}()

	t := meta.NewMeta(txn)

	b.ResetTimer()
	var ids []int64
	for i := 0; i < b.N; i++ {
		ids, _ = t.GenGlobalIDs(10)
	}
	require.Len(b, ids, 10)
	require.Equal(b, int64(b.N)*10, ids[9])
}

func BenchmarkGenGlobalIDOneByOne(b *testing.B) {
	store, err := mockstore.NewMockStore()
	require.Nil(b, err)
	defer func() {
		err := store.Close()
		require.Nil(b, err)
	}()

	txn, err := store.Begin()
	require.Nil(b, err)
	defer func() {
		err := txn.Rollback()
		require.Nil(b, err)
	}()

	t := meta.NewMeta(txn)

	b.ResetTimer()
	var id int64
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			id, _ = t.GenGlobalID()
		}
	}
	require.Equal(b, int64(b.N)*10, id)
}

func anyMatch(t *testing.T, ids []int64, candidates ...[]int64) {
	var match bool
OUTER:
	for _, cand := range candidates {
		if len(ids) != len(cand) {
			continue
		}
		for i, v := range cand {
			if ids[i] != v {
				continue OUTER
			}
		}
		match = true
		break
	}
	require.True(t, match)
}
