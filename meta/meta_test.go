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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) TestMeta(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	t := meta.NewMeta(txn)

	n, err := t.GenGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GetGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := t.GenGlobalIDs(3)
		c.Assert(err, IsNil)
		anyMatch(c, ids, []int64{2, 3, 4}, []int64{6, 7, 8})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := t.GenGlobalIDs(4)
		c.Assert(err, IsNil)
		anyMatch(c, ids, []int64{5, 6, 7, 8}, []int64{2, 3, 4, 5})
	}()
	wg.Wait()

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	n, err = t.GenSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	dbInfo := &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = t.CreateDatabase(dbInfo)
	c.Assert(err, IsNil)

	err = t.CreateDatabase(dbInfo)
	c.Assert(err, NotNil)

	v, err := t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, dbInfo)

	dbInfo.Name = model.NewCIStr("aa")
	err = t.UpdateDatabase(dbInfo)
	c.Assert(err, IsNil)

	v, err = t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, dbInfo)

	dbs, err := t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, DeepEquals, []*model.DBInfo{dbInfo})

	tbInfo := &model.TableInfo{
		ID:   1,
		Name: model.NewCIStr("t"),
	}
	err = t.CreateTableOrView(1, tbInfo)
	c.Assert(err, IsNil)

	n, err = t.GenAutoTableID(1, 1, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	n, err = t.GetAutoTableID(1, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	err = t.CreateTableOrView(1, tbInfo)
	c.Assert(err, NotNil)

	tbInfo.Name = model.NewCIStr("tt")
	err = t.UpdateTable(1, tbInfo)
	c.Assert(err, IsNil)

	table, err := t.GetTable(1, 1)
	c.Assert(err, IsNil)
	c.Assert(table, DeepEquals, tbInfo)

	table, err = t.GetTable(1, 2)
	c.Assert(err, IsNil)
	c.Assert(table, IsNil)

	tbInfo2 := &model.TableInfo{
		ID:   2,
		Name: model.NewCIStr("bb"),
	}
	err = t.CreateTableOrView(1, tbInfo2)
	c.Assert(err, IsNil)

	tables, err := t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, []*model.TableInfo{tbInfo, tbInfo2})
	// Generate an auto id.
	n, err = t.GenAutoTableID(1, 2, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))
	// Make sure the auto id key-value entry is there.
	n, err = t.GetAutoTableID(1, 2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	err = t.DropTableOrView(1, tbInfo2.ID, true)
	c.Assert(err, IsNil)
	// Make sure auto id key-value entry is gone.
	n, err = t.GetAutoTableID(1, 2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	tables, err = t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, []*model.TableInfo{tbInfo})

	// Test case for drop a table without delete auto id key-value entry.
	tid := int64(100)
	tbInfo100 := &model.TableInfo{
		ID:   tid,
		Name: model.NewCIStr("t_rename"),
	}
	// Create table.
	err = t.CreateTableOrView(1, tbInfo100)
	c.Assert(err, IsNil)
	// Update auto ID.
	currentDBID := int64(1)
	n, err = t.GenAutoTableID(currentDBID, tid, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))
	// Fail to update auto ID.
	// The table ID doesn't exist.
	nonExistentID := int64(1234)
	_, err = t.GenAutoTableID(currentDBID, nonExistentID, 10)
	c.Assert(err, NotNil)
	// Fail to update auto ID.
	// The current database ID doesn't exist.
	currentDBID = nonExistentID
	_, err = t.GenAutoTableID(currentDBID, tid, 10)
	c.Assert(err, NotNil)
	// Test case for CreateTableAndSetAutoID.
	tbInfo3 := &model.TableInfo{
		ID:   3,
		Name: model.NewCIStr("tbl3"),
	}
	err = t.CreateTableAndSetAutoID(1, tbInfo3, 123, 0)
	c.Assert(err, IsNil)
	id, err := t.GetAutoTableID(1, tbInfo3.ID)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(123))
	// Test case for GenAutoTableIDKeyValue.
	key, val := t.GenAutoTableIDKeyValue(1, tbInfo3.ID, 1234)
	c.Assert(val, DeepEquals, []byte(strconv.FormatInt(1234, 10)))
	c.Assert(key, DeepEquals, []byte{0x6d, 0x44, 0x42, 0x3a, 0x31, 0x0, 0x0, 0x0, 0x0, 0xfb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x0, 0x0, 0x0, 0xfc})

	err = t.DropDatabase(1)
	c.Assert(err, IsNil)
	err = t.DropDatabase(currentDBID)
	c.Assert(err, IsNil)

	dbs, err = t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, HasLen, 0)

	bootstrapVer, err := t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(0))

	err = t.FinishBootstrap(int64(1))
	c.Assert(err, IsNil)

	bootstrapVer, err = t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(1))

	// Test case for meta.FinishBootstrap with a version.
	err = t.FinishBootstrap(int64(10))
	c.Assert(err, IsNil)
	bootstrapVer, err = t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(10))

	// Test case for SchemaDiff.
	schemaDiff := &model.SchemaDiff{
		Version:    100,
		SchemaID:   1,
		Type:       model.ActionTruncateTable,
		TableID:    2,
		OldTableID: 3,
	}
	err = t.SetSchemaDiff(schemaDiff)
	c.Assert(err, IsNil)
	readDiff, err := t.GetSchemaDiff(schemaDiff.Version)
	c.Assert(err, IsNil)
	c.Assert(readDiff, DeepEquals, schemaDiff)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Test for DDLJobHistoryKey.
	key = meta.DDLJobHistoryKey(t, 888)
	c.Assert(key, DeepEquals, []byte{0x6d, 0x44, 0x44, 0x4c, 0x4a, 0x6f, 0x62, 0x48, 0x69, 0xff, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x78, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7})
}

func (s *testSuite) TestSnapshot(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	txn, _ := store.Begin()
	m := meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ := m.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	txn.Commit(context.Background())

	ver1, _ := store.CurrentVersion()
	time.Sleep(time.Millisecond)
	txn, _ = store.Begin()
	m = meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ = m.GetGlobalID()
	c.Assert(n, Equals, int64(2))
	txn.Commit(context.Background())

	snapshot, _ := store.GetSnapshot(ver1)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	n, _ = snapMeta.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	_, err = snapMeta.GenGlobalID()
	c.Assert(err, NotNil)
}

func (s *testSuite) TestDDL(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	defer txn.Rollback()

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

	err = t.UpdateDDLReorgStartHandle(job, 1)
	c.Assert(err, IsNil)

	i, j, k, err := t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))
	c.Assert(j, Equals, int64(math.MaxInt64))
	c.Assert(k, Equals, int64(0))

	err = t.UpdateDDLReorgHandle(job, 1, 2, 3)
	c.Assert(err, IsNil)

	i, j, k, err = t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))
	c.Assert(j, Equals, int64(2))
	c.Assert(k, Equals, int64(3))

	err = t.RemoveDDLReorgHandle(job)
	c.Assert(err, IsNil)

	i, j, k, err = t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(0))
	// The default value for endHandle is MaxInt64, not 0.
	c.Assert(j, Equals, int64(math.MaxInt64))
	c.Assert(k, Equals, int64(0))

	// Test GetDDLReorgHandle failed.
	_, _, _, err = t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)

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
		job.DecodeArgs(&arg1)
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
	defer txn1.Rollback()

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
}

func (s *testSuite) BenchmarkGenGlobalIDs(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	t := meta.NewMeta(txn)

	c.ResetTimer()
	var ids []int64
	for i := 0; i < c.N; i++ {
		ids, _ = t.GenGlobalIDs(10)
	}
	c.Assert(ids, HasLen, 10)
	c.Assert(ids[9], Equals, int64(c.N)*10)
}

func anyMatch(c *C, ids []int64, candidates ...[]int64) {
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
	c.Assert(match, IsTrue)
}
