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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) TestMeta(c *C) {
	defer testleak.AfterTest(c)()
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
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
	err = t.CreateTable(1, tbInfo)
	c.Assert(err, IsNil)

	n, err = t.GenAutoTableID(1, 1, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	n, err = t.GetAutoTableID(1, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	err = t.CreateTable(1, tbInfo)
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
	err = t.CreateTable(1, tbInfo2)
	c.Assert(err, IsNil)

	tables, err := t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, []*model.TableInfo{tbInfo, tbInfo2})

	err = t.DropTable(1, 2)
	c.Assert(err, IsNil)

	tables, err = t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, []*model.TableInfo{tbInfo})

	err = t.DropDatabase(1)
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
	err = t.SetSchemaDiff(schemaDiff.Version, schemaDiff)
	c.Assert(err, IsNil)
	readDiff, err := t.GetSchemaDiff(schemaDiff.Version)
	c.Assert(readDiff, DeepEquals, schemaDiff)

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestSnapshot(c *C) {
	defer testleak.AfterTest(c)()
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer store.Close()

	txn, _ := store.Begin()
	m := meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ := m.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	txn.Commit()

	ver1, _ := store.CurrentVersion()
	time.Sleep(time.Millisecond)
	txn, _ = store.Begin()
	m = meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ = m.GetGlobalID()
	c.Assert(n, Equals, int64(2))
	txn.Commit()

	snapshot, _ := store.GetSnapshot(ver1)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	n, _ = snapMeta.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	_, err = snapMeta.GenGlobalID()
	c.Assert(err, NotNil)
}

func (s *testSuite) TestDDL(c *C) {
	defer testleak.AfterTest(c)()
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	defer txn.Rollback()

	t := meta.NewMeta(txn)

	owner := &model.Owner{OwnerID: "1"}
	err = t.SetDDLJobOwner(owner)
	c.Assert(err, IsNil)
	ov, err := t.GetDDLJobOwner()
	c.Assert(err, IsNil)
	c.Assert(owner, DeepEquals, ov)

	job := &model.Job{ID: 1}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	n, err := t.DDLJobQueueLen()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	v, err := t.GetDDLJob(0)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)
	v, err = t.GetDDLJob(1)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
	job.ID = 2
	err = t.UpdateDDLJob(0, job)
	c.Assert(err, IsNil)

	err = t.UpdateDDLReorgHandle(job, 1)
	c.Assert(err, IsNil)

	h, err := t.GetDDLReorgHandle(job)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))

	err = t.RemoveDDLReorgHandle(job)
	c.Assert(err, IsNil)

	v, err = t.DeQueueDDLJob()
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	err = t.AddHistoryDDLJob(job)
	c.Assert(err, IsNil)
	v, err = t.GetHistoryDDLJob(2)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	all, err := t.GetAllHistoryDDLJobs()
	c.Assert(err, IsNil)
	var lastID int64
	for _, job := range all {
		c.Assert(job.ID, Greater, lastID)
		lastID = job.ID
	}

	// DDL background job test
	err = t.SetBgJobOwner(owner)
	c.Assert(err, IsNil)
	ov, err = t.GetBgJobOwner()
	c.Assert(err, IsNil)
	c.Assert(owner, DeepEquals, ov)

	bgJob := &model.Job{ID: 1}
	err = t.EnQueueBgJob(bgJob)
	c.Assert(err, IsNil)
	n, err = t.BgJobQueueLen()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	v, err = t.GetBgJob(0)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, bgJob)
	v, err = t.GetBgJob(1)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
	bgJob.ID = 2
	err = t.UpdateBgJob(0, bgJob)
	c.Assert(err, IsNil)

	v, err = t.DeQueueBgJob()
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, bgJob)

	err = t.AddHistoryBgJob(bgJob)
	c.Assert(err, IsNil)
	v, err = t.GetHistoryBgJob(2)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, bgJob)

	err = txn.Commit()
	c.Assert(err, IsNil)
}
