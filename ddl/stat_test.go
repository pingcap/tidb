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

package ddl

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testStatSuite{})
var _ = SerialSuites(&testSerialStatSuite{})

type testStatSuite struct {
}

func (s *testStatSuite) SetUpSuite(c *C) {
}

func (s *testStatSuite) TearDownSuite(c *C) {
}

type testSerialStatSuite struct {
}

func (s *testStatSuite) getDDLSchemaVer(c *C, d *ddl) int64 {
	m, err := d.Stats(nil)
	c.Assert(err, IsNil)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

func (s *testStatSuite) TestStat(c *C) {
	store := testCreateStore(c, "test_stat")
	defer store.Close()

	d := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// TODO: Get this information from etcd.
	//	m, err := d.Stats(nil)
	//	c.Assert(err, IsNil)
	//	c.Assert(m[ddlOwnerID], Equals, d.uuid)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo.Name},
	}

	ctx := mock.NewContext()
	ctx.Store = store
	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := s.getDDLSchemaVer(c, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			c.Assert(s.getDDLSchemaVer(c, d), GreaterEqual, ver)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			c.Assert(err, IsNil)
			// TODO: Get this information from etcd.
			// m, err := d.Stats(nil)
			// c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testSerialStatSuite) TestDDLStatsInfo(c *C) {
	store := testCreateStore(c, "test_stat")
	defer store.Close()

	d := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	tblInfo := testTableInfo(c, d, "t", 2)
	ctx := testNewContext(d)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	t := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err := t.AddRecord(ctx, types.MakeDatums(1, 1))
	c.Assert(err, IsNil)
	_, err = t.AddRecord(ctx, types.MakeDatums(2, 2))
	c.Assert(err, IsNil)
	_, err = t.AddRecord(ctx, types.MakeDatums(3, 3))
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"), IsNil)
	}()

	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	exit := false
	for !exit {
		select {
		case err := <-done:
			c.Assert(err, IsNil)
			exit = true
		case <-TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			c.Assert(err, IsNil)
			c.Assert(varMap[ddlJobReorgHandle], Equals, "1")
		}
	}
}
