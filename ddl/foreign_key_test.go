// Copyright 2016 PingCAP, Inc.
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
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testForeighKeySuite{})

type testForeighKeySuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo
	d      *ddl
	ctx    context.Context
}

func (s *testForeighKeySuite) SetUpSuite(c *C) {
	trySkipTest(c)

	s.store = testCreateStore(c, "test_foreign")
}

func (s *testForeighKeySuite) TearDownSuite(c *C) {
	trySkipTest(c)

	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testForeighKeySuite) testCreateForeignKey(c *C, tblInfo *model.TableInfo, fkName string, keys []string, refTable string, refKeys []string, onDelete ast.ReferOptionType, onUpdate ast.ReferOptionType) *model.Job {
	FKName := model.NewCIStr(fkName)
	Keys := make([]model.CIStr, len(keys))
	for i, key := range keys {
		Keys[i] = model.NewCIStr(key)
	}

	RefTable := model.NewCIStr(refTable)
	RefKeys := make([]model.CIStr, len(refKeys))
	for i, key := range refKeys {
		RefKeys[i] = model.NewCIStr(key)
	}

	fkID, err := s.d.genGlobalID()
	c.Assert(err, IsNil)
	fkInfo := &model.FKInfo{
		ID:       fkID,
		Name:     FKName,
		RefTable: RefTable,
		RefCols:  RefKeys,
		Cols:     Keys,
		OnDelete: int(onDelete),
		OnUpdate: int(onUpdate),
		State:    model.StateNone,
	}

	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddForeignKey,
		Args:     []interface{}{fkInfo},
	}
	err = s.d.doDDLJob(s.ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropForeignKey(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, foreignKeyName string) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropForeignKey,
		Args:     []interface{}{model.NewCIStr(foreignKeyName)},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func getForeignKey(t table.Table, name string) *model.FKInfo {
	for _, fk := range t.Meta().ForeignKeys {
		// only public foreign key can be read.
		if fk.State != model.StatePublic {
			continue
		}
		if fk.Name.L == strings.ToLower(name) {
			return fk
		}
	}
	return nil
}

func (s *testForeighKeySuite) testForeignKeyExist(c *C, t table.Table, name string, isExist bool) {
	fk := getForeignKey(t, name)
	if isExist {
		c.Assert(fk, NotNil)
	} else {
		c.Assert(fk, IsNil)
	}
}

func (s *testForeighKeySuite) TestForeignKey(c *C) {
	defer testleak.AfterTest(c)()
	d := newDDL(s.store, nil, nil, 100*time.Millisecond)
	s.d = d
	s.dbInfo = testSchemaInfo(c, d, "test_foreign")
	ctx := testNewContext(c, d)
	s.ctx = ctx
	testCreateSchema(c, ctx, d, s.dbInfo)
	tblInfo := testTableInfo(c, d, "t", 3)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	err = ctx.CommitTxn()
	c.Assert(err, IsNil)

	checkOK := false
	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if job.State != model.JobDone {
			return
		}

		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
		s.testForeignKeyExist(c, t, "c1_fk", true)
		checkOK = true
	}

	d.hook = tc

	d.close()
	d.start()

	job := s.testCreateForeignKey(c, tblInfo, "c1_fk", []string{"c1"}, "t2", []string{"c1"}, ast.ReferOptionCascade, ast.ReferOptionSetNull)

	testCheckJobDone(c, d, job, true)

	err = ctx.CommitTxn()
	c.Assert(err, IsNil)
	c.Assert(checkOK, IsTrue)

	checkOK = false
	tc.onJobUpdated = func(job *model.Job) {
		if job.State != model.JobDone {
			return
		}
		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
		s.testForeignKeyExist(c, t, "c1_fk", false)
		checkOK = true
	}

	d.close()
	d.start()

	job = testDropForeignKey(c, ctx, d, s.dbInfo, tblInfo, "c1_fk")
	testCheckJobDone(c, d, job, false)
	c.Assert(checkOK, IsTrue)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	tc.onJobUpdated = func(job *model.Job) {
	}

	d.close()
	d.start()

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	err = ctx.CommitTxn()
	c.Assert(err, IsNil)

	d.close()
}
