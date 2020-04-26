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
	"context"
	"strings"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

var _ = Suite(&testForeighKeySuite{})

type testForeighKeySuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo
	d      *ddl
	ctx    sessionctx.Context
}

func (s *testForeighKeySuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_foreign")
}

func (s *testForeighKeySuite) TearDownSuite(c *C) {
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

	fkInfo := &model.FKInfo{
		Name:     FKName,
		RefTable: RefTable,
		RefCols:  RefKeys,
		Cols:     Keys,
		OnDelete: int(onDelete),
		OnUpdate: int(onUpdate),
		State:    model.StateNone,
	}

	job := &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}
	err := s.ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	err = s.d.doDDLJob(s.ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropForeignKey(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, foreignKeyName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(foreignKeyName)},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

func (s *testForeighKeySuite) TestForeignKey(c *C) {
	d := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(s.store),
		WithLease(testLease),
	)
	defer d.Stop()
	s.d = d
	s.dbInfo = testSchemaInfo(c, d, "test_foreign")
	ctx := testNewContext(d)
	s.ctx = ctx
	testCreateSchema(c, ctx, d, s.dbInfo)
	tblInfo := testTableInfo(c, d, "t", 3)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// fix data race
	var mu sync.Mutex
	checkOK := false
	var hookErr error
	tc := &TestDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if job.State != model.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t table.Table
		t, err = testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk == nil {
			hookErr = errors.New("foreign key not exists")
			return
		}
		checkOK = true
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	job := s.testCreateForeignKey(c, tblInfo, "c1_fk", []string{"c1"}, "t2", []string{"c1"}, ast.ReferOptionCascade, ast.ReferOptionSetNull)
	testCheckJobDone(c, d, job, true)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})

	mu.Lock()
	checkOK = false
	mu.Unlock()
	// fix data race pr/#9491
	tc2 := &TestDDLCallback{}
	tc2.onJobUpdated = func(job *model.Job) {
		if job.State != model.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t table.Table
		t, err = testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk != nil {
			hookErr = errors.New("foreign key has not been dropped")
			return
		}
		checkOK = true
	}
	d.SetHook(tc2)

	job = testDropForeignKey(c, ctx, d, s.dbInfo, tblInfo, "c1_fk")
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr = hookErr
	ok = checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}
