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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/types"
)

type testCtxKeyType int

func (k testCtxKeyType) String() string {
	return "test_ctx_key"
}

const testCtxKey testCtxKeyType = 0

func (s *testDDLSuite) TestReorg(c *C) {
	store := testCreateStore(c, "test_reorg")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()

	time.Sleep(testLease)

	ctx := testNewContext(d)

	ctx.SetValue(testCtxKey, 1)
	c.Assert(ctx.Value(testCtxKey), Equals, 1)
	ctx.ClearValue(testCtxKey)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	rowCount := int64(10)
	handle := int64(100)
	f := func() error {
		d.generalWorker().reorgCtx.setRowCount(rowCount)
		d.generalWorker().reorgCtx.setNextHandle(handle)
		time.Sleep(1*ReorgWaitTimeout + 100*time.Millisecond)
		return nil
	}
	job := &model.Job{
		ID:          1,
		SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
	}
	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	rInfo := &reorgInfo{
		Job: job,
	}
	err = d.generalWorker().runReorgJob(m, rInfo, nil, d.lease, f)
	c.Assert(err, NotNil)

	// The longest to wait for 5 seconds to make sure the function of f is returned.
	for i := 0; i < 1000; i++ {
		time.Sleep(5 * time.Millisecond)
		err = d.generalWorker().runReorgJob(m, rInfo, nil, d.lease, f)
		if err == nil {
			c.Assert(job.RowCount, Equals, rowCount)
			c.Assert(d.generalWorker().reorgCtx.rowCount, Equals, int64(0))

			// Test whether reorgInfo's Handle is update.
			err = txn.Commit(context.Background())
			c.Assert(err, IsNil)
			err = ctx.NewTxn(context.Background())
			c.Assert(err, IsNil)

			m = meta.NewMeta(txn)
			info, err1 := getReorgInfo(d.ddlCtx, m, job, nil)
			c.Assert(err1, IsNil)
			c.Assert(info.StartHandle, Equals, handle)
			c.Assert(d.generalWorker().reorgCtx.doneHandle, Equals, int64(0))
			break
		}
	}
	c.Assert(err, IsNil)

	job = &model.Job{
		ID:          2,
		SchemaID:    1,
		Type:        model.ActionCreateSchema,
		Args:        []interface{}{model.NewCIStr("test")},
		SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
	}

	var info *reorgInfo
	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		info, err1 = getReorgInfo(d.ddlCtx, t, job, nil)
		c.Assert(err1, IsNil)
		err1 = info.UpdateReorgMeta(txn, 1, 0, 0)
		c.Assert(err1, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		info, err1 = getReorgInfo(d.ddlCtx, t, job, nil)
		c.Assert(err1, IsNil)
		c.Assert(info.StartHandle, Greater, int64(0))
		return nil
	})
	c.Assert(err, IsNil)

	d.Stop()
	err = d.generalWorker().runReorgJob(m, rInfo, nil, d.lease, func() error {
		time.Sleep(4 * testLease)
		return nil
	})
	c.Assert(err, NotNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testDDLSuite) TestReorgOwner(c *C) {
	store := testCreateStore(c, "test_reorg_owner")
	defer store.Close()

	d1 := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d1.Stop()

	ctx := testNewContext(d1)

	testCheckOwner(c, d1, true)

	d2 := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d2.Stop()

	dbInfo := testSchemaInfo(c, d1, "test")
	testCreateSchema(c, ctx, d1, dbInfo)

	tblInfo := testTableInfo(c, d1, "t", 3)
	testCreateTable(c, ctx, d1, dbInfo, tblInfo)
	t := testGetTable(c, d1, dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeDatums(i, i, i))
		c.Assert(err, IsNil)
	}

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDDLCallback{}
	tc.onJobRunBefore = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteReorganization {
			d1.Stop()
		}
	}

	d1.SetHook(tc)

	testDropSchema(c, ctx, d1, dbInfo)

	err = kv.RunInNewTxn(d1.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		db, err1 := t.GetDatabase(dbInfo.ID)
		c.Assert(err1, IsNil)
		c.Assert(db, IsNil)
		return nil
	})
	c.Assert(err, IsNil)
}
