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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

type testCtxKeyType int

func (k testCtxKeyType) String() string {
	return "test_ctx_key"
}

const testCtxKey testCtxKeyType = 0

func (s *testDDLSuite) TestReorg(c *C) {
	store := testCreateStore(c, "test_reorg")
	defer store.Close()

	lease := 50 * time.Millisecond
	d := newDDL(store, nil, nil, lease)
	defer d.close()

	time.Sleep(lease)

	ctx := testNewContext(c, d)

	ctx.SetValue(testCtxKey, 1)
	c.Assert(ctx.Value(testCtxKey), Equals, 1)
	ctx.ClearValue(testCtxKey)

	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	txn.Set([]byte("a"), []byte("b"))
	err = ctx.FinishTxn(true)
	c.Assert(err, IsNil)

	txn, err = ctx.GetTxn(false)
	c.Assert(err, IsNil)
	txn.Set([]byte("a"), []byte("b"))
	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	done := make(chan struct{})
	f := func() error {
		time.Sleep(200 * time.Millisecond)
		close(done)
		return nil
	}
	err = d.runReorgJob(f)
	c.Assert(err, NotNil)

	<-done
	err = d.runReorgJob(f)
	c.Assert(err, IsNil)

	d.close()
	err = d.runReorgJob(func() error {
		time.Sleep(1 * time.Second)
		return nil
	})
	c.Assert(err, NotNil)
	d.start()

	job := &model.Job{
		ID:       1,
		SchemaID: 1,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{model.NewCIStr("test")},
	}

	var info *reorgInfo
	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		info, err = d.getReorgInfo(t, job)
		c.Assert(err, IsNil)
		err = info.UpdateHandle(txn, 1)
		c.Assert(err, IsNil)

		return nil
	})
	c.Assert(err, IsNil)

	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		info, err = d.getReorgInfo(t, job)
		c.Assert(err, IsNil)
		c.Assert(info.Handle, Greater, int64(0))
		return nil
	})
	c.Assert(err, IsNil)

	err = info.RemoveHandle()
	c.Assert(err, IsNil)
}
