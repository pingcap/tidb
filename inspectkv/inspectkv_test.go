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

package inspectkv

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testInspectSuite{})

type testInspectSuite struct {
}

func (s *testInspectSuite) TestInspect(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory:test_inspect")
	c.Assert(err, IsNil)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)

	t := meta.NewMeta(txn)

	dbInfo := &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = t.CreateDatabase(dbInfo)
	c.Assert(err, IsNil)

	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c"),
		ID:           0,
		Offset:       0,
		DefaultValue: 1,
		State:        model.StatePublic,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
	}
	idx := &model.IndexInfo{
		Name:   model.NewCIStr("c"),
		ID:     1,
		Unique: true,
		Columns: []*model.IndexColumn{{
			Name:   model.NewCIStr("c"),
			Offset: 0,
			Length: 255,
		}},
		State: model.StatePublic,
	}
	tbInfo := &model.TableInfo{
		ID:      1,
		Name:    model.NewCIStr("t"),
		State:   model.StatePublic,
		Columns: []*model.ColumnInfo{col},
		Indices: []*model.IndexInfo{idx},
	}
	err = t.CreateTable(dbInfo.ID, tbInfo)
	c.Assert(err, IsNil)

	owner := &model.Owner{OwnerID: "owner"}
	err = t.SetDDLOwner(owner)
	c.Assert(err, IsNil)
	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
	}
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	info, err := GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Owner, DeepEquals, owner)
	c.Assert(info.Job, DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, int64(0))
	txn.Commit()

	ctx := newmockContext(store)
	c.Assert(err, IsNil)
	alloc := autoid.NewAllocator(store, dbInfo.ID)
	tb := tables.TableFromMeta(alloc, tbInfo)
	indices := tb.Indices()
	_, err = tb.AddRecord(ctx, []interface{}{10}, 0)
	c.Assert(err, IsNil)
	ctx.FinishTxn(false)
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	data, _, err := ScanSnapshotTableData(store, ver, tb, tb.FirstKey(), 1)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, [][]interface{}{{int64(10)}})

	_, err = tb.AddRecord(ctx, []interface{}{20}, 0)
	c.Assert(err, IsNil)
	ctx.FinishTxn(false)
	txn, err = store.Begin()
	c.Assert(err, IsNil)

	handles, vals, nextKey, err := ScanTableData(tb, txn, tb.FirstKey(), 1)
	c.Assert(err, IsNil)
	c.Assert(handles, DeepEquals, []int64{int64(1)})
	c.Assert(vals, DeepEquals, [][]interface{}{{int64(10)}})
	handles, vals, nextKey, err = ScanTableData(tb, txn, nextKey, 1)
	c.Assert(err, IsNil)
	c.Assert(handles, DeepEquals, []int64{int64(2)})
	c.Assert(vals, DeepEquals, [][]interface{}{{int64(20)}})
	handles, vals, nextKey, err = ScanTableData(tb, txn, nextKey, 1)
	c.Assert(handles, IsNil)
	c.Assert(vals, IsNil)
	c.Assert(nextKey, Equals, "")
	c.Assert(err, IsNil)

	handles, vals, nextVals, err := ScanIndexData(tb.IndexPrefix(), indices[0], txn, []interface{}{int64(10)}, 1)
	c.Assert(err, IsNil)
	c.Assert(handles, DeepEquals, []int64{int64(1)})
	c.Assert(vals, DeepEquals, [][]interface{}{{int64(10)}})
	handles, vals, nextVals, err = ScanIndexData(tb.IndexPrefix(), indices[0], txn, nextVals, 1)
	c.Assert(err, IsNil)
	c.Assert(handles, DeepEquals, []int64{int64(2)})
	c.Assert(vals, DeepEquals, [][]interface{}{{int64(20)}})
	handles, vals, nextVals, err = ScanIndexData(tb.IndexPrefix(), indices[0], txn, nextVals, 1)
	c.Assert(handles, IsNil)
	c.Assert(vals, IsNil)
	c.Assert(nextVals, DeepEquals, []interface{}{[]byte{}})
	c.Assert(err, IsNil)
}

// mockContext represents mocked context.Context.
type mockContext struct {
	values map[fmt.Stringer]interface{}
	txn    kv.Transaction
	store  kv.Storage
}

func (c *mockContext) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

func (c *mockContext) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

func (c *mockContext) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

func (c *mockContext) GetTxn(forceNew bool) (kv.Transaction, error) {
	if c.txn != nil {
		return c.txn, nil
	}

	var err error
	c.txn, err = c.store.Begin()
	return c.txn, err
}

func (c *mockContext) FinishTxn(rollback bool) error {
	if c.txn == nil {
		return nil
	}

	err := c.txn.Commit()
	c.txn = nil
	return err
}

func newmockContext(store kv.Storage) *mockContext {
	ctx := &mockContext{
		values: make(map[fmt.Stringer]interface{}),
		txn:    nil,
		store:  store,
	}

	v := &variable.SessionVars{
		Users:         make(map[string]string),
		Systems:       make(map[string]string),
		PreparedStmts: make(map[string]interface{}),
	}
	ctx.SetValue(variable.SessionVarsKey, v)

	return ctx
}
