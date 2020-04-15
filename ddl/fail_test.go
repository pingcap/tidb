// Copyright 2018 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/types"
)

func (s *testColumnChangeSuite) TestFailBeforeDecodeArgs(c *C) {
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	defer d.Stop()
	// create table t_fail (c1 int, c2 int);
	tblInfo := testTableInfo(c, d, "t_fail", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	// insert t_fail values (1, 2);
	originTable := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	_, err = originTable.AddRecord(ctx, row)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDDLCallback{}
	first := true
	stateCnt := 0
	tc.onJobRunBefore = func(job *model.Job) {
		// It can be other schema states except failed schema state.
		// This schema state can only appear once.
		if job.SchemaState == model.StateWriteOnly {
			stateCnt++
		} else if job.SchemaState == model.StateWriteReorganization {
			if first {
				c.Assert(failpoint.Enable("github.com/pingcap/tidb/v4/ddl/errorBeforeDecodeArgs", `return(true)`), IsNil)
				first = false
			} else {
				c.Assert(failpoint.Disable("github.com/pingcap/tidb/v4/ddl/errorBeforeDecodeArgs"), IsNil)
			}
		}
	}
	d.SetHook(tc)
	defaultValue := int64(3)
	job := testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultValue)
	// Make sure the schema state only appears once.
	c.Assert(stateCnt, Equals, 1)
	testCheckJobDone(c, d, job, true)
}
