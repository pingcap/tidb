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
	gofail "github.com/coreos/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

func (s *testColumnChangeSuite) TestFailBeforeDecodeArgs(c *C) {
	defer testleak.AfterTest(c)()
	d := testNewDDL(goctx.Background(), nil, s.store, nil, nil, testLease)
	defer d.Stop()
	// create table t_fail (c1 int, c2 int);
	tblInfo := testTableInfo(c, d, "t_fail", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	// insert t_fail values (1, 2);
	originTable := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	_, err = originTable.AddRecord(ctx, row, false)
	c.Assert(err, IsNil)
	err = ctx.Txn().Commit(goctx.Background())
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
				gofail.Enable("github.com/pingcap/tidb/ddl/errorBeforeDecodeArgs", `return(true)`)
				first = false
			} else {
				gofail.Disable("github.com/pingcap/tidb/ddl/errorBeforeDecodeArgs")
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
