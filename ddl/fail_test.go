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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func (s *testColumnChangeSuite) TestFailBeforeDecodeArgs(t *testing.T) {
	t.Parallel()

	d := testNewDDLAndStart(
		context.Background(),
		t,
		WithStore(s.store),
		WithLease(testLease),
	)
	defer func() {
		err := d.Stop()
		require.NoError(t, err)
	}()
	// create table t_fail (c1 int, c2 int);
	tblInfo := testTableInfo(t, d, "t_fail", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	require.NoError(t, err)
	testCreateTable(t, ctx, d, s.dbInfo, tblInfo)
	// insert t_fail values (1, 2);
	originTable := testGetTable(t, d, s.dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	_, err = originTable.AddRecord(ctx, row)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

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
				require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/errorBeforeDecodeArgs", `return(true)`))
				first = false
			} else {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/errorBeforeDecodeArgs"))
			}
		}
	}
	d.SetHook(tc)
	defaultValue := int64(3)
	job := testCreateColumn(t, ctx, d, s.dbInfo, tblInfo, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultValue)
	// Make sure the schema state only appears once.
	require.Equal(t, 1, stateCnt)
	testCheckJobDone(t, d, job, true)
}
