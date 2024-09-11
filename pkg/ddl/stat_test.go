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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestGetDDLInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sess := tk.Session()
	tk.MustExec("begin")
	txn, err := sess.Txn(true)
	require.NoError(t, err)

	dbInfo2 := &model.DBInfo{
		ID: 2,
	}
	job := &model.Job{
		Version:  model.JobVersion1,
		ID:       1,
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
		RowCount: 0,
	}
	job1 := &model.Job{
		Version:  model.JobVersion1,
		ID:       2,
		SchemaID: dbInfo2.ID,
		Type:     model.ActionAddIndex,
		RowCount: 0,
	}

	err = addDDLJobs(sess, txn, job)
	require.NoError(t, err)

	info, err := ddl.GetDDLInfo(sess)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 1)
	require.Equal(t, job, info.Jobs[0])
	require.Nil(t, info.ReorgHandle)

	// two jobs
	err = addDDLJobs(sess, txn, job1)
	require.NoError(t, err)

	info, err = ddl.GetDDLInfo(sess)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 2)
	require.Equal(t, job, info.Jobs[0])
	require.Equal(t, job1, info.Jobs[1])
	require.Nil(t, info.ReorgHandle)

	tk.MustExec("rollback")
}

func addDDLJobs(sess sessiontypes.Session, txn kv.Transaction, job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return err
	}
	_, err = sess.Execute(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), fmt.Sprintf("insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values (%d, %t, %s, %s, %s, %d, %t)",
		job.ID, job.MayNeedReorg(), strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), strconv.Quote(strconv.FormatInt(job.TableID, 10)), util.WrapKey2String(b), job.Type, false))
	return err
}

func TestIssue42268(t *testing.T) {
	// issue 42268 missing table name in 'admin show ddl' result during drop table
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_0")
	tk.MustExec("create table t_0 (c1 int, c2 int)")

	tbl := external.GetTableByName(t, tk, "test", "t_0")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateNone:
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			rs := tk1.MustQuery("admin show ddl jobs")
			tblName := fmt.Sprintf("%s", rs.Rows()[0][2])
			require.Equal(t, tblName, "t_0")
		}
	})

	tk.MustExec("drop table t_0")
}
