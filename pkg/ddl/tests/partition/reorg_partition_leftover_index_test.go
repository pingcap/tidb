// Copyright 2026 PingCAP, Inc.
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

package partition

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const leftoverIndexTableSQL = `create table t (id bigint not null, a int, s int, ct int not null,
	unique key uk_id (id, ct),
	key idx_l (s),
	key idx_g (a, s, ct) global)
	partition by range (ct) (
	partition p0 values less than (10),
	partition p1 values less than (20),
	partition pMax values less than (maxvalue))`

func countUpdateMutations(t *testing.T, tk *testkit.TestKit) int {
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t set a = a + 1 where id = 3")
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	n := txn.Len()
	tk.MustExec("rollback")
	return n
}

func TestReorgPartitionCancelNonUniqueGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(leftoverIndexTableSQL)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")
	baseIndices := len(external.GetTableByName(t, tk, "test", "t").Meta().Indices)
	baseMutations := countUpdateMutations(t, tk)

	var cancelState model.SchemaState
	var lastRollbackStates []model.SchemaState
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionReorganizePartition && job.Type != model.ActionAlterTablePartitioning {
			return
		}
		if job.IsRollingback() {
			lastRollbackStates = lastRollbackStates[:0]
			for _, idx := range external.GetTableByName(t, testkit.NewTestKit(t, store), "test", "t").Meta().Indices {
				if idx.State != model.StatePublic {
					lastRollbackStates = append(lastRollbackStates, idx.State)
				}
			}
			return
		}
		if cancelState == model.StateNone || job.SchemaState != cancelState {
			return
		}
		cancelState = model.StateNone
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
	})

	for _, ddl := range []string{
		"alter table t reorganize partition pMax into (partition p2 values less than (30), partition pMax values less than (maxvalue))",
		"alter table t partition by range (ct) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30), partition pMax values less than (maxvalue))",
	} {
		for _, state := range []model.SchemaState{
			model.StateDeleteOnly,
			model.StateWriteOnly,
			model.StateWriteReorganization,
			model.StateDeleteReorganization,
		} {
			cancelState = state
			lastRollbackStates = nil
			tk.MustGetErrCode(ddl, errno.ErrCancelledDDLJob)
			tblInfo := external.GetTableByName(t, tk, "test", "t").Meta()
			// The copies must be delete only in the step that removes them.
			require.NotEmpty(t, lastRollbackStates, "%s, cancel at %s", ddl, state)
			for _, s := range lastRollbackStates {
				require.Equal(t, model.StateDeleteOnly, s, "%s, cancel at %s", ddl, state)
			}
			require.Len(t, tblInfo.Indices, baseIndices, "%s, cancel at %s", ddl, state)
			for _, idx := range tblInfo.Indices {
				require.Equal(t, model.StatePublic, idx.State, "%s, cancel at %s, index %s", ddl, state, idx.Name.O)
			}
			require.Equal(t, baseMutations, countUpdateMutations(t, tk), "%s, cancel at %s", ddl, state)
			tk.MustExec("admin check table t")
		}
	}
}

func TestReorgPartitionRollbackResumeAfterOwnerChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(leftoverIndexTableSQL)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")
	baseIndices := len(external.GetTableByName(t, tk, "test", "t").Meta().Indices)
	baseMutations := countUpdateMutations(t, tk)

	cancelled, resigned := false, false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionReorganizePartition {
			return
		}
		if !cancelled && job.SchemaState == model.StateWriteReorganization {
			cancelled = true
			testkit.NewTestKit(t, store).MustExec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			return
		}
		// Change the owner between the rollback steps, the new owner must resume the rollback.
		if job.IsRollingback() && !resigned {
			resigned = true
			require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
		}
	})
	tk.MustGetErrCode("alter table t reorganize partition pMax into (partition p2 values less than (30), partition pMax values less than (maxvalue))", errno.ErrCancelledDDLJob)
	require.True(t, resigned)
	tblInfo := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Len(t, tblInfo.Indices, baseIndices)
	for _, idx := range tblInfo.Indices {
		require.Equal(t, model.StatePublic, idx.State, idx.Name.O)
	}
	require.Equal(t, baseMutations, countUpdateMutations(t, tk))
	tk.MustExec("admin check table t")
}

func TestReorgPartitionGlobalIndexCopySharedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, ct int not null,
		key idx_a (a) global,
		key idx_e ((a + 1)) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 1, 5), (2, 2, 15), (3, 3, 25)")
	check := func() {
		tk.MustExec("admin check table t")
		tk.MustQuery("show columns from t where Field = 'a'").CheckAt([]int{3}, testkit.Rows("MUL"))
		tk.MustQuery("select count(*) from t use index(idx_e) where a + 1 > 2").Check(testkit.Rows("2"))
	}

	cancelAtWriteReorg := true
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if !cancelAtWriteReorg || job.Type != model.ActionReorganizePartition || job.SchemaState != model.StateWriteReorganization {
			return
		}
		cancelAtWriteReorg = false
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
	})
	tk.MustGetErrCode("alter table t reorganize partition pMax into (partition p1 values less than (20), partition pMax values less than (maxvalue))", errno.ErrCancelledDDLJob)
	check()
	tk.MustExec("alter table t reorganize partition pMax into (partition p1 values less than (20), partition pMax values less than (maxvalue))")
	check()
}
