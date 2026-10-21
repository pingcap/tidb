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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
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

func TestDropLeftoverReorgPartitionIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(leftoverIndexTableSQL)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")
	baseMutations := countUpdateMutations(t, tk)

	tblInfo := external.GetTableByName(t, tk, "test", "t").Meta().Clone()
	dom := domain.GetDomain(tk.Session())
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	publicGlobal := tblInfo.FindIndexByName("idx_g")
	// A delete only copy comes before a writable one, both must pass delete only before removal.
	var leftoverIDs []int64
	for _, state := range []model.SchemaState{model.StateDeleteOnly, model.StateWriteReorganization} {
		leftover := publicGlobal.Clone()
		tblInfo.MaxIndexID++
		leftover.ID = tblInfo.MaxIndexID
		leftover.State = state
		tblInfo.Indices = append(tblInfo.Indices, leftover)
		leftoverIDs = append(leftoverIDs, leftover.ID)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		if err := m.UpdateTable(dbInfo.ID, tblInfo); err != nil {
			return err
		}
		ver, err := m.GenSchemaVersion()
		if err != nil {
			return err
		}
		return m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionAddIndex, SchemaID: dbInfo.ID, TableID: tblInfo.ID})
	}))
	require.NoError(t, dom.Reload())
	require.Greater(t, countUpdateMutations(t, tk), baseMutations)

	seenDeleteOnly := make(map[int64]bool)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionDropIndex {
			return
		}
		for _, idx := range external.GetTableByName(t, testkit.NewTestKit(t, store), "test", "t").Meta().Indices {
			if idx.State == model.StateDeleteOnly {
				seenDeleteOnly[idx.ID] = true
			}
		}
	})
	cleanupSQL := "alter table t drop index `_tidb$_global_idx_cleanup`"
	require.ErrorContains(t, tk.ExecToErr(cleanupSQL+", add index idx_tmp (s)"), "multi schema change for cleanup global indexes")
	tk.MustExec(cleanupSQL)
	require.ErrorContains(t, tk.ExecToErr(cleanupSQL), "no non-public global index")
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Len(t, after.Indices, 3)
	require.Equal(t, publicGlobal.ID, after.FindIndexByName("idx_g").ID)
	require.Equal(t, model.StatePublic, after.FindIndexByName("idx_g").State)
	require.Equal(t, baseMutations, countUpdateMutations(t, tk))
	tk.MustExec("admin check table t")
	tk.MustQuery("select count(*) from t use index(idx_g) where a >= 0").Check(testkit.Rows("4"))
	for _, id := range leftoverIDs {
		require.True(t, seenDeleteOnly[id], "index %d skipped delete only", id)
		require.Nil(t, after.FindIndexByID(id))
		idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tblInfo.ID, id))
		tk.MustQuery(fmt.Sprintf(`select count(*) from (select start_key from mysql.gc_delete_range
			union all select start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Check(testkit.Rows("1"))
	}

	// A real index with the reserved name is dropped as usual.
	tk.MustExec("create index `_tidb$_global_idx_cleanup` on t (s)")
	tk.MustExec(cleanupSQL)
	require.Nil(t, external.GetTableByName(t, tk, "test", "t").Meta().FindIndexByName("_tidb$_global_idx_cleanup"))
	require.Len(t, external.GetTableByName(t, tk, "test", "t").Meta().Indices, 3)
}
