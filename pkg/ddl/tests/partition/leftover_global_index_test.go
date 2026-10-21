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
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestCleanupLeftoverGlobalIndexesOnOwnerChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, s int, ct int not null,
		unique key uk_id (id, ct),
		key idx_g (a, s, ct) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")

	tblInfo := external.GetTableByName(t, tk, "test", "t").Meta().Clone()
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	publicGlobal := tblInfo.FindIndexByName("idx_g")
	addIndex := func(name string, state model.SchemaState) int64 {
		idx := publicGlobal.Clone()
		tblInfo.MaxIndexID++
		idx.ID = tblInfo.MaxIndexID
		idx.Name = ast.NewCIStr(name)
		idx.State = state
		tblInfo.Indices = append(tblInfo.Indices, idx)
		return idx.ID
	}
	leftoverIDs := []int64{
		addIndex("idx_g", model.StateDeleteOnly),
		addIndex("idx_g", model.StateWriteReorganization),
	}
	// A non-public global index without a public twin, like one being added, must be kept.
	addingID := addIndex("idx_adding", model.StateWriteReorganization)
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
	require.Len(t, external.GetTableByName(t, tk, "test", "t").Meta().Indices, 5)

	// The new owner scans the partitioned tables once it is elected.
	require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
	require.Eventually(t, func() bool {
		require.NoError(t, dom.Reload())
		after := external.GetTableByName(t, tk, "test", "t").Meta()
		return after.FindIndexByID(leftoverIDs[0]) == nil && after.FindIndexByID(leftoverIDs[1]) == nil
	}, 30*time.Second, 100*time.Millisecond)

	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Len(t, after.Indices, 3)
	require.NotNil(t, after.FindIndexByID(addingID))
	require.Equal(t, publicGlobal.ID, after.FindIndexByName("idx_g").ID)
	require.Equal(t, model.StatePublic, after.FindIndexByName("idx_g").State)
	tk.MustExec("admin check table t")
	tk.MustQuery("select count(*) from t use index(idx_g) where a >= 0").Check(testkit.Rows("4"))
	for _, id := range leftoverIDs {
		idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tblInfo.ID, id))
		tk.MustQuery(fmt.Sprintf(`select count(*) from (select start_key from mysql.gc_delete_range
			union all select start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Check(testkit.Rows("1"))
	}
}
