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

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

// createLeftoverGlobalIndexes creates table test.t and adds two copies of its public global index
// idx_g in different non-public states.
func createLeftoverGlobalIndexes(t *testing.T, store kv.Storage, dom *domain.Domain, tk *testkit.TestKit) (
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, leftoverIDs []int64) {
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, s int, ct int not null,
		unique key uk_id (id, ct),
		key idx_g (a, s, ct) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")
	return addIndexCopies(t, store, dom, tk, "idx_g", model.StateDeleteOnly, model.StateWriteReorganization)
}

// addIndexCopies adds a copy of the public index of test.t in each given state, like a cancelled
// REORGANIZE PARTITION leaves behind.
func addIndexCopies(t *testing.T, store kv.Storage, dom *domain.Domain, tk *testkit.TestKit, idxName string, states ...model.SchemaState) (
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, copyIDs []int64) {
	tblInfo = external.GetTableByName(t, tk, "test", "t").Meta().Clone()
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	public := tblInfo.FindIndexByName(idxName)
	require.Equal(t, model.StatePublic, public.State)
	for _, state := range states {
		idx := public.Clone()
		tblInfo.MaxIndexID++
		idx.ID = tblInfo.MaxIndexID
		idx.State = state
		tblInfo.Indices = append(tblInfo.Indices, idx)
		copyIDs = append(copyIDs, idx.ID)
	}
	updateTableMeta(t, store, dom, dbInfo.ID, tblInfo)
	return dbInfo, tblInfo, copyIDs
}

func updateTableMeta(t *testing.T, store kv.Storage, dom *domain.Domain, dbID int64, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		if err := m.UpdateTable(dbID, tblInfo); err != nil {
			return err
		}
		ver, err := m.GenSchemaVersion()
		if err != nil {
			return err
		}
		return m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionAddIndex, SchemaID: dbID, TableID: tblInfo.ID})
	}))
	require.NoError(t, dom.Reload())
}

// checkLeftoverGlobalIndexesDropped checks that only public indexes are left in test.t, the table
// data is consistent, and delete ranges are queued for the leftovers.
func checkLeftoverGlobalIndexesDropped(t *testing.T, tk *testkit.TestKit, tableID int64, leftoverIDs []int64) {
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	for _, id := range leftoverIDs {
		require.Nil(t, after.FindIndexByID(id))
	}
	for _, idx := range after.Indices {
		require.Equal(t, model.StatePublic, idx.State, idx.Name.O)
	}
	tk.MustExec("admin check table t")
	for _, id := range leftoverIDs {
		idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, id))
		tk.MustQuery(fmt.Sprintf(`select count(*) from (select start_key from mysql.gc_delete_range
			union all select start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Check(testkit.Rows("1"))
	}
}

func checkPublicGlobalIndex(t *testing.T, tk *testkit.TestKit, tblInfo *model.TableInfo) {
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Equal(t, tblInfo.FindIndexByName("idx_g").ID, after.FindIndexByName("idx_g").ID)
	tk.MustQuery("select count(*) from t use index(idx_g) where a >= 0").Check(testkit.Rows("4"))
}

func TestCleanupLeftoverGlobalIndexesOnOwnerChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, tblInfo, leftoverIDs := createLeftoverGlobalIndexes(t, store, dom, tk)

	// The new owner scans the partitioned tables once it is elected.
	require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
	require.Eventually(t, func() bool {
		require.NoError(t, dom.Reload())
		after := external.GetTableByName(t, tk, "test", "t").Meta()
		return after.FindIndexByID(leftoverIDs[0]) == nil && after.FindIndexByID(leftoverIDs[1]) == nil
	}, 30*time.Second, 100*time.Millisecond)
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo.ID, leftoverIDs)
	checkPublicGlobalIndex(t, tk, tblInfo)
}

func TestCleanupLeftoverIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	dbInfo, tblInfo, leftoverIDs := createLeftoverGlobalIndexes(t, store, dom, tk)

	ctx := context.Background()
	indexIDs, ver, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.Greater(t, ver, int64(0))
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo.ID, leftoverIDs)
	checkPublicGlobalIndex(t, tk, tblInfo)

	indexIDs, _, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Empty(t, indexIDs)
}

func TestCleanupLeftoverIndexesAfterReorganizePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// A successful REORGANIZE PARTITION keeps DDLChangedIndex.
	tk.MustExec("use test")
	tk.MustExec(`create table t2 (a int, b int, unique key uk_b (b) global)
		partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))`)
	tk.MustExec("alter table t2 reorganize partition p0, p1 into (partition p01 values less than (20))")
	pi := external.GetTableByName(t, tk, "test", "t2").Meta().Partition
	require.Equal(t, model.StateNone, pi.DDLState)
	require.Empty(t, pi.AddingDefinitions)
	require.Empty(t, pi.DroppingDefinitions)
	require.NotEmpty(t, pi.DDLChangedIndex)

	dbInfo, tblInfo, leftoverIDs := createLeftoverGlobalIndexes(t, store, dom, tk)
	tblInfo.Partition.DDLChangedIndex = map[int64]bool{tblInfo.FindIndexByName("idx_g").ID: true}
	updateTableMeta(t, store, dom, dbInfo.ID, tblInfo)

	indexIDs, _, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo.ID, leftoverIDs)
	checkPublicGlobalIndex(t, tk, tblInfo)
}

func TestCleanupLeftoverIndexesWithoutPublicTwin(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	dbInfo, tblInfo, leftoverIDs := createLeftoverGlobalIndexes(t, store, dom, tk)

	// DROP INDEX drops the first index with the name, which is the public one.
	tk.MustExec("alter table t drop index idx_g")
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Nil(t, after.FindIndexByID(tblInfo.FindIndexByName("idx_g").ID))
	require.NotNil(t, after.FindIndexByID(leftoverIDs[0]))

	indexIDs, _, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo.ID, leftoverIDs)
	require.Nil(t, external.GetTableByName(t, tk, "test", "t").Meta().FindIndexByName("idx_g"))
}

func TestCleanupLeftoverExpressionIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, ct int not null,
		key idx_e ((a + 1)) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 1, 5), (2, 2, 15), (3, 3, 25)")
	dbInfo, tblInfo, leftoverIDs := addIndexCopies(t, store, dom, tk, "idx_e", model.StateDeleteOnly)
	hiddenCol := tblInfo.Columns[tblInfo.FindIndexByName("idx_e").Columns[0].Offset]
	require.True(t, hiddenCol.Hidden)

	indexIDs, _, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo.ID, leftoverIDs)
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Len(t, after.Columns, len(tblInfo.Columns))
	require.NotNil(t, model.FindColumnInfo(after.Columns, hiddenCol.Name.L))
	tk.MustQuery("select count(*) from t use index(idx_e) where a + 1 > 2").Check(testkit.Rows("2"))
}
