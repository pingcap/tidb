// Copyright 2025 PingCAP, Inc.
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
	"errors"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

// Unit tests for keyrange building

type mockCodec struct {
	tikv.Codec
}

func (mockCodec) GetKeyspaceMeta() *keyspacepb.KeyspaceMeta {
	return nil
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

func TestAffinityBuildGroupDefinitionsTable(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       123,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable},
	}

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(&mockCodec{}, tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	ranges := groups[ddl.GetTableAffinityGroupID(&mockCodec{}, 123)]
	require.Len(t, ranges, 1)
	require.Equal(t, pdhttp.AffinityGroupKeyRange{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(123)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(124)...),
	}, ranges[0])
}

func TestAffinityGroupIDIncludesKeyspaceInNextGen(t *testing.T) {
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)
	otherCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 43},
	})
	require.NoError(t, err)

	tableID := int64(123)
	partitionID := int64(456)
	tableGroupID := ddl.GetTableAffinityGroupID(codec, tableID)
	partitionGroupID := ddl.GetPartitionAffinityGroupID(codec, tableID, partitionID)
	if kerneltype.IsNextGen() {
		require.Equal(t, "_tidb_ks42_t_123", tableGroupID)
		require.Equal(t, "_tidb_ks42_pt_123_p456", partitionGroupID)
		require.NotEqual(t, tableGroupID, ddl.GetTableAffinityGroupID(otherCodec, tableID))
		require.NotEqual(t, partitionGroupID, ddl.GetPartitionAffinityGroupID(otherCodec, tableID, partitionID))

		// PD limits group IDs to 64 characters from this charset. Check the
		// longest format with the maximum keyspace, table, and partition IDs.
		maxCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 16777215},
		})
		require.NoError(t, err)
		const maxID int64 = 1<<63 - 1
		maxGroupID := ddl.GetPartitionAffinityGroupID(maxCodec, maxID, maxID)
		require.Equal(t, "_tidb_ks16777215_pt_9223372036854775807_p9223372036854775807", maxGroupID)
		require.Len(t, maxGroupID, 60)
		require.Regexp(t, `^[-A-Za-z0-9_]{1,64}$`, maxGroupID)
	} else {
		require.Equal(t, "_tidb_t_123", tableGroupID)
		require.Equal(t, "_tidb_pt_123_p456", partitionGroupID)
	}

	// Check the ID and encoded range together using real codecs for two keyspaces.
	for _, targetCodec := range []tikv.Codec{codec, otherCodec} {
		wrongCodec := codec
		if targetCodec.GetKeyspaceID() == codec.GetKeyspaceID() {
			wrongCodec = otherCodec
		}
		for _, tbl := range []*model.TableInfo{
			{ID: tableID, Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable}},
			{
				ID:       tableID,
				Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
				Partition: &model.PartitionInfo{
					Enable:      true,
					Definitions: []model.PartitionDefinition{{ID: partitionID}},
				},
			},
		} {
			prefix := "_tidb_"
			if kerneltype.IsNextGen() {
				prefix = fmt.Sprintf("_tidb_ks%d_", targetCodec.GetKeyspaceID())
			}
			expectedID := prefix + "t_123"
			physicalID := tableID
			if tbl.Partition != nil {
				expectedID = prefix + "pt_123_p456"
				physicalID = partitionID
			}
			groups, err := ddl.BuildAffinityGroupDefinitionsForTest(targetCodec, tbl, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Contains(t, groups, expectedID)
			require.Len(t, groups[expectedID], 1)
			keyRange := groups[expectedID][0]
			start, end, err := targetCodec.DecodeRegionRange(keyRange.StartKey, keyRange.EndKey)
			require.NoError(t, err)
			require.Equal(t, tablecodec.EncodeTablePrefix(physicalID), kv.Key(start))
			require.Equal(t, tablecodec.EncodeTablePrefix(physicalID+1), kv.Key(end))
			_, _, err = wrongCodec.DecodeRegionRange(keyRange.StartKey, keyRange.EndKey)
			require.Error(t, err, "the range must not belong to the other keyspace")
		}
	}
}

func TestAffinityBuildGroupDefinitionsPartition(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       50,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
		Partition: &model.PartitionInfo{
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 1},
				{ID: 3},
			},
		},
	}

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(&mockCodec{}, tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(1)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(2)...),
	}}, groups[ddl.GetPartitionAffinityGroupID(&mockCodec{}, 50, 1)])
	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(3)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(4)...),
	}}, groups[ddl.GetPartitionAffinityGroupID(&mockCodec{}, 50, 3)])
}

func TestAffinityBuildGroupDefinitionsPartitionMissing(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       1,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
	}

	_, err := ddl.BuildAffinityGroupDefinitionsForTest(nil, tbl, nil)
	require.Error(t, err)
}

// PD interaction tests

type affinityGroupCheck struct {
	codec        tikv.Codec
	tableID      int64
	partitionIDs []int64
	shouldExist  bool
	rangeCount   int
	comment      string
}

func (c *affinityGroupCheck) check(t *testing.T) {
	ctx := context.Background()

	// Collect all group IDs to check
	var groupIDs []string
	if len(c.partitionIDs) == 0 {
		// Non-partitioned table
		groupIDs = []string{ddl.GetTableAffinityGroupID(c.codec, c.tableID)}
	} else {
		// Partitioned table
		for _, partID := range c.partitionIDs {
			groupIDs = append(groupIDs, ddl.GetPartitionAffinityGroupID(c.codec, c.tableID, partID))
		}
	}

	// Get affinity groups from PD
	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err, c.comment)

	if c.shouldExist {
		require.Equal(t, len(groupIDs), len(groups), c.comment)
		for _, id := range groupIDs {
			group, ok := groups[id]
			require.True(t, ok, "group %s should exist, comment: %s", id, c.comment)
			require.NotNil(t, group, c.comment)
			require.Equal(t, id, group.ID, c.comment)
			if c.rangeCount > 0 {
				require.Equal(t, c.rangeCount, group.RangeCount, c.comment)
			}
		}
	} else {
		require.Empty(t, groups, "groups should not exist, comment: %s", c.comment)
	}
}

func checkAffinityGroupsInPD(t *testing.T, do *domain.Domain, dbName, tbName string, shouldExist bool) {
	tblInfo, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tbName))
	require.NoError(t, err)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, do.Store(), false, func(ctx context.Context, txn kv.Transaction) error {
		_ = meta.NewMutator(txn)

		check := &affinityGroupCheck{
			codec:       do.Store().GetCodec(),
			tableID:     tblInfo.Meta().ID,
			shouldExist: shouldExist,
			rangeCount:  1,
			comment:     fmt.Sprintf("table: %s.%s", dbName, tbName),
		}

		if tblInfo.Meta().Partition != nil {
			check.partitionIDs = make([]int64, 0, len(tblInfo.Meta().Partition.Definitions))
			for _, def := range tblInfo.Meta().Partition.Definitions {
				check.partitionIDs = append(check.partitionIDs, def.ID)
			}
		}

		check.check(t)
		return nil
	}))
}

func TestAffinityPDInteraction(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	dropTable := func() {
		tk.MustExec("drop table if exists t1, t2, t3, tp1, tp2, tp3")
	}
	dropTable()
	defer dropTable()

	// Test 1: Create table with affinity='table'
	tk.MustExec("create table t1(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 2: Create partitioned table with affinity='partition'
	tk.MustExec("create table tp1(a int) affinity = 'partition' partition by hash(a) partitions 4")
	checkAffinityGroupsInPD(t, dom, "test", "tp1", true)

	// Test 3: Alter table to remove affinity
	tk.MustExec("alter table t1 affinity = ''")
	checkAffinityGroupsInPD(t, dom, "test", "t1", false)

	// Test 4: Alter table to add affinity back
	tk.MustExec("alter table t1 affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 5: Drop table should clean up affinity groups
	// Get table ID before drop
	t1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t1ID := t1Info.Meta().ID

	tk.MustExec("drop table t1")
	// Verify affinity groups are deleted
	ctx := context.Background()
	groups, err := affinity.GetGroups(ctx, []string{ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), t1ID)})
	require.NoError(t, err)
	require.Empty(t, groups, "affinity groups should be deleted after dropping table")

	// Test 6: TRUNCATE TABLE should preserve affinity
	tk.MustExec("create table t2(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)

	t2Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	oldTableID := t2Info.Meta().ID

	tk.MustExec("truncate table t2")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)
	// Old table ID's affinity group should be deleted
	groups, err = affinity.GetGroups(ctx, []string{ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), oldTableID)})
	require.NoError(t, err)
	require.Empty(t, groups, "old table's affinity groups should be deleted after truncate")

	// Test 7: TRUNCATE PARTITION should preserve affinity
	tk.MustExec("create table tp2(a int) affinity = 'partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)

	// Get partition ID before truncate
	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp2"))
	require.NoError(t, err)
	oldPartitionID := tblInfo.Meta().Partition.Definitions[0].ID

	tk.MustExec("alter table tp2 truncate partition p0")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)
	// Old partition's affinity group should be deleted
	groups, err = affinity.GetGroups(ctx, []string{ddl.GetPartitionAffinityGroupID(dom.Store().GetCodec(), tblInfo.Meta().ID, oldPartitionID)})
	require.NoError(t, err)
	require.Empty(t, groups, "old partition's affinity group should be deleted after truncate partition")

	// Test 8: ALTER TABLE AFFINITY idempotency - partition -> partition
	tk.MustExec("create table tp3(a int) affinity = 'partition' partition by hash(a) partitions 3")
	checkAffinityGroupsInPD(t, dom, "test", "tp3", true)
	// Set affinity to partition again (idempotent operation)
	tk.MustExec("alter table tp3 affinity = 'partition'")
	checkAffinityGroupsInPD(t, dom, "test", "tp3", true)

	// Test 9: ALTER TABLE AFFINITY idempotency - table -> table
	tk.MustExec("create table t3(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t3", true)
	// Set affinity to table again (idempotent operation)
	tk.MustExec("alter table t3 affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t3", true)
}

func TestAffinityDropDatabase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists test_affinity_db")
	tk.MustExec("create database test_affinity_db")
	tk.MustExec("use test_affinity_db")

	// Create multiple tables with affinity
	tk.MustExec("create table t1(a int) affinity = 'table'")
	tk.MustExec("create table t2(a int) affinity = 'table'")
	tk.MustExec("create table tp1(a int) affinity = 'partition' partition by hash(a) partitions 2")

	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t1", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t2", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "tp1", true)

	// Get table IDs before dropping database
	t1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t2Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tp1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("tp1"))
	require.NoError(t, err)

	// Drop database should clean up all affinity groups
	tk.MustExec("drop database test_affinity_db")

	ctx := context.Background()
	groupIDs := make([]string, 0, 2+len(tp1Info.Meta().Partition.Definitions))
	codec := dom.Store().GetCodec()
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t1Info.Meta().ID))
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t2Info.Meta().ID))
	for _, def := range tp1Info.Meta().Partition.Definitions {
		groupIDs = append(groupIDs, ddl.GetPartitionAffinityGroupID(codec, tp1Info.Meta().ID, def.ID))
	}

	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err)
	require.Empty(t, groups, "all affinity groups should be deleted after dropping database")
}

func TestAffinityRecoverRecreatesGroups(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	tk.MustExec("create database rec")
	tk.MustExec("use rec")
	tk.MustExec("create table t1(a int) affinity = 'table'")
	tk.MustExec("create table tp(a int) affinity = 'partition' partition by hash(a) partitions 2")
	tk.MustExec("create table t_plain(a int)")

	// RECOVER TABLE.
	tk.MustExec("drop table t1")
	tk.MustExec("recover table t1")
	checkAffinityGroupsInPD(t, dom, "rec", "t1", true)

	// FLASHBACK TABLE ... TO a new name after TRUNCATE.
	tk.MustExec("truncate table tp")
	tk.MustExec("flashback table tp to tp_old")
	checkAffinityGroupsInPD(t, dom, "rec", "tp", true)
	checkAffinityGroupsInPD(t, dom, "rec", "tp_old", true)

	// FLASHBACK DATABASE.
	tk.MustExec("drop database rec")
	tk.MustExec("flashback database rec")
	for _, tbl := range []string{"t1", "tp", "tp_old"} {
		checkAffinityGroupsInPD(t, dom, "rec", tbl, true)
	}
	checkAffinityGroupsInPD(t, dom, "rec", "t_plain", false)
}

func TestAffinityRecoverWhenGroupCreationFails(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	tk.MustExec("create database rec")
	tk.MustExec("use rec")
	tk.MustExec("create table a(x int) affinity = 'table'")
	tk.MustExec("create table b(x int) affinity = 'table'")
	tk.MustExec("insert into a values (1), (2)")
	tk.MustExec("insert into b values (3)")
	tk.MustExec("drop database rec")

	const fp = "github.com/pingcap/tidb/pkg/ddl/beforeCreateRecoveredTableAffinityGroups"
	testfailpoint.EnableCall(t, fp, func(tblInfo *model.TableInfo, err *error) {
		if tblInfo.Name.L == "b" {
			*err = errors.New("mock create affinity groups error")
		}
	})
	// The recovery is not cancelled; it warns about the table without groups.
	tk.MustExec("flashback database rec")
	tk.MustQuery("show warnings").CheckContain("failed to create affinity groups for recovered tables b")
	tk.MustQuery("select * from rec.a order by x").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select * from rec.b").Check(testkit.Rows("3"))
	checkAffinityGroupsInPD(t, dom, "rec", "a", true)
	checkAffinityGroupsInPD(t, dom, "rec", "b", false)

	// The groups can be created again.
	testfailpoint.Disable(t, fp)
	tk.MustExec("alter table rec.b affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "rec", "b", true)
}

func TestAffinityRecoverRetriesGroupCreation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	require.NoError(t, gcutil.EnableGC(tk.Session()))
	testfailpoint.Enable(t, "tikvclient/mockCommitError", `return(true)`)

	tk.MustExec("create database rec")
	tk.MustExec("use rec")
	tk.MustExec("create table a(x int) affinity = 'table'")
	tk.MustExec("insert into a values (1), (2)")
	tblInfo, err := dom.InfoSchema().TableInfoByName(ast.NewCIStr("rec"), ast.NewCIStr("a"))
	require.NoError(t, err)
	groupOfA := ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), tblInfo.ID)

	// In each case, the first attempt of the recovery step fails to commit, so
	// the job runs the step again.
	var (
		attempts int
		fpErr    error
	)
	failFirstAttempt := func(failCreation bool, onFirst func()) {
		attempts, fpErr = 0, nil
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeCreateRecoveredTableAffinityGroups",
			func(_ *model.TableInfo, err *error) {
				attempts++
				if attempts == 1 {
					onFirst()
					if failCreation {
						*err = errors.New("mock create affinity groups error")
					}
					// The commit error fires once on the next commit, which is
					// the commit of this step.
					fpErr = failpoint.Enable("tikvclient/mockCommitErrorOpt", `return(true)`)
				}
			})
	}

	// The first attempt fails to create the groups; the second attempt creates
	// them and drops the warning of the first.
	tk.MustExec("drop database rec")
	failFirstAttempt(true, func() {})
	tk.MustExec("flashback database rec")
	require.NoError(t, fpErr)
	require.Equal(t, 2, attempts)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from rec.a order by x").Check(testkit.Rows("1", "2"))
	checkAffinityGroupsInPD(t, dom, "rec", "a", true)

	// The first attempt creates the groups and the second attempt is cancelled.
	// The groups are kept, because another job may recover the same table ID,
	// and a later recovery takes them over. Moving the GC safe point past the
	// drop makes the safe point check of the second attempt cancel the job.
	cancelSecondAttempt := func() {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	}
	for _, c := range []struct{ drop, recover string }{
		{"drop table rec.a", "recover table rec.a"},
		{"drop database rec", "flashback database rec"},
	} {
		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec(c.drop)
		failFirstAttempt(false, cancelSecondAttempt)
		require.Error(t, tk.ExecToErr(c.recover))
		require.NoError(t, fpErr)
		require.Equal(t, 1, attempts)
		require.False(t, dom.InfoSchema().TableExists(ast.NewCIStr("rec"), ast.NewCIStr("a")), c.recover)
		groups, err := affinity.GetGroups(context.Background(), []string{groupOfA})
		require.NoError(t, err)
		require.Contains(t, groups, groupOfA, c.recover)

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec(c.recover)
		tk.MustQuery("show warnings").Check(testkit.Rows())
		checkAffinityGroupsInPD(t, dom, "rec", "a", true)
	}
}

func TestAffinityRecoverCancelledByParallelRecoveryKeepsGroups(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	// Both jobs are queued before the first one runs. The second one is
	// cancelled because the first one has recovered the table, and must not
	// delete the groups of the recovered table.
	tk.MustExec("create database test_db_state")
	tk.MustExec("create table test_db_state.t (a int) affinity = 'table'")
	tk.MustExec("drop table test_db_state.t")
	sql := "flashback table t to t1"
	tk1, tk2, ch := prepareTestControlParallelExecSQL(t, store)
	var err1, err2 error
	var wg util.WaitGroupWrapper
	wg.Run(func() { err1 = tk1.ExecToErr(sql) })
	wg.Run(func() {
		<-ch
		err2 = tk2.ExecToErr(sql)
	})
	wg.Wait()
	require.NoError(t, err1)
	require.ErrorContains(t, err2, "Table 't1' already exists")
	checkAffinityGroupsInPD(t, dom, "test_db_state", "t1", true)
}
