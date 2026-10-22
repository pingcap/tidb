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
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

// Unit tests for keyrange building

type mockCodec struct {
	tikv.Codec
}

type truncateAffinityPDClient struct {
	pdhttp.Client
	manager          affinity.Manager
	failCreate       atomic.Bool
	failDelete       atomic.Bool
	failOnce         atomic.Bool
	createCalls      atomic.Int64
	createFailures   atomic.Int64
	nilDeleteContext atomic.Bool
}

func (c *truncateAffinityPDClient) CreateAffinityGroups(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, _ ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	c.createCalls.Add(1)
	if c.failCreate.Load() || c.failOnce.Swap(false) {
		c.createFailures.Add(1)
		return nil, fmt.Errorf("injected affinity creation failure")
	}
	return nil, c.manager.CreateAffinityGroupsIfNotExists(ctx, groups)
}

func (c *truncateAffinityPDClient) BatchDeleteAffinityGroups(ctx context.Context, ids []string, _ bool) error {
	// Match the real HTTP client's context validation; the mock manager ignores it.
	if _, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://pd/affinity-groups", nil); err != nil {
		c.nilDeleteContext.Store(ctx == nil)
		return err
	}
	if c.failDelete.Load() {
		return fmt.Errorf("injected affinity deletion failure")
	}
	return c.manager.DeleteAffinityGroups(ctx, ids)
}

func (c *truncateAffinityPDClient) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	return c.manager.GetAffinityGroups(ctx, ids)
}

func TestAffinityTruncatePartitionFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := &truncateAffinityPDClient{manager: affinity.NewMockManager()}
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table p(a int, b int, unique key idx(b) global) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into p values (1,1),(2,2),(11,11),(12,12)")
	before, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	beforeMeta := before.Meta().Clone()
	cli.failCreate.Store(true)
	require.ErrorContains(t, tk.ExecToErr("alter table p truncate partition p0"), "injected affinity creation failure")
	cli.failCreate.Store(false)
	after, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	require.Equal(t, beforeMeta, after.Meta().Clone(), "failed affinity creation must not publish new partition IDs or DDL state")
	tk.MustQuery("select * from p order by a").Check(testkit.Rows("1 1", "2 2", "11 11", "12 12"))
	tk.MustQuery("select * from p use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "11 11", "12 12"))
	tk.MustExec("admin check table p")
	checkAffinityGroupsInPD(t, dom, "test", "p", true)

	// A failure after replacement is visible must retry, never cancel the job.
	// This also protects jobs that were started by an older TiDB version.
	var injected atomic.Bool
	const beforeStep = "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"
	testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StateDeleteReorganization && injected.CompareAndSwap(false, true) {
			cli.failOnce.Store(true)
		}
	})
	tk.MustExec("alter table p truncate partition p0")
	testfailpoint.Disable(t, beforeStep)
	require.True(t, injected.Load())
	require.Equal(t, int64(2), cli.createFailures.Load(), "one failure before replacement and one during finalization")
	after, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	require.NotEqual(t, beforeMeta.Partition.Definitions[0].ID, after.Meta().Partition.Definitions[0].ID)
	require.Equal(t, beforeMeta.Partition.Definitions[1].ID, after.Meta().Partition.Definitions[1].ID)
	require.Equal(t, model.StateNone, after.Meta().Partition.DDLState)
	require.Empty(t, after.Meta().Partition.DroppingDefinitions)
	tk.MustQuery("select * from p order by a").Check(testkit.Rows("11 11", "12 12"))
	tk.MustExec("admin check table p")
	checkAffinityGroupsInPD(t, dom, "test", "p", true)
	oldID := mustPartitionAffinityGroupID(t, store.GetCodec(), beforeMeta.ID, beforeMeta.Partition.Definitions[0].ID)
	groups, err := affinity.GetGroups(context.Background(), []string{oldID})
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf("select count(*) from (select end_key from mysql.gc_delete_range union all select end_key from mysql.gc_delete_range_done) ranges_to_delete where end_key='%x'", []byte(tablecodec.EncodeTablePrefix(beforeMeta.Partition.Definitions[0].ID+1)))).Rows()
		return rows[0][0] == "1"
	}, 5*time.Second, 10*time.Millisecond)
	// Both the failed attempt and the successful retry should reach PD.
	require.Greater(t, cli.createCalls.Load(), int64(2))

	for _, scenario := range []string{"delete succeeds", "delete fails", "metadata not committed"} {
		t.Run(scenario, func(t *testing.T) {
			tk.MustExec("drop table if exists pc")
			tk.MustExec("create table pc(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into pc values (1),(11)")
			beforeCancel, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("pc"))
			require.NoError(t, err)
			beforeMeta := beforeCancel.Meta().Clone()
			db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
			require.True(t, ok)
			persistedBefore, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, beforeMeta.ID)
			require.NoError(t, err)
			tkCancel := testkit.NewTestKit(t, store)
			cli.failDelete.Store(scenario == "delete fails")
			t.Cleanup(func() { cli.failDelete.Store(false) })
			var cancelOnce atomic.Bool
			cancelDone := make(chan error, 1)
			var replacementID atomic.Int64
			const updateHook = "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr"
			if scenario == "metadata not committed" {
				// Fail the first step after PD has created the replacement groups,
				// but before the replacement IDs reach persistent metadata.
				testfailpoint.Enable(t, updateHook, "return(1)")
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
					if job.TableID == beforeMeta.ID && job.SchemaState == model.StateWriteOnly {
						testfailpoint.Disable(t, updateHook)
					}
				})
			}
			testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
				if job.TableID == beforeMeta.ID && job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StateWriteOnly && cancelOnce.CompareAndSwap(false, true) {
					args, err := model.GetTruncateTableArgs(job)
					if err != nil {
						cancelDone <- err
						return
					}
					replacementID.Store(args.NewPartitionIDs[0])
					persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, beforeMeta.ID)
					if err != nil {
						cancelDone <- err
						return
					}
					if wantEmpty := scenario == "metadata not committed"; wantEmpty != (len(persisted.Partition.NewPartitionIDs) == 0) {
						cancelDone <- fmt.Errorf("unexpected persisted replacement partition state")
						return
					}
					errs, cancelErr := ddl.CancelJobs(context.Background(), tkCancel.Session(), []int64{job.ID})
					if len(errs) > 0 && errs[0] != nil {
						cancelDone <- errs[0]
						return
					}
					cancelDone <- cancelErr
				}
			})
			require.Error(t, tk.ExecToErr("alter table pc truncate partition p0"))
			select {
			case cancelErr := <-cancelDone:
				require.NoError(t, cancelErr)
			default:
				require.FailNow(t, "expected partition truncate cancellation")
			}
			require.True(t, cancelOnce.Load())
			require.NotZero(t, replacementID.Load())
			var afterCancel table.Table
			require.Eventually(t, func() bool {
				afterCancel, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("pc"))
				return err == nil && afterCancel.Meta().Partition.DDLState == model.StateNone
			}, 5*time.Second, 10*time.Millisecond)
			persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, beforeMeta.ID)
			require.NoError(t, err)
			require.Equal(t, persistedBefore.Partition, persisted.Partition)
			require.Equal(t, beforeMeta.Partition.Definitions, afterCancel.Meta().Partition.Definitions)
			require.Empty(t, afterCancel.Meta().Partition.NewPartitionIDs)
			require.Equal(t, model.ActionNone, afterCancel.Meta().Partition.DDLAction)
			require.False(t, cli.nilDeleteContext.Load())
			tk.MustQuery("select * from pc order by a").Check(testkit.Rows("1", "11"))
			checkAffinityGroupsInPD(t, dom, "test", "pc", true)
			newGroupID := mustPartitionAffinityGroupID(t, store.GetCodec(), beforeMeta.ID, replacementID.Load())
			groups, err := affinity.GetGroups(context.Background(), []string{newGroupID})
			require.NoError(t, err)
			if scenario == "delete fails" {
				require.Contains(t, groups, newGroupID, "failed cleanup must not prevent metadata rollback")
			} else {
				require.Empty(t, groups, "successful rollback must remove replacement groups")
			}
		})
	}
}

func newAffinityMockCodec(t *testing.T) tikv.Codec {
	t.Helper()
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)
	return mockCodec{Codec: codec}
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

type affinityTestCodec struct {
	tikv.Codec
	meta *keyspacepb.KeyspaceMeta
	id   tikv.KeyspaceID
}

func (c affinityTestCodec) GetKeyspaceMeta() *keyspacepb.KeyspaceMeta { return c.meta }
func (c affinityTestCodec) GetKeyspaceID() tikv.KeyspaceID            { return c.id }

func TestAffinityBuildGroupDefinitionsTable(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       123,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable},
	}

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(newAffinityMockCodec(t), tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	ranges := groups[mustTableAffinityGroupID(t, newAffinityMockCodec(t), 123)]
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

	if kerneltype.IsNextGen() {
		unsetCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{})
		require.NoError(t, err)
		require.Zero(t, unsetCodec.GetKeyspaceID(), "client-go treats an unset identity as zero")
		for _, tc := range []struct {
			name  string
			codec tikv.Codec
		}{
			{"nil", nil},
			{"APIv1", tikv.NewCodecV1(tikv.ModeTxn)},
			{"missing metadata", affinityTestCodec{Codec: codec}},
			{"unset identity", unsetCodec},
			{"APIv3 identity", affinityTestCodec{Codec: codec, meta: &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_KeyspaceIdentity{}}}},
			{"out of range", affinityTestCodec{Codec: codec, meta: &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 16777216}}, id: 16777216}},
			{"mismatched ID", affinityTestCodec{Codec: codec, meta: codec.GetKeyspaceMeta(), id: 43}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				invalidCodec := tc.codec
				id, err := ddl.GetTableAffinityGroupID(invalidCodec, 123)
				require.ErrorContains(t, err, "affinity requires a valid APIv2 keyspace codec in NextGen")
				require.Empty(t, id)
				id, err = ddl.GetPartitionAffinityGroupID(invalidCodec, 123, 456)
				require.ErrorContains(t, err, "affinity requires a valid APIv2 keyspace codec in NextGen")
				require.Empty(t, id)
				for _, level := range []string{ast.TableAffinityLevelTable, ast.TableAffinityLevelPartition} {
					groups, err := ddl.BuildAffinityGroupDefinitionsForTest(invalidCodec, &model.TableInfo{
						ID: 123, Affinity: &model.TableAffinityInfo{Level: level},
						Partition: &model.PartitionInfo{Enable: true, Definitions: []model.PartitionDefinition{{ID: 456}}},
					}, nil)
					require.ErrorContains(t, err, "affinity requires a valid APIv2 keyspace codec in NextGen")
					require.Empty(t, groups)
				}
			})
		}
	}
	zeroCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 0},
	})
	require.NoError(t, err)
	if kerneltype.IsNextGen() {
		require.Equal(t, "_tidb_ks0_t_123", mustTableAffinityGroupID(t, zeroCodec, 123))
		require.Equal(t, "_tidb_ks0_pt_123_p456", mustPartitionAffinityGroupID(t, zeroCodec, 123, 456))
	} else {
		for _, classicCodec := range []tikv.Codec{nil, tikv.NewCodecV1(tikv.ModeTxn), zeroCodec} {
			require.Equal(t, "_tidb_t_123", mustTableAffinityGroupID(t, classicCodec, 123))
			require.Equal(t, "_tidb_pt_123_p456", mustPartitionAffinityGroupID(t, classicCodec, 123, 456))
		}
	}

	tableID := int64(123)
	partitionID := int64(456)
	tableGroupID := mustTableAffinityGroupID(t, codec, tableID)
	partitionGroupID := mustPartitionAffinityGroupID(t, codec, tableID, partitionID)
	if kerneltype.IsNextGen() {
		require.Equal(t, "_tidb_ks42_t_123", tableGroupID)
		require.Equal(t, "_tidb_ks42_pt_123_p456", partitionGroupID)
		require.NotEqual(t, tableGroupID, mustTableAffinityGroupID(t, otherCodec, tableID))
		require.NotEqual(t, partitionGroupID, mustPartitionAffinityGroupID(t, otherCodec, tableID, partitionID))

		// PD limits group IDs to 64 characters from this charset. Check the
		// longest format with the maximum keyspace, table, and partition IDs.
		maxCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 16777215},
		})
		require.NoError(t, err)
		const maxID int64 = 1<<63 - 1
		maxGroupID := mustPartitionAffinityGroupID(t, maxCodec, maxID, maxID)
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

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(newAffinityMockCodec(t), tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(1)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(2)...),
	}}, groups[mustPartitionAffinityGroupID(t, newAffinityMockCodec(t), 50, 1)])
	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(3)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(4)...),
	}}, groups[mustPartitionAffinityGroupID(t, newAffinityMockCodec(t), 50, 3)])
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
		groupIDs = []string{mustTableAffinityGroupID(t, c.codec, c.tableID)}
	} else {
		// Partitioned table
		for _, partID := range c.partitionIDs {
			groupIDs = append(groupIDs, mustPartitionAffinityGroupID(t, c.codec, c.tableID, partID))
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
	groups, err := affinity.GetGroups(ctx, []string{mustTableAffinityGroupID(t, dom.Store().GetCodec(), t1ID)})
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
	groups, err = affinity.GetGroups(ctx, []string{mustTableAffinityGroupID(t, dom.Store().GetCodec(), oldTableID)})
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
	groups, err = affinity.GetGroups(ctx, []string{mustPartitionAffinityGroupID(t, dom.Store().GetCodec(), tblInfo.Meta().ID, oldPartitionID)})
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
	groupIDs = append(groupIDs, mustTableAffinityGroupID(t, codec, t1Info.Meta().ID))
	groupIDs = append(groupIDs, mustTableAffinityGroupID(t, codec, t2Info.Meta().ID))
	for _, def := range tp1Info.Meta().Partition.Definitions {
		groupIDs = append(groupIDs, mustPartitionAffinityGroupID(t, codec, tp1Info.Meta().ID, def.ID))
	}

	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err)
	require.Empty(t, groups, "all affinity groups should be deleted after dropping database")
}

func mustTableAffinityGroupID(t *testing.T, codec tikv.Codec, tableID int64) string {
	t.Helper()
	id, err := ddl.GetTableAffinityGroupID(codec, tableID)
	require.NoError(t, err)
	return id
}

func TestAffinityInvalidCodecDDL(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("codec validation is specific to NextGen")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int) affinity='table'")
	tk.MustExec("create table p(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("create table plain(a int)")
	tk.MustExec("create table plain_p(a int) partition by hash(a) partitions 2")
	// The internal masking-policy UPDATE flushes preceding metadata writes into
	// the DDL transaction, so checking the cached InfoSchema alone misses loss.
	tk.MustExec("create masking policy mask_t on t(a) as a")
	tk.MustExec("create masking policy mask_p on p(a) as a")
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	for _, name := range []string{"t", "p", "plain", "plain_p"} {
		tk.MustExec("insert into " + name + " values (1),(2),(11),(12)")
	}
	const codecFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockAffinityCodec"
	invalidateCodec := true
	testfailpoint.EnableCall(t, codecFailpoint, func(codec *tikv.Codec) {
		if invalidateCodec {
			*codec = nil
		}
	})
	for _, query := range []string{
		"create table c(a int) affinity='table'",
		"create table cp(a int) affinity='partition' partition by hash(a) partitions 2",
	} {
		t.Run(query, func(t *testing.T) {
			require.ErrorContains(t, tk.ExecToErr(query), "affinity requires a valid APIv2 keyspace codec in NextGen")
		})
	}
	require.False(t, dom.InfoSchema().TableExists(ast.NewCIStr("test"), ast.NewCIStr("c")))
	require.False(t, dom.InfoSchema().TableExists(ast.NewCIStr("test"), ast.NewCIStr("cp")))
	for _, tc := range []struct{ name, query string }{
		{"plain", "alter table plain affinity='table'"},
		{"plain_p", "alter table plain_p affinity='partition'"},
		{"t", "truncate table t"},
		{"p", "truncate table p"},
		{"p", "alter table p truncate partition p0"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			before, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tc.name))
			require.NoError(t, err)
			beforeMeta := before.Meta().Clone()
			persistedBefore, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, beforeMeta.ID)
			require.NoError(t, err)
			require.ErrorContains(t, tk.ExecToErr(tc.query), "affinity requires a valid APIv2 keyspace codec in NextGen")
			after, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tc.name))
			require.NoError(t, err)
			require.Equal(t, beforeMeta, after.Meta().Clone())
			persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, beforeMeta.ID)
			require.NoError(t, err)
			require.Equal(t, persistedBefore, persisted, "failed DDL must preserve persisted table metadata")
			tk.MustQuery("select * from " + tc.name + " order by a").Check(testkit.Rows("1", "2", "11", "12"))
		})
	}
	// Affinity cleanup is best effort: an invalid codec must not block DROP.
	tk.MustExec("drop table t, p")
	require.False(t, dom.InfoSchema().TableExists(ast.NewCIStr("test"), ast.NewCIStr("t")))
	require.False(t, dom.InfoSchema().TableExists(ast.NewCIStr("test"), ast.NewCIStr("p")))
	invalidateCodec = false
	tk.MustExec("create database affinity_drop")
	tk.MustExec("create table affinity_drop.t(a int) affinity='table'")
	invalidateCodec = true
	tk.MustExec("drop database affinity_drop")
	require.False(t, dom.InfoSchema().SchemaExists(ast.NewCIStr("affinity_drop")))
}

func mustPartitionAffinityGroupID(t *testing.T, codec tikv.Codec, tableID, partitionID int64) string {
	t.Helper()
	id, err := ddl.GetPartitionAffinityGroupID(codec, tableID, partitionID)
	require.NoError(t, err)
	return id
}
