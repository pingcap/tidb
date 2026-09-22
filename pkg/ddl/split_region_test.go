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

package ddl

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestSplitRegionScatterConfig(t *testing.T) {
	t.Run("partitioned table keeps physical IDs and scatter group IDs separate", func(t *testing.T) {
		const (
			tableID       int64 = 100
			partition0ID  int64 = 101
			partition1ID  int64 = 102
			localIndexID  int64 = 11
			globalIndexID int64 = 22
		)

		parts := []model.PartitionDefinition{
			{ID: partition0ID, Name: ast.NewCIStr("p0")},
			{ID: partition1ID, Name: ast.NewCIStr("p1")},
		}
		tblInfo := &model.TableInfo{
			ID:              tableID,
			Name:            ast.NewCIStr("t"),
			ShardRowIDBits:  2,
			PreSplitRegions: 1,
			Partition:       &model.PartitionInfo{Enable: true, Definitions: parts},
			Indices: []*model.IndexInfo{
				{ID: localIndexID, Name: ast.NewCIStr("idx_local")},
				{ID: globalIndexID, Name: ast.NewCIStr("idx_global"), Global: true},
			},
		}

		type indexKey struct {
			tableID int64
			indexID int64
		}

		for _, test := range []struct {
			name            string
			scope           string
			expectedGroupID int64
		}{
			{name: "table", scope: vardef.ScatterTable, expectedGroupID: tableID},
			{name: "global", scope: vardef.ScatterGlobal, expectedGroupID: GlobalScatterGroupID},
		} {
			t.Run(test.name, func(t *testing.T) {
				store := &fakeAutoPreSplitStore{}

				splitPartitionTableRegion(mock.NewContext(), store, tblInfo, parts, test.scope)

				var (
					keyTableIDs []int64
					indexKeys   []indexKey
				)
				for i, call := range store.calls {
					require.Truef(t, call.scatter, "split call %d did not enable scatter", i)
					require.Truef(t, call.hasGroupID, "split call %d did not set a scatter group ID", i)
					require.Equalf(t, test.expectedGroupID, call.groupID, "split call %d used the wrong scatter group ID", i)
					for _, key := range call.keys {
						keyTableIDs = append(keyTableIDs, tablecodec.DecodeTableID(key))
						keyTableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
						if err == nil && !isRecord {
							indexKeys = append(indexKeys, indexKey{tableID: keyTableID, indexID: indexID})
						}
					}
				}

				require.ElementsMatch(t, []int64{
					tableID,
					partition0ID, partition0ID, partition0ID,
					partition1ID, partition1ID, partition1ID,
				}, keyTableIDs)
				require.ElementsMatch(t, []indexKey{
					{tableID: tableID, indexID: globalIndexID},
					{tableID: partition0ID, indexID: localIndexID + 1},
					{tableID: partition1ID, indexID: localIndexID + 1},
				}, indexKeys)
			})
		}
	})

	t.Run("partitioned split policies keep record keys on physical IDs", func(t *testing.T) {
		const (
			tableID       int64 = 100
			partition0ID  int64 = 101
			partition1ID  int64 = 102
			localIndexID  int64 = 11
			globalIndexID int64 = 22
		)

		tblInfo, _ := buildAutoPreSplitTestTableInfoFromSQL(t,
			"create table t (id bigint primary key, val bigint, index idx_local(val)) partition by range (id) (partition p0 values less than (100), partition p1 values less than (200))",
			tableID)
		require.Len(t, tblInfo.Partition.Definitions, 2)
		parts := []model.PartitionDefinition{
			{ID: partition0ID, Name: ast.NewCIStr("p0")},
			{ID: partition1ID, Name: ast.NewCIStr("p1")},
		}
		tblInfo.Partition.Definitions = parts

		splitPolicy := &model.RegionSplitPolicy{
			Lower:   []string{"0"},
			Upper:   []string{"10000"},
			Regions: 2,
		}
		tblInfo.TableSplitPolicy = splitPolicy

		localIndexInfo := tblInfo.FindIndexByName("idx_local")
		require.NotNil(t, localIndexInfo, "expected idx_local in table info")
		localIndexInfo.ID = localIndexID
		localIndexInfo.RegionSplitPolicy = splitPolicy

		globalIndex := &model.IndexInfo{
			ID:                globalIndexID,
			Name:              ast.NewCIStr("idx_global"),
			Global:            true,
			Columns:           localIndexInfo.Columns,
			State:             model.StatePublic,
			RegionSplitPolicy: splitPolicy,
		}
		tblInfo.Indices = append(tblInfo.Indices, globalIndex)

		store := &fakeAutoPreSplitStore{}
		splitPartitionTableRegion(mock.NewContext(), store, tblInfo, parts, vardef.ScatterOff)

		var recordKeyTableIDs []int64
		globalIndexKeyTableIDs := make(map[int64]struct{})
		localIndexKeyTableIDs := make(map[int64]struct{})
		for _, call := range store.calls {
			require.NotEmpty(t, call.keys)
			for _, key := range call.keys {
				keyTableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
				require.NoError(t, err)
				if isRecord {
					recordKeyTableIDs = append(recordKeyTableIDs, keyTableID)
					continue
				}
				switch indexID {
				case globalIndexID, globalIndexID + 1:
					globalIndexKeyTableIDs[keyTableID] = struct{}{}
				case localIndexID, localIndexID + 1:
					localIndexKeyTableIDs[keyTableID] = struct{}{}
				}
			}
		}

		require.NotEmpty(t, recordKeyTableIDs, "expected table split policy record keys")
		for _, id := range recordKeyTableIDs {
			require.NotEqual(t, tableID, id, "record split keys must not use the logical table ID")
		}
		require.Contains(t, recordKeyTableIDs, partition0ID)
		require.Contains(t, recordKeyTableIDs, partition1ID)

		require.NotEmpty(t, globalIndexKeyTableIDs, "expected global index split policy keys")
		require.Len(t, globalIndexKeyTableIDs, 1)
		_, hasLogicalGlobalIndexKey := globalIndexKeyTableIDs[tableID]
		require.True(t, hasLogicalGlobalIndexKey, "global index split policy keys must use the logical table ID")

		require.NotEmpty(t, localIndexKeyTableIDs, "expected local index split policy keys")
		_, hasLogicalLocalIndexKey := localIndexKeyTableIDs[tableID]
		require.False(t, hasLogicalLocalIndexKey, "local index split policy keys must not use the logical table ID")
		_, hasPartition0LocalIndexKey := localIndexKeyTableIDs[partition0ID]
		require.True(t, hasPartition0LocalIndexKey, "expected local index split policy keys for partition 0")
		_, hasPartition1LocalIndexKey := localIndexKeyTableIDs[partition1ID]
		require.True(t, hasPartition1LocalIndexKey, "expected local index split policy keys for partition 1")
	})

	t.Run("split policies use the persisted scatter scope", func(t *testing.T) {
		tblInfo, _ := buildAutoPreSplitTestTableInfoFromSQL(t, "create table t (id bigint primary key, index idx (id))", 200)
		tblInfo.TableSplitPolicy = &model.RegionSplitPolicy{
			Lower:   []string{"0"},
			Upper:   []string{"10000"},
			Regions: 2,
		}
		workerCtx := mock.NewContext()
		// Keep the live session setting opposite to every test scope to verify the persisted scope is used.
		workerCtx.GetSessionVars().ScatterRegion = vardef.ScatterOff

		for _, test := range []struct {
			name            string
			scope           string
			expectedGroupID int64
		}{
			{name: "table", scope: vardef.ScatterTable, expectedGroupID: tblInfo.ID},
			{name: "global", scope: vardef.ScatterGlobal, expectedGroupID: GlobalScatterGroupID},
		} {
			t.Run(test.name, func(t *testing.T) {
				store := &fakeAutoPreSplitStore{}

				splitTableRegion(workerCtx, store, tblInfo, test.scope)

				require.Len(t, store.calls, 1)
				call := store.calls[0]
				if !call.scatter {
					t.Error("split policy did not enable scatter from the persisted scope")
				}
				require.True(t, call.hasGroupID)
				require.Equal(t, test.expectedGroupID, call.groupID)
				require.NotEmpty(t, call.keys)
				for _, key := range call.keys {
					require.Equal(t, tblInfo.ID, tablecodec.DecodeTableID(key))
				}
			})
		}
	})
}

// buildSplitPolicyTestTableInfo parses createSQL and builds the table info for
// its persisted split policy test.
func buildSplitPolicyTestTableInfo(t *testing.T, createSQL string) (*model.TableInfo, *ast.CreateTableStmt) {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok, "unexpected statement type %T", stmt)
	tblInfo, err := BuildTableInfoFromAST(metabuild.NewContext(), createStmt)
	require.NoError(t, err)
	tblInfo.ID = 100
	return tblInfo, createStmt
}

// splitKeysForPersistedPolicy normalizes the split policies declared in
// createSQL and returns the keys produced when they are applied, emulating what
// a later table operation (for example truncate) does with the stored policy.
func splitKeysForPersistedPolicy(t *testing.T, createSQL string) [][]byte {
	t.Helper()
	tblInfo, createStmt := buildSplitPolicyTestTableInfo(t, createSQL)
	require.NotEmpty(t, createStmt.SplitIndex, "expected a split policy in %q", createSQL)

	sctx := mock.NewContext()
	for _, splitOpt := range createStmt.SplitIndex {
		policy, indexName, err := normalizeSplitPolicy(sctx.GetExprCtx(), splitOpt, tblInfo)
		require.NoError(t, err)
		if indexName == "" {
			tblInfo.TableSplitPolicy = policy
			continue
		}
		idxInfo := tblInfo.FindIndexByName(indexName)
		require.NotNil(t, idxInfo)
		idxInfo.RegionSplitPolicy = policy
	}

	store := &fakeAutoPreSplitStore{}
	splitTableRegion(sctx, store, tblInfo, vardef.ScatterOff)
	require.Len(t, store.calls, 1)
	return store.calls[0].keys
}

func TestSplitPolicyConvertsBoundsToColumnType(t *testing.T) {
	// A string literal bound for an integer handle column must be converted to
	// the handle type, otherwise both bounds collapse to zero and no region is
	// split. See https://github.com/pingcap/tidb/issues/71395.
	t.Run("string literal bound on integer handle", func(t *testing.T) {
		keys := splitKeysForPersistedPolicy(t,
			"create table t (id bigint primary key) split between ('0') and ('10000') regions 4")
		recordPrefix := tablecodec.GenTableRecordPrefix(100)
		require.Equal(t, [][]byte{
			tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(2500)),
			tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(5000)),
			tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(7500)),
		}, keys)
	})

	// Bounds of an index policy must be converted to the indexed column type,
	// so integer literals and the equivalent string literals produce the same
	// index split keys.
	t.Run("integer literal bound on string index column", func(t *testing.T) {
		const tableSQL = "create table t (id bigint primary key, name varchar(100), index idx_name(name)) split index idx_name between (%s) and (%s) regions 3"
		fromIntLiterals := splitKeysForPersistedPolicy(t, fmt.Sprintf(tableSQL, "0", "100"))
		fromStringLiterals := splitKeysForPersistedPolicy(t, fmt.Sprintf(tableSQL, "'0'", "'100'"))
		require.NotEmpty(t, fromIntLiterals)
		require.Equal(t, fromStringLiterals, fromIntLiterals)
	})

	// A table-level policy on a clustered common-handle table must convert its
	// string bounds to the primary key column types.
	t.Run("string bound on common handle columns", func(t *testing.T) {
		const tableSQL = "create table t (a int, b varchar(20), primary key (a, b) clustered) split between (%s) and (%s) regions 4"
		converted := splitKeysForPersistedPolicy(t, fmt.Sprintf(tableSQL, "'0', 'x'", "'100', 'z'"))
		typed := splitKeysForPersistedPolicy(t, fmt.Sprintf(tableSQL, "0, 'x'", "100, 'z'"))
		require.NotEmpty(t, converted)
		require.Equal(t, typed, converted)
	})

	// A bound that cannot be converted to the target column type must fail the
	// DDL instead of persisting a policy that is silently skipped later.
	t.Run("unconvertible bound is rejected", func(t *testing.T) {
		tblInfo, createStmt := buildSplitPolicyTestTableInfo(t,
			"create table t (id bigint primary key) split between ('abc') and ('10000') regions 4")
		_, _, err := normalizeSplitPolicy(mock.NewContext().GetExprCtx(), createStmt.SplitIndex[0], tblInfo)
		require.Error(t, err)
		require.True(t, types.ErrTruncated.Equal(err), "unexpected error: %v", err)
	})
}
