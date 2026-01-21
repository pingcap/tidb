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

package partition

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

// TestGlobalIndexVersion0 tests that global indexes are created with the previous version.
func TestGlobalIndexVersion0(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create a partitioned table
	tk.MustExec(`CREATE TABLE tp (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)

	tk.MustExec(`CREATE TABLE t (a INT, b INT,PRIMARY KEY (a) NONCLUSTERED)`)
	tk.MustExec(`insert into tp (a,b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)
	tk.MustExec(`insert into t (a,b) values (101,1),(102,2),(103,3),(104,4),(105,15),(106,16),(107,17),(108,18),(109,19)`)
	tk.MustExec(`ALTER TABLE tp EXCHANGE PARTITION p1 WITH TABLE t`)
	tk.MustExec(`delete from tp where a in (2,103,4,105)`)
	tk.MustQuery(`select *,_tidb_rowid from tp`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"101 1 1", // Duplicate _tidb_rowid with same non-unique index value
		"102 2 2",
		"104 4 4",
		"106 16 6", // Duplicate _tidb_rowid
		"107 17 7", // Duplicate _tidb_rowid
		"108 18 8", // Duplicate _tidb_rowid
		"109 19 9", // Duplicate _tidb_rowid
		"3 3 3",
		"5 5 5",
		"6 6 6",
		"7 7 7",
		"8 8 8",
		"9 9 9"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion", "return(0)"))
	tk.MustExec(`create index idx_b on tp(b) global`)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion"))
	tk.MustQuery(`select count(*) from tp use index(idx_b)`).Check(testkit.Rows("13"))
	tk.MustQuery(`select count(*) from tp ignore index(idx_b)`).Check(testkit.Rows("14"))
	tk.MustContainErrMsg(`admin check table tp`, "[admin:8223]data inconsistency in table: tp, index: idx_b, handle:")

	tk.MustContainErrMsg(`update tp set b = 16 where a = 6`, `[tikv:8141]assertion failed: key: `)
	tk.MustContainErrMsg(`update tp set b = 7 where b = 17`, `[tikv:8141]assertion failed: key: `)
	tk.MustQuery(`select b,_tidb_rowid from tp where b in (7,16)`).Sort().Check(testkit.Rows("16 6", "7 7"))
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Sort().Check(testkit.Rows("101 1 1"))
	tk.MustQuery(`select a,b,_tidb_rowid from tp ignore index(idx_b) where b = 1`).Sort().Check(testkit.Rows("1 1 1", "101 1 1"))
	tk.MustExec(`update tp set b = 22 where a = 1`)
	tk.MustExec(`update tp set b = 1 where a = 1`)
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Sort().Check(testkit.Rows("1 1 1"))

	tk.MustExec(`delete from tp where a = 101`)
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Check(testkit.Rows())
	tk.MustQuery(`select a,b,_tidb_rowid from tp ignore index(idx_b) where b = 1`).Check(testkit.Rows("1 1 1"))
	tk.MustContainErrMsg(`delete from tp where a = 1`, `[tikv:8141]assertion failed: key: `)

	// Get the table info and verify the index version
	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	require.NotNil(t, tblInfo)

	// Find the global index
	var globalIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_b" {
			globalIdx = idx
			break
		}
	}
	require.NotNil(t, globalIdx, "Global index idx_b not found")
	require.True(t, globalIdx.Global, "Index should be global")

	require.Equal(t, model.GlobalIndexVersionLegacy, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionLegacy)

	// Create a non-global index and unique global index and verify they have version 0
	tk.MustExec("CREATE INDEX idx_a ON tp(a)")
	tk.MustExec(`CREATE UNIQUE INDEX idx_ab ON tp(a,b) GLOBAL`)

	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()

	globalIdx = nil
	var localIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_a" {
			localIdx = idx
		}
		if idx.Name.O == "idx_ab" {
			globalIdx = idx
		}
		if localIdx != nil && globalIdx != nil {
			break
		}
	}
	require.NotNil(t, localIdx, "Local index idx_a not found")
	require.False(t, localIdx.Global, "Index should not be global")
	require.Equal(t, uint8(0), localIdx.GlobalIndexVersion,
		"Local index should have version 0")

	require.NotNil(t, globalIdx, "Global index idx_ab not found")
	require.True(t, globalIdx.Global, "Index should be global")
	require.Equal(t, model.GlobalIndexVersionLegacy, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionLegacy)

	// Verify that clustered tables get V0 global indexes
	tk.MustExec(`CREATE TABLE tpc (
		a INT,
		b INT,
		PRIMARY KEY (a) CLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)
	tk.MustExec(`CREATE INDEX idx_b ON tpc(b) GLOBAL`)

	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tpc"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()

	globalIdx = nil
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_b" {
			globalIdx = idx
			break
		}
	}
	require.NotNil(t, globalIdx, "Global index idx_b not found")
	require.True(t, globalIdx.Global, "Index should be global")
	require.Equal(t, model.GlobalIndexVersionLegacy, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionLegacy)
}

// TestGlobalIndexVersion2 tests that global indexes are created with version V2.
func TestGlobalIndexVersion2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create a partitioned table
	tk.MustExec(`CREATE TABLE tp (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)

	tk.MustExec(`CREATE TABLE t (a INT, b INT,PRIMARY KEY (a) NONCLUSTERED)`)
	tk.MustExec(`insert into tp (a,b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)
	tk.MustExec(`insert into t (a,b) values (101,1),(102,2),(103,3),(104,4),(105,15),(106,16),(107,17),(108,18),(109,19)`)
	tk.MustExec(`ALTER TABLE tp EXCHANGE PARTITION p1 WITH TABLE t`)
	tk.MustExec(`delete from tp where a in (2,103,4,105)`)
	tk.MustQuery(`select *,_tidb_rowid from tp`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"101 1 1", // Duplicate _tidb_rowid with same non-unique index value
		"102 2 2",
		"104 4 4",
		"106 16 6", // Duplicate _tidb_rowid
		"107 17 7", // Duplicate _tidb_rowid
		"108 18 8", // Duplicate _tidb_rowid
		"109 19 9", // Duplicate _tidb_rowid
		"3 3 3",
		"5 5 5",
		"6 6 6",
		"7 7 7",
		"8 8 8",
		"9 9 9"))

	tk.MustExec(`create index idx_b on tp(b) global`)
	tk.MustQuery(`select count(*) from tp use index(idx_b)`).Check(testkit.Rows("14"))
	tk.MustQuery(`select count(*) from tp ignore index(idx_b)`).Check(testkit.Rows("14"))
	tk.MustExec(`admin check table tp`)

	tk.MustExec(`update tp set b = 16 where a = 6`)
	tk.MustExec(`update tp set b = 7 where b = 17`)
	tk.MustQuery(`select a,b,_tidb_rowid from tp where b in (7,16)`).Sort().Check(testkit.Rows("106 16 6", "107 7 7", "6 16 6", "7 7 7"))
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Sort().Check(testkit.Rows("1 1 1", "101 1 1"))
	tk.MustQuery(`select a,b,_tidb_rowid from tp ignore index(idx_b) where b = 1`).Sort().Check(testkit.Rows("1 1 1", "101 1 1"))
	tk.MustExec(`update tp set b = 22 where a = 1`)
	tk.MustExec(`update tp set b = 1 where a = 1`)
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Sort().Check(testkit.Rows("1 1 1", "101 1 1"))

	tk.MustExec(`delete from tp where a = 101`)
	tk.MustQuery(`select a,b,_tidb_rowid from tp use index(idx_b) where b = 1`).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(`select a,b,_tidb_rowid from tp ignore index(idx_b) where b = 1`).Check(testkit.Rows("1 1 1"))
	tk.MustExec(`delete from tp where a = 1`)

	// Get the table info and verify the index version
	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	require.NotNil(t, tblInfo)

	// Find the global index
	var globalIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_b" {
			globalIdx = idx
			break
		}
	}
	require.NotNil(t, globalIdx, "Global index idx_b not found")
	require.True(t, globalIdx.Global, "Index should be global")

	require.Equal(t, model.GlobalIndexVersionV2, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionV2)

	// Create a non-global index and verify it has version 0
	tk.MustExec("CREATE INDEX idx_a ON tp(a)")

	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()

	var localIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_a" {
			localIdx = idx
			break
		}
	}
	require.NotNil(t, localIdx, "Local index idx_a not found")
	require.False(t, localIdx.Global, "Index should not be global")
	require.Equal(t, uint8(0), localIdx.GlobalIndexVersion,
		"Local index should have version 0")
}

// TestGlobalIndexVersionConstants verifies the version constants are defined correctly.
func TestGlobalIndexVersionConstants(t *testing.T) {
	// Verify version constants are defined
	require.Equal(t, uint8(0), model.GlobalIndexVersionLegacy)
	require.Equal(t, uint8(1), model.GlobalIndexVersionV1)
	require.Equal(t, uint8(2), model.GlobalIndexVersionV2)
}

// indexKeyValue stores a key-value pair from an index
type indexKeyValue struct {
	key   kv.Key
	value []byte
}

// TestGlobalIndexNonUniqueNonClusteredKeyValueFormat verifies the key and value format
// for non-unique global indexes on non-clustered tables across V0, V1, and V2 versions.
// - V0: partition ID is NOT in the key, only in the value
// - V1: partition ID is in BOTH the key AND the value
// - V2: partition ID is ONLY in the key (not in the value)
func TestGlobalIndexNonUniqueNonClusteredKeyValueFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create three identical partitioned tables for V0, V1, and V2
	for _, version := range []string{"v0", "v1", "v2"} {
		tk.MustExec("CREATE TABLE tp_" + version + ` (
			a INT,
			b INT,
			PRIMARY KEY (a) NONCLUSTERED
		) PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (200)
		)`)
	}

	// Insert initial data into all tables
	for _, version := range []string{"v0", "v1", "v2"} {
		tk.MustExec("INSERT INTO tp_" + version + " (a, b) VALUES (1, 10), (2, 20), (3, 30), (101, 10), (102, 20)")
	}

	// Create global indexes with different versions
	// V0
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion", "return(0)"))
	tk.MustExec("CREATE INDEX idx_b ON tp_v0(b) GLOBAL")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion"))

	// V1
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion", "return(1)"))
	tk.MustExec("CREATE INDEX idx_b ON tp_v1(b) GLOBAL")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion"))

	// V2 (default)
	tk.MustExec("CREATE INDEX idx_b ON tp_v2(b) GLOBAL")

	// Perform some updates and deletes on all tables
	for _, version := range []string{"v0", "v1", "v2"} {
		tk.MustExec("UPDATE tp_" + version + " SET b = 25 WHERE a = 2")
		tk.MustExec("DELETE FROM tp_" + version + " WHERE a = 3")
		tk.MustExec("INSERT INTO tp_" + version + " (a, b) VALUES (4, 40), (103, 30)")
	}

	// Verify index versions
	dom := domain.GetDomain(tk.Session())
	for _, tc := range []struct {
		tableName       string
		expectedVersion uint8
	}{
		{"tp_v0", model.GlobalIndexVersionLegacy},
		{"tp_v1", model.GlobalIndexVersionV1},
		{"tp_v2", model.GlobalIndexVersionV2},
	} {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tc.tableName))
		require.NoError(t, err)
		var idx *model.IndexInfo
		for _, i := range tbl.Meta().Indices {
			if i.Name.O == "idx_b" {
				idx = i
				break
			}
		}
		require.NotNil(t, idx, "Index idx_b not found on %s", tc.tableName)
		require.Equal(t, tc.expectedVersion, idx.GlobalIndexVersion,
			"Table %s should have index version %d", tc.tableName, tc.expectedVersion)
	}

	// Collect key-values from all three indexes
	kvsByVersion := make(map[string][]indexKeyValue)
	for _, version := range []string{"v0", "v1", "v2"} {
		tableName := "tp_" + version
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableName))
		require.NoError(t, err)

		var idx *model.IndexInfo
		for _, i := range tbl.Meta().Indices {
			if i.Name.O == "idx_b" {
				idx = i
				break
			}
		}
		require.NotNil(t, idx)

		require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		txn, err := tk.Session().Txn(true)
		require.NoError(t, err)

		prefix := tablecodec.EncodeTableIndexPrefix(tbl.Meta().ID, idx.ID)
		it, err := txn.Iter(prefix, nil)
		require.NoError(t, err)

		var kvs []indexKeyValue
		for it.Valid() {
			if !it.Key().HasPrefix(prefix) {
				break
			}
			kvs = append(kvs, indexKeyValue{
				key:   it.Key().Clone(),
				value: append([]byte(nil), it.Value()...),
			})
			err = it.Next()
			require.NoError(t, err)
		}
		it.Close()
		err = txn.Rollback()
		require.NoError(t, err)

		kvsByVersion[version] = kvs
	}

	// Verify same number of entries in each index
	require.Equal(t, len(kvsByVersion["v0"]), len(kvsByVersion["v1"]),
		"V0 and V1 should have the same number of index entries")
	require.Equal(t, len(kvsByVersion["v1"]), len(kvsByVersion["v2"]),
		"V1 and V2 should have the same number of index entries")

	// Get partition IDs for each table
	partIDsByVersion := make(map[string][]int64)
	colsLenByVersion := make(map[string]int)
	for _, version := range []string{"v0", "v1", "v2"} {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_"+version))
		require.NoError(t, err)
		partInfo := tbl.Meta().GetPartitionInfo()
		require.NotNil(t, partInfo)
		partIDsByVersion[version] = []int64{
			partInfo.Definitions[0].ID, // p0: a < 100
			partInfo.Definitions[1].ID, // p1: 100 <= a < 200
		}
		for _, idx := range tbl.Meta().Indices {
			if idx.Name.O == "idx_b" {
				colsLenByVersion[version] = len(idx.Columns)
				break
			}
		}
	}

	// Verify key format: V1 and V2 should have partition ID in key, V0 should not
	for i, kvV0 := range kvsByVersion["v0"] {
		kvV1 := kvsByVersion["v1"][i]
		kvV2 := kvsByVersion["v2"][i]

		// V0: partition ID should NOT be decodable from key
		_, errV0 := tablecodec.DecodePartitionIDFromGlobalIndexKey(kvV0.key, colsLenByVersion["v0"])
		require.Error(t, errV0, "V0 key should not have partition ID in key (entry %d)", i)

		// V1: partition ID should be in key
		pidV1, errV1 := tablecodec.DecodePartitionIDFromGlobalIndexKey(kvV1.key, colsLenByVersion["v1"])
		require.NoError(t, errV1, "V1 key should have partition ID in key (entry %d)", i)
		v1PartIDs := partIDsByVersion["v1"]
		require.True(t, pidV1 == v1PartIDs[0] || pidV1 == v1PartIDs[1],
			"V1 partition ID should be valid: got %d, expected one of %v", pidV1, v1PartIDs)

		// V2: partition ID should be in key
		pidV2, errV2 := tablecodec.DecodePartitionIDFromGlobalIndexKey(kvV2.key, colsLenByVersion["v2"])
		require.NoError(t, errV2, "V2 key should have partition ID in key (entry %d)", i)
		v2PartIDs := partIDsByVersion["v2"]
		require.True(t, pidV2 == v2PartIDs[0] || pidV2 == v2PartIDs[1],
			"V2 partition ID should be valid: got %d, expected one of %v", pidV2, v2PartIDs)
	}

	// Verify value format: V0 and V1 should have partition ID in value, V2 should NOT
	for i, kvV0 := range kvsByVersion["v0"] {
		kvV1 := kvsByVersion["v1"][i]
		kvV2 := kvsByVersion["v2"][i]

		// Split index values
		segsV0 := tablecodec.SplitIndexValue(kvV0.value)
		segsV1 := tablecodec.SplitIndexValue(kvV1.value)
		segsV2 := tablecodec.SplitIndexValue(kvV2.value)

		// V0: partition ID should be in value
		require.NotNil(t, segsV0.PartitionID, "V0 value should have partition ID (entry %d)", i)
		_, pidV0Val, err := codec.DecodeInt(segsV0.PartitionID)
		require.NoError(t, err)
		v0PartIDs := partIDsByVersion["v0"]
		require.True(t, pidV0Val == v0PartIDs[0] || pidV0Val == v0PartIDs[1],
			"V0 value partition ID should be valid: got %d, expected one of %v", pidV0Val, v0PartIDs)

		// V1: partition ID should be in value
		require.NotNil(t, segsV1.PartitionID, "V1 value should have partition ID (entry %d)", i)
		_, pidV1Val, err := codec.DecodeInt(segsV1.PartitionID)
		require.NoError(t, err)
		v1PartIDs := partIDsByVersion["v1"]
		require.True(t, pidV1Val == v1PartIDs[0] || pidV1Val == v1PartIDs[1],
			"V1 value partition ID should be valid: got %d, expected one of %v", pidV1Val, v1PartIDs)

		// V2: partition ID should NOT be in value
		require.Nil(t, segsV2.PartitionID, "V2 value should NOT have partition ID (entry %d)", i)
	}

	// Verify that V1 has partition ID in both key and value (same partition ID)
	for i, kvV1 := range kvsByVersion["v1"] {
		pidFromKey, err := tablecodec.DecodePartitionIDFromGlobalIndexKey(kvV1.key, colsLenByVersion["v1"])
		require.NoError(t, err)

		segs := tablecodec.SplitIndexValue(kvV1.value)
		require.NotNil(t, segs.PartitionID)
		_, pidFromValue, err := codec.DecodeInt(segs.PartitionID)
		require.NoError(t, err)

		require.Equal(t, pidFromKey, pidFromValue,
			"V1 entry %d: partition ID in key (%d) should match partition ID in value (%d)",
			i, pidFromKey, pidFromValue)
	}
}
