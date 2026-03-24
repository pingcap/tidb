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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
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
	// Version V1 is now default for unique nullable indexes
	require.Equal(t, model.GlobalIndexVersionV1, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionV1)

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

// TestGlobalIndexVersion1 tests that global indexes are created with version V1.
func TestGlobalIndexVersion1(t *testing.T) {
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

	require.Equal(t, model.GlobalIndexVersionV1, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionV1)

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

// TestGlobalIndexTruncateAndDropPartition verifies that TRUNCATE PARTITION and
// DROP PARTITION correctly clean up only the affected partition's global index entries
// without corrupting entries belonging to other partitions.
func TestGlobalIndexTruncateAndDropPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// --- TRUNCATE PARTITION ---
	tk.MustExec(`CREATE TABLE tp_trunc (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200),
		PARTITION p2 VALUES LESS THAN (300)
	)`)

	// Insert data into all 3 partitions.
	tk.MustExec("INSERT INTO tp_trunc VALUES (1, 10), (2, 20), (3, 30)") // p0
	tk.MustExec("INSERT INTO tp_trunc VALUES (101, 110), (102, 120)")    // p1
	tk.MustExec("INSERT INTO tp_trunc VALUES (201, 210), (202, 220)")    // p2

	// Create a V2 global index (default for non-unique non-clustered).
	tk.MustExec("CREATE INDEX idx_b ON tp_trunc(b) GLOBAL")

	// Verify all data is accessible via the global index.
	tk.MustQuery("SELECT count(*) FROM tp_trunc USE INDEX(idx_b)").Check(testkit.Rows("7"))
	tk.MustExec("ADMIN CHECK TABLE tp_trunc")

	// Truncate partition p1.
	tk.MustExec("ALTER TABLE tp_trunc TRUNCATE PARTITION p1")

	// Verify: p0 and p2 data should still be accessible, p1 data gone.
	tk.MustQuery("SELECT count(*) FROM tp_trunc USE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT count(*) FROM tp_trunc IGNORE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT a, b FROM tp_trunc USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "2 20", "3 30", "201 210", "202 220"))
	tk.MustExec("ADMIN CHECK TABLE tp_trunc")

	// Insert new data into the truncated partition.
	tk.MustExec("INSERT INTO tp_trunc VALUES (111, 1110), (112, 1120)")
	tk.MustQuery("SELECT count(*) FROM tp_trunc USE INDEX(idx_b)").Check(testkit.Rows("7"))
	tk.MustExec("ADMIN CHECK TABLE tp_trunc")

	// --- DROP PARTITION ---
	tk.MustExec(`CREATE TABLE tp_drop (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200),
		PARTITION p2 VALUES LESS THAN (300),
		PARTITION pmax VALUES LESS THAN MAXVALUE
	)`)

	tk.MustExec("INSERT INTO tp_drop VALUES (1, 10), (2, 20), (3, 30)") // p0
	tk.MustExec("INSERT INTO tp_drop VALUES (101, 110), (102, 120)")    // p1
	tk.MustExec("INSERT INTO tp_drop VALUES (201, 210)")                // p2
	tk.MustExec("INSERT INTO tp_drop VALUES (301, 310)")                // pmax

	tk.MustExec("CREATE INDEX idx_b ON tp_drop(b) GLOBAL")

	tk.MustQuery("SELECT count(*) FROM tp_drop USE INDEX(idx_b)").Check(testkit.Rows("7"))
	tk.MustExec("ADMIN CHECK TABLE tp_drop")

	// Drop partition p1.
	tk.MustExec("ALTER TABLE tp_drop DROP PARTITION p1")

	// Verify: p0, p2, pmax data should still be accessible.
	tk.MustQuery("SELECT count(*) FROM tp_drop USE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT count(*) FROM tp_drop IGNORE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT a, b FROM tp_drop USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "2 20", "3 30", "201 210", "301 310"))
	tk.MustExec("ADMIN CHECK TABLE tp_drop")

	// Drop another partition (p2).
	tk.MustExec("ALTER TABLE tp_drop DROP PARTITION p2")
	tk.MustQuery("SELECT count(*) FROM tp_drop USE INDEX(idx_b)").Check(testkit.Rows("4"))
	tk.MustQuery("SELECT a, b FROM tp_drop USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "2 20", "3 30", "301 310"))
	tk.MustExec("ADMIN CHECK TABLE tp_drop")

	// --- EXCHANGE PARTITION with duplicate _tidb_rowid + DROP ---
	// This is the core scenario: after EXCHANGE PARTITION, different partitions
	// can have rows with the same _tidb_rowid. V2 global index must handle this.
	tk.MustExec(`CREATE TABLE tp_exch (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)

	tk.MustExec(`CREATE TABLE t_plain (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)`)
	tk.MustExec("INSERT INTO tp_exch VALUES (1, 10), (2, 20)")       // p0, rowid 1,2
	tk.MustExec("INSERT INTO t_plain VALUES (101, 110), (102, 120)") // rowid 1,2

	// Exchange: p1 gets t_plain's data (rowid 1,2 — same as p0's rowids!)
	tk.MustExec("ALTER TABLE tp_exch EXCHANGE PARTITION p1 WITH TABLE t_plain")

	// Now create the V2 global index — this must handle duplicate rowids across partitions.
	tk.MustExec("CREATE INDEX idx_b ON tp_exch(b) GLOBAL")

	// Both partitions' data should be accessible.
	tk.MustQuery("SELECT count(*) FROM tp_exch USE INDEX(idx_b)").Check(testkit.Rows("4"))
	tk.MustQuery("SELECT a, b FROM tp_exch USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "2 20", "101 110", "102 120"))
	tk.MustExec("ADMIN CHECK TABLE tp_exch")

	// Now truncate p1 (the exchanged partition with duplicate rowids).
	tk.MustExec("ALTER TABLE tp_exch TRUNCATE PARTITION p1")

	// p0 data should remain intact.
	tk.MustQuery("SELECT count(*) FROM tp_exch USE INDEX(idx_b)").Check(testkit.Rows("2"))
	tk.MustQuery("SELECT a, b FROM tp_exch USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "2 20"))
	tk.MustExec("ADMIN CHECK TABLE tp_exch")

	// --- REORGANIZE PARTITION ---
	tk.MustExec(`CREATE TABLE tp_reorg (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200),
		PARTITION p2 VALUES LESS THAN (300)
	)`)

	tk.MustExec("INSERT INTO tp_reorg VALUES (1, 10), (50, 50)")      // p0
	tk.MustExec("INSERT INTO tp_reorg VALUES (101, 110), (150, 150)") // p1
	tk.MustExec("INSERT INTO tp_reorg VALUES (201, 210)")             // p2

	tk.MustExec("CREATE INDEX idx_b ON tp_reorg(b) GLOBAL")
	tk.MustQuery("SELECT count(*) FROM tp_reorg USE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustExec("ADMIN CHECK TABLE tp_reorg")

	// Reorganize: merge p0 and p1 into a single partition.
	tk.MustExec("ALTER TABLE tp_reorg REORGANIZE PARTITION p0, p1 INTO (PARTITION p01 VALUES LESS THAN (200))")

	// All data should still be accessible, including p2's row which was not reorganized.
	tk.MustQuery("SELECT count(*) FROM tp_reorg USE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT count(*) FROM tp_reorg IGNORE INDEX(idx_b)").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT a, b FROM tp_reorg USE INDEX(idx_b) ORDER BY b").Check(testkit.Rows(
		"1 10", "50 50", "101 110", "150 150", "201 210"))
	tk.MustExec("ADMIN CHECK TABLE tp_reorg")

	// --- UNIQUE GLOBAL INDEX WITH NULLs (V2) ---
	// Unique indexes with nullable columns get V2, which puts partition ID in the key.
	tk.MustExec(`CREATE TABLE tp_null (
		a INT,
		b INT,
		c INT,
		PRIMARY KEY (a) NONCLUSTERED,
		UNIQUE INDEX idx_c(c) GLOBAL
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)

	// Insert rows with NULL values in the unique column — should be allowed.
	tk.MustExec("INSERT INTO tp_null VALUES (1, 10, NULL)")
	tk.MustExec("INSERT INTO tp_null VALUES (101, 110, NULL)") // Same NULL in different partition.
	tk.MustExec("INSERT INTO tp_null VALUES (2, 20, 100)")
	tk.MustExec("INSERT INTO tp_null VALUES (102, 120, 200)")

	// Verify all data accessible via global index.
	tk.MustQuery("SELECT count(*) FROM tp_null USE INDEX(idx_c)").Check(testkit.Rows("4"))
	tk.MustQuery("SELECT a, c FROM tp_null USE INDEX(idx_c) WHERE c IS NOT NULL ORDER BY c").Check(testkit.Rows(
		"2 100", "102 200"))
	tk.MustExec("ADMIN CHECK TABLE tp_null")

	// Truncate one partition with NULL entries.
	tk.MustExec("ALTER TABLE tp_null TRUNCATE PARTITION p0")
	tk.MustQuery("SELECT count(*) FROM tp_null USE INDEX(idx_c)").Check(testkit.Rows("2"))
	tk.MustQuery("SELECT count(*) FROM tp_null IGNORE INDEX(idx_c)").Check(testkit.Rows("2"))
	tk.MustExec("ADMIN CHECK TABLE tp_null")
}

// TestUpdateIndexesResetsGlobalIndexVersion verifies that UPDATE INDEXES resets
// GlobalIndexVersion to 0 when switching an index from GLOBAL to local.
// Without this fix, the stale GlobalIndexVersion=V1 on a local index causes
// GenIndexKey to expect a PartitionHandle, triggering DML errors.
func TestUpdateIndexesResetsGlobalIndexVersion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// CREATE TABLE with a non-unique GLOBAL index, but UPDATE INDEXES flips it to LOCAL.
	// buildTablePartitionInfo must reset GlobalIndexVersion to 0 for the local index,
	// otherwise the persisted metadata will have Global=false, GlobalIndexVersion=V1.
	tk.MustExec(`CREATE TABLE t_upd_idx (
		a INT,
		b INT,
		KEY idx_b(b) GLOBAL
	) PARTITION BY HASH (a) PARTITIONS 3 UPDATE INDEXES (idx_b LOCAL)`)

	// Verify metadata: Global=false and GlobalIndexVersion=0.
	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_upd_idx"))
	require.NoError(t, err)
	var idxB *model.IndexInfo
	for _, idx := range tbl.Meta().Indices {
		if idx.Name.O == "idx_b" {
			idxB = idx
			break
		}
	}
	require.NotNil(t, idxB)
	require.False(t, idxB.Global, "Index should be local after UPDATE INDEXES")
	require.Equal(t, uint8(0), idxB.GlobalIndexVersion,
		"GlobalIndexVersion should be reset to 0 for local index")

	// DML should work without "handle is not a PartitionHandle" errors.
	tk.MustExec("INSERT INTO t_upd_idx VALUES (1, 10), (2, 20), (3, 30)")
	tk.MustExec("UPDATE t_upd_idx SET b = 99 WHERE a = 1")
	tk.MustExec("DELETE FROM t_upd_idx WHERE a = 2")
	tk.MustQuery("SELECT a, b FROM t_upd_idx ORDER BY a").Check(testkit.Rows(
		"1 99", "3 30"))
	tk.MustExec("ADMIN CHECK TABLE t_upd_idx")
}
