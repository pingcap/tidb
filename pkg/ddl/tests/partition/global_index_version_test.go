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
