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

// TestGlobalIndexVersionMetadata tests that global indexes are created with the correct version.
func TestGlobalIndexVersionMetadata(t *testing.T) {
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
	tk.MustExec(`insert into tp (a,b) values (1,1),(2,2),(3,3)`)
	tk.MustExec(`insert into t (a,b) values (101,1),(102,2),(103,3)`)
	tk.MustExec(`ALTER TABLE tp EXCHANGE PARTITION p1 WITH TABLE t`)
	tk.MustExec(`delete from tp where a in (2,103)`)
	tk.MustQuery(`select *,_tidb_rowid from tp`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"101 1 1", // Duplicate _tidb_rowid!
		"102 2 2",
		"3 3 3"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion", "return(0)"))
	tk.MustExec(`create index idx_b on tp(b) global`)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion"))
	tk.MustQuery(`select count(*) from tp use index(idx_b)`).Check(testkit.Rows("3"))
	tk.MustQuery(`select count(*) from tp ignore index(idx_b)`).Check(testkit.Rows("4"))
	tk.MustContainErrMsg(`admin check table tp`, "[admin:8223]data inconsistency in table: tp, index: idx_b, handle:")

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

	// Verify the version is set to GlobalIndexVersionCurrent (2)
	require.Equal(t, model.GlobalIndexVersionLegacy, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionLegacy)

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

	tk.MustExec(`drop index idx_b on tp`)
	tk.MustExec(`create index idx_b on tp(b) global`)
	tk.MustQuery(`select count(*) from tp use index(idx_b)`).Check(testkit.Rows("4"))
	tk.MustQuery(`select count(*) from tp ignore index(idx_b)`).Check(testkit.Rows("4"))
	tk.MustExec(`admin check table tp`)

	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	require.NotNil(t, tblInfo)

	// Find the new global index
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_b" {
			globalIdx = idx
			break
		}
	}
	require.NotNil(t, globalIdx, "Global index idx_b not found")
	require.True(t, globalIdx.Global, "Index should be global")

	// Verify the version is set to GlobalIndexVersionCurrent (2)
	require.Equal(t, model.GlobalIndexVersionV1, globalIdx.GlobalIndexVersion,
		"Global index should have version %d", model.GlobalIndexVersionV1)
}

// TestLegacyGlobalIndexStillWorks tests that indexes with version=0 (legacy) still function.
func TestLegacyGlobalIndexStillWorks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create a partitioned table
	tk.MustExec(`CREATE TABLE tp (
		a INT,
		b INT,
		PRIMARY KEY (a) NONCLUSTERED
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
	)`)

	// Create a global index
	tk.MustExec("CREATE INDEX idx_b ON tp(b) GLOBAL")

	// Simulate a legacy index by setting version to 0
	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == "idx_b" {
			idx.GlobalIndexVersion = 0 // Legacy version
			break
		}
	}

	// Insert data - this should work even with legacy version=0
	// Note: Legacy version means the old format without partition ID in the key
	// This would actually have the duplicate handle bug, but the code should not crash
	tk.MustExec("INSERT INTO tp VALUES (1, 10), (2, 20)")

	// Query should work (though might have bugs if EXCHANGE PARTITION was used)
	result := tk.MustQuery("SELECT COUNT(*) FROM tp USE INDEX(idx_b)")
	result.Check(testkit.Rows("2"))
}

// TestGlobalIndexVersionConstants verifies the version constants are defined correctly.
func TestGlobalIndexVersionConstants(t *testing.T) {
	// Verify version constants are defined
	require.Equal(t, uint8(0), model.GlobalIndexVersionLegacy)
	require.Equal(t, uint8(1), model.GlobalIndexVersionV1)
	require.Equal(t, uint8(2), model.GlobalIndexVersionV2)
}
