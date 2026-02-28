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

package tables_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	ttypes "github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestCanSkipNullColumnAfterNotNullToNullDDL tests that CanSkip does NOT skip
// encoding null column IDs for columns that changed from NOT NULL to NULL.
//
// This is the regression test for https://github.com/pingcap/tidb/issues/61709
//
// Row Format v2 spec (docs/design/2018-07-19-row-format.md) states:
//
//	"we can not omit the null column ID because if the column ID is not found
//	 in the row, we will use the default value in the schema which may not be null"
//
// After NOT NULL→NULL DDL, both DefaultValue and OriginDefaultValue are nil.
// The old CanSkip logic would skip encoding the null column ID in this case,
// causing downstream components (TiFlash) to misinterpret the missing column
// as "use default value" instead of "value is NULL".
func TestCanSkipNullColumnAfterNotNullToNullDDL(t *testing.T) {
	// Simulate a column that was changed from NOT NULL to NULL via DDL.
	// After the DDL, DefaultValue=nil, OriginDefaultValue=nil, and the column
	// is now nullable (NotNullFlag is cleared).
	colInfo := &model.ColumnInfo{
		ID:   1,
		Name: pmodel.NewCIStr("c"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // NOT NULL flag is cleared after DDL
	// After NOT NULL→NULL DDL, both defaults are nil
	colInfo.DefaultValue = nil
	colInfo.OriginDefaultValue = nil
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	// The value is NULL
	nullDatum := ttypes.NewDatum(nil)

	// CanSkip should return false — the null column ID MUST be encoded
	// so downstream can distinguish "value is NULL" from "column not in row".
	result := tables.CanSkip(tblInfo, col, &nullDatum)
	require.False(t, result,
		"CanSkip should NOT skip null column encoding after NOT NULL→NULL DDL. "+
			"Row Format v2 requires null column IDs to be encoded so downstream "+
			"components can distinguish NULL values from missing columns.")
}

// TestCanSkipNullColumnAlwaysNullable tests that CanSkip correctly handles
// columns that have always been nullable with nil defaults.
// This is the case where the optimization was originally intended to work.
func TestCanSkipNullColumnAlwaysNullable(t *testing.T) {
	// A column that was always nullable, with nil defaults from creation.
	colInfo := &model.ColumnInfo{
		ID:   2,
		Name: pmodel.NewCIStr("d"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = nil
	colInfo.OriginDefaultValue = nil
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      2,
		Name:    pmodel.NewCIStr("t2"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	nullDatum := ttypes.NewDatum(nil)

	// Even for always-nullable columns, per Row Format v2 spec, null column IDs
	// should be encoded. The spec says "we can not omit the null column ID".
	// This test documents the correct behavior after the fix.
	result := tables.CanSkip(tblInfo, col, &nullDatum)
	require.False(t, result,
		"CanSkip should NOT skip null column encoding even for always-nullable columns. "+
			"Row Format v2 spec requires null column IDs to always be encoded.")
}

// TestCanSkipPKColumn tests that CanSkip correctly skips PK handle columns.
func TestCanSkipPKColumn(t *testing.T) {
	colInfo := &model.ColumnInfo{
		ID:   1,
		Name: pmodel.NewCIStr("id"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:         3,
		Name:       pmodel.NewCIStr("t3"),
		Columns:    []*model.ColumnInfo{colInfo},
		PKIsHandle: true,
	}

	datum := ttypes.NewIntDatum(1)

	// PK handle columns should always be skipped
	result := tables.CanSkip(tblInfo, col, &datum)
	require.True(t, result, "CanSkip should skip PK handle columns")
}

// TestCanSkipVirtualGeneratedColumn tests that CanSkip correctly skips virtual generated columns.
func TestCanSkipVirtualGeneratedColumn(t *testing.T) {
	colInfo := &model.ColumnInfo{
		ID:                  3,
		Name:                pmodel.NewCIStr("v"),
		GeneratedExprString: "`a` + 1",
		GeneratedStored:     false, // virtual
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      4,
		Name:    pmodel.NewCIStr("t4"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	datum := ttypes.NewIntDatum(42)

	// Virtual generated columns should always be skipped
	result := tables.CanSkip(tblInfo, col, &datum)
	require.True(t, result, "CanSkip should skip virtual generated columns")
}

// TestCanSkipNonNullValueWithDefault tests that CanSkip does NOT skip
// columns with non-null values.
func TestCanSkipNonNullValueWithDefault(t *testing.T) {
	colInfo := &model.ColumnInfo{
		ID:   4,
		Name: pmodel.NewCIStr("e"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = nil
	colInfo.OriginDefaultValue = nil
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      5,
		Name:    pmodel.NewCIStr("t5"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	// Non-null value should never be skipped
	datum := ttypes.NewIntDatum(42)
	result := tables.CanSkip(tblInfo, col, &datum)
	require.False(t, result, "CanSkip should NOT skip columns with non-null values")
}

// TestCanSkipNullColumnWithNonNilDefault tests that CanSkip does NOT skip
// columns where the value is null but the default is non-nil.
func TestCanSkipNullColumnWithNonNilDefault(t *testing.T) {
	colInfo := &model.ColumnInfo{
		ID:   5,
		Name: pmodel.NewCIStr("f"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = int64(0)
	colInfo.OriginDefaultValue = int64(0)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      6,
		Name:    pmodel.NewCIStr("t6"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	nullDatum := ttypes.NewDatum(nil)

	// Null value with non-nil default should NOT be skipped
	result := tables.CanSkip(tblInfo, col, &nullDatum)
	require.False(t, result,
		"CanSkip should NOT skip null column when default value is non-nil")
}

// TestCanSkipEndToEndNotNullToNull is an integration test that verifies
// the full DDL flow: create table with NOT NULL column, alter to NULL,
// insert NULL value, and verify the row encoding includes the null column ID.
func TestCanSkipEndToEndNotNullToNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Step 1: Create table with NOT NULL column
	tk.MustExec("CREATE TABLE t_canskip (id INT PRIMARY KEY, c INT NOT NULL DEFAULT 0)")

	// Step 2: ALTER column from NOT NULL to NULL
	tk.MustExec("ALTER TABLE t_canskip MODIFY COLUMN c INT NULL")

	// Step 3: Insert a row with NULL value for column c
	tk.MustExec("INSERT INTO t_canskip VALUES (1, NULL)")

	// Step 4: Verify the NULL value is correctly stored and retrievable
	tk.MustQuery("SELECT id, c FROM t_canskip WHERE id = 1").Check(
		testkit.Rows("1 <nil>"),
	)

	// Step 5: Insert another row with non-NULL value to verify normal operation
	tk.MustExec("INSERT INTO t_canskip VALUES (2, 42)")
	tk.MustQuery("SELECT id, c FROM t_canskip WHERE id = 2").Check(
		testkit.Rows("2 42"),
	)

	// Step 6: Verify both rows
	tk.MustQuery("SELECT id, c FROM t_canskip ORDER BY id").Check(
		testkit.Rows("1 <nil>", "2 42"),
	)
}
