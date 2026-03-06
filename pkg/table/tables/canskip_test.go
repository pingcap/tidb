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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
		Name: ast.NewCIStr("c"),
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
		Name:    ast.NewCIStr("t"),
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
		Name: ast.NewCIStr("d"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = nil
	colInfo.OriginDefaultValue = nil
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      2,
		Name:    ast.NewCIStr("t2"),
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
		Name: ast.NewCIStr("id"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:         3,
		Name:       ast.NewCIStr("t3"),
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
		Name:                ast.NewCIStr("v"),
		GeneratedExprString: "`a` + 1",
		GeneratedStored:     false, // virtual
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      4,
		Name:    ast.NewCIStr("t4"),
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
		Name: ast.NewCIStr("e"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = nil
	colInfo.OriginDefaultValue = nil
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      5,
		Name:    ast.NewCIStr("t5"),
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
		Name: ast.NewCIStr("f"),
	}
	colInfo.SetType(mysql.TypeLong)
	colInfo.SetFlag(0) // nullable
	colInfo.DefaultValue = int64(0)
	colInfo.OriginDefaultValue = int64(0)
	colInfo.State = model.StatePublic

	col := &table.Column{ColumnInfo: colInfo}

	tblInfo := &model.TableInfo{
		ID:      6,
		Name:    ast.NewCIStr("t6"),
		Columns: []*model.ColumnInfo{colInfo},
	}

	nullDatum := ttypes.NewDatum(nil)

	// Null value with non-nil default should NOT be skipped
	result := tables.CanSkip(tblInfo, col, &nullDatum)
	require.False(t, result,
		"CanSkip should NOT skip null column when default value is non-nil")
}

// TestCanSkipEndToEndNotNullToNull is an integration test that verifies
// the CanSkip behavior through the full DDL + DML flow.
// It tests scenarios that produce DefaultValue=nil and OriginDefaultValue=nil,
// which is the exact trigger condition for the bug in issue #61709.
// After the fix, inserting NULL must encode the null column ID in the row.
func TestCanSkipEndToEndNotNullToNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Scenario 1: Column created as nullable with no explicit DEFAULT.
	// This produces DefaultValue=nil, OriginDefaultValue=nil.
	tk.MustExec("CREATE TABLE t_canskip1 (id INT PRIMARY KEY, c INT NULL)")

	// Verify schema state: both defaults must be nil to hit the bug trigger.
	tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_canskip1"))
	require.NoError(t, err)
	colC := tbl1.Meta().Columns[1]
	require.Equal(t, "c", colC.Name.L)
	require.Nil(t, colC.DefaultValue, "DefaultValue should be nil for nullable column without DEFAULT")
	require.Nil(t, colC.OriginDefaultValue, "OriginDefaultValue should be nil for nullable column without DEFAULT")

	// Insert NULL — this is the exact path that triggered the bug.
	tk.MustExec("INSERT INTO t_canskip1 VALUES (1, NULL)")
	tk.MustQuery("SELECT id, c FROM t_canskip1 WHERE id = 1").Check(
		testkit.Rows("1 <nil>"),
	)

	// Scenario 2: ADD COLUMN as nullable with no explicit DEFAULT.
	tk.MustExec("CREATE TABLE t_canskip2 (id INT PRIMARY KEY)")
	tk.MustExec("ALTER TABLE t_canskip2 ADD COLUMN d INT NULL")

	// Verify schema state after ADD COLUMN.
	tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_canskip2"))
	require.NoError(t, err)
	colD := tbl2.Meta().Columns[1]
	require.Equal(t, "d", colD.Name.L)
	require.Nil(t, colD.DefaultValue, "DefaultValue should be nil after ADD COLUMN nullable")
	require.Nil(t, colD.OriginDefaultValue, "OriginDefaultValue should be nil after ADD COLUMN nullable")

	tk.MustExec("INSERT INTO t_canskip2 VALUES (1, NULL)")
	tk.MustQuery("SELECT id, d FROM t_canskip2 WHERE id = 1").Check(
		testkit.Rows("1 <nil>"),
	)

	// Verify non-NULL values still work correctly.
	tk.MustExec("INSERT INTO t_canskip1 VALUES (2, 42)")
	tk.MustExec("INSERT INTO t_canskip2 VALUES (2, 42)")
	tk.MustQuery("SELECT id, c FROM t_canskip1 ORDER BY id").Check(
		testkit.Rows("1 <nil>", "2 42"),
	)
	tk.MustQuery("SELECT id, d FROM t_canskip2 ORDER BY id").Check(
		testkit.Rows("1 <nil>", "2 42"),
	)
}
