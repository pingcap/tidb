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
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestResolveAlterTableInlinePrimaryKey_RewriteAddColumn(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t add column id int auto_increment primary key", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	specs, err := ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.NoError(t, err)
	require.Len(t, specs, 2)

	require.Equal(t, ast.AlterTableAddColumns, specs[0].Tp)
	require.Len(t, specs[0].NewColumns, 1)
	colDef := specs[0].NewColumns[0]
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionAutoIncrement))
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionNotNull))
	require.False(t, containsColumnOption(colDef, ast.ColumnOptionPrimaryKey))

	require.Equal(t, ast.AlterTableAddConstraint, specs[1].Tp)
	require.NotNil(t, specs[1].Constraint)
	require.Equal(t, ast.ConstraintPrimaryKey, specs[1].Constraint.Tp)
	require.Equal(t, mysql.PrimaryKeyName, specs[1].Constraint.Name)
	require.Len(t, specs[1].Constraint.Keys, 1)
	require.Equal(t, "id", specs[1].Constraint.Keys[0].Column.Name.O)
}

func TestResolveAlterTableInlinePrimaryKey_RewriteModifyColumn(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t modify column c int auto_increment primary key", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	specs, err := ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.NoError(t, err)
	require.Len(t, specs, 2)

	require.Equal(t, ast.AlterTableModifyColumn, specs[0].Tp)
	require.Len(t, specs[0].NewColumns, 1)
	colDef := specs[0].NewColumns[0]
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionAutoIncrement))
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionNotNull))
	require.False(t, containsColumnOption(colDef, ast.ColumnOptionPrimaryKey))

	require.Equal(t, ast.AlterTableAddConstraint, specs[1].Tp)
	require.NotNil(t, specs[1].Constraint)
	require.Equal(t, ast.ConstraintPrimaryKey, specs[1].Constraint.Tp)
	require.Len(t, specs[1].Constraint.Keys, 1)
	require.Equal(t, "c", specs[1].Constraint.Keys[0].Column.Name.O)
}

func TestResolveAlterTableInlinePrimaryKey_RewriteChangeColumn(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t change column c c int auto_increment primary key", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	specs, err := ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.NoError(t, err)
	require.Len(t, specs, 2)

	require.Equal(t, ast.AlterTableChangeColumn, specs[0].Tp)
	require.Len(t, specs[0].NewColumns, 1)
	colDef := specs[0].NewColumns[0]
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionAutoIncrement))
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionNotNull))
	require.False(t, containsColumnOption(colDef, ast.ColumnOptionPrimaryKey))

	require.Equal(t, ast.AlterTableAddConstraint, specs[1].Tp)
	require.NotNil(t, specs[1].Constraint)
	require.Equal(t, ast.ConstraintPrimaryKey, specs[1].Constraint.Tp)
	require.Len(t, specs[1].Constraint.Keys, 1)
	require.Equal(t, "c", specs[1].Constraint.Keys[0].Column.Name.O)
}

func TestResolveAlterTableInlinePrimaryKey_RewriteAddColumnsOrder(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t add (id int auto_increment primary key, c int)", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	specs, err := ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.NoError(t, err)
	require.Len(t, specs, 3)

	require.Equal(t, ast.AlterTableAddColumns, specs[0].Tp)
	require.Equal(t, "id", specs[0].NewColumns[0].Name.Name.O)
	require.Equal(t, ast.AlterTableAddConstraint, specs[1].Tp)
	require.Equal(t, ast.ConstraintPrimaryKey, specs[1].Constraint.Tp)
	require.Equal(t, ast.AlterTableAddColumns, specs[2].Tp)
	require.Equal(t, "c", specs[2].NewColumns[0].Name.Name.O)
}

func TestResolveAlterTableInlinePrimaryKey_NoRewriteWithoutAutoIncrement(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t modify column c int primary key", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	specs, err := ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Equal(t, ast.AlterTableModifyColumn, specs[0].Tp)

	colDef := specs[0].NewColumns[0]
	require.False(t, containsColumnOption(colDef, ast.ColumnOptionAutoIncrement))
	require.True(t, containsColumnOption(colDef, ast.ColumnOptionPrimaryKey))
}

func TestResolveAlterTableInlinePrimaryKey_MultiplePrimaryKeyError(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("alter table t add (a int auto_increment primary key, b int auto_increment primary key)", "", "")
	require.NoError(t, err)

	sctx := mock.NewContext()
	_, err = ResolveAlterTableSpec(sctx, stmt.(*ast.AlterTableStmt).Specs)
	require.ErrorIs(t, err, infoschema.ErrMultiplePriKey)
}
