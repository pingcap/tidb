// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDeniedByBDRWhenAddColumn(t *testing.T) {
	tests := []struct {
		name     string
		options  []*ast.ColumnOption
		expected bool
	}{
		{
			name:     "Test with no options(implicit nullable)",
			options:  []*ast.ColumnOption{},
			expected: false,
		},
		{
			name:     "Test with nullable option",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionNull}},
			expected: false,
		},
		{
			name:     "Test with implicit nullable and defaultValue options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}},
			expected: false,
		},
		{
			name:     "Test with nullable and defaultValue options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionNotNull}, {Tp: ast.ColumnOptionDefaultValue}},
			expected: false,
		},
		{
			name:     "Test with comment options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}},
			expected: false,
		},
		{
			name:     "Test with generated options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionGenerated}},
			expected: false,
		},
		{
			name:     "Test with comment and generated options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}, {Tp: ast.ColumnOptionGenerated}},
			expected: false,
		},
		{
			name:     "Test with other options",
			options:  []*ast.ColumnOption{{Tp: ast.ColumnOptionCheck}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deniedByBDRWhenAddColumn(tt.options)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDeniedByBDRWhenModifyColumn(t *testing.T) {
	tests := []struct {
		name         string
		newFieldType types.FieldType
		oldFieldType types.FieldType
		options      []*ast.ColumnOption
		expected     bool
	}{
		{
			name:         "Test when newFieldType and oldFieldType are not equal",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeVarchar),
			options:      []*ast.ColumnOption{},
			expected:     true,
		},
		{
			name:         "Test when only defaultValue option is provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}},
			expected:     false,
		},
		{
			name:         "Test when defaultValue and comment options are provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionDefaultValue}, {Tp: ast.ColumnOptionComment}},
			expected:     false,
		},
		{
			name:         "Test when other options are provided",
			newFieldType: *types.NewFieldType(mysql.TypeLong),
			oldFieldType: *types.NewFieldType(mysql.TypeLong),
			options:      []*ast.ColumnOption{{Tp: ast.ColumnOptionComment}},
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deniedByBDRWhenModifyColumn(tt.newFieldType, tt.oldFieldType, tt.options)
			require.Equal(t, tt.expected, result)
		})
	}
}
