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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestFTSModifierAllowsNativePushdown(t *testing.T) {
	tests := []struct {
		name     string
		modifier ast.FulltextSearchModifier
		expected bool
	}{
		{
			name:     "natural language mode (default)",
			modifier: ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode),
			expected: true,
		},
		{
			name:     "boolean mode",
			modifier: ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode),
			expected: false,
		},
		{
			name:     "natural language mode with query expansion",
			modifier: ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode | ast.FulltextSearchModifierWithQueryExpansion),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, ftsModifierAllowsNativePushdown(tt.modifier))
		})
	}
}

func TestTableHasPublicFTSIndexOnColumn(t *testing.T) {
	ftsIdx := func(name, column string, state model.SchemaState) *model.IndexInfo {
		return &model.IndexInfo{
			Name:         ast.NewCIStr(name),
			State:        state,
			Tp:           ast.IndexTypeInvalid,
			Columns:      []*model.IndexColumn{{Name: ast.NewCIStr(column)}},
			FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeStandardV1},
		}
	}
	plainIdx := func(name, column string) *model.IndexInfo {
		return &model.IndexInfo{
			Name:    ast.NewCIStr(name),
			State:   model.StatePublic,
			Tp:      ast.IndexTypeBtree,
			Columns: []*model.IndexColumn{{Name: ast.NewCIStr(column)}},
		}
	}

	tests := []struct {
		name     string
		indices  []*model.IndexInfo
		column   string
		expected bool
	}{
		{
			name:     "no indices",
			indices:  nil,
			column:   "title",
			expected: false,
		},
		{
			name:     "only non-FTS index on the column",
			indices:  []*model.IndexInfo{plainIdx("idx_title", "title")},
			column:   "title",
			expected: false,
		},
		{
			name:     "public FTS index on the column",
			indices:  []*model.IndexInfo{ftsIdx("ft_title", "title", model.StatePublic)},
			column:   "title",
			expected: true,
		},
		{
			name:     "non-public FTS index on the column",
			indices:  []*model.IndexInfo{ftsIdx("ft_title", "title", model.StateWriteReorganization)},
			column:   "title",
			expected: false,
		},
		{
			name:     "FTS index on a different column",
			indices:  []*model.IndexInfo{ftsIdx("ft_body", "body", model.StatePublic)},
			column:   "title",
			expected: false,
		},
		{
			name: "FTS index covers the column among many indices",
			indices: []*model.IndexInfo{
				plainIdx("idx_id", "id"),
				ftsIdx("ft_body", "body", model.StatePublic),
				ftsIdx("ft_title", "title", model.StatePublic),
			},
			column:   "title",
			expected: true,
		},
		{
			name:     "case-insensitive column match",
			indices:  []*model.IndexInfo{ftsIdx("ft_title", "Title", model.StatePublic)},
			column:   "title",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tblInfo := &model.TableInfo{Indices: tt.indices}
			require.Equal(t, tt.expected, tableHasPublicFTSIndexOnColumn(tblInfo, tt.column))
		})
	}
}
