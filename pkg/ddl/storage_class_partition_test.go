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

	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestStorageClassAddPartitionUsesCheckedDefinitions(t *testing.T) {
	tests := []struct {
		name     string
		create   string
		alter    string
		tier     string
		lessThan string
		inValue  string
	}{
		{
			name: "range expression",
			create: `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"300"}}'
partition by range (id) (partition p0 values less than (100), partition p1 values less than (200))`,
			alter:    `alter table t add partition (partition p2 values less than (100 + 200))`,
			tier:     model.StorageClassTierIA,
			lessThan: "300",
		},
		{
			name: "list expression",
			create: `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "values_in":["4"]}}'
partition by list (id) (partition p0 values in (1, 2))`,
			alter:   `alter table t add partition (partition p1 values in (2 + 2))`,
			tier:    model.StorageClassTierIA,
			inValue: "4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := metabuild.NewContext()
			tbInfo := buildStorageClassTableInfo(t, ctx, tt.create)
			spec := parseStorageClassAlterSpec(t, tt.alter)
			partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
			require.NoError(t, err)

			clonedMeta := tbInfo.Clone()
			tmp := *partInfo
			oldDefCount := len(tbInfo.Partition.Definitions)
			tmp.Definitions = append(clonePartitionDefinitions(tbInfo.Partition.Definitions), tmp.Definitions...)
			clonedMeta.Partition = &tmp
			require.NoError(t, checkPartitionDefinitionConstraints(ctx.GetExprCtx(), clonedMeta))
			require.NoError(t, updatePartInfoDefinitionsFromFinalDefinitions(clonedMeta, partInfo, oldDefCount))

			require.Len(t, partInfo.Definitions, 1)
			require.Equal(t, tt.tier, partInfo.Definitions[0].StorageClassTier)
			if tt.lessThan != "" {
				require.Equal(t, tt.lessThan, partInfo.Definitions[0].LessThan[0])
			}
			if tt.inValue != "" {
				require.Equal(t, tt.inValue, partInfo.Definitions[0].InValues[0][0])
			}
		})
	}
}

func TestStorageClassReorganizePartitionUsesCheckedDefinitions(t *testing.T) {
	sctx := mock.NewContext()
	ctx := NewMetaBuildContextWithSctx(sctx)
	tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"200"}}'
partition by range (id) (partition p0 values less than (100), partition p1 values less than (300))`)
	spec := parseStorageClassAlterSpec(t, `alter table t reorganize partition p1 into (
partition p1 values less than (100 + 100),
partition p2 values less than (300))`)
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
	require.NoError(t, err)
	partNames := make([]string, 0, len(spec.PartitionNames))
	for _, name := range spec.PartitionNames {
		partNames = append(partNames, name.L)
	}
	firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNames, tbInfo.Partition)
	require.NoError(t, err)

	require.NoError(t, checkReorgPartitionDefs(sctx, model.ActionReorganizePartition, tbInfo, partInfo, firstPartIdx, lastPartIdx, idMap))
	require.Len(t, partInfo.Definitions, 2)
	require.Equal(t, "200", partInfo.Definitions[0].LessThan[0])
	require.Equal(t, model.StorageClassTierIA, partInfo.Definitions[0].StorageClassTier)
	require.Equal(t, "300", partInfo.Definitions[1].LessThan[0])
	require.Equal(t, model.StorageClassTierStandard, partInfo.Definitions[1].StorageClassTier)
}

func TestStorageClassRemovePartitioningIgnoresPartitionScopes(t *testing.T) {
	sctx := mock.NewContext()
	ctx := NewMetaBuildContextWithSctx(sctx)
	tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"200"}}'
partition by range (id) (partition p0 values less than (100), partition p1 values less than (200))`)
	partNames := make([]string, 0, len(tbInfo.Partition.Definitions))
	for _, def := range tbInfo.Partition.Definitions {
		partNames = append(partNames, def.Name.L)
	}

	meta := tbInfo.Clone()
	meta.Partition.Type = ast.PartitionTypeNone
	spec := &ast.AlterTableSpec{
		Tp: ast.AlterTableRemovePartitioning,
		PartDefinitions: []*ast.PartitionDefinition{{
			Name: ast.NewCIStr("CollapsedPartitions"),
		}},
	}
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	require.NoError(t, err)
	require.NoError(t, checkReorgPartitionDefs(sctx, model.ActionRemovePartitioning, tbInfo, partInfo, 0, len(partNames)-1, nil))
}

func buildStorageClassTableInfo(t *testing.T, ctx *metabuild.Context, sql string) *model.TableInfo {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	tbInfo, err := BuildTableInfoFromAST(ctx, createStmt)
	require.NoError(t, err)
	return tbInfo
}

func parseStorageClassAlterSpec(t *testing.T, sql string) *ast.AlterTableSpec {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	alterStmt, ok := stmt.(*ast.AlterTableStmt)
	require.True(t, ok)
	require.Len(t, alterStmt.Specs, 1)
	return alterStmt.Specs[0]
}
