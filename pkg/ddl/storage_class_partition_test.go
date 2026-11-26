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

			oldDefCount := len(tbInfo.Partition.Definitions)
			require.NoError(t, CheckAndUpdateAddedPartitionDefinitions(ctx.GetExprCtx(), tbInfo, partInfo, oldDefCount))

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

	t.Run("validation failure preserves input definitions", func(t *testing.T) {
		ctx := metabuild.NewContext()
		tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int)
partition by list (id) (partition p0 values in (1))`)
		spec := parseStorageClassAlterSpec(t, `alter table t add partition (
partition p1 values in (2 + 0), partition p2 values in (2))`)
		partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
		require.NoError(t, err)
		requestedValue := partInfo.Definitions[0].InValues[0][0]

		err = CheckAndUpdateAddedPartitionDefinitions(ctx.GetExprCtx(), tbInfo, partInfo, len(tbInfo.Partition.Definitions))
		require.ErrorContains(t, err, "Multiple definition of same constant in list partitioning")
		require.Equal(t, [][]string{{"1"}}, tbInfo.Partition.Definitions[0].InValues)
		require.Equal(t, [][]string{{requestedValue}}, partInfo.Definitions[0].InValues)
		require.Equal(t, [][]string{{"2"}}, partInfo.Definitions[1].InValues)
	})

	t.Run("alter without storage class keeps existing metadata", func(t *testing.T) {
		ctx := metabuild.NewContext()
		tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": "IA"}'
partition by range (id) (partition p0 values less than (100), partition p1 values less than (200))`)
		require.Equal(t, model.StorageClassTierIA, tbInfo.StorageClassTier)
		require.Len(t, tbInfo.Partition.Definitions, 2)
		require.Equal(t, model.StorageClassTierIA, tbInfo.Partition.Definitions[0].StorageClassTier)
		require.Equal(t, model.StorageClassTierIA, tbInfo.Partition.Definitions[1].StorageClassTier)

		tbInfo.EngineAttribute = `{"future_field":true}`
		require.NoError(t, onAlterTableStorageClassSettings(nil, tbInfo))
		require.Equal(t, model.StorageClassTierIA, tbInfo.StorageClassTier)
		require.Equal(t, model.StorageClassTierIA, tbInfo.Partition.Definitions[0].StorageClassTier)
		require.Equal(t, model.StorageClassTierIA, tbInfo.Partition.Definitions[1].StorageClassTier)
	})
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

	requested := partInfo.Clone().Definitions
	original := tbInfo.Partition.Clone().Definitions
	definitions, err := checkAndNormalizeReorgPartitionDefinitions(sctx.GetExprCtx(), model.ActionReorganizePartition, tbInfo, partInfo, firstPartIdx, lastPartIdx, idMap)
	require.NoError(t, err)
	require.Equal(t, requested, partInfo.Definitions)
	require.Equal(t, original, tbInfo.Partition.Definitions)
	partInfo.Definitions = definitions
	require.Len(t, partInfo.Definitions, 2)
	require.Equal(t, "200", partInfo.Definitions[0].LessThan[0])
	require.Equal(t, model.StorageClassTierIA, partInfo.Definitions[0].StorageClassTier)
	require.Equal(t, "300", partInfo.Definitions[1].LessThan[0])
	require.Equal(t, model.StorageClassTierStandard, partInfo.Definitions[1].StorageClassTier)

	t.Run("range boundary failure preserves requested expression", func(t *testing.T) {
		spec := parseStorageClassAlterSpec(t, `alter table t reorganize partition p0 into (
partition p0 values less than (25 + 25))`)
		partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
		require.NoError(t, err)
		requested := partInfo.Clone().Definitions
		definitions, err := checkAndNormalizeReorgPartitionDefinitions(sctx.GetExprCtx(), model.ActionReorganizePartition, tbInfo, partInfo, 0, 0, map[int]struct{}{0: {}})
		require.ErrorContains(t, err, "VALUES LESS THAN value must be strictly increasing")
		require.Nil(t, definitions)
		require.Equal(t, requested, partInfo.Definitions)
		require.Equal(t, original, tbInfo.Partition.Definitions)
	})
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
	_, err = checkAndNormalizeReorgPartitionDefinitions(sctx.GetExprCtx(), model.ActionRemovePartitioning, tbInfo, partInfo, 0, len(partNames)-1, nil)
	require.NoError(t, err)
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

func TestIntervalPartitionDefinitionsPreserveRequest(t *testing.T) {
	ctx := metabuild.NewContext()
	tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int)
partition by range (id) (partition p0 values less than (10), partition p1 values less than (20))`)
	for _, sql := range []string{
		`alter table t last partition less than (40)`,
		`alter table t last partition less than (35)`,
	} {
		t.Run(sql, func(t *testing.T) {
			spec := parseStorageClassAlterSpec(t, sql)
			original := parseStorageClassAlterSpec(t, sql)
			first, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
			if sql == `alter table t last partition less than (35)` {
				require.ErrorContains(t, err, "not matching FIRST + n INTERVALs")
				require.Nil(t, first)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"30", "40"}, []string{first.Definitions[0].LessThan[0], first.Definitions[1].LessThan[0]})
			}
			require.Equal(t, original, spec)
			second, secondErr := BuildAddedPartitionInfo(ctx.GetExprCtx(), tbInfo, spec)
			require.Equal(t, first, second)
			if err != nil {
				require.EqualError(t, secondErr, err.Error())
			} else {
				require.NoError(t, secondErr)
			}
			require.Equal(t, original, spec)
		})
	}
	t.Run("existing definitions do not share append capacity", func(t *testing.T) {
		var first ast.ExprNode = ast.NewValueExpr(20, "", "")
		var last ast.ExprNode = ast.NewValueExpr(30, "", "")
		prefix := &ast.PartitionDefinition{Name: ast.NewCIStr("prefix")}
		sentinel := &ast.PartitionDefinition{Name: ast.NewCIStr("untouched")}
		backing := []*ast.PartitionDefinition{prefix, sentinel, sentinel}
		options := &ast.PartitionOptions{
			PartitionMethod: ast.PartitionMethod{
				Interval: &ast.PartitionInterval{
					IntervalExpr:  ast.PartitionIntervalExpr{Expr: ast.NewValueExpr(10, "", ""), TimeUnit: ast.TimeUnitInvalid},
					FirstRangeEnd: &first,
					LastRangeEnd:  &last,
				},
			},
			Definitions: backing[:1],
		}
		require.NoError(t, GeneratePartDefsFromInterval(ctx.GetExprCtx(), ast.AlterTablePartition, tbInfo, options))
		require.Len(t, options.Definitions, 3)
		require.Same(t, prefix, options.Definitions[0])
		require.Same(t, sentinel, backing[1])
		require.Same(t, sentinel, backing[2])
	})
}

func TestBuildIntervalPartitionInfoPreservesRequest(t *testing.T) {
	for _, tt := range []struct {
		name     string
		interval string
		wantErr  string
		bounds   []string
		names    []string
	}{
		{
			name:     "NULL and MAXVALUE partitions",
			interval: `interval (10) first partition less than (10) last partition less than (30) null partition maxvalue partition`,
			bounds:   []string{"-2147483648", "10", "20", "30", "MAXVALUE"},
		},
		{
			name: "user-defined names",
			interval: `interval (10) first partition less than (10) last partition less than (30)
(partition first_named values less than (10),
partition middle_named values less than (20), partition last_named values less than (30))`,
			bounds: []string{"10", "20", "30"},
			names:  []string{"first_named", "middle_named", "last_named"},
		},
		{
			name: "unsupported user-defined options",
			interval: `interval (10) first partition less than (10) last partition less than (30)
(partition first_named values less than (10) comment 'first',
partition middle_named values less than (20), partition last_named values less than (30))`,
			wantErr: "partition first_named does have unsupported options",
		},
		{
			name:     "unmatched endpoint",
			interval: `interval (10) first partition less than (10) last partition less than (25)`,
			wantErr:  "not matching FIRST + n INTERVALs",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := metabuild.NewContext()
			sql := `create table t (id int) partition by range columns (id) ` + tt.interval
			stmt, err := parser.New().ParseOneStmt(sql, "", "")
			require.NoError(t, err)
			original, err := parser.New().ParseOneStmt(sql, "", "")
			require.NoError(t, err)
			request := stmt.(*ast.CreateTableStmt).Partition
			for range 2 {
				tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (id int)`)
				err = buildTablePartitionInfo(ctx, request, tbInfo)
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
				} else {
					require.NoError(t, err)
					bounds := make([]string, 0, len(tbInfo.Partition.Definitions))
					names := make([]string, 0, len(tbInfo.Partition.Definitions))
					for _, def := range tbInfo.Partition.Definitions {
						bounds = append(bounds, def.LessThan[0])
						names = append(names, def.Name.O)
					}
					require.Equal(t, tt.bounds, bounds)
					if tt.names != nil {
						require.Equal(t, tt.names, names)
					}
				}
				require.Equal(t, original.(*ast.CreateTableStmt).Partition, request)
			}
		})
	}
}
