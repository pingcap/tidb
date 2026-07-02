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

package ddl_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestBuildStorageClassSettingsFromJSON(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name   string
		input  string
		expect *model.StorageClassSettings
	}{
		{
			name:  "valid string tier",
			input: `"STANDARD"`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name:   "invalid string tier",
			input:  `"INVALID"`,
			expect: nil,
		},
		{
			name: "valid no scope",
			input: `{
				"tier": "STANDARD"
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name: "valid names in",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:    "STANDARD",
						NamesIn: []string{"part1", "part2"},
					},
				},
			},
		},
		{
			name: "valid less than",
			input: `{
				"tier": "STANDARD",
				"less_than": "100"
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:     "STANDARD",
						LessThan: stringPtr("100"),
					},
				},
			},
		},
		{
			name: "valid values in",
			input: `{
				"tier": "STANDARD",
				"values_in": ["100", "200"]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:     "STANDARD",
						ValuesIn: []string{"100", "200"},
					},
				},
			},
		},
		{
			name: "invalid multiple scopes",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"],
				"values_in": ["100", "200"]
			}`,
			expect: nil,
		},
		{
			name: "invalid unknown field",
			input: `{
				"tier": "STANDARD",
				"unknown": "100"
			}`,
			expect: nil,
		},
		{
			name: "invalid JSON",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"
			}`,
			expect: nil,
		},
		{
			name:   "invalid trailing JSON",
			input:  `{"tier":"STANDARD"} {"tier":"IA"}`,
			expect: nil,
		},
		{
			name: "multiple tiers",
			input: `[
				{"tier": "IA", "names_in": ["part1", "part2"]},
				{"tier": "STANDARD"}
			]`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1", "part2"}},
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name: "multiple tiers normalized",
			input: `[
				{"tier": "ia", "names_in": ["Part1"]},
				{"tier": "standard", "transitions": [{"tier": "ia", "after_days": 30}]}
			]`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
					{Tier: "STANDARD", Transitions: []model.StorageClassTransitRule{
						{Tier: "IA", AfterDays: 30},
					}},
				},
			},
		},
		{
			name:   "invalid unknown field in list",
			input:  `[{"tier": "STANDARD", "unknown": "100"}]`,
			expect: nil,
		},
		{
			name:   "invalid null def in list",
			input:  `[null]`,
			expect: nil,
		},
		{
			name:   "invalid null def mixed in list",
			input:  `[{"tier": "STANDARD"}, null]`,
			expect: nil,
		},
		{
			name: "valid transitions",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 30}]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", Transitions: []model.StorageClassTransitRule{
						{Tier: "IA", AfterDays: 30},
					}},
				},
			},
		},
		{
			name: "redundant transitions",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 30}, {"tier": "IA", "after_days": 60}]
			}`,
			expect: nil,
		},
		{
			name: "transitions from cold to hot",
			input: `{
				"tier": "IA",
				"transitions": [{"tier": "STANDARD", "after_days": 30}]
			}`,
			expect: nil,
		},
		{
			name: "transitions from cold to hot 2",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 15}, {"tier": "STANDARD", "after_days": 30}]
			}`,
			expect: nil,
		},
		{
			name: "transitions with transit time of 0",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 0}]
			}`,
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ddl.BuildStorageClassSettingsFromJSON(json.RawMessage([]byte(tt.input)))
			if tt.expect != nil {
				require.NoError(err)
				require.Equal(tt.expect, got)
			} else {
				require.Error(err)
			}
		})
	}

	got, err := ddl.BuildStorageClassSettingsFromJSON(nil)
	require.NoError(err)
	expect := &model.StorageClassSettings{
		Defs: []*model.StorageClassDef{
			{Tier: "STANDARD"},
		},
	}
	require.Equal(expect, got)
}

func TestBuildStorageClassForTable(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name     string
		settings *model.StorageClassSettings
		expected string
	}{
		{
			name:     "no storage class settings",
			settings: nil,
			expected: "",
		},
		{
			name: "no scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA"},
				},
			},
			expected: "IA",
		},
		{
			name: "no matching scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			expected: "STANDARD",
		},
		{
			name: "multiply tiers",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", NamesIn: []string{"part1"}},
					{Tier: "STANDARD", NamesIn: []string{"part2"}},
					{Tier: "IA"},
				},
			},
			expected: "IA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := &model.TableInfo{}
			err := ddl.BuildStorageClassForTable(tbInfo, tt.settings)
			require.NoError(err)
			require.Equal(tt.expected, tbInfo.StorageClassTier)
		})
	}
}

func TestBuildStorageClassForPartitions(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name          string
		settings      *model.StorageClassSettings
		partitions    []model.PartitionDefinition
		partitionType ast.PartitionType
		expected      []string
	}{
		{
			name:     "no storage class settings",
			settings: nil,
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			expected: []string{"", ""},
		},
		{
			name: "no scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA"},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			expected: []string{"IA", "IA"},
		},
		{
			name: "no scope definition on hash partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA"},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			partitionType: ast.PartitionTypeHash,
			expected:      []string{"IA", "IA"},
		},
		{
			name: "names_in scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			expected: []string{"IA", "STANDARD"},
		},
		{
			name: "names_in invalid on hash partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			partitionType: ast.PartitionTypeHash,
			expected:      nil,
		},
		{
			name: "names_in invalid on key partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			partitionType: ast.PartitionTypeKey,
			expected:      nil,
		},
		{
			name: "partition scopes override no-scope default",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", NamesIn: []string{"part1"}},
					{Tier: "IA"},
					{Tier: "STANDARD", NamesIn: []string{"part2"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			expected: []string{"STANDARD", "STANDARD"},
		},
		{
			name: "partition scope wins when no-scope default appears first",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}},
				{Name: ast.CIStr{L: "part2"}},
			},
			expected: []string{"IA", "STANDARD"},
		},
		{
			name: "less_than scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", LessThan: stringPtr("200")},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, LessThan: []string{"100"}},
				{Name: ast.CIStr{L: "part2"}, LessThan: []string{"200"}},
				{Name: ast.CIStr{L: "part3"}, LessThan: []string{"1000"}},
			},
			expected: []string{"IA", "IA", "STANDARD"},
		},
		{
			name: "values_in scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", ValuesIn: []string{"2", "3"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, InValues: [][]string{{"1"}, {"2"}}},
				{Name: ast.CIStr{L: "part2"}, InValues: [][]string{{"3"}}},
				{Name: ast.CIStr{L: "part3"}, InValues: [][]string{{"4"}}},
			},
			expected: []string{"IA", "IA", "STANDARD"},
		},
		{
			name: "less_than maxvalue includes literal and maxvalue upper bounds",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", LessThan: stringPtr("MAXVALUE")},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, LessThan: []string{"'MAXVALUE'"}},
				{Name: ast.CIStr{L: "part2"}, LessThan: []string{"MAXVALUE"}},
			},
			expected: []string{"IA", "IA"},
		},
		{
			name: "values_in keyword does not match quoted literal",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", ValuesIn: []string{"DEFAULT"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, InValues: [][]string{{"'DEFAULT'"}}},
				{Name: ast.CIStr{L: "part2"}, InValues: [][]string{{"DEFAULT"}}},
			},
			expected: []string{"STANDARD", "IA"},
		},
		{
			name: "less_than invalid on list partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", LessThan: stringPtr("200")},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, InValues: [][]string{{"1"}}},
			},
			expected: nil,
		},
		{
			name: "less_than invalid on multi-column range partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", LessThan: stringPtr("200")},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, LessThan: []string{"100", "200"}},
			},
			expected: nil,
		},
		{
			name: "less_than invalid numeric range value",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", LessThan: stringPtr("abc")},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, LessThan: []string{"100"}},
			},
			expected: nil,
		},
		{
			name: "values_in invalid on range partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", ValuesIn: []string{"1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, LessThan: []string{"100"}},
			},
			expected: nil,
		},
		{
			name: "values_in invalid on multi-column list partition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", ValuesIn: []string{"1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: ast.CIStr{L: "part1"}, InValues: [][]string{{"1", "2"}}},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := &model.TableInfo{}
			if tt.partitions != nil {
				tbInfo.Partition = &model.PartitionInfo{
					Definitions: tt.partitions,
				}
				if tt.partitionType != ast.PartitionTypeNone {
					tbInfo.Partition.Type = tt.partitionType
				} else if len(tt.partitions) > 0 {
					switch {
					case len(tt.partitions[0].LessThan) > 0:
						tbInfo.Partition.Type = ast.PartitionTypeRange
					case len(tt.partitions[0].InValues) > 0:
						tbInfo.Partition.Type = ast.PartitionTypeList
					}
				}
			}
			err := ddl.BuildStorageClassForPartitions(tt.partitions, tbInfo, tt.settings)
			if tt.expected == nil {
				require.Error(err)
				return
			}
			require.NoError(err)
			for i, part := range tbInfo.Partition.Definitions {
				require.Equal(tt.expected[i], part.StorageClassTier)
			}
		})
	}
}

func TestStorageClassPartitionScopesUseNormalizedValues(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		tiers      []string
		lessThan   []string
		listValues []string
	}{
		{
			name: "range expression",
			sql: `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"200"}}'
		partition by range (id) (partition p0 values less than (100 + 100), partition p1 values less than (300))`,
			tiers:    []string{"IA", "STANDARD"},
			lessThan: []string{"200", "300"},
		},
		{
			name: "range expression unsigned",
			sql: `create table t (id bigint unsigned) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"18446744073709551614"}}'
		partition by range (id) (partition p0 values less than (18446744073709551614), partition p1 values less than (18446744073709551615))`,
			tiers:    []string{"IA", "STANDARD"},
			lessThan: []string{"18446744073709551614", "18446744073709551615"},
		},
		{
			name: "range columns integer",
			sql: `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"20"}}'
		partition by range columns (id) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))`,
			tiers:    []string{"IA", "IA", "STANDARD"},
			lessThan: []string{"10", "20", "30"},
		},
		{
			name: "range columns datetime",
			sql: `create table t (created_at datetime) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"2026-05-01 00:00:00"}}'
	partition by range columns (created_at) (partition p202604 values less than ('2026-04-01 00:00:00'), partition p202605 values less than ('2026-05-01 00:00:00'), partition p202606 values less than ('2026-06-01 00:00:00'))`,
			tiers:    []string{"IA", "IA", "STANDARD"},
			lessThan: []string{"'2026-04-01 00:00:00'", "'2026-05-01 00:00:00'", "'2026-06-01 00:00:00'"},
		},
		{
			name: "range columns string numeric literal",
			sql: `create table t (name varchar(20)) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"2"}}'
	partition by range columns (name) (partition p10 values less than ('10'), partition p2 values less than ('2'), partition p3 values less than ('3'))`,
			tiers:    []string{"IA", "IA", "STANDARD"},
			lessThan: []string{"'10'", "'2'", "'3'"},
		},
		{
			name: "range columns string uses column collation",
			sql: `create table t (name char(10) collate utf8mb4_unicode_ci) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"G"}}'
	partition by range columns (name) (partition p0 values less than ('a'), partition p1 values less than ('G'))`,
			tiers:    []string{"IA", "IA"},
			lessThan: []string{"'a'", "'G'"},
		},
		{
			name: "list expression",
			sql: `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "values_in":["2"]}}'
partition by list (id) (partition p0 values in (1 + 1), partition p1 values in (3))`,
			tiers:      []string{"IA", "STANDARD"},
			listValues: []string{"2", "3"},
		},
		{
			name: "range columns maxvalue includes literal and maxvalue upper bounds",
			sql: `create table t (name varchar(20)) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"MAXVALUE"}}'
	partition by range columns (name) (partition p0 values less than ('MAXVALUE'), partition p1 values less than (MAXVALUE))`,
			tiers:    []string{"IA", "IA"},
			lessThan: []string{"'MAXVALUE'", "MAXVALUE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := buildTableInfoFromCreateSQL(t, tt.sql)
			require.NotNil(t, tbInfo.Partition)
			require.Len(t, tbInfo.Partition.Definitions, len(tt.tiers))
			for i, tier := range tt.tiers {
				part := tbInfo.Partition.Definitions[i]
				require.Equal(t, tier, part.StorageClassTier)
				if len(tt.lessThan) > 0 {
					require.Equal(t, tt.lessThan[i], part.LessThan[0])
				}
				if len(tt.listValues) > 0 {
					require.Equal(t, tt.listValues[i], part.InValues[0][0])
				}
			}
		})
	}
}

func TestStorageClassPartitionScopesRejectInvalidLessThanValue(t *testing.T) {
	sql := `create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"abc"}}'
	partition by range (id) (partition p0 values less than (100), partition p1 values less than (200))`
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	_, err = ddl.BuildTableInfoFromAST(metabuild.NewContext(), createStmt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid 'less_than' value")
}

func buildTableInfoFromCreateSQL(t *testing.T, sql string) *model.TableInfo {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	tbInfo, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), createStmt)
	require.NoError(t, err)
	return tbInfo
}

func TestStorageClassString(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name        string
		tier        string
		transitions []model.StorageClassTransitRule
		expected    string
	}{
		{
			name:     "no transitions",
			tier:     "STANDARD",
			expected: "STANDARD",
		},
		{
			name:     "IA with no transitions",
			tier:     "IA",
			expected: "IA",
		},
		{
			name:        "with transitions",
			tier:        "STANDARD",
			transitions: []model.StorageClassTransitRule{{Tier: "IA", AfterDays: 30}},
			expected:    `{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ti := model.TableInfo{
				StorageClassTier:        tt.tier,
				StorageClassTransitions: tt.transitions,
			}

			result := ti.StorageClassString()
			require.Equal(tt.expected, result)
		})
	}
}

func TestGetEngineAttributeFromStorageClassTableOptions(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name     string
		options  []*ast.TableOption
		expected string
		found    bool
		hasErr   bool
	}{
		{
			name: "storage class sugar",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionStorageClass, StrValue: "ia"},
			},
			expected: `{"storage_class":"IA"}`,
			found:    true,
		},
		{
			name: "engine attribute",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`},
			},
			expected: `{"storage_class":"STANDARD"}`,
			found:    true,
		},
		{
			name: "repeated engine attribute keeps last value",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`},
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"IA"}`},
			},
			expected: `{"storage_class":"IA"}`,
			found:    true,
		},
		{
			name: "repeated engine attribute rejects invalid earlier value",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{`},
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"IA"}`},
			},
			hasErr: true,
		},
		{
			name: "engine attribute then storage class",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`},
				{Tp: ast.TableOptionStorageClass, StrValue: "IA"},
			},
			hasErr: true,
		},
		{
			name: "storage class then engine attribute",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionStorageClass, StrValue: "IA"},
				{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`},
			},
			hasErr: true,
		},
		{
			name: "invalid storage class tier",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionStorageClass, StrValue: "cold"},
			},
			hasErr: true,
		},
		{
			name: "repeated storage class rejects invalid earlier value",
			options: []*ast.TableOption{
				{Tp: ast.TableOptionStorageClass, StrValue: "cold"},
				{Tp: ast.TableOptionStorageClass, StrValue: "IA"},
			},
			hasErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found, err := ddl.GetEngineAttributeFromStorageClassTableOptions(tt.options)
			if tt.hasErr {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tt.found, found)
			require.JSONEq(tt.expected, got)
		})
	}
}

func TestCheckStorageClassConflictInAlterTableSpecs(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name   string
		specs  []*ast.AlterTableSpec
		hasErr bool
	}{
		{
			name: "same spec conflict",
			specs: []*ast.AlterTableSpec{
				{
					Tp: ast.AlterTableOption,
					Options: []*ast.TableOption{
						{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`},
						{Tp: ast.TableOptionStorageClass, StrValue: "IA"},
					},
				},
			},
			hasErr: true,
		},
		{
			name: "separate specs conflict",
			specs: []*ast.AlterTableSpec{
				{
					Tp:      ast.AlterTableOption,
					Options: []*ast.TableOption{{Tp: ast.TableOptionEngineAttribute, StrValue: `{"storage_class":"STANDARD"}`}},
				},
				{
					Tp:      ast.AlterTableOption,
					Options: []*ast.TableOption{{Tp: ast.TableOptionStorageClass, StrValue: "IA"}},
				},
			},
			hasErr: true,
		},
		{
			name: "single form",
			specs: []*ast.AlterTableSpec{
				{
					Tp:      ast.AlterTableOption,
					Options: []*ast.TableOption{{Tp: ast.TableOptionStorageClass, StrValue: "IA"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ddl.CheckStorageClassConflictInAlterTableSpecs(tt.specs)
			if tt.hasErr {
				require.Error(err)
				return
			}
			require.NoError(err)
		})
	}
}

func TestGetSimpleTableStorageClassForShowCreate(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name            string
		engineAttribute string
		expected        string
		ok              bool
	}{
		{
			name:            "simple string storage class",
			engineAttribute: `{"storage_class":"IA"}`,
			expected:        "IA",
			ok:              true,
		},
		{
			name:            "simple object storage class",
			engineAttribute: `{"storage_class":{"tier":"ia"}}`,
			expected:        "IA",
			ok:              true,
		},
		{
			name:            "simple list storage class",
			engineAttribute: `{"storage_class":[{"tier":"ia"}]}`,
			expected:        "IA",
			ok:              true,
		},
		{
			name:            "with transitions falls back to engine attribute",
			engineAttribute: `{"storage_class":{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}}`,
			ok:              false,
		},
		{
			name:            "with scope falls back to engine attribute",
			engineAttribute: `{"storage_class":{"tier":"IA","names_in":["p0"]}}`,
			ok:              false,
		},
		{
			name:            "with additional engine attribute field falls back to engine attribute",
			engineAttribute: `{"storage_class":"IA","future_field":true}`,
			ok:              false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := ddl.GetSimpleTableStorageClassForShowCreate(&model.TableInfo{
				EngineAttribute: tt.engineAttribute,
			})
			require.NoError(err)
			require.Equal(tt.ok, ok)
			require.Equal(tt.expected, got)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
