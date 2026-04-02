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
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestBuildStorageClassSettingsFromJSON(t *testing.T) {
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
			name: "multiple tiers names in normalized",
			input: `[
				{"tier": "IA", "names_in": ["Part1", "PART2"]},
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
			name: "multiple tiers invalid unknown field",
			input: `[
				{"tier": "IA", "unknown": "value"}
			]`,
			expect: nil,
		},
		{
			name: "multiple tiers invalid multiple scopes",
			input: `[
				{"tier": "STANDARD", "names_in": ["part1"], "values_in": ["100"]}
			]`,
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
				"transitions": [{"tier": "IA", "after_days": 0}]
			}`,
			expect: nil,
		},
		{
			name: "invalid transitions",
			input: `{
				"tier": "IA",
				"transitions": [{"tier": "STANDARD", "after_days": 30}]
			}`,
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, err := ddl.BuildStorageClassSettingsFromJSON(json.RawMessage(tt.input))
			if tt.expect == nil {
				require.Error(t, err)
				require.Nil(t, settings)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expect, settings)
			}
		})
	}
}

func TestBuildStorageClassForTable(t *testing.T) {
	require := require.New(t)

	tbInfo := &model.TableInfo{
		ID:   1,
		Name: pmodel.NewCIStr("t1"),
	}

	tests := []struct {
		name   string
		input  *model.StorageClassSettings
		expect string
	}{
		{
			name: "empty settings",
		},
		{
			name: "no scope",
			input: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
				},
			},
			expect: "STANDARD",
		},
		{
			name: "with scope but no match",
			input: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", NamesIn: []string{"part1", "part2"}},
				},
			},
			expect: "STANDARD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ddl.BuildStorageClassForTable(tbInfo, tt.input)
			require.NoError(err)
			require.Equal(tt.expect, tbInfo.StorageClassTier)
		})
	}
}

func TestBuildStorageClassForPartitions(t *testing.T) {
	require := require.New(t)

	settings := &model.StorageClassSettings{
		Defs: []*model.StorageClassDef{
			{Tier: "IA", NamesIn: []string{"part1", "part2"}},
			{Tier: "STANDARD"},
		},
	}

	tbInfo := &model.TableInfo{
		ID:   1,
		Name: pmodel.NewCIStr("t1"),
	}

	partitions := []model.PartitionDefinition{
		{Name: pmodel.NewCIStr("part1")},
		{Name: pmodel.NewCIStr("part3")},
	}

	err := ddl.BuildStorageClassForPartitions(partitions, tbInfo, settings)
	require.NoError(err)

	require.Equal("IA", partitions[0].StorageClassTier)
	require.Equal("STANDARD", partitions[1].StorageClassTier)
}

func stringPtr(s string) *string {
	return &s
}
