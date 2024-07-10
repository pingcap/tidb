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

package infoschema

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

type mockAlloc struct {
	autoid.Allocator
	tp autoid.AllocatorType
}

func (m *mockAlloc) GetType() autoid.AllocatorType {
	return m.tp
}

func TestGetKeptAllocators(t *testing.T) {
	checkAllocators := func(allocators autoid.Allocators, expected []autoid.AllocatorType) {
		require.Len(t, allocators.Allocs, len(expected))
		for i, tp := range expected {
			require.Equal(t, tp, allocators.Allocs[i].GetType())
		}
	}
	allocators := autoid.Allocators{Allocs: []autoid.Allocator{
		&mockAlloc{tp: autoid.RowIDAllocType},
		&mockAlloc{tp: autoid.AutoIncrementType},
		&mockAlloc{tp: autoid.AutoRandomType},
	}}
	cases := []struct {
		diff     *model.SchemaDiff
		expected []autoid.AllocatorType
	}{
		{
			diff:     &model.SchemaDiff{Type: model.ActionTruncateTable},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType, autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionRebaseAutoID},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionModifyTableAutoIdCache},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionRebaseAutoRandomBase},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionAddColumn, model.ActionRebaseAutoID}},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionModifyTableAutoIdCache}},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionRebaseAutoRandomBase}},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionAddColumn}},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType, autoid.AutoRandomType},
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			res := getKeptAllocators(c.diff, allocators)
			checkAllocators(res, c.expected)
		})
	}
}
