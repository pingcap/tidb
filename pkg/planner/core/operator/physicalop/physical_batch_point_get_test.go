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

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPruneCommonHandleDuplicateValues(t *testing.T) {
	fieldType := types.NewFieldType(mysql.TypeVarchar)
	column := &model.ColumnInfo{ID: 1, Offset: 0, FieldType: *fieldType}
	primaryIndex := &model.IndexInfo{
		ID:      1,
		Primary: true,
		Unique:  true,
		Columns: []*model.IndexColumn{{Offset: 0, Length: types.UnspecifiedLength}},
	}
	tableInfo := &model.TableInfo{
		ID:             1,
		Columns:        []*model.ColumnInfo{column},
		Indices:        []*model.IndexInfo{primaryIndex},
		IsCommonHandle: true,
	}
	indexValues := func(values ...any) [][]types.Datum {
		result := make([][]types.Datum, 0, len(values))
		for _, value := range values {
			result = append(result, []types.Datum{types.NewDatum(value)})
		}
		return result
	}

	testCases := []struct {
		name     string
		input    [][]types.Datum
		expected []string
	}{
		{name: "empty", input: nil, expected: nil},
		{name: "no duplicates", input: indexValues("a", "b", "c"), expected: []string{"a", "b", "c"}},
		{name: "duplicates and null", input: indexValues("b", "a", "b", nil, "c", "a"), expected: []string{"b", "a", "c"}},
		{name: "all duplicates", input: indexValues("a", "a", "a"), expected: []string{"a"}},
		{name: "only nulls", input: indexValues(nil, nil), expected: nil},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			plan := &BatchPointGetPlan{
				TblInfo:     tableInfo,
				IndexInfo:   primaryIndex,
				IndexValues: testCase.input,
			}

			handles, isTableDual, err := plan.PrunePartitionsAndValues(mock.NewContext())
			require.NoError(t, err)
			require.False(t, isTableDual)
			require.Len(t, handles, len(testCase.expected))
			require.Len(t, plan.IndexValues, len(testCase.expected))
			for i, expected := range testCase.expected {
				actual, err := plan.IndexValues[i][0].ToString()
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}
