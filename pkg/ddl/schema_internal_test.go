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

	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
)

func TestAppendPhysicalTableIDs(t *testing.T) {
	table := &model.TableInfo{
		ID: 100,
		Partition: &model.PartitionInfo{
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 101},
				{ID: 102},
			},
		},
	}

	ids := appendPhysicalTableIDs([]int64{1}, table)

	require.Equal(t, []int64{1, 100, 101, 102}, ids)
}

func TestBuildTableLabelRuleOwnership(t *testing.T) {
	codec := tikv.NewCodecV1(tikv.ModeTxn)
	tbl := &model.TableInfo{Name: ast.NewCIStr("t")}
	id := label.NewRuleID(codec, "db", "t", "")
	rule := (&label.Rule{Labels: []pd.RegionLabel{{Key: "a", Value: "table"}}}).Reset(codec, "old_db", "old_t", "", 100)
	before := rule.String()
	ids := []int64{30, 10, 20}
	result := buildTableLabelRule(codec, "db", tbl, ids, map[string]*label.Rule{id: rule})
	require.Equal(t, id, result.ID)
	require.Equal(t, before, rule.String())
	require.Equal(t, []int64{30, 10, 20}, ids)
	require.Nil(t, buildTableLabelRule(codec, "db", tbl, ids, nil))
}

func TestRestoreOriginalPartitionDefinitions(t *testing.T) {
	t.Run("nonadjacent LIST partitions with reused names", func(t *testing.T) {
		current := []model.PartitionDefinition{
			{ID: 100, Name: ast.NewCIStr("p1"), InValues: [][]string{{"10", "20"}}},
			{ID: 10, Name: ast.NewCIStr("p0"), InValues: [][]string{{"0"}}},
			{ID: 30, Name: ast.NewCIStr("p2"), InValues: [][]string{{"2"}}},
			{ID: 101, Name: ast.NewCIStr("p3"), InValues: [][]string{{"30", "40"}}},
			{ID: 50, Name: ast.NewCIStr("p4"), InValues: [][]string{{"4"}}},
			{ID: 102, Name: ast.NewCIStr("p5"), InValues: [][]string{{"50"}}},
		}
		dropping := []model.PartitionDefinition{
			{ID: 20, Name: ast.NewCIStr("p1"), InValues: [][]string{{"1"}}},
			{ID: 40, Name: ast.NewCIStr("p3"), InValues: [][]string{{"3"}}},
		}
		originalIDs := []int64{10, 20, 30, 40, 50}
		currentBefore := clonePartitionDefinitions(current)
		droppingBefore := clonePartitionDefinitions(dropping)

		restored, err := restoreOriginalPartitionDefinitions(originalIDs, current, dropping)

		require.NoError(t, err)
		require.Equal(t, []model.PartitionDefinition{
			{ID: 10, Name: ast.NewCIStr("p0"), InValues: [][]string{{"0"}}},
			{ID: 20, Name: ast.NewCIStr("p1"), InValues: [][]string{{"1"}}},
			{ID: 30, Name: ast.NewCIStr("p2"), InValues: [][]string{{"2"}}},
			{ID: 40, Name: ast.NewCIStr("p3"), InValues: [][]string{{"3"}}},
			{ID: 50, Name: ast.NewCIStr("p4"), InValues: [][]string{{"4"}}},
		}, restored)
		require.Equal(t, []int64{10, 20, 30, 40, 50}, originalIDs)
		require.Equal(t, currentBefore, current)
		require.Equal(t, droppingBefore, dropping)
	})

	t.Run("independent definitions and nested values", func(t *testing.T) {
		current := []model.PartitionDefinition{
			{ID: 1, LessThan: []string{"10"}, InValues: [][]string{{"1", "2"}, {"3", "4"}}},
		}
		dropping := []model.PartitionDefinition{
			{ID: 2, LessThan: []string{"20"}, InValues: [][]string{{"5", "6"}, {"7", "8"}}},
		}
		currentBefore := clonePartitionDefinitions(current)
		droppingBefore := clonePartitionDefinitions(dropping)

		restored, err := restoreOriginalPartitionDefinitions([]int64{1, 2}, current, dropping)

		require.NoError(t, err)
		require.Len(t, restored, 2)
		for i := range restored {
			restored[i].ID = 100
			restored[i].LessThan[0] = "changed"
			restored[i].InValues[0][0] = "changed"
			restored[i].InValues[1] = []string{"replaced"}
		}
		require.Equal(t, currentBefore, current)
		require.Equal(t, droppingBefore, dropping)
	})

	t.Run("missing original ID", func(t *testing.T) {
		current := []model.PartitionDefinition{{ID: 1, InValues: [][]string{{"1"}}}}
		dropping := []model.PartitionDefinition{{ID: 3, InValues: [][]string{{"3"}}}}
		currentBefore := clonePartitionDefinitions(current)
		droppingBefore := clonePartitionDefinitions(dropping)

		restored, err := restoreOriginalPartitionDefinitions([]int64{1, 2, 3}, current, dropping)

		require.ErrorContains(t, err, "2")
		require.Nil(t, restored)
		require.Equal(t, currentBefore, current)
		require.Equal(t, droppingBefore, dropping)
	})
}
