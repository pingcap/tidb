// Copyright 2023 PingCAP, Inc.
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

package lockstats

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestPopulatePartitionIDAndNames(t *testing.T) {
	fakeInfo := infoschema.MockInfoSchema([]*model.TableInfo{
		tInfo(1, "t1", "p1", "p2"),
	})

	table := &ast.TableName{
		Schema: model.NewCIStr("test"),
		Name:   model.NewCIStr("t1"),
		PartitionNames: []model.CIStr{
			model.NewCIStr("p1"),
			model.NewCIStr("p2"),
		},
	}

	gotTID, gotPIDNames, err := populatePartitionIDAndNames(table, table.PartitionNames, fakeInfo)
	require.NoError(t, err)
	require.Equal(t, int64(1), gotTID)
	require.Equal(t, map[int64]string{
		2: "p1",
		3: "p2",
	}, gotPIDNames)

	// Empty partition names.
	_, _, err = populatePartitionIDAndNames(nil, nil, fakeInfo)
	require.Error(t, err)
}

func TestPopulateTableAndPartitionIDs(t *testing.T) {
	fakeInfo := infoschema.MockInfoSchema([]*model.TableInfo{
		tInfo(1, "t1", "p1", "p2"),
		tInfo(4, "t2"),
	})

	tables := []*ast.TableName{
		{
			Schema: model.NewCIStr("test"),
			Name:   model.NewCIStr("t1"),
			PartitionNames: []model.CIStr{
				model.NewCIStr("p1"),
				model.NewCIStr("p2"),
			},
		},
		{
			Schema: model.NewCIStr("test"),
			Name:   model.NewCIStr("t2"),
		},
	}

	tableWithPartitions, err := populateTableAndPartitionIDs(tables, fakeInfo)
	require.NoError(t, err)
	require.Equal(t, 2, len(tableWithPartitions))
	require.Equal(t, "test.t1", tableWithPartitions[1].FullName)
	require.Equal(t, "test.t1 partition (p1)", tableWithPartitions[1].PartitionInfo[2])
	require.Equal(t, "test.t2", tableWithPartitions[4].FullName)
	// Empty table list.
	_, err = populateTableAndPartitionIDs(nil, fakeInfo)
	require.Error(t, err)
}

func tInfo(id int, tableName string, partitionNames ...string) *model.TableInfo {
	tbl := &model.TableInfo{
		ID:   int64(id),
		Name: model.NewCIStr(tableName),
	}
	if len(partitionNames) > 0 {
		tbl.Partition = &model.PartitionInfo{
			Enable: true,
		}
		for i, partitionName := range partitionNames {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions, model.PartitionDefinition{
				ID:   int64(id + 1 + i),
				Name: model.NewCIStr(partitionName),
			})
		}
	}

	return tbl
}
