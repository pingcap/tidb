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

package notifier

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestLeftoverWhenUnmarshal(t *testing.T) {
	r := &listResult{}
	changesReused := []*SchemaChange{
		{event: &SchemaChangeEvent{inner: &jsonSchemaChangeEvent{
			TableInfo: &model.TableInfo{
				Name:    pmodel.NewCIStr("old"),
				Columns: []*model.ColumnInfo{{Name: pmodel.NewCIStr("c1")}},
				Indices: []*model.IndexInfo{
					{Name: pmodel.NewCIStr("i1")},
					{Name: pmodel.NewCIStr("i2")},
					{Name: pmodel.NewCIStr("i3")},
				},
			},
		}}},
		{event: &SchemaChangeEvent{inner: &jsonSchemaChangeEvent{
			AddedPartInfo: &model.PartitionInfo{Expr: "test"},
		}}},
		nil,
	}

	newTableInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("new"),
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("c2")},
			{Name: pmodel.NewCIStr("c3")},
		},
		Indices: []*model.IndexInfo{
			{Name: pmodel.NewCIStr("i4")},
		},
		Constraints: []*model.ConstraintInfo{
			{Name: pmodel.NewCIStr("c1")},
		},
	}

	newTableInfoJSON, err := json.Marshal(jsonSchemaChangeEvent{TableInfo: newTableInfo})
	require.NoError(t, err)
	sameRow := chunk.MutRowFromDatums([]types.Datum{
		types.NewIntDatum(1), types.NewIntDatum(1),
		types.NewBytesDatum(newTableInfoJSON), types.NewUintDatum(0),
	}).ToRow()
	rows := []chunk.Row{sameRow, sameRow, sameRow}

	err = r.unmarshalSchemaChanges(rows, changesReused)
	require.NoError(t, err)

	require.Equal(t, newTableInfo, changesReused[0].event.inner.TableInfo)
	require.Equal(t, newTableInfo, changesReused[1].event.inner.TableInfo)
	// The leftover will not be cleaned right after unmarshal. It will be cleaned be
	// GC later. Because we use type field to determine read which field, so the
	// leftover will not affect the result.
	require.NotNil(t, changesReused[1].event.inner.AddedPartInfo)
	require.Equal(t, newTableInfo, changesReused[2].event.inner.TableInfo)
}
