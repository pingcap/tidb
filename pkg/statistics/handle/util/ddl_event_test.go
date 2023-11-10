package util

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestEventString(t *testing.T) {
	// Create an Event object
	e := &Event{
		Tp: model.ActionAddColumn,
		tableInfo: &model.TableInfo{
			ID:   1,
			Name: model.NewCIStr("Table1"),
		},
		partInfo: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 2},
				{ID: 3},
			},
		},
		oldTableInfo: &model.TableInfo{
			ID:   4,
			Name: model.NewCIStr("Table2"),
		},
		oldPartInfo: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 5},
				{ID: 6},
			},
		},
		columnInfos: []*model.ColumnInfo{
			{ID: 7, Name: model.NewCIStr("Column1")},
			{ID: 8, Name: model.NewCIStr("Column2")},
		},
	}

	// Call the String method
	result := e.String()

	// Check the result
	expected := "(Event Type: add column, Table ID: 1, Table Name: Table1, " +
		"Partition IDs: [2 3], Old Table ID: 4, Old Table Name: Table2, " +
		"Old Partition IDs: [5 6], Column ID: 7, Column Name: Column1, " +
		"Column ID: 8, Column Name: Column2"
	require.Equal(t, expected, result)
}
