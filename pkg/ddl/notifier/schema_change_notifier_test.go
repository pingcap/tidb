package notifier

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestEventString(t *testing.T) {
	// Create an Event object
	e := &SchemaChangeEvent{
		tp: model.ActionAddColumn,
		tableInfo: &model.TableInfo{
			ID:   1,
			Name: pmodel.NewCIStr("Table1"),
		},
		addedPartInfo: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 2},
				{ID: 3},
			},
		},
		oldTableInfo: &model.TableInfo{
			ID:   4,
			Name: pmodel.NewCIStr("Table2"),
		},
		droppedPartInfo: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 5},
				{ID: 6},
			},
		},
		columnInfos: []*model.ColumnInfo{
			{ID: 7, Name: pmodel.NewCIStr("Column1")},
			{ID: 8, Name: pmodel.NewCIStr("Column2")},
		},
	}

	// Call the String method
	result := e.String()

	// Check the result
	expected := "(Event Type: add column, Table ID: 1, Table Name: Table1, Old Table ID: 4, Old Table Name: Table2," +
		" Partition ID: 2, Partition ID: 3, Dropped Partition ID: 5, Dropped Partition ID: 6, " +
		"Column ID: 7, Column Name: Column1, Column ID: 8, Column Name: Column2)"
	require.Equal(t, expected, result)
}
