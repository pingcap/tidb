package testutil

import (
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
)

// NewMockStatisticsTable creates a mock statistics table with given columns and indices.
// each column and index consumes 4 bytes memory
func NewMockStatisticsTable(columns int, indices int, withCMS, withTopN, withHist bool) *statistics.Table {
	t := &statistics.Table{}
	t.Columns = make(map[int64]*statistics.Column)
	t.Indices = make(map[int64]*statistics.Index)
	for i := 1; i <= columns; i++ {
		t.Columns[int64(i)] = &statistics.Column{
			Info:              &model.ColumnInfo{ID: int64(i)},
			StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		}
		if withCMS {
			t.Columns[int64(i)].CMSketch = statistics.NewCMSketch(1, 1)
		}
		if withTopN {
			t.Columns[int64(i)].TopN = statistics.NewTopN(1)
			t.Columns[int64(i)].TopN.AppendTopN([]byte{}, 1)
		}
		if withHist {
			t.Columns[int64(i)].Histogram = *statistics.NewHistogram(0, 10, 0, 0, types.NewFieldType(mysql.TypeBlob), 1, 0)
		}
	}
	for i := 1; i <= indices; i++ {
		t.Indices[int64(i)] = &statistics.Index{
			Info:              &model.IndexInfo{ID: int64(i)},
			StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		}
		if withCMS {
			t.Indices[int64(i)].CMSketch = statistics.NewCMSketch(1, 1)
		}
		if withTopN {
			t.Indices[int64(i)].TopN = statistics.NewTopN(1)
			t.Indices[int64(i)].TopN.AppendTopN([]byte{}, 1)
		}
		if withHist {
			t.Indices[int64(i)].Histogram = *statistics.NewHistogram(0, 10, 0, 0, types.NewFieldType(mysql.TypeBlob), 1, 0)
		}
	}
	return t
}

// MockTableAppendColumn appends a column to the table.
func MockTableAppendColumn(t *statistics.Table) {
	index := int64(len(t.Columns) + 1)
	t.Columns[index] = &statistics.Column{
		Info:     &model.ColumnInfo{ID: index},
		CMSketch: statistics.NewCMSketch(1, 1),
	}
}

// MockTableAppendIndex appends an index to the table.
func MockTableAppendIndex(t *statistics.Table) {
	index := int64(len(t.Indices) + 1)
	t.Indices[index] = &statistics.Index{
		Info:     &model.IndexInfo{ID: index},
		CMSketch: statistics.NewCMSketch(1, 1),
	}
}

// MockTableRemoveColumn removes the last column of the table.
func MockTableRemoveColumn(t *statistics.Table) {
	delete(t.Columns, int64(len(t.Columns)))
}

// MockTableRemoveIndex removes the last index of the table.
func MockTableRemoveIndex(t *statistics.Table) {
	delete(t.Indices, int64(len(t.Indices)))
}
