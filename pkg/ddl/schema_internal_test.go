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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
)

func TestDropPartitionPropagatesMetadataUpdateError(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr", "return(1)")

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		require.NoError(t, metaMut.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
		definitions := []model.PartitionDefinition{
			{ID: 3, Name: ast.NewCIStr("p0"), LessThan: []string{"10"}},
			{ID: 4, Name: ast.NewCIStr("p1"), LessThan: []string{"20"}},
		}
		tblInfo := &model.TableInfo{
			ID: 2, Name: ast.NewCIStr("t"), State: model.StatePublic,
			Partition: &model.PartitionInfo{
				Type: ast.PartitionTypeRange, Enable: true, Num: 2,
				Definitions: definitions, DroppingDefinitions: definitions[:1],
				DDLState: model.StateWriteOnly, DDLAction: model.ActionDropTablePartition,
			},
		}
		require.NoError(t, metaMut.CreateTableOrView(1, tblInfo))
		job := &model.Job{
			Version: model.GetJobVerInUse(), Type: model.ActionDropTablePartition,
			SchemaID: 1, SchemaName: "test", TableID: 2, TableName: "t",
			State: model.JobStateRunning, SchemaState: model.StateWriteOnly,
		}
		job.FillArgs(&model.TablePartitionArgs{PartNames: []string{"p0"}})
		_, err := job.Encode(true)
		require.NoError(t, err)
		jobCtx := &jobContext{metaMut: metaMut, store: store, stepCtx: ctx}

		_, err = (&worker{}).onDropTablePartition(jobCtx, job)
		require.ErrorContains(t, err, "mock update version and tableInfo error")
		return nil
	})
	require.NoError(t, err)
}

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
	})

	t.Run("missing original ID", func(t *testing.T) {
		current := []model.PartitionDefinition{{ID: 1}}
		dropping := []model.PartitionDefinition{{ID: 3}}

		restored, err := restoreOriginalPartitionDefinitions([]int64{1, 2, 3}, current, dropping)

		require.ErrorContains(t, err, "missing partition ID 2")
		require.Nil(t, restored)
	})
}

func TestClonePartitionDefinitions(t *testing.T) {
	t.Run("RANGE", func(t *testing.T) {
		definitions := []model.PartitionDefinition{{ID: 1, LessThan: []string{"10"}}}
		cloned := clonePartitionDefinitions(definitions)
		cloned[0].ID = 2
		cloned[0].LessThan[0] = "20"
		require.Equal(t, []model.PartitionDefinition{{ID: 1, LessThan: []string{"10"}}}, definitions)
	})
	t.Run("LIST", func(t *testing.T) {
		definitions := []model.PartitionDefinition{{ID: 1, InValues: [][]string{{"1", "2"}, {"3", "4"}}}}
		cloned := clonePartitionDefinitions(definitions)
		cloned[0].InValues[0][0] = "changed"
		cloned[0].InValues[1] = []string{"replaced"}
		require.Equal(t, []model.PartitionDefinition{{ID: 1, InValues: [][]string{{"1", "2"}, {"3", "4"}}}}, definitions)
	})
}

func TestRemovePartitionIndexes(t *testing.T) {
	columns := []*model.ColumnInfo{
		{ID: 1, Name: ast.NewCIStr("a"), Offset: 0},
		{ID: 2, Name: ast.NewCIStr("b"), Offset: 1},
		{ID: 3, Name: ast.NewCIStr("_shared"), Offset: 2, Hidden: true},
		{ID: 4, Name: ast.NewCIStr("_private"), Offset: 3, Hidden: true},
		{ID: 5, Name: ast.NewCIStr("tail"), Offset: 4},
	}
	index := func(id int64, name string, column *model.ColumnInfo, global, unique bool) *model.IndexInfo {
		return &model.IndexInfo{
			ID: id, Name: ast.NewCIStr(name), State: model.StatePublic,
			Global: global, Unique: unique,
			Columns: []*model.IndexColumn{{Name: column.Name, Offset: column.Offset}},
		}
	}
	oldGlobal := index(1, "idx_a", columns[0], true, false)
	newGlobal := index(2, "idx_a", columns[0], true, false)
	localUnique := index(3, "idx_b", columns[1], false, true)
	privateExpression := index(4, "idx_private", columns[3], false, false)
	oldExpression := index(5, "idx_shared", columns[2], true, false)
	newExpression := index(6, "idx_shared", columns[2], true, false)
	unrelated := index(7, "idx_tail", columns[4], false, false)
	unrelated.State = model.StateDeleteOnly
	tblInfo := &model.TableInfo{
		ID: 100, Columns: append([]*model.ColumnInfo(nil), columns...),
		Indices: []*model.IndexInfo{oldGlobal, newGlobal, localUnique, privateExpression, oldExpression, newExpression, unrelated},
	}
	for _, idx := range tblInfo.Indices {
		AddIndexColumnFlag(tblInfo, idx)
	}

	cleanup := removePartitionIndexes(tblInfo, []*model.IndexInfo{oldGlobal, localUnique, privateExpression, oldExpression})
	// A partitioning change may replace the table ID after collecting cleanup IDs.
	tblInfo.ID = 200

	require.Equal(t, []model.TableIDIndexID{{TableID: 100, IndexID: 1}, {TableID: 100, IndexID: 5}}, cleanup)
	require.Equal(t, []*model.IndexInfo{newGlobal, newExpression, unrelated}, tblInfo.Indices)
	require.Equal(t, model.StateDeleteOnly, unrelated.State)
	require.Equal(t, []*model.ColumnInfo{columns[0], columns[1], columns[2], columns[4]}, tblInfo.Columns)
	require.True(t, mysql.HasMultipleKeyFlag(columns[0].GetFlag()))
	require.False(t, mysql.HasUniKeyFlag(columns[1].GetFlag()))
	require.True(t, mysql.HasMultipleKeyFlag(columns[2].GetFlag()))
	require.Equal(t, 3, columns[4].Offset)
	require.Equal(t, 3, unrelated.Columns[0].Offset)
	for offset, col := range tblInfo.Columns {
		require.Equal(t, offset, col.Offset)
	}
}

func TestDropSchemaEventBatchEnd(t *testing.T) {
	// Tables are named by their number of partitions.
	newTable := func(partitionCnt int) *notifier.MiniTableInfoForSchemaEvent {
		tbl := &notifier.MiniTableInfoForSchemaEvent{ID: 1, Name: ast.NewCIStr("t")}
		for range partitionCnt {
			tbl.Partitions = append(tbl.Partitions, &notifier.MiniPartitionInfoForSchemaEvent{ID: 1, Name: ast.NewCIStr("p")})
		}
		return tbl
	}
	sizeOf := func(partitionCnt int) int {
		data, err := json.Marshal(newTable(partitionCnt))
		require.NoError(t, err)
		return len(data)
	}
	batchSizes := func(maxTables, maxSize int, partitionCounts ...int) []int {
		tables := make([]*notifier.MiniTableInfoForSchemaEvent, 0, len(partitionCounts))
		for _, cnt := range partitionCounts {
			tables = append(tables, newTable(cnt))
		}
		var sizes []int
		for start := 0; start < len(tables); {
			end, err := dropSchemaEventBatchEnd(tables, start, maxTables, maxSize)
			require.NoError(t, err)
			require.Greater(t, end, start)
			sizes = append(sizes, end-start)
			start = end
		}
		return sizes
	}

	require.Nil(t, batchSizes(100, 1000))
	// Only the number of tables limits the batches of small tables.
	require.Equal(t, []int{3, 3, 1}, batchSizes(3, 100*sizeOf(0), 0, 0, 0, 0, 0, 0, 0))
	// The partitions add to the size of a table.
	maxSize := sizeOf(4) + sizeOf(5)
	require.Equal(t, []int{2, 1, 2}, batchSizes(100, maxSize, 4, 5, 9, 3, 5))
	require.Equal(t, []int{1, 1}, batchSizes(100, maxSize, 5, 5))
	// A table exceeding the limit on its own still gets a batch.
	require.Equal(t, []int{1, 1, 1, 2}, batchSizes(100, maxSize, 0, 20, 30, 0, 0))
}

// TestDropSchemaEventSize checks that the drop schema events stay small also for tables with
// the maximum number of partitions and the longest names, whatever characters the names use.
func TestDropSchemaEventSize(t *testing.T) {
	// Tables per schema event, larger than the number of tables below.
	const tablesPerEvent = 500
	dbInfo := &model.DBInfo{ID: 1, Name: ast.NewCIStr(strings.Repeat("d", mysql.MaxDatabaseNameLength))}
	// Characters of 1 and 4 bytes, and one that is escaped when encoded.
	for _, char := range []string{"P", "\U0001F600", "<"} {
		var tables []*notifier.MiniTableInfoForSchemaEvent
		for i, partitionCnt := range []int{mysql.PartitionCountLimit, mysql.PartitionCountLimit, 100, 100, 100, mysql.PartitionCountLimit / 4, 0, 0} {
			tblInfo := &model.TableInfo{
				ID:        int64(1) << 40,
				Name:      ast.NewCIStr(strings.Repeat(char, mysql.MaxTableNameLength)),
				Partition: &model.PartitionInfo{},
			}
			for j := range partitionCnt {
				prefix := fmt.Sprintf("P%d_%d_", i, j)
				// Partition names have the same length limit as table names.
				name := prefix + strings.Repeat(char, mysql.MaxTableNameLength-len(prefix))
				tblInfo.Partition.Definitions = append(tblInfo.Partition.Definitions, model.PartitionDefinition{
					ID:   int64(1)<<40 + int64(j),
					Name: ast.NewCIStr(name),
				})
			}
			tables = append(tables, notifier.NewMiniTableInfoForSchemaEvent(tblInfo))
		}

		var batches []int
		for start := 0; start < len(tables); {
			end, err := dropSchemaEventBatchEnd(tables, start, tablesPerEvent, maxDropSchemaEventSize)
			require.NoError(t, err)
			require.Greater(t, end, start)
			if end-start > 1 {
				data, err := json.Marshal(notifier.NewDropSchemaEvent(dbInfo, tables[start:end]))
				require.NoError(t, err)
				// The event has a little more than the tables, like the schema name.
				require.Less(t, len(data), maxDropSchemaEventSize+4096, "char %q", char)
			}
			batches = append(batches, end-start)
			start = end
		}
		// The two tables with the maximum number of partitions do not fit in one event,
		// while the smaller tables share events.
		require.Greater(t, len(batches), 1, "char %q", char)
		require.Less(t, len(batches), len(tables), "char %q", char)
	}
	require.Less(t, maxDropSchemaEventSize, config.DefTxnEntrySizeLimit/2)
}
