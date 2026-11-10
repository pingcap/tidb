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
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
