// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDropAndTruncatePartition(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Stop())
	}()
	dbInfo, err := testSchemaInfo(d, "test_partition")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)
	// generate 5 partition in tableInfo.
	tblInfo, partIDs := buildTableInfoWithPartition(t, d)
	ctx := testNewContext(d)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	testDropPartition(t, ctx, d, dbInfo, tblInfo, []string{"p0", "p1"})
	testTruncatePartition(t, ctx, d, dbInfo, tblInfo, []int64{partIDs[3], partIDs[4]})
}

func buildTableInfoWithPartition(t *testing.T, d *ddl) (*model.TableInfo, []int64) {
	tbl := &model.TableInfo{
		Name: model.NewCIStr("t"),
	}
	col := &model.ColumnInfo{
		Name:      model.NewCIStr("c"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        allocateColumnID(tbl),
	}
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	tbl.ID = genIDs[0]
	tbl.Columns = []*model.ColumnInfo{col}
	tbl.Charset = "utf8"
	tbl.Collate = "utf8_bin"

	partIDs, err := d.genGlobalIDs(5)
	require.NoError(t, err)
	partInfo := &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tbl.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{
			{
				ID:       partIDs[0],
				Name:     model.NewCIStr("p0"),
				LessThan: []string{"100"},
			},
			{
				ID:       partIDs[1],
				Name:     model.NewCIStr("p1"),
				LessThan: []string{"200"},
			},
			{
				ID:       partIDs[2],
				Name:     model.NewCIStr("p2"),
				LessThan: []string{"300"},
			},
			{
				ID:       partIDs[3],
				Name:     model.NewCIStr("p3"),
				LessThan: []string{"400"},
			},
			{
				ID:       partIDs[4],
				Name:     model.NewCIStr("p4"),
				LessThan: []string{"500"},
			},
		},
	}
	tbl.Partition = partInfo
	return tbl, partIDs
}

func buildDropPartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partNames},
	}
}

func testDropPartition(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	job := buildDropPartitionJob(dbInfo, tblInfo, partNames)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildTruncatePartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionTruncateTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{pids},
	}
}

func testTruncatePartition(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64) *model.Job {
	job := buildTruncatePartitionJob(dbInfo, tblInfo, pids)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
