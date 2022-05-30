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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDropAndTruncatePartition(t *testing.T) {
	store, domain, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

	d := domain.DDL()
	dbInfo, err := testSchemaInfo(store, "test_partition")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), d, dbInfo)
	// generate 5 partition in tableInfo.
	tblInfo, partIDs := buildTableInfoWithPartition(t, store)
	ctx := testkit.NewTestKit(t, store).Session()
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	testDropPartition(t, ctx, d, dbInfo, tblInfo, []string{"p0", "p1"})
	testTruncatePartition(t, ctx, d, dbInfo, tblInfo, []int64{partIDs[3], partIDs[4]})
}

func buildTableInfoWithPartition(t *testing.T, store kv.Storage) (*model.TableInfo, []int64) {
	tbl := &model.TableInfo{
		Name: model.NewCIStr("t"),
	}
	tbl.MaxColumnID++
	col := &model.ColumnInfo{
		Name:      model.NewCIStr("c"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        tbl.MaxColumnID,
	}
	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	tbl.ID = genIDs[0]
	tbl.Columns = []*model.ColumnInfo{col}
	tbl.Charset = "utf8"
	tbl.Collate = "utf8_bin"

	partIDs, err := genGlobalIDs(store, 5)
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
		SchemaID:    dbInfo.ID,
		TableID:     tblInfo.ID,
		SchemaState: model.StatePublic,
		Type:        model.ActionDropTablePartition,
		BinlogInfo:  &model.HistoryInfo{},
		Args:        []interface{}{partNames},
	}
}

func testDropPartition(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	job := buildDropPartitionJob(dbInfo, tblInfo, partNames)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
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

func testTruncatePartition(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64) *model.Job {
	job := buildTruncatePartitionJob(dbInfo, tblInfo, pids)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
