// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func testTableInfoWith2IndexOnFirstColumn(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	normalInfo, err := testTableInfo(d, name, num)
	require.NoError(t, err)
	idxs := make([]*model.IndexInfo, 0, 2)
	for i := range idxs {
		idx := &model.IndexInfo{
			Name:    model.NewCIStr(fmt.Sprintf("i%d", i+1)),
			State:   model.StatePublic,
			Columns: []*model.IndexColumn{{Name: model.NewCIStr("c1")}},
		}
		idxs = append(idxs, idx)
	}
	normalInfo.Indices = idxs
	normalInfo.Columns[0].FieldType.Flen = 11
	return normalInfo
}

// testTableInfo creates a test table with num int columns and with no index.
func testTableInfo(d *ddl, name string, num int) (*model.TableInfo, error) {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)

	if err != nil {
		return nil, err
	}
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	for i := range cols {
		col := &model.ColumnInfo{
			Name:         model.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset:       i,
			DefaultValue: i + 1,
			State:        model.StatePublic,
		}

		col.FieldType = *types.NewFieldType(mysql.TypeLong)
		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
	}
	tblInfo.Columns = cols
	tblInfo.Charset = "utf8"
	tblInfo.Collate = "utf8_bin"
	return tblInfo, nil
}

// testTableInfoWithPartition creates a test table with num int columns and with no index.
func testTableInfoWithPartition(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	tblInfo, err := testTableInfo(d, name, num)
	require.NoError(t, err)
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	pid := genIDs[0]
	tblInfo.Partition = &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tblInfo.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			ID:       pid,
			Name:     model.NewCIStr("p0"),
			LessThan: []string{"maxvalue"},
		}},
	}

	return tblInfo
}

// testTableInfoWithPartitionLessThan creates a test table with num int columns and one partition specified with lessthan.
func testTableInfoWithPartitionLessThan(t *testing.T, d *ddl, name string, num int, lessthan string) *model.TableInfo {
	tblInfo := testTableInfoWithPartition(t, d, name, num)
	tblInfo.Partition.Definitions[0].LessThan = []string{lessthan}
	return tblInfo
}

func testAddedNewTablePartitionInfo(t *testing.T, d *ddl, tblInfo *model.TableInfo, partName, lessthan string) *model.PartitionInfo {
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	pid := genIDs[0]
	// the new added partition should change the partition state to state none at the beginning.
	return &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tblInfo.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			ID:       pid,
			Name:     model.NewCIStr(partName),
			LessThan: []string{lessthan},
		}},
	}
}

// testViewInfo creates a test view with num int columns.
func testViewInfo(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	viewCols := make([]model.CIStr, num)

	var stmtBuffer bytes.Buffer
	stmtBuffer.WriteString("SELECT ")
	for i := range cols {
		col := &model.ColumnInfo{
			Name:   model.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset: i,
			State:  model.StatePublic,
		}

		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
		viewCols[i] = col.Name
		stmtBuffer.WriteString(cols[i].Name.L + ",")
	}
	stmtBuffer.WriteString("1 FROM t")

	view := model.ViewInfo{Cols: viewCols, Security: model.SecurityDefiner, Algorithm: model.AlgorithmMerge,
		SelectStmt: stmtBuffer.String(), CheckOption: model.CheckOptionCascaded, Definer: &auth.UserIdentity{CurrentUser: true}}

	tblInfo.View = &view
	tblInfo.Columns = cols

	return tblInfo
}

func testCreateTable(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testCreateView(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo, false, 0},
	}

	require.True(t, tblInfo.IsView())
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.doDDLJob(ctx, job))

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testDropTable(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.doDDLJob(ctx, job))

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCheckTableState(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		info, err := m.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.NoError(t, err)
			return nil
		}

		require.Equal(t, info.Name, tblInfo.Name)
		require.Equal(t, info.State, state)
		return nil
	}))
}

func testGetTable(t *testing.T, d *ddl, schemaID int64, tableID int64) table.Table {
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	require.NoError(t, err)
	return tbl
}

// for drop indexes
func createTestTableForDropIndexes(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, name string, num int) *model.TableInfo {
	tableInfo, err := testTableInfo(d, name, num)
	require.NoError(t, err)
	var idxs []*model.IndexInfo
	for i := 0; i < num; i++ {
		idxName := model.NewCIStr(fmt.Sprintf("i%d", i+1))
		idx := &model.IndexInfo{
			Name:    idxName,
			State:   model.StatePublic,
			Columns: []*model.IndexColumn{{Name: model.NewCIStr(fmt.Sprintf("c%d", i+1))}},
		}
		idxs = append(idxs, idx)
	}
	tableInfo.Indices = idxs
	testCreateTable(t, ctx, d, dbInfo, tableInfo)
	return tableInfo
}
