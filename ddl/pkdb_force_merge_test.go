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
	"fmt"
	"testing"

	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestGetForceMergeRangesForGCDeleteRangeUsesDeleteRangeKeys(t *testing.T) {
	setDropTableForceMergeEnabledForTest(t, true)

	dr := ddlutil.DelRangeTask{
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}
	historyJob := newForceMergeGCJob(
		model.ActionDropTable,
		101,
		"test",
		"t",
		newForceMergeGCTableInfo(101, "t"),
	)

	ranges := GetForceMergeRangesForGCDeleteRange(historyJob, dr, nil)
	require.Equal(t, []infosync.ForceMergeKeyRange{{
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}}, ranges)
}

func TestGetForceMergeRangesForGCDeleteRangeSkipsSystemTable(t *testing.T) {
	setDropTableForceMergeEnabledForTest(t, true)

	ranges := GetForceMergeRangesForGCDeleteRange(
		newForceMergeGCJob(
			model.ActionDropTable,
			1,
			"mysql",
			"tidb",
			newForceMergeGCTableInfo(1, "tidb"),
		),
		ddlutil.DelRangeTask{StartKey: []byte("start"), EndKey: []byte("end")},
		nil,
	)
	require.Nil(t, ranges)
}

func TestGetForceMergeRangesForGCDeleteRangeSkipsTemporaryTable(t *testing.T) {
	setDropTableForceMergeEnabledForTest(t, true)

	tblInfo := newForceMergeGCTableInfo(102, "tmp")
	tblInfo.TempTableType = model.TempTableGlobal

	ranges := GetForceMergeRangesForGCDeleteRange(
		newForceMergeGCJob(model.ActionDropTable, 102, "test", "tmp", tblInfo),
		ddlutil.DelRangeTask{StartKey: []byte("start"), EndKey: []byte("end")},
		nil,
	)
	require.Nil(t, ranges)
}

func TestGetForceMergeRangesForGCDeleteRangeAddsLogicalRangeOnceForPartitionedTable(t *testing.T) {
	setDropTableForceMergeEnabledForTest(t, true)

	historyJob := newForceMergeGCJob(
		model.ActionTruncateTable,
		200,
		"test",
		"pt",
		newForceMergeGCTableInfo(200, "pt", 201, 202),
	)
	gcLogicalTableCache := make(map[int64]struct{})

	firstRanges := GetForceMergeRangesForGCDeleteRange(historyJob, ddlutil.DelRangeTask{
		StartKey: []byte("p1-start"),
		EndKey:   []byte("p1-end"),
	}, gcLogicalTableCache)
	secondRanges := GetForceMergeRangesForGCDeleteRange(historyJob, ddlutil.DelRangeTask{
		StartKey: []byte("p2-start"),
		EndKey:   []byte("p2-end"),
	}, gcLogicalTableCache)

	require.Equal(t, []infosync.ForceMergeKeyRange{
		{
			StartKey: []byte("p1-start"),
			EndKey:   []byte("p1-end"),
		},
		{
			StartKey: tablecodec.EncodeTablePrefix(200),
			EndKey:   tablecodec.EncodeTablePrefix(201),
		},
	}, firstRanges)
	require.Equal(t, []infosync.ForceMergeKeyRange{{
		StartKey: []byte("p2-start"),
		EndKey:   []byte("p2-end"),
	}}, secondRanges)
}

func TestGetForceMergeRangesForGCDeleteRangeSkipsLogicalRangeForPartitionDDL(t *testing.T) {
	setDropTableForceMergeEnabledForTest(t, true)

	historyJob := newForceMergeGCJob(
		model.ActionDropTablePartition,
		300,
		"test",
		"pt",
		newForceMergeGCTableInfo(300, "pt", 301),
	)

	ranges := GetForceMergeRangesForGCDeleteRange(historyJob, ddlutil.DelRangeTask{
		StartKey: []byte("p-start"),
		EndKey:   []byte("p-end"),
	}, make(map[int64]struct{}))
	require.Equal(t, []infosync.ForceMergeKeyRange{{
		StartKey: []byte("p-start"),
		EndKey:   []byte("p-end"),
	}}, ranges)
}

func TestGetMergeEmptyRegionsKeyRanges(t *testing.T) {
	is := newMergeEmptyRegionsInfoSchema(t,
		newMergeEmptyRegionsDBInfo(1, "test",
			newMergeEmptyRegionsTableInfo(2, "t2"),
			newMergeEmptyRegionsTableInfo(5, "t5"),
			newMergeEmptyRegionsTableInfo(8, "t8"),
		),
	)

	maxTableID, ranges := GetMergeEmptyRegionsKeyRanges(is, 2)
	require.Equal(t, int64(8), maxTableID)
	require.Equal(t, []infosync.ForceMergeKeyRange{
		buildMergeEmptyRegionsTableIDRange(3, 4),
		buildMergeEmptyRegionsTableIDRange(6, 7),
	}, ranges)
}

func TestGetMergeEmptyRegionsKeyRangesKeepsPartitionedTableGlobalIndexKeyspace(t *testing.T) {
	pt := newMergeEmptyRegionsTableInfo(12, "pt", 13, 15)
	addMergeEmptyRegionsGlobalIndex(pt)

	is := newMergeEmptyRegionsInfoSchema(t,
		newMergeEmptyRegionsDBInfo(1, "test", pt),
	)

	maxTableID, ranges := GetMergeEmptyRegionsKeyRanges(is, 10)
	require.Equal(t, int64(15), maxTableID)
	require.Equal(t, []infosync.ForceMergeKeyRange{
		buildMergeEmptyRegionsTableIDRange(10, 11),
		buildMergeEmptyRegionsTableIDRange(14, 14),
	}, ranges)
}

func TestGetMergeEmptyRegionsKeyRangesSkipsLiveSystemAndTemporaryTablesFromMaxTableID(t *testing.T) {
	tempTbl := newMergeEmptyRegionsTableInfo(7, "tmp")
	tempTbl.TempTableType = model.TempTableGlobal

	is := newMergeEmptyRegionsInfoSchema(t,
		newMergeEmptyRegionsDBInfo(1, "test",
			newMergeEmptyRegionsTableInfo(4, "t4"),
			tempTbl,
			newMergeEmptyRegionsTableInfo(10, "t10"),
		),
		newMergeEmptyRegionsDBInfo(2, "mysql",
			newMergeEmptyRegionsTableInfo(8, "user"),
		),
	)

	maxTableID, ranges := GetMergeEmptyRegionsKeyRanges(is, 1)
	require.Equal(t, int64(10), maxTableID)
	require.Equal(t, []infosync.ForceMergeKeyRange{
		buildMergeEmptyRegionsTableIDRange(1, 3),
		buildMergeEmptyRegionsTableIDRange(5, 6),
		buildMergeEmptyRegionsTableIDRange(9, 9),
	}, ranges)
}

func setDropTableForceMergeEnabledForTest(t *testing.T, enabled bool) {
	t.Helper()

	oldValue := variable.EnableDropTableForceMerge.Load()
	variable.EnableDropTableForceMerge.Store(enabled)
	t.Cleanup(func() {
		variable.EnableDropTableForceMerge.Store(oldValue)
	})
}

func newForceMergeGCJob(
	actionType model.ActionType,
	tableID int64,
	schemaName string,
	tableName string,
	tblInfo *model.TableInfo,
) *model.Job {
	return &model.Job{
		Type:       actionType,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		BinlogInfo: &model.HistoryInfo{
			TableInfo: tblInfo,
		},
	}
}

func newForceMergeGCTableInfo(tableID int64, tableName string, partitionIDs ...int64) *model.TableInfo {
	tblInfo := &model.TableInfo{
		ID:    tableID,
		Name:  model.NewCIStr(tableName),
		State: model.StatePublic,
	}
	if len(partitionIDs) == 0 {
		return tblInfo
	}

	defs := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for idx, partitionID := range partitionIDs {
		lessThan := []string{fmt.Sprintf("%d", idx+1)}
		if idx == len(partitionIDs)-1 {
			lessThan = []string{"MAXVALUE"}
		}
		defs = append(defs, model.PartitionDefinition{
			ID:       partitionID,
			Name:     model.NewCIStr(fmt.Sprintf("p%d", idx)),
			LessThan: lessThan,
		})
	}
	tblInfo.Partition = &model.PartitionInfo{
		Type:        model.PartitionTypeRange,
		Expr:        "a",
		Enable:      true,
		Definitions: defs,
	}
	return tblInfo
}

type mergeEmptyRegionsInfoSchema struct {
	infoschema.InfoSchema
	dbInfos      []*model.DBInfo
	schemaTables map[string][]table.Table
}

func (is *mergeEmptyRegionsInfoSchema) AllSchemas() []*model.DBInfo {
	return is.dbInfos
}

func (is *mergeEmptyRegionsInfoSchema) SchemaTables(schema model.CIStr) []table.Table {
	return is.schemaTables[schema.L]
}

func newMergeEmptyRegionsInfoSchema(t *testing.T, dbInfos ...*model.DBInfo) infoschema.InfoSchema {
	t.Helper()

	schemaTables := make(map[string][]table.Table, len(dbInfos))
	for _, dbInfo := range dbInfos {
		tbls := make([]table.Table, 0, len(dbInfo.Tables))
		for _, tblInfo := range dbInfo.Tables {
			tbl := tables.MockTableFromMeta(tblInfo)
			require.NotNil(t, tbl)
			tbls = append(tbls, tbl)
		}
		schemaTables[dbInfo.Name.L] = tbls
	}
	return &mergeEmptyRegionsInfoSchema{
		InfoSchema:   infoschema.MockInfoSchema(nil),
		dbInfos:      dbInfos,
		schemaTables: schemaTables,
	}
}

func newMergeEmptyRegionsDBInfo(schemaID int64, schemaName string, tblInfos ...*model.TableInfo) *model.DBInfo {
	return &model.DBInfo{
		ID:     schemaID,
		Name:   model.NewCIStr(schemaName),
		State:  model.StatePublic,
		Tables: tblInfos,
	}
}

func newMergeEmptyRegionsTableInfo(tableID int64, tableName string, partitionIDs ...int64) *model.TableInfo {
	tblInfo := newForceMergeGCTableInfo(tableID, tableName, partitionIDs...)
	tblInfo.Columns = []*model.ColumnInfo{{
		ID:        1,
		Name:      model.NewCIStr("a"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}}
	if tblInfo.Partition != nil {
		tblInfo.Partition.Enable = true
		tblInfo.Partition.Type = model.PartitionTypeRange
	}
	return tblInfo
}

func addMergeEmptyRegionsGlobalIndex(tblInfo *model.TableInfo) {
	tblInfo.Indices = []*model.IndexInfo{{
		ID:     1,
		Name:   model.NewCIStr("idx"),
		State:  model.StatePublic,
		Global: true,
		Columns: []*model.IndexColumn{{
			Name:   tblInfo.Columns[0].Name,
			Offset: 0,
			Length: types.UnspecifiedLength,
		}},
	}}
}
