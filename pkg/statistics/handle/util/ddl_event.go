// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// DDLEvent contains the information of a ddl event that is used to update stats.
type DDLEvent struct {
	// For different ddl types, the following fields are used.
	// They have different meanings for different ddl types.
	// Please do **not** use these fields directly, use the corresponding
	// NewXXXEvent functions instead.
	tableInfo    *model.TableInfo
	partInfo     *model.PartitionInfo
	oldTableInfo *model.TableInfo
	oldPartInfo  *model.PartitionInfo
	columnInfos  []*model.ColumnInfo
	// schemaID is the ID of the schema that the table belongs to.
	// Used to filter out the system or memory tables.
	schemaID int64
	// This value is used to store the table ID during a transition.
	// It applies when a table structure is being changed from partitioned to non-partitioned, or vice versa.
	oldTableID int64
	tp         model.ActionType
}

// IsMemOrSysDB checks whether the table is in the memory or system database.
func (e *DDLEvent) IsMemOrSysDB(sctx sessionctx.Context) (bool, error) {
	intest.Assert(e.schemaID != 0, "schemaID should not be 0, please set it when creating the event")
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	schema, ok := is.SchemaByID(e.schemaID)
	if !ok {
		return false, fmt.Errorf("schema not found for table %s", e.tableInfo.Name)
	}
	return util.IsMemOrSysDB(schema.Name.L), nil
}

// NewCreateTableEvent creates a new ddl event that creates a table.
func NewCreateTableEvent(
	schemaID int64,
	newTableInfo *model.TableInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:        model.ActionCreateTable,
		schemaID:  schemaID,
		tableInfo: newTableInfo,
	}
}

// GetCreateTableInfo gets the table info of the table that is created.
func (e *DDLEvent) GetCreateTableInfo() (newTableInfo *model.TableInfo) {
	return e.tableInfo
}

// NewTruncateTableEvent creates a new ddl event that truncates a table.
func NewTruncateTableEvent(
	schemaID int64,
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:           model.ActionTruncateTable,
		schemaID:     schemaID,
		tableInfo:    newTableInfo,
		oldTableInfo: droppedTableInfo,
	}
}

// GetTruncateTableInfo gets the table info of the table that is truncated.
func (e *DDLEvent) GetTruncateTableInfo() (newTableInfo *model.TableInfo, droppedTableInfo *model.TableInfo) {
	return e.tableInfo, e.oldTableInfo
}

// NewDropTableEvent creates a new ddl event that drops a table.
func NewDropTableEvent(
	schemaID int64,
	droppedTableInfo *model.TableInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:           model.ActionDropTable,
		schemaID:     schemaID,
		oldTableInfo: droppedTableInfo,
	}
}

// GetDropTableInfo gets the table info of the table that is dropped.
func (e *DDLEvent) GetDropTableInfo() (newTableInfo *model.TableInfo) {
	return e.oldTableInfo
}

// NewAddColumnEvent creates a new ddl event that
// adds a column.
func NewAddColumnEvent(
	schemaID int64,
	newTableInfo *model.TableInfo,
	newColumnInfo []*model.ColumnInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionAddColumn,
		schemaID:    schemaID,
		tableInfo:   newTableInfo,
		columnInfos: newColumnInfo,
	}
}

// GetAddColumnInfo gets the table info of the table that is added a column.
func (e *DDLEvent) GetAddColumnInfo() (newTableInfo *model.TableInfo, newColumnInfo []*model.ColumnInfo) {
	return e.tableInfo, e.columnInfos
}

// NewModifyColumnEvent creates a new ddl event that
// modifies a column.
func NewModifyColumnEvent(
	schemaID int64,
	newTableInfo *model.TableInfo,
	modifiedColumnInfo []*model.ColumnInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionModifyColumn,
		schemaID:    schemaID,
		tableInfo:   newTableInfo,
		columnInfos: modifiedColumnInfo,
	}
}

// GetModifyColumnInfo gets the table info of the table that is modified a column.
func (e *DDLEvent) GetModifyColumnInfo() (newTableInfo *model.TableInfo, modifiedColumnInfo []*model.ColumnInfo) {
	return e.tableInfo, e.columnInfos
}

// NewAddPartitionEvent creates a new ddl event that adds partitions.
func NewAddPartitionEvent(
	schemaID int64,
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:        model.ActionAddTablePartition,
		schemaID:  schemaID,
		tableInfo: globalTableInfo,
		partInfo:  addedPartInfo,
	}
}

// GetAddPartitionInfo gets the table info of the table that is added partitions.
func (e *DDLEvent) GetAddPartitionInfo() (globalTableInfo *model.TableInfo, addedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.partInfo
}

// NewDropPartitionEvent creates a new ddl event that drops partitions.
func NewDropPartitionEvent(
	schemaID int64,
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionDropTablePartition,
		schemaID:    schemaID,
		tableInfo:   globalTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetDropPartitionInfo gets the table info of the table that is dropped partitions.
func (e *DDLEvent) GetDropPartitionInfo() (globalTableInfo *model.TableInfo, droppedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.oldPartInfo
}

// NewExchangePartitionEvent creates a new ddl event that exchanges a partition.
// Please make sure pass the information before the exchange.
func NewExchangePartitionEvent(
	schemaID int64,
	globalTableInfo *model.TableInfo,
	originalPartInfo *model.PartitionInfo,
	originalTableInfo *model.TableInfo,
) *DDLEvent {
	if len(originalPartInfo.Definitions) != 1 {
		allIDs := make([]int64, 0, len(originalPartInfo.Definitions))
		allNames := make([]string, 0, len(originalPartInfo.Definitions))
		for _, def := range originalPartInfo.Definitions {
			allIDs = append(allIDs, def.ID)
			allNames = append(allNames, def.Name.O)
		}
		logutil.StatsLogger().Error("Exchange partition should only have one partition to exchange",
			zap.Int64("globalTableID", globalTableInfo.ID),
			zap.String("globalTableName", globalTableInfo.Name.O),
			zap.Int64("tableID", originalTableInfo.ID),
			zap.String("tableName", originalTableInfo.Name.O),
			zap.Int64s("allPartitionIDs", allIDs),
			zap.Strings("allPartitionNames", allNames),
		)
	}
	return &DDLEvent{
		tp:           model.ActionExchangeTablePartition,
		schemaID:     schemaID,
		tableInfo:    globalTableInfo,
		partInfo:     originalPartInfo,
		oldTableInfo: originalTableInfo,
	}
}

// GetExchangePartitionInfo gets the table info of the table that is exchanged a partition.
// Note: All information pertains to the state before the exchange.
func (e *DDLEvent) GetExchangePartitionInfo() (
	globalTableInfo *model.TableInfo,
	originalPartInfo *model.PartitionInfo,
	originalTableInfo *model.TableInfo,
) {
	return e.tableInfo, e.partInfo, e.oldTableInfo
}

// NewReorganizePartitionEvent creates a new ddl event that reorganizes partitions.
// We also use it for increasing or decreasing the number of hash partitions.
func NewReorganizePartitionEvent(
	schemaID int64,
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionReorganizePartition,
		schemaID:    schemaID,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetReorganizePartitionInfo gets the table info of the table that is reorganized partitions.
func (e *DDLEvent) GetReorganizePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewTruncatePartitionEvent creates a new ddl event that truncates partitions.
func NewTruncatePartitionEvent(
	schemaID int64,
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionTruncateTablePartition,
		schemaID:    schemaID,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetTruncatePartitionInfo gets the table info of the table that is truncated partitions.
func (e *DDLEvent) GetTruncatePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewAddPartitioningEvent creates a new ddl event that converts a single table to a partitioned table.
// For example, `alter table t partition by range (c1) (partition p1 values less than (10))`.
func NewAddPartitioningEvent(
	schemaID int64,
	oldSingleTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:         model.ActionAlterTablePartitioning,
		schemaID:   schemaID,
		oldTableID: oldSingleTableID,
		tableInfo:  newGlobalTableInfo,
		partInfo:   addedPartInfo,
	}
}

// GetAddPartitioningInfo gets the table info of the table that is converted to a partitioned table.
func (e *DDLEvent) GetAddPartitioningInfo() (
	oldSingleTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	return e.oldTableID, e.tableInfo, e.partInfo
}

// NewRemovePartitioningEvent creates a new ddl event that converts a partitioned table to a single table.
// For example, `alter table t remove partitioning`.
func NewRemovePartitioningEvent(
	schemaID int64,
	oldPartitionedTableID int64,
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *DDLEvent {
	return &DDLEvent{
		tp:          model.ActionRemovePartitioning,
		schemaID:    schemaID,
		oldTableID:  oldPartitionedTableID,
		tableInfo:   newSingleTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetRemovePartitioningInfo gets the table info of the table that is converted to a single table.
func (e *DDLEvent) GetRemovePartitioningInfo() (
	oldPartitionedTableID int64,
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.oldTableID, e.tableInfo, e.oldPartInfo
}

// NewFlashbackClusterEvent creates a new ddl event that flashes back the cluster.
func NewFlashbackClusterEvent() *DDLEvent {
	return &DDLEvent{
		tp: model.ActionFlashbackCluster,
	}
}

// GetType returns the type of the ddl event.
func (e *DDLEvent) GetType() model.ActionType {
	return e.tp
}

// String implements fmt.Stringer interface.
func (e *DDLEvent) String() string {
	ret := fmt.Sprintf("(Event Type: %s", e.tp)
	if e.schemaID != 0 {
		ret += fmt.Sprintf(", Schema ID: %d", e.schemaID)
	}
	if e.tableInfo != nil {
		ret += fmt.Sprintf(", Table ID: %d, Table Name: %s", e.tableInfo.ID, e.tableInfo.Name)
	}
	if e.partInfo != nil {
		ids := make([]int64, 0, len(e.partInfo.Definitions))
		for _, def := range e.partInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Partition IDs: %v", ids)
	}
	if e.oldTableInfo != nil {
		ret += fmt.Sprintf(", Old Table ID: %d, Old Table Name: %s", e.oldTableInfo.ID, e.oldTableInfo.Name)
	}
	if e.oldPartInfo != nil {
		ids := make([]int64, 0, len(e.oldPartInfo.Definitions))
		for _, def := range e.oldPartInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Old Partition IDs: %v", ids)
	}
	for _, columnInfo := range e.columnInfos {
		ret += fmt.Sprintf(", Column ID: %d, Column Name: %s", columnInfo.ID, columnInfo.Name)
	}

	return ret
}
