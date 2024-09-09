// Copyright 2024 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// SchemaChangeEvent stands for a schema change event. DDL will generate one
// event or multiple events (only for multi-schema change DDL). The caller should
// check the GetType of SchemaChange and call the corresponding getter function
// to retrieve the needed information.
type SchemaChangeEvent struct {
	// todo: field and method will be added in the next few pr on demand
	newTableInfo    *model.TableInfo
	oldTableInfo    *model.TableInfo
	addedPartInfo   *model.PartitionInfo
	deletedPartInfo *model.PartitionInfo
	columnInfos     []*model.ColumnInfo
	oldTableID4Part int64

	tp model.ActionType
}

// String implements fmt.Stringer interface.
func (s *SchemaChangeEvent) String() string {
	if s == nil {
		return "nil SchemaChangeEvent"
	}

	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "(Event Type: %s", s.tp)
	if s.newTableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Table ID: %d, Table Name: %s", s.newTableInfo.ID, s.newTableInfo.Name)
	}
	if s.oldTableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID: %d, Old Table Name: %s", s.oldTableInfo.ID, s.oldTableInfo.Name)
	}
	if s.oldTableID4Part != 0 {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID for Partition: %d", s.oldTableID4Part)
	}
	if s.addedPartInfo != nil {
		for _, partDef := range s.addedPartInfo.Definitions {
			_, _ = fmt.Fprintf(&sb, ", Partition Name: %s, Partition ID: %d", partDef.Name, partDef.ID)
		}
	}
	if s.deletedPartInfo != nil {
		for _, partDef := range s.deletedPartInfo.Definitions {
			_, _ = fmt.Fprintf(&sb, ", Deleted Partition Name: %s, Deleted Partition ID: %d", partDef.Name, partDef.ID)
		}
	}
	for _, columnInfo := range s.columnInfos {
		_, _ = fmt.Fprintf(&sb, ", Column ID: %d, Column Name: %s", columnInfo.ID, columnInfo.Name)
	}
	sb.WriteString(")")

	return sb.String()
}

// GetType returns the type of the schema change event.
func (s *SchemaChangeEvent) GetType() model.ActionType {
	if s == nil {
		return model.ActionNone
	}
	return s.tp
}

// NewCreateTableEvent creates a SchemaChangeEvent whose type is
// ActionCreateTable.
func NewCreateTableEvent(
	newTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionCreateTable,
		newTableInfo: newTableInfo,
	}
}

// GetCreateTableInfo returns the table info of the SchemaChangeEvent whose type
// is ActionCreateTable.
func (s *SchemaChangeEvent) GetCreateTableInfo() *model.TableInfo {
	intest.Assert(s.tp == model.ActionCreateTable)
	return s.newTableInfo
}

// NewTruncateTableEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTable.
func NewTruncateTableEvent(
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionTruncateTable,
		newTableInfo: newTableInfo,
		oldTableInfo: droppedTableInfo,
	}
}

// GetTruncateTableInfo returns the new and old table info of the
// SchemaChangeEvent whose type is ActionTruncateTable.
func (s *SchemaChangeEvent) GetTruncateTableInfo() (
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) {
	intest.Assert(s.tp == model.ActionTruncateTable)
	return s.newTableInfo, s.oldTableInfo
}

// NewDropTableEvent creates a SchemaChangeEvent whose type is ActionDropTable.
func NewDropTableEvent(
	droppedTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionDropTable,
		oldTableInfo: droppedTableInfo,
	}
}

// GetDropTableInfo returns the table info of the SchemaChangeEvent whose type is ActionDropTable.
func (s *SchemaChangeEvent) GetDropTableInfo() (newTableInfo *model.TableInfo) {
	intest.Assert(s.tp == model.ActionDropTable)
	return s.oldTableInfo
}

// NewAddColumnEvent creates a SchemaChangeEvent whose type is ActionAddColumn.
func NewAddColumnEvent(
	tableInfo *model.TableInfo,
	newColumnInfo []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionAddColumn,
		newTableInfo: tableInfo,
		columnInfos:  newColumnInfo,
	}
}

// GetAddColumnInfo returns the table info of the SchemaChangeEvent whose type is ActionAddColumn.
func (s *SchemaChangeEvent) GetAddColumnInfo() (newTableInfo *model.TableInfo, newColumnInfo []*model.ColumnInfo) {
	intest.Assert(s.tp == model.ActionAddColumn)
	return s.newTableInfo, s.columnInfos
}

// NewModifyColumnEvent creates a SchemaChangeEvent whose type is ActionModifyColumn.
func NewModifyColumnEvent(
	tableInfo *model.TableInfo,
	modifiedColumnInfo []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionModifyColumn,
		newTableInfo: tableInfo,
		columnInfos:  modifiedColumnInfo,
	}
}

// GetModifyColumnInfo returns the table info of and column info the SchemaChangeEvent whose type is ActionModifyColumn.
func (s *SchemaChangeEvent) GetModifyColumnInfo() (newTableInfo *model.TableInfo, modifiedColumnInfo []*model.ColumnInfo) {
	intest.Assert(s.tp == model.ActionModifyColumn)
	return s.newTableInfo, s.columnInfos
}

// NewAddPartitionEvent creates a SchemaChangeEvent whose type is
// ActionAddPartition.
// NewAddPartitionEvent creates a new ddl event that adds partitions.
func NewAddPartitionEvent(
	globalTableInfo *model.TableInfo,
	newPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:            model.ActionAddTablePartition,
		newTableInfo:  globalTableInfo,
		addedPartInfo: newPartInfo,
	}
}

// GetAddPartitionInfo returns the table info and partition info of the
// SchemaChangeEvent whose type is ActionAddPartition.
func (s *SchemaChangeEvent) GetAddPartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionAddTablePartition)
	return s.newTableInfo, s.addedPartInfo
}

// NewTruncatePartitionEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTablePartition.
func NewTruncatePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	deletedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionTruncateTablePartition,
		newTableInfo:    globalTableInfo,
		addedPartInfo:   addedPartInfo,
		deletedPartInfo: deletedPartInfo,
	}
}

// GetTruncatePartitionInfo returns the table info, added partition info and
// deleted partition info of the SchemaChangeEvent whose type is
// ActionTruncateTablePartition.
func (s *SchemaChangeEvent) GetTruncatePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	deletedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionTruncateTablePartition)
	return s.newTableInfo, s.addedPartInfo, s.deletedPartInfo
}

// NewDropPartitionEvent creates a SchemaChangeEvent whose type is
// ActionDropTablePartition.
func NewDropPartitionEvent(
	globalTableInfo *model.TableInfo,
	deletedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionDropTablePartition,
		newTableInfo:    globalTableInfo,
		deletedPartInfo: deletedPartInfo,
	}
}

// GetDropPartitionInfo returns the table info and partition info of the
// SchemaChangeEvent whose type is ActionDropTablePartition.
func (s *SchemaChangeEvent) GetDropPartitionInfo() (globalTableInfo *model.TableInfo, droppedPartInfo *model.PartitionInfo) {
	intest.Assert(s.tp == model.ActionDropTablePartition)
	return s.newTableInfo, s.deletedPartInfo
}

// NewExchangePartitionEvent creates a SchemaChangeEvent whose type is
// ActionExchangeTablePartition.
func NewExchangePartitionEvent(
	globalTableInfo *model.TableInfo,
	originalPartInfo *model.PartitionInfo,
	originalTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	if len(originalPartInfo.Definitions) != 1 {
		allIDs := make([]int64, 0, len(originalPartInfo.Definitions))
		allNames := make([]string, 0, len(originalPartInfo.Definitions))
		for _, def := range originalPartInfo.Definitions {
			allIDs = append(allIDs, def.ID)
			allNames = append(allNames, def.Name.O)
		}
		logutil.SampleLogger().Error("Exchange partition should only have one partition to exchange",
			zap.Int64("globalTableID", globalTableInfo.ID),
			zap.String("globalTableName", globalTableInfo.Name.O),
			zap.Int64("tableID", originalTableInfo.ID),
			zap.String("tableName", originalTableInfo.Name.O),
			zap.Int64s("allPartitionIDs", allIDs),
			zap.Strings("allPartitionNames", allNames),
		)
	}
	return &SchemaChangeEvent{
		tp:            model.ActionExchangeTablePartition,
		newTableInfo:  globalTableInfo,
		addedPartInfo: originalPartInfo,
		oldTableInfo:  originalTableInfo,
	}
}

// GetExchangePartitionInfo returns the table info, exchanged partition info and
// original table info of the SchemaChangeEvent whose type is
// ActionExchangeTablePartition.
func (s *SchemaChangeEvent) GetExchangePartitionInfo() (
	globalTableInfo *model.TableInfo,
	originalPartInfo *model.PartitionInfo,
	originalTableInfo *model.TableInfo,
) {
	intest.Assert(s.tp == model.ActionExchangeTablePartition)
	return s.newTableInfo, s.addedPartInfo, s.oldTableInfo
}

// NewReorganizePartitionEvent creates a SchemaChangeEvent whose type is
// ActionReorganizePartition.
func NewReorganizePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionReorganizePartition,
		newTableInfo:    globalTableInfo,
		addedPartInfo:   addedPartInfo,
		deletedPartInfo: droppedPartInfo,
	}
}

// GetReorganizePartitionInfo returns the global table info, added partition info
// and deleted partition info of the SchemaChangeEvent whose type is
// ActionReorganizePartition.
func (s *SchemaChangeEvent) GetReorganizePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionReorganizePartition)
	return s.newTableInfo, s.addedPartInfo, s.deletedPartInfo
}

// NewAddPartitioningEvent creates a SchemaChangeEvent whose type is
// ActionAlterTablePartitioning.
func NewAddPartitioningEvent(
	oldSingleTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionAlterTablePartitioning,
		oldTableID4Part: oldSingleTableID,
		newTableInfo:    newGlobalTableInfo,
		addedPartInfo:   addedPartInfo,
	}
}

// GetAddPartitioningInfo returns the old single table ID, new global table info
// and added partition info of the SchemaChangeEvent whose type is
// ActionAlterTablePartitioning.
func (s *SchemaChangeEvent) GetAddPartitioningInfo() (
	oldSingleTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionAlterTablePartitioning)
	return s.oldTableID4Part, s.newTableInfo, s.addedPartInfo
}
