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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// SchemaChangeEvent stands for a schema change event. DDL will generate one
// event or multiple events (only for multi-schema change DDL). The caller should
// check the GetType of SchemaChange and call the corresponding getter function
// to retrieve the needed information.
type SchemaChangeEvent struct {
	// todo: field and method will be added in the next few pr on demand
	tableInfo       *model.TableInfo
	oldTableInfo    *model.TableInfo
	addedPartInfo   *model.PartitionInfo
	droppedPartInfo *model.PartitionInfo
	columnInfos     []*model.ColumnInfo
	// nonPartTableID is used to store the non-partitioned table that is converted to
	// a partitioned table in NewAddPartitioningEvent.
	nonPartTableID int64

	tp model.ActionType
}

// String implements fmt.Stringer interface.
func (s *SchemaChangeEvent) String() string {
	if s == nil {
		return "nil SchemaChangeEvent"
	}

	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "(Event Type: %s", s.tp)
	if s.tableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Table ID: %d, Table Name: %s", s.tableInfo.ID, s.tableInfo.Name)
	}
	if s.oldTableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID: %d, Old Table Name: %s", s.oldTableInfo.ID, s.oldTableInfo.Name)
	}
	if s.nonPartTableID != 0 {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID for Partition: %d", s.nonPartTableID)
	}
	if s.addedPartInfo != nil {
		for _, partDef := range s.addedPartInfo.Definitions {
			_, _ = fmt.Fprintf(&sb, ", Partition Name: %s, Partition ID: %d", partDef.Name, partDef.ID)
		}
	}
	if s.droppedPartInfo != nil {
		for _, partDef := range s.droppedPartInfo.Definitions {
			_, _ = fmt.Fprintf(&sb, ", Dropped Partition Name: %s, Dropped Partition ID: %d", partDef.Name, partDef.ID)
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
		tp:        model.ActionCreateTable,
		tableInfo: newTableInfo,
	}
}

// GetCreateTableInfo returns the table info of the SchemaChangeEvent whose type
// is ActionCreateTable.
func (s *SchemaChangeEvent) GetCreateTableInfo() *model.TableInfo {
	intest.Assert(s.tp == model.ActionCreateTable)
	return s.tableInfo
}

// NewTruncateTableEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTable.
func NewTruncateTableEvent(
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:           model.ActionTruncateTable,
		tableInfo:    newTableInfo,
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
	return s.tableInfo, s.oldTableInfo
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

// GetDropTableInfo returns the table info of the SchemaChangeEvent whose type is
// ActionDropTable.
func (s *SchemaChangeEvent) GetDropTableInfo() (droppedTableInfo *model.TableInfo) {
	intest.Assert(s.tp == model.ActionDropTable)
	return s.oldTableInfo
}

// NewAddColumnEvent creates a SchemaChangeEvent whose type is ActionAddColumn.
func NewAddColumnEvent(
	tableInfo *model.TableInfo,
	newColumnInfos []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:          model.ActionAddColumn,
		tableInfo:   tableInfo,
		columnInfos: newColumnInfos,
	}
}

// GetAddColumnInfo returns the table info of the SchemaChangeEvent whose type is
// ActionAddColumn.
func (s *SchemaChangeEvent) GetAddColumnInfo() (
	tableInfo *model.TableInfo,
	columnInfos []*model.ColumnInfo,
) {
	intest.Assert(s.tp == model.ActionAddColumn)
	return s.tableInfo, s.columnInfos
}

// NewModifyColumnEvent creates a SchemaChangeEvent whose type is
// ActionModifyColumn.
func NewModifyColumnEvent(
	tableInfo *model.TableInfo,
	modifiedColumnInfo []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:          model.ActionModifyColumn,
		tableInfo:   tableInfo,
		columnInfos: modifiedColumnInfo,
	}
}

// GetModifyColumnInfo returns the table info and modified column info the
// SchemaChangeEvent whose type is ActionModifyColumn.
func (s *SchemaChangeEvent) GetModifyColumnInfo() (
	newTableInfo *model.TableInfo,
	modifiedColumnInfo []*model.ColumnInfo,
) {
	intest.Assert(s.tp == model.ActionModifyColumn)
	return s.tableInfo, s.columnInfos
}

// NewAddPartitionEvent creates a SchemaChangeEvent whose type is
// ActionAddPartition.
func NewAddPartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:            model.ActionAddTablePartition,
		tableInfo:     globalTableInfo,
		addedPartInfo: addedPartInfo,
	}
}

// GetAddPartitionInfo returns the global table info and partition info of the
// SchemaChangeEvent whose type is ActionAddPartition.
func (s *SchemaChangeEvent) GetAddPartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionAddTablePartition)
	return s.tableInfo, s.addedPartInfo
}

// NewTruncatePartitionEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTablePartition.
func NewTruncatePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionTruncateTablePartition,
		tableInfo:       globalTableInfo,
		addedPartInfo:   addedPartInfo,
		droppedPartInfo: droppedPartInfo,
	}
}

// GetTruncatePartitionInfo returns the global table info, added partition info
// and deleted partition info of the SchemaChangeEvent whose type is
// ActionTruncateTablePartition.
func (s *SchemaChangeEvent) GetTruncatePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionTruncateTablePartition)
	return s.tableInfo, s.addedPartInfo, s.droppedPartInfo
}

// NewDropPartitionEvent creates a SchemaChangeEvent whose type is
// ActionDropTablePartition.
func NewDropPartitionEvent(
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:              model.ActionDropTablePartition,
		tableInfo:       globalTableInfo,
		droppedPartInfo: droppedPartInfo,
	}
}

// GetDropPartitionInfo returns the global table info and dropped partition info
// of the SchemaChangeEvent whose type is ActionDropTablePartition.
func (s *SchemaChangeEvent) GetDropPartitionInfo() (
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionDropTablePartition)
	return s.tableInfo, s.droppedPartInfo
}

// NewExchangePartitionEvent creates a SchemaChangeEvent whose type is
// ActionExchangeTablePartition.
func NewExchangePartitionEvent(
	globalTableInfo *model.TableInfo,
	partInfo *model.PartitionInfo,
	nonPartTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:            model.ActionExchangeTablePartition,
		tableInfo:     globalTableInfo,
		addedPartInfo: partInfo,
		oldTableInfo:  nonPartTableInfo,
	}
}

// GetExchangePartitionInfo returns the global table info, exchanged partition
// info and original non-partitioned table info of the SchemaChangeEvent whose
// type is ActionExchangeTablePartition.
func (s *SchemaChangeEvent) GetExchangePartitionInfo() (
	globalTableInfo *model.TableInfo,
	partInfo *model.PartitionInfo,
	nonPartTableInfo *model.TableInfo,
) {
	intest.Assert(s.tp == model.ActionExchangeTablePartition)
	return s.tableInfo, s.addedPartInfo, s.oldTableInfo
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
		tableInfo:       globalTableInfo,
		addedPartInfo:   addedPartInfo,
		droppedPartInfo: droppedPartInfo,
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
	return s.tableInfo, s.addedPartInfo, s.droppedPartInfo
}

// NewAddPartitioningEvent creates a SchemaChangeEvent whose type is
// ActionAlterTablePartitioning. It means that a non-partitioned table is
// converted to a partitioned table. For example, `alter table t partition by
// range (c1) (partition p1 values less than (10))`.
func NewAddPartitioningEvent(
	nonPartTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		tp:             model.ActionAlterTablePartitioning,
		nonPartTableID: nonPartTableID,
		tableInfo:      newGlobalTableInfo,
		addedPartInfo:  addedPartInfo,
	}
}

// GetAddPartitioningInfo returns the old table ID of non-partitioned table, new
// global table info and added partition info of the SchemaChangeEvent whose type
// is ActionAlterTablePartitioning.
func (s *SchemaChangeEvent) GetAddPartitioningInfo() (
	nonPartTableID int64,
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.tp == model.ActionAlterTablePartitioning)
	return s.nonPartTableID, s.tableInfo, s.addedPartInfo
}
