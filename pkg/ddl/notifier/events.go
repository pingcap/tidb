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

package notifier

import (
	"encoding/json"
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
	inner *jsonSchemaChangeEvent
}

// String implements fmt.Stringer interface.
func (s *SchemaChangeEvent) String() string {
	if s == nil {
		return "nil SchemaChangeEvent"
	}

	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "(Event Type: %s", s.inner.Tp)
	if s.inner.TableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Table ID: %d, Table Name: %s", s.inner.TableInfo.ID, s.inner.TableInfo.Name)
	}
	for _, tableInfo := range s.inner.TableInfos {
		_, _ = fmt.Fprintf(&sb, ", Table ID: %d, Table Name: %s", tableInfo.ID, tableInfo.Name)
	}
	if s.inner.OldTableInfo != nil {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID: %d, Old Table Name: %s", s.inner.OldTableInfo.ID, s.inner.OldTableInfo.Name)
	}
	if s.inner.OldTableID4Partition != 0 {
		_, _ = fmt.Fprintf(&sb, ", Old Table ID for Partition: %d", s.inner.OldTableID4Partition)
	}
	if s.inner.AddedPartInfo != nil {
		for _, partDef := range s.inner.AddedPartInfo.Definitions {
			if partDef.Name.L != "" {
				_, _ = fmt.Fprintf(&sb, ", Partition Name: %s", partDef.Name)
			}
			_, _ = fmt.Fprintf(&sb, ", Partition ID: %d", partDef.ID)
		}
	}
	if s.inner.DroppedPartInfo != nil {
		for _, partDef := range s.inner.DroppedPartInfo.Definitions {
			if partDef.Name.L != "" {
				_, _ = fmt.Fprintf(&sb, ", Dropped Partition Name: %s", partDef.Name)
			}
			_, _ = fmt.Fprintf(&sb, ", Dropped Partition ID: %d", partDef.ID)
		}
	}
	for _, columnInfo := range s.inner.Columns {
		_, _ = fmt.Fprintf(&sb, ", Column ID: %d, Column Name: %s", columnInfo.ID, columnInfo.Name)
	}
	for _, indexInfo := range s.inner.Indexes {
		_, _ = fmt.Fprintf(&sb, ", Index ID: %d, Index Name: %s", indexInfo.ID, indexInfo.Name)
	}
	sb.WriteString(")")

	return sb.String()
}

// GetType returns the type of the schema change event.
func (s *SchemaChangeEvent) GetType() model.ActionType {
	if s == nil {
		return model.ActionNone
	}
	return s.inner.Tp
}

// NewCreateTablesEvent creates a SchemaChangeEvent whose type is
// ActionCreateTables.
// The type of the ActionCreateTable should also be included in ActionCreateTables.
func NewCreateTablesEvent(
	newTableInfos []*model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:         model.ActionCreateTables,
			TableInfos: newTableInfos,
		},
	}
}

// GetCreateTablesInfo returns the table info of the SchemaChangeEvent whose type
// is ActionCreateTable.
func (s *SchemaChangeEvent) GetCreateTablesInfo() []*model.TableInfo {
	intest.Assert(s.inner.Tp == model.ActionCreateTables)
	return s.inner.TableInfos
}

// NewTruncateTableEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTable.
func NewTruncateTableEvent(
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:           model.ActionTruncateTable,
			TableInfo:    newTableInfo,
			OldTableInfo: droppedTableInfo,
		},
	}
}

// GetTruncateTableInfo returns the new and old table info of the
// SchemaChangeEvent whose type is ActionTruncateTable.
func (s *SchemaChangeEvent) GetTruncateTableInfo() (
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionTruncateTable)
	return s.inner.TableInfo, s.inner.OldTableInfo
}

// NewDropTableEvent creates a SchemaChangeEvent whose type is ActionDropTable.
func NewDropTableEvent(
	droppedTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:           model.ActionDropTable,
			OldTableInfo: droppedTableInfo,
		},
	}
}

// GetDropTableInfo returns the table info of the SchemaChangeEvent whose type is
// ActionDropTable.
func (s *SchemaChangeEvent) GetDropTableInfo() (droppedTableInfo *model.TableInfo) {
	intest.Assert(s.inner.Tp == model.ActionDropTable)
	return s.inner.OldTableInfo
}

// NewAddColumnEvent creates a SchemaChangeEvent whose type is ActionAddColumn.
func NewAddColumnEvent(
	tableInfo *model.TableInfo,
	newColumns []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:        model.ActionAddColumn,
			TableInfo: tableInfo,
			Columns:   newColumns,
		},
	}
}

// GetAddColumnInfo returns the table info of the SchemaChangeEvent whose type is
// ActionAddColumn.
func (s *SchemaChangeEvent) GetAddColumnInfo() (
	tableInfo *model.TableInfo,
	columnInfos []*model.ColumnInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionAddColumn)
	return s.inner.TableInfo, s.inner.Columns
}

// NewModifyColumnEvent creates a SchemaChangeEvent whose type is
// ActionModifyColumn.
func NewModifyColumnEvent(
	tableInfo *model.TableInfo,
	modifiedColumns []*model.ColumnInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:        model.ActionModifyColumn,
			TableInfo: tableInfo,
			Columns:   modifiedColumns,
		},
	}
}

// GetModifyColumnInfo returns the table info and modified column info the
// SchemaChangeEvent whose type is ActionModifyColumn.
func (s *SchemaChangeEvent) GetModifyColumnInfo() (
	newTableInfo *model.TableInfo,
	modifiedColumns []*model.ColumnInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionModifyColumn)
	return s.inner.TableInfo, s.inner.Columns
}

// NewAddPartitionEvent creates a SchemaChangeEvent whose type is
// ActionAddPartition.
func NewAddPartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:            model.ActionAddTablePartition,
			TableInfo:     globalTableInfo,
			AddedPartInfo: addedPartInfo,
		},
	}
}

// GetAddPartitionInfo returns the global table info and partition info of the
// SchemaChangeEvent whose type is ActionAddPartition.
func (s *SchemaChangeEvent) GetAddPartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionAddTablePartition)
	return s.inner.TableInfo, s.inner.AddedPartInfo
}

// NewTruncatePartitionEvent creates a SchemaChangeEvent whose type is
// ActionTruncateTablePartition.
func NewTruncatePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:              model.ActionTruncateTablePartition,
			TableInfo:       globalTableInfo,
			AddedPartInfo:   addedPartInfo,
			DroppedPartInfo: droppedPartInfo,
		},
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
	intest.Assert(s.inner.Tp == model.ActionTruncateTablePartition)
	return s.inner.TableInfo, s.inner.AddedPartInfo, s.inner.DroppedPartInfo
}

// NewDropPartitionEvent creates a SchemaChangeEvent whose type is
// ActionDropTablePartition.
func NewDropPartitionEvent(
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:              model.ActionDropTablePartition,
			TableInfo:       globalTableInfo,
			DroppedPartInfo: droppedPartInfo,
		},
	}
}

// GetDropPartitionInfo returns the global table info and dropped partition info
// of the SchemaChangeEvent whose type is ActionDropTablePartition.
func (s *SchemaChangeEvent) GetDropPartitionInfo() (
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionDropTablePartition)
	return s.inner.TableInfo, s.inner.DroppedPartInfo
}

// NewExchangePartitionEvent creates a SchemaChangeEvent whose type is
// ActionExchangeTablePartition.
func NewExchangePartitionEvent(
	globalTableInfo *model.TableInfo,
	partInfo *model.PartitionInfo,
	nonPartTableInfo *model.TableInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:            model.ActionExchangeTablePartition,
			TableInfo:     globalTableInfo,
			AddedPartInfo: partInfo,
			OldTableInfo:  nonPartTableInfo,
		},
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
	intest.Assert(s.inner.Tp == model.ActionExchangeTablePartition)
	return s.inner.TableInfo, s.inner.AddedPartInfo, s.inner.OldTableInfo
}

// NewReorganizePartitionEvent creates a SchemaChangeEvent whose type is
// ActionReorganizePartition.
func NewReorganizePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:              model.ActionReorganizePartition,
			TableInfo:       globalTableInfo,
			AddedPartInfo:   addedPartInfo,
			DroppedPartInfo: droppedPartInfo,
		},
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
	intest.Assert(s.inner.Tp == model.ActionReorganizePartition)
	return s.inner.TableInfo, s.inner.AddedPartInfo, s.inner.DroppedPartInfo
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
		inner: &jsonSchemaChangeEvent{
			Tp:                   model.ActionAlterTablePartitioning,
			OldTableID4Partition: nonPartTableID,
			TableInfo:            newGlobalTableInfo,
			AddedPartInfo:        addedPartInfo,
		},
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
	intest.Assert(s.inner.Tp == model.ActionAlterTablePartitioning)
	return s.inner.OldTableID4Partition, s.inner.TableInfo, s.inner.AddedPartInfo
}

// NewRemovePartitioningEvent creates a schema change event whose type is
// ActionRemovePartitioning.
func NewRemovePartitioningEvent(
	oldPartitionedTableID int64,
	nonPartitionTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:                   model.ActionRemovePartitioning,
			OldTableID4Partition: oldPartitionedTableID,
			TableInfo:            nonPartitionTableInfo,
			DroppedPartInfo:      droppedPartInfo,
		},
	}
}

// GetRemovePartitioningInfo returns the table info and partition info of the SchemaChangeEvent whose type is
// ActionRemovePartitioning.
func (s *SchemaChangeEvent) GetRemovePartitioningInfo() (
	oldPartitionedTableID int64,
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionRemovePartitioning)
	return s.inner.OldTableID4Partition, s.inner.TableInfo, s.inner.DroppedPartInfo
}

// NewAddIndexEvent creates a schema change event whose type is ActionAddIndex.
func NewAddIndexEvent(
	tableInfo *model.TableInfo,
	newIndexes []*model.IndexInfo,
) *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp:        model.ActionAddIndex,
			TableInfo: tableInfo,
			Indexes:   newIndexes,
		},
	}
}

// GetAddIndexInfo returns the table info and added index info of the
// SchemaChangeEvent whose type is ActionAddIndex.
func (s *SchemaChangeEvent) GetAddIndexInfo() (
	tableInfo *model.TableInfo,
	indexes []*model.IndexInfo,
) {
	intest.Assert(s.inner.Tp == model.ActionAddIndex)
	return s.inner.TableInfo, s.inner.Indexes
}

// NewFlashbackClusterEvent creates a schema change event whose type is
// ActionFlashbackCluster.
func NewFlashbackClusterEvent() *SchemaChangeEvent {
	return &SchemaChangeEvent{
		inner: &jsonSchemaChangeEvent{
			Tp: model.ActionFlashbackCluster,
		},
	}
}

// jsonSchemaChangeEvent is used by SchemaChangeEvent when needed to (un)marshal data,
// we want to hide the details to subscribers, so SchemaChangeEvent contain this struct.
type jsonSchemaChangeEvent struct {
	TableInfo       *model.TableInfo     `json:"table_info,omitempty"`
	TableInfos      []*model.TableInfo   `json:"table_infos,omitempty"`
	OldTableInfo    *model.TableInfo     `json:"old_table_info,omitempty"`
	AddedPartInfo   *model.PartitionInfo `json:"added_partition_info,omitempty"`
	DroppedPartInfo *model.PartitionInfo `json:"dropped_partition_info,omitempty"`
	Columns         []*model.ColumnInfo  `json:"columns,omitempty"`
	Indexes         []*model.IndexInfo   `json:"indexes,omitempty"`
	// OldTableID4Partition is used to store the table ID when a table transitions from being partitioned to non-partitioned,
	// or vice versa.
	OldTableID4Partition int64 `json:"old_table_id_for_partition,omitempty"`

	Tp model.ActionType `json:"type,omitempty"`
}

// MarshalJSON implements the json.Marshaler interface.
func (s *SchemaChangeEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.inner)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *SchemaChangeEvent) UnmarshalJSON(b []byte) error {
	var j jsonSchemaChangeEvent
	err := json.Unmarshal(b, &j)
	if err == nil {
		s.inner = &j
	}
	return err
}
