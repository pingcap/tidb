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
	newTableInfo    *model.TableInfo
	oldTableInfo    *model.TableInfo
	addedPartInfo   *model.PartitionInfo
	deletedPartInfo *model.PartitionInfo

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
