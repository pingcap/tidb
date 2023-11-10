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

package ddl

import "github.com/pingcap/tidb/pkg/parser/model"

// Event contains the information of a ddl event that is used to update stats.
type Event struct {
	// For different ddl types, the following fields are used.
	// They have different meanings for different ddl types.
	// Please do **not** use these fields directly, use the corresponding
	// NewXXXEvent functions instead.
	tableInfo    *model.TableInfo
	partInfo     *model.PartitionInfo
	oldTableInfo *model.TableInfo
	oldPartInfo  *model.PartitionInfo

	// We expose the action type field to the outside, because some ddl events
	// do not need other fields.
	// If your ddl event needs other fields, please add them with the
	// corresponding NewXXXEvent function and give them clear names.
	Tp model.ActionType
}

// NewCreateTableEvent creates a new ddl event that creates a table.
func NewCreateTableEvent(
	newTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:        model.ActionCreateTable,
		tableInfo: newTableInfo,
	}
}

func (e *Event) getCreateTableInfo() (newTableInfo *model.TableInfo) {
	return e.tableInfo
}

// NewTruncateTableEvent creates a new ddl event that truncates a table.
func NewTruncateTableEvent(
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionTruncateTable,
		tableInfo:    newTableInfo,
		oldTableInfo: droppedTableInfo,
	}
}

func (e *Event) getTruncateTableInfo() (newTableInfo *model.TableInfo, droppedTableInfo *model.TableInfo) {
	return e.tableInfo, e.oldTableInfo
}

// NewDropTableEvent creates a new ddl event that drops a table.
func NewDropTableEvent(
	droppedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionDropTable,
		oldTableInfo: droppedTableInfo,
	}
}

func (e *Event) getDropTableInfo() (newTableInfo *model.TableInfo) {
	return e.oldTableInfo
}

// NewAddColumnEvent creates a new ddl event that
// adds a column.
func NewAddColumnEvent(
	newTableInfoWithNewColumnInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:        model.ActionAddColumn,
		tableInfo: newTableInfoWithNewColumnInfo,
	}
}

func (e *Event) getAddColumnInfo() (newTableInfoWithNewColumnInfo *model.TableInfo) {
	return e.tableInfo
}

// NewModifyColumnEvent creates a new ddl event that
// modifies a column.
func NewModifyColumnEvent(
	newTableInfoWithNewColumnInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:        model.ActionModifyColumn,
		tableInfo: newTableInfoWithNewColumnInfo,
	}
}

func (e *Event) getModifyColumnInfo() (newTableInfoWithNewColumnInfo *model.TableInfo) {
	return e.tableInfo
}

// NewAddTablePartitionEvent creates a new ddl event that adds a partition.
func NewAddTablePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:        model.ActionAddTablePartition,
		tableInfo: globalTableInfo,
		partInfo:  addedPartInfo,
	}
}

func (e *Event) getAddTablePartitionInfo() (globalTableInfo *model.TableInfo, addedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.partInfo
}

// NewDropPartitionEvent creates a new ddl event that drops a partition.
func NewDropPartitionEvent(
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionDropTablePartition,
		tableInfo:   globalTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

func (e *Event) getDropPartitionInfo() (globalTableInfo *model.TableInfo, droppedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.oldPartInfo
}

// NewExchangePartitionEvent creates a new ddl event that exchanges a partition.
func NewExchangePartitionEvent(
	globalTableInfo *model.TableInfo,
	exchangedPartInfo *model.PartitionInfo,
	exchangedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionExchangeTablePartition,
		tableInfo:    globalTableInfo,
		partInfo:     exchangedPartInfo,
		oldTableInfo: exchangedTableInfo,
	}
}

func (e *Event) getExchangePartitionInfo() (
	globalTableInfo *model.TableInfo,
	exchangedPartInfo *model.PartitionInfo,
	exchangedTableInfo *model.TableInfo,
) {
	return e.tableInfo, e.partInfo, e.oldTableInfo
}

// NewReorganizePartitionEvent creates a new ddl event that reorganizes a partition.
// We also use it for increasing or decreasing the number of hash partitions.
func NewReorganizePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionReorganizePartition,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

func (e *Event) getReorganizePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewTruncatePartitionEvent creates a new ddl event that truncates a partition.
func NewTruncatePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionTruncateTablePartition,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

func (e *Event) getTruncatePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewAddPartitioningEvent creates a new ddl event that adds partitioning.
// For example, `alter table t partition by range (c1) (partition p1 values less than (10))`.
func NewAddPartitioningEvent(
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:        model.ActionAlterTablePartitioning,
		tableInfo: newGlobalTableInfo,
		partInfo:  addedPartInfo,
	}
}

func (e *Event) getAddPartitioningInfo() (newGlobalTableInfo *model.TableInfo, addedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.partInfo
}

// NewRemovePartitioningEvent creates a new ddl event that removes partitioning.
// For example, `alter table t remove partitioning`.
func NewRemovePartitioningEvent(
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionRemovePartitioning,
		tableInfo:   newSingleTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

func (e *Event) getRemovePartitioningInfo() (newSingleTableInfo *model.TableInfo, droppedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.oldPartInfo
}
