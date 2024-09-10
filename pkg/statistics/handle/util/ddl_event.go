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

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// DDLEvent contains the information of a ddl event that is used to update stats.
type DDLEvent struct {
	// todo: replace DDLEvent by SchemaChangeEvent gradually
	SchemaChangeEvent *ddlutil.SchemaChangeEvent
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
