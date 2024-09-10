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
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/meta/model"
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
