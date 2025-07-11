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

package infoschema

import (
	stdctx "context"

	"github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
type InfoSchema interface {
	context.MetaOnlyInfoSchema
	TableByName(ctx stdctx.Context, schema, table ast.CIStr) (table.Table, error)
	TableByID(ctx stdctx.Context, id int64) (table.Table, bool)
	// TableItemByID returns a lightweight table meta specified by the given ID,
	// without loading the whole info from storage.
	// So it's all in memory operation. No need to worry about network or disk cost.
	TableItemByID(id int64) (TableItem, bool)
	// TableIDByPartitionID is a pure memory operation, returns the table ID by the partition ID.
	// It's all in memory operation. No need to worry about network or disk cost.
	TableIDByPartitionID(partitionID int64) (tableID int64, ok bool)
	FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition)
	// TableItemByPartitionID returns a lightweight table meta specified by the partition ID,
	// without loading the whole info from storage.
	// So it's all in memory operation. No need to worry about network or disk cost.
	TableItemByPartitionID(partitionID int64) (TableItem, bool)
	GetAutoIDRequirement() autoid.Requirement
	base() *infoSchema
}
