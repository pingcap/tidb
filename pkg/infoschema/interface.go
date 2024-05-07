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
	"github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
type InfoSchema interface {
	context.MetaOnlyInfoSchema
	TableByName(schema, table model.CIStr) (table.Table, error)
	TableByID(id int64) (table.Table, bool)
	SchemaTables(schema model.CIStr) []table.Table
	FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition)
	base() *infoSchema
}
