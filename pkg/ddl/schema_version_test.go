// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestShouldCheckAssumedServer(t *testing.T) {
	for _, isLegacyNextGen := range []bool{false, true} {
		mysqlID, tableID := metadef.SystemDatabaseID, metadef.TiDBTableID
		if isLegacyNextGen {
			mysqlID, tableID = 1, 9999
		}
		// Newly created built-ins already carry the owning schema ID in the job.
		require.Equal(t, kerneltype.IsNextGen(), shouldCheckAssumedServer(&model.Job{
			Type: model.ActionCreateTable, SchemaID: mysqlID, TableID: tableID, SchemaName: "mysql", TableName: "tidb",
		}))
		// The owner's InfoSchema may not contain a newly created legacy built-in yet.
		require.Equal(t, kerneltype.IsNextGen() && isLegacyNextGen,
			shouldCheckAssumedServer(&model.Job{
				Type: model.ActionCreateTable, SchemaID: mysqlID, TableID: 10001, SchemaName: "mysql", TableName: "tidb_import_jobs",
			}))
		for _, job := range []*model.Job{
			{TableID: 100},
			{SchemaID: mysqlID},
			{SchemaID: mysqlID, TableID: 10000, SchemaName: "mysql", TableName: "custom_table"},
			{SchemaID: 2, TableID: 10001, SchemaName: "test", TableName: "tidb_import_jobs"},
			{TableID: 100, InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "mysql", Table: "tidb"}}},
		} {
			require.False(t, shouldCheckAssumedServer(job))
		}
		// Native NextGen recognizes reserved IDs even before the new table is loaded.
		require.Equal(t, kerneltype.IsNextGen() && !isLegacyNextGen,
			shouldCheckAssumedServer(&model.Job{SchemaID: mysqlID, TableID: metadef.ReservedGlobalIDLowerBound + 1}))
	}
}
