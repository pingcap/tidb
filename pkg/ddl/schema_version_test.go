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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestShouldCheckAssumedServer(t *testing.T) {
	for _, isLegacyNextGen := range []bool{false, true} {
		mysqlID, tableID := metadef.SystemDatabaseID, metadef.TiDBTableID
		if isLegacyNextGen {
			mysqlID, tableID = 1, 9999
		}
		db := &model.DBInfo{ID: mysqlID, Name: ast.NewCIStr("mysql"), State: model.StatePublic}
		db.Deprecated.Tables = []*model.TableInfo{
			{ID: tableID, DBID: mysqlID, Name: ast.NewCIStr("tidb"), State: model.StatePublic},
			{ID: 10000, DBID: mysqlID, Name: ast.NewCIStr("custom_table"), State: model.StatePublic},
		}
		builder := infoschema.NewBuilder(nil, 0, nil, infoschema.NewData(), false).WithCrossKS(true)
		require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{db}, nil, nil, nil, 1))
		is := builder.Build(0)
		require.Equal(t, kerneltype.IsNextGen(), shouldCheckAssumedServer(&model.Job{TableID: tableID}, is, isLegacyNextGen))
		for _, job := range []*model.Job{
			{TableID: 100},
			{SchemaID: mysqlID},
			{TableID: 10000, SchemaName: "mysql", TableName: "custom_table"},
			{TableID: 100, InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "mysql", Table: "tidb"}}},
		} {
			require.False(t, shouldCheckAssumedServer(job, is, isLegacyNextGen))
		}
		// Native NextGen recognizes reserved IDs even before the new table is loaded.
		require.Equal(t, kerneltype.IsNextGen() && !isLegacyNextGen,
			shouldCheckAssumedServer(&model.Job{TableID: metadef.ReservedGlobalIDLowerBound + 1}, is, isLegacyNextGen))
	}
}
