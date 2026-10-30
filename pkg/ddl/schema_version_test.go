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
	// This keyspace has mysql.stats_meta at legacy ID 9999; even a reserved
	// table ID in a user schema must not identify a system table.
	is := infoschema.MockInfoSchema([]*model.TableInfo{{ID: 100, Name: ast.NewCIStr("user_table")}, {ID: metadef.ReservedGlobalIDUpperBound, Name: ast.NewCIStr("reserved_user_table")}})
	require.False(t, shouldCheckAssumedServer(&model.Job{TableID: 100}, is))
	require.False(t, shouldCheckAssumedServer(&model.Job{TableID: metadef.ReservedGlobalIDUpperBound}, is))
	require.Equal(t, kerneltype.IsNextGen(), shouldCheckAssumedServer(&model.Job{TableID: 9999}, is))
	// A schema ID alone must not classify an unknown table as a system table.
	require.False(t, shouldCheckAssumedServer(&model.Job{SchemaID: 2, TableID: 10000}, is))
	require.False(t, shouldCheckAssumedServer(&model.Job{SchemaID: 2}, is))
	require.False(t, shouldCheckAssumedServer(&model.Job{TableID: 9999}, nil))
}
