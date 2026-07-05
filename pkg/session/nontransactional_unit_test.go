// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/stretchr/testify/require"
)

func TestNonTransactionalDMLHandleDescriptorSupportedShapes(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")

	cases := []struct {
		name       string
		createSQL  string
		stmtSQL    string
		kind       nonTransactionalDMLHandleKind
		columnName string
	}{
		{
			name:       "tidb row id",
			createSQL:  "create table t_rowid(a int, b int)",
			stmtSQL:    "batch on _tidb_rowid limit 2 delete from t_rowid where b >= 0",
			kind:       nonTransactionalDMLHandleExtra,
			columnName: "_tidb_rowid",
		},
		{
			name:       "signed int clustered primary key",
			createSQL:  "create table t_int(id bigint primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 update t_int set b = b + 1 where b >= 0",
			kind:       nonTransactionalDMLHandleInt,
			columnName: "id",
		},
		{
			name:       "varchar binary common handle",
			createSQL:  "create table t_varchar(id varchar(128) collate utf8mb4_bin primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 delete from t_varchar where id >= 'v1:pacer_largepayload0001'",
			kind:       nonTransactionalDMLHandleCommonBinary,
			columnName: "id",
		},
		{
			name:       "varbinary common handle",
			createSQL:  "create table t_varbinary(id varbinary(128) primary key clustered, b int)",
			stmtSQL:    "batch on id limit 2 update t_varbinary set b = 10 where id >= x'76313a30303031'",
			kind:       nonTransactionalDMLHandleCommonBinary,
			columnName: "id",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			MustExec(t, se, "drop table if exists t_rowid, t_int, t_varchar, t_varbinary")
			MustExec(t, se, tt.createSQL)

			desc, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, tt.stmtSQL)
			require.NoError(t, err)
			require.Equal(t, tt.kind, desc.kind)
			require.Equal(t, tt.columnName, desc.columnName.Name.L)
		})
	}
}

func TestNonTransactionalDMLHandleDescriptorRejectedShapes(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "use test")

	cases := []struct {
		name      string
		createSQL string
		stmtSQL   string
		errText   string
	}{
		{
			name:      "unsigned clustered primary key",
			createSQL: "create table t_reject(id bigint unsigned primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support unsigned integer clustered primary keys",
		},
		{
			name:      "non binary varchar collation",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_general_ci primary key clustered, b int)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "requires binary collation",
		},
		{
			name:      "composite common handle",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_bin, b int, primary key(id, b) clustered)",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support composite clustered primary keys",
		},
		{
			name:      "partitioned table",
			createSQL: "create table t_reject(id bigint primary key clustered, b int) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "doesn't support partitioned tables",
		},
		{
			name:      "secondary index shard column",
			createSQL: "create table t_reject(id varchar(64) collate utf8mb4_bin, b int, key(id))",
			stmtSQL:   "batch on id limit 2 delete from t_reject",
			errText:   "requires _tidb_rowid or a clustered primary key",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			MustExec(t, se, "drop table if exists t_reject")
			MustExec(t, se, tt.createSQL)

			_, err := buildNonTransactionalDMLHandleDescriptorForTest(t, se, tt.stmtSQL)
			require.ErrorContains(t, err, tt.errText)
		})
	}
}

func buildNonTransactionalDMLHandleDescriptorForTest(t *testing.T, se sessiontypes.Session, sql string) (*nonTransactionalDMLHandleDescriptor, error) {
	ctx := context.Background()
	stmts, err := se.Parse(ctx, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt, ok := stmts[0].(*ast.NonTransactionalDMLStmt)
	require.True(t, ok)

	nodeW := resolve.NewNodeW(stmt)
	require.NoError(t, core.Preprocess(ctx, se, nodeW))
	tableName, _, shardColumnInfo, tableSources, err := buildSelectSQL(stmt, nodeW.GetResolveContext(), se)
	require.NoError(t, err)
	return buildNonTransactionalDMLHandleDescriptor(se, stmt, tableName, shardColumnInfo, tableSources)
}
