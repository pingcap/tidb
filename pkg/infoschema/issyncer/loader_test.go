// Copyright 2025 PingCAP, Inc.
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

package issyncer

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestLoadFromTS(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	l := &Loader{
		store:     store,
		infoCache: infoschema.NewCache(nil, 1),
		logger:    logutil.BgLogger(),
	}
	ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err := l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas := is.AllSchemas()
	// only 2 memory schemas are there
	require.Len(t, allSchemas, 2)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)

	// hit cache
	_, hitCache, _, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	require.True(t, hitCache)
	require.Nil(t, changes)

	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
		require.NoError(t, mu.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: 1,
			TableID:  1,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	l = &Loader{
		store:     store,
		infoCache: infoschema.NewCache(nil, 1),
		logger:    logutil.BgLogger(),
	}
	is, hitCache, oldSchemaVersion, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas = is.AllSchemas()
	require.EqualValues(t, 1, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 3)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "test" }))
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)
	tbls, err := is.SchemaTableInfos(ctx, ast.NewCIStr("test"))
	require.NoError(t, err)
	require.Len(t, tbls, 1)
	require.Equal(t, "t", tbls[0].Name.L)

	// load from diff
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		require.NoError(t, mu.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: ast.NewCIStr("t1"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: 1,
			TableID:  2,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas = is.AllSchemas()
	require.EqualValues(t, 2, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 3)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "test" }))
	require.False(t, hitCache)
	require.EqualValues(t, 1, oldSchemaVersion)
	require.Len(t, changes.PhyTblIDS, 1)
	tbls, err = is.SchemaTableInfos(ctx, ast.NewCIStr("test"))
	require.NoError(t, err)
	require.Len(t, tbls, 2)
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t" }))
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t1" }))
}
