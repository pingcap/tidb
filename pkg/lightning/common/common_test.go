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

package common_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	tmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newTableInfo(t *testing.T,
	dbID, tableID int64,
	createTableSQL string, kvStore kv.Storage,
) *model.TableInfo {
	p := parser.New()
	se := tmock.NewContext()

	node, err := p.ParseOneStmt(createTableSQL, "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
	require.NoError(t, err)
	tableInfo.State = model.StatePublic

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnLightning)
	err = kv.RunInNewTxn(ctx, kvStore, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		if err := m.CreateDatabase(&model.DBInfo{ID: dbID}); err != nil && !errors.ErrorEqual(err, meta.ErrDBExists) {
			return err
		}
		return m.CreateTableOrView(dbID, "", tableInfo)
	})
	require.NoError(t, err)
	return tableInfo
}

func TestAllocGlobalAutoID(t *testing.T) {
	storePath := t.TempDir()
	kvStore, err := mockstore.NewMockStore(mockstore.WithPath(storePath))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, kvStore.Close())
	})

	cases := []struct {
		tableID              int64
		createTableSQL       string
		expectErrStr         string
		expectAllocatorTypes []autoid.AllocatorType
	}{
		// autoID, autoIncrID = false, false
		{
			tableID:              11,
			createTableSQL:       "create table t11 (a int primary key clustered)",
			expectErrStr:         "has no auto ID",
			expectAllocatorTypes: nil,
		},
		{
			tableID:              12,
			createTableSQL:       "create table t12 (a int primary key clustered) AUTO_ID_CACHE 1",
			expectErrStr:         "has no auto ID",
			expectAllocatorTypes: nil,
		},
		// autoID, autoIncrID = true, false
		{
			tableID:              21,
			createTableSQL:       "create table t21 (a int)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              22,
			createTableSQL:       "create table t22 (a int) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		// autoID, autoIncrID = false, true
		{
			tableID:              31,
			createTableSQL:       "create table t31 (a int primary key clustered auto_increment)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              32,
			createTableSQL:       "create table t32 (a int primary key clustered auto_increment) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType},
		},
		// autoID, autoIncrID = true, true
		{
			tableID:              41,
			createTableSQL:       "create table t41 (a int primary key nonclustered auto_increment)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              42,
			createTableSQL:       "create table t42 (a int primary key nonclustered auto_increment) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType},
		},
		// autoRandomID
		{
			tableID:              51,
			createTableSQL:       "create table t51 (a bigint primary key auto_random)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoRandomType},
		},
	}
	ctx := context.Background()
	for _, c := range cases {
		ti := newTableInfo(t, 1, c.tableID, c.createTableSQL, kvStore)
		allocators, err := common.GetGlobalAutoIDAlloc(mockRequirement{kvStore}, 1, ti)
		if c.expectErrStr == "" {
			require.NoError(t, err, c.tableID)
			require.NoError(t, common.RebaseGlobalAutoID(ctx, 123, mockRequirement{kvStore}, 1, ti))
			base, idMax, err := common.AllocGlobalAutoID(ctx, 100, mockRequirement{kvStore}, 1, ti)
			require.NoError(t, err, c.tableID)
			require.Equal(t, int64(123), base, c.tableID)
			require.Equal(t, int64(223), idMax, c.tableID)
			// all allocators are rebased and allocated
			for _, alloc := range allocators {
				base2, max2, err := alloc.Alloc(ctx, 100, 1, 1)
				require.NoError(t, err, c.tableID)
				require.Equal(t, int64(223), base2, c.tableID)
				require.Equal(t, int64(323), max2, c.tableID)
			}
		} else {
			require.ErrorContains(t, err, c.expectErrStr, c.tableID)
		}
		var allocatorTypes []autoid.AllocatorType
		for _, alloc := range allocators {
			allocatorTypes = append(allocatorTypes, alloc.GetType())
		}
		require.Equal(t, c.expectAllocatorTypes, allocatorTypes, c.tableID)
	}
}

type mockRequirement struct {
	kv.Storage
}

func (r mockRequirement) Store() kv.Storage {
	return r.Storage
}

func (r mockRequirement) AutoIDClient() *autoid.ClientDiscover {
	return nil
}

func TestRebaseTableAllocators(t *testing.T) {
	storePath := t.TempDir()
	kvStore, err := mockstore.NewMockStore(mockstore.WithPath(storePath))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, kvStore.Close())
	})
	ti := newTableInfo(t, 1, 42,
		"create table t42 (a int primary key nonclustered auto_increment) AUTO_ID_CACHE 1", kvStore)
	allocators, err := common.GetGlobalAutoIDAlloc(mockRequirement{kvStore}, 1, ti)
	require.NoError(t, err)
	require.Len(t, allocators, 2)
	for _, alloc := range allocators {
		id, err := alloc.NextGlobalAutoID()
		require.NoError(t, err)
		require.Equal(t, int64(1), id)
	}
	ctx := context.Background()
	allocatorTypes := make([]autoid.AllocatorType, 0, len(allocators))
	// rebase to 123
	for _, alloc := range allocators {
		require.NoError(t, alloc.Rebase(ctx, 123, false))
		allocatorTypes = append(allocatorTypes, alloc.GetType())
	}
	require.Equal(t, []autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType}, allocatorTypes)
	// this call does nothing
	require.NoError(t, common.RebaseTableAllocators(ctx, nil, mockRequirement{kvStore}, 1, ti))
	for _, alloc := range allocators {
		nextID, err := alloc.NextGlobalAutoID()
		require.NoError(t, err)
		require.Equal(t, int64(124), nextID)
	}
	// this call rebase AutoIncrementType allocator to 223
	require.NoError(t, common.RebaseTableAllocators(ctx, map[autoid.AllocatorType]int64{
		autoid.AutoIncrementType: 223,
	}, mockRequirement{kvStore}, 1, ti))
	next, err := allocators[0].NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(224), next)
	next, err = allocators[1].NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(124), next)
	// this call rebase AutoIncrementType allocator to 323, RowIDAllocType allocator to 423
	require.NoError(t, common.RebaseTableAllocators(ctx, map[autoid.AllocatorType]int64{
		autoid.AutoIncrementType: 323,
		autoid.RowIDAllocType:    423,
	}, mockRequirement{kvStore}, 1, ti))
	next, err = allocators[0].NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(324), next)
	next, err = allocators[1].NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(424), next)
}
