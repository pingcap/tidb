// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestInitMetaTable(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, store.Close())
	}()

	require.NoError(t, session.InitMetaTable(store))

	tbls := map[string]struct{}{
		"tidb_ddl_job":     {},
		"tidb_ddl_reorg":   {},
		"tidb_ddl_history": {},
	}

	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		exists, err := m.CheckDDLTableExists()
		require.NoError(t, err)
		require.True(t, exists)
		dbs, err := m.ListDatabases()
		require.NoError(t, err)
		require.Len(t, dbs, 1)
		tables, err := m.ListTables(dbs[0].ID)
		require.NoError(t, err)
		require.Len(t, tables, 3)
		for _, tbl := range tables {
			_, ok := tbls[tbl.Name.L]
			require.True(t, ok)
			require.Equal(t, model.StatePublic, tbl.State)
			require.NotEqual(t, 0, tbl.ID)
		}
		return nil
	}))
}
