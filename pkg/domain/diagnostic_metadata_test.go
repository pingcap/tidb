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

package domain

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestLoadDiagnosticMetadata(t *testing.T) {
	oldSize, oldText, oldMDL := vardef.SchemaCacheSize.Load(), vardef.SchemaCacheSizeOriginText.Load(), vardef.IsMDLEnabled()
	t.Cleanup(func() {
		vardef.SchemaCacheSize.Store(oldSize)
		vardef.SchemaCacheSizeOriginText.Store(oldText)
		vardef.SetEnableMDL(oldMDL)
	})
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = LoadDiagnosticMetadata(ctx, store)
	require.ErrorContains(t, err, "already bootstrapped keyspace")
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).FinishBootstrap(123)
	}))
	version, err := LoadDiagnosticMetadata(ctx, store)
	require.NoError(t, err)
	require.EqualValues(t, 123, version)
	require.EqualValues(t, vardef.DefTiDBSchemaCacheSize, vardef.SchemaCacheSize.Load())
	require.True(t, vardef.IsMDLEnabled())
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		reader := meta.NewReader(txn)
		_, missing, err := reader.GetSchemaCacheSize()
		require.NoError(t, err)
		require.True(t, missing, "default cache size must not be persisted")
		_, missing, err = reader.GetMetadataLock()
		require.NoError(t, err)
		require.True(t, missing, "default MDL setting must not be persisted")
		return nil
	}))
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		writer := meta.NewMutator(txn)
		if err := writer.SetSchemaCacheSize(1024); err != nil {
			return err
		}
		return writer.SetMetadataLock(false)
	}))
	version, err = LoadDiagnosticMetadata(ctx, store)
	require.NoError(t, err)
	require.EqualValues(t, 123, version)
	require.EqualValues(t, 1024, vardef.SchemaCacheSize.Load())
	require.Equal(t, "1024", vardef.SchemaCacheSizeOriginText.Load())
	require.False(t, vardef.IsMDLEnabled())
}
