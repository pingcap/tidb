// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

// TestRollbackAlloc tests that when the allocation transaction commit failed,
// the local variable base and end doesn't change.
func TestRollbackAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	ctx := context.Background()
	injectConf := new(kv.InjectionConfig)
	injectConf.SetCommitError(errors.New("injected"))
	injectedStore := kv.NewInjectedStore(store, injectConf)
	alloc := autoid.NewAllocator(injectedStore, 1, false, autoid.RowIDAllocType)
	_, _, err = alloc.Alloc(ctx, 2, 1, 1, 1)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())

	err = alloc.Rebase(2, 100, true)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())
}
