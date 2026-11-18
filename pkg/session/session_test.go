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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestInitLegacyECDSASessionToken(t *testing.T) {
	original := vardef.IsLegacyECDSASessionTokenDisabled()
	t.Cleanup(func() { vardef.SetLegacyECDSASessionTokenDisabled(original) })
	t.Run("fresh bootstrap", func(t *testing.T) {
		vardef.SetLegacyECDSASessionTokenDisabled(false)
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		dom, err := BootstrapSession(store)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		defer dom.Close()
		require.True(t, vardef.IsLegacyECDSASessionTokenDisabled())
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		disabled, err := meta.NewReader(store.GetSnapshot(version)).GetLegacyECDSASessionTokenDisabled()
		require.NoError(t, err)
		require.True(t, disabled)
	})
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	vardef.SetLegacyECDSASessionTokenDisabled(false)
	// Existing clusters without the metadata key retain upgrade compatibility.
	require.NoError(t, initLegacyECDSASessionToken(store, false))
	require.False(t, vardef.IsLegacyECDSASessionTokenDisabled())

	injectedErr := errors.New("legacy session token metadata failure")
	failingStore := &legacySessionTokenStorage{
		Storage:      store,
		commitErr:    injectedErr,
		beforeCommit: func() { require.False(t, vardef.IsLegacyECDSASessionTokenDisabled()) },
	}
	require.ErrorIs(t, initLegacyECDSASessionToken(failingStore, true), injectedErr)
	require.False(t, vardef.IsLegacyECDSASessionTokenDisabled())
	require.NoError(t, initLegacyECDSASessionToken(store, false))
	require.False(t, vardef.IsLegacyECDSASessionTokenDisabled())

	failingStore.commitErr = nil
	failingStore.readErr = injectedErr
	require.ErrorIs(t, initLegacyECDSASessionToken(failingStore, false), injectedErr)
	require.False(t, vardef.IsLegacyECDSASessionTokenDisabled())
	// Startup must propagate metadata read failures before initializing a domain.
	_, err = BootstrapSession(failingStore)
	require.ErrorIs(t, err, injectedErr)

	// Fresh bootstrap commits the transition before publishing it in memory.
	failingStore.readErr = nil
	require.NoError(t, initLegacyECDSASessionToken(failingStore, true))
	require.True(t, vardef.IsLegacyECDSASessionTokenDisabled())
	vardef.SetLegacyECDSASessionTokenDisabled(false)
	require.NoError(t, initLegacyECDSASessionToken(store, false))
	require.True(t, vardef.IsLegacyECDSASessionTokenDisabled())
	require.NoError(t, initLegacyECDSASessionToken(store, true))

	// A missing flag from another store must never undo an observed transition.
	emptyStore, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emptyStore.Close()) })
	require.NoError(t, initLegacyECDSASessionToken(emptyStore, false))
	require.True(t, vardef.IsLegacyECDSASessionTokenDisabled())
}

type legacySessionTokenStorage struct {
	kv.Storage
	readErr      error
	commitErr    error
	beforeCommit func()
}

func (s *legacySessionTokenStorage) Begin(opts ...tikv.TxnOption) (kv.Transaction, error) {
	txn, err := s.Storage.Begin(opts...)
	if err != nil {
		return nil, err
	}
	return &legacySessionTokenTransaction{Transaction: txn, readErr: s.readErr, commitErr: s.commitErr, beforeCommit: s.beforeCommit}, nil
}

type legacySessionTokenTransaction struct {
	kv.Transaction
	readErr      error
	commitErr    error
	beforeCommit func()
}

func (t *legacySessionTokenTransaction) Get(ctx context.Context, key kv.Key, opts ...kv.GetOption) (kv.ValueEntry, error) {
	if t.readErr != nil {
		return kv.ValueEntry{}, t.readErr
	}
	return t.Transaction.Get(ctx, key, opts...)
}

func (t *legacySessionTokenTransaction) Commit(ctx context.Context) error {
	if t.beforeCommit != nil {
		t.beforeCommit()
	}
	if t.commitErr != nil {
		if err := t.Transaction.Rollback(); err != nil {
			return err
		}
		return t.commitErr
	}
	return t.Transaction.Commit(ctx)
}

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestMDLInitializationPolicies(t *testing.T) {
	original := variable.EnableMDL.Load()
	t.Cleanup(func() { variable.EnableMDL.Store(original) })
	for _, initializer := range []struct {
		name   string
		policy mdlInitPolicy
	}{
		{"normal", mdlInitEnableOnNull},
		{"bootstrap", mdlInitForceEnable},
		{"upgrade", mdlInitKeepDisabledOnNull},
		{"BR", mdlInitKeepDisabledOnNull},
	} {
		for _, state := range []string{"missing", "enabled", "disabled"} {
			t.Run(initializer.name+"/"+state, func(t *testing.T) {
				store, err := mockstore.NewMockStore()
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close()) }()
				if state != "missing" {
					txn, err := store.Begin()
					require.NoError(t, err)
					require.NoError(t, meta.NewMutator(txn).SetMetadataLock(state == "enabled"))
					require.NoError(t, txn.Commit(context.Background()))
				}
				expectedEnabled := state == "enabled" || initializer.name == "bootstrap" ||
					(state == "missing" && initializer.name == "normal")
				variable.EnableMDL.Store(!expectedEnabled)
				_, err = initMDLVariable(store, initializer.policy)
				require.NoError(t, err)
				require.Equal(t, expectedEnabled, variable.EnableMDL.Load())
				txn, err := store.Begin()
				require.NoError(t, err)
				enabled, missing, err := meta.NewMutator(txn).GetMetadataLock()
				require.NoError(t, err)
				require.NoError(t, txn.Rollback())
				require.Equal(t, expectedEnabled, enabled)
				require.Equal(t, state == "missing" && !expectedEnabled, missing)
			})
		}
	}
}
