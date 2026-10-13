// Copyright 2026 PingCAP, Inc.
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

package conflictedkv

import (
	"context"
	goerrors "errors"
	"testing"

	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type commitErrorStorage struct {
	tidbkv.Storage
	commitErrors   []error
	commitAttempts int
}

func (s *commitErrorStorage) Begin(opts ...tikv.TxnOption) (tidbkv.Transaction, error) {
	txn, err := s.Storage.Begin(opts...)
	if err != nil {
		return nil, err
	}
	return &commitErrorTxn{Transaction: txn, store: s}, nil
}

type commitErrorTxn struct {
	tidbkv.Transaction
	store *commitErrorStorage
}

func (txn *commitErrorTxn) Commit(ctx context.Context) error {
	attempt := txn.store.commitAttempts
	txn.store.commitAttempts++
	if attempt < len(txn.store.commitErrors) && txn.store.commitErrors[attempt] != nil {
		if err := txn.Transaction.Rollback(); err != nil {
			return err
		}
		return txn.store.commitErrors[attempt]
	}
	return txn.Transaction.Commit(ctx)
}

func TestDeleteBufferedKeysReturnsCommitError(t *testing.T) {
	ctx := context.Background()
	value := []byte("still-present")

	t.Run("propagates non-retryable commit error", func(t *testing.T) {
		// Regression test for https://github.com/pingcap/tidb/issues/69792.
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		key := tidbkv.Key("commit-error/conflict-key")

		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, txn.Set(key, value))
		require.NoError(t, txn.Commit(ctx))

		commitErr := goerrors.New("injected commit error")
		commitErrStore := &commitErrorStorage{Storage: store, commitErrors: []error{commitErr}}
		deleter := &Deleter{
			store:  commitErrStore,
			logger: zap.NewNop(),
		}
		err = deleter.deleteBufferedKeys(ctx, []tidbkv.Key{key})
		require.ErrorIs(t, err, commitErr)
		require.Equal(t, 1, commitErrStore.commitAttempts)

		readTxn, err := store.Begin()
		require.NoError(t, err)
		got, err := readTxn.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, value, got.Value)
		require.NoError(t, readTxn.Rollback())
	})

	t.Run("retries transaction write conflict", func(t *testing.T) {
		// Regression test for https://github.com/pingcap/tidb/issues/69799.
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		key := tidbkv.Key("commit-error/retry-conflict-key")

		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, txn.Set(key, value))
		require.NoError(t, txn.Commit(ctx))

		writeConflictErr := tidbkv.ErrWriteConflict.FastGenByArgs(
			uint64(1), uint64(2), uint64(0), "", "", "", "", "Optimistic",
		)
		retryStore := &commitErrorStorage{
			Storage:      store,
			commitErrors: []error{writeConflictErr},
		}
		deleter := &Deleter{
			store:  retryStore,
			logger: zap.NewNop(),
		}
		require.NoError(t, deleter.deleteKeysWithRetry(ctx, []tidbkv.Key{key}))
		require.Equal(t, 2, retryStore.commitAttempts)

		readTxn, err := store.Begin()
		require.NoError(t, err)
		_, err = readTxn.Get(ctx, key)
		require.True(t, tidbkv.ErrNotExist.Equal(err))
		require.NoError(t, readTxn.Rollback())
	})
}
