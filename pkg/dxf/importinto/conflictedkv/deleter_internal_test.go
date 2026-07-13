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
	err error
}

func (s *commitErrorStorage) Begin(opts ...tikv.TxnOption) (tidbkv.Transaction, error) {
	txn, err := s.Storage.Begin(opts...)
	if err != nil {
		return nil, err
	}
	return &commitErrorTxn{Transaction: txn, err: s.err}, nil
}

type commitErrorTxn struct {
	tidbkv.Transaction
	err error
}

func (txn *commitErrorTxn) Commit(context.Context) error {
	if err := txn.Transaction.Rollback(); err != nil {
		return err
	}
	return txn.err
}

func TestDeleteBufferedKeysReturnsCommitError(t *testing.T) {
	// Regression test for https://github.com/pingcap/tidb/issues/69792.
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	key := tidbkv.Key("commit-error/conflict-key")
	value := []byte("still-present")

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(key, value))
	require.NoError(t, txn.Commit(ctx))

	commitErr := goerrors.New("injected commit error")
	deleter := &Deleter{
		store:  &commitErrorStorage{Storage: store, err: commitErr},
		logger: zap.NewNop(),
	}
	err = deleter.deleteBufferedKeys(ctx, []tidbkv.Key{key})
	require.ErrorIs(t, err, commitErr)

	readTxn, err := store.Begin()
	require.NoError(t, err)
	got, err := readTxn.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, got.Value)
	require.NoError(t, readTxn.Rollback())
}
