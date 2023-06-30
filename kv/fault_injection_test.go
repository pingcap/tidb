// Copyright 2017 PingCAP, Inc.
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

package kv

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestFaultInjectionBasic(t *testing.T) {
	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)

	storage := NewInjectedStore(newMockStorage(), &cfg)
	txn, err := storage.Begin()
	require.NoError(t, err)

	_, err = storage.Begin(tikv.WithTxnScope(GlobalTxnScope), tikv.WithStartTS(0))
	require.NoError(t, err)

	ver := Version{Ver: 1}
	snap := storage.GetSnapshot(ver)
	b, err := txn.Get(context.TODO(), []byte{'a'})
	require.NotNil(t, err)
	require.Equal(t, err1.Error(), err.Error())
	require.Nil(t, b)

	b, err = snap.Get(context.TODO(), []byte{'a'})
	require.NotNil(t, err)
	require.Equal(t, err1.Error(), err.Error())
	require.Nil(t, b)

	bs, err := snap.BatchGet(context.Background(), nil)
	require.NotNil(t, err)
	require.Equal(t, err1.Error(), err.Error())
	require.Nil(t, bs)

	bs, err = txn.BatchGet(context.Background(), nil)
	require.NotNil(t, err)
	require.Equal(t, err1.Error(), err.Error())
	require.Nil(t, bs)

	err = txn.Commit(context.Background())
	require.NotNil(t, err)
	require.Equal(t, err1.Error(), err.Error())

	cfg.SetGetError(nil)
	cfg.SetCommitError(nil)

	storage = NewInjectedStore(newMockStorage(), &cfg)
	txn, err = storage.Begin()
	require.Nil(t, err)

	snap = storage.GetSnapshot(ver)
	b, err = txn.Get(context.TODO(), []byte{'a'})
	require.Nil(t, err)
	require.Nil(t, b)

	bs, err = txn.BatchGet(context.Background(), nil)
	require.Nil(t, err)
	require.Nil(t, bs)

	b, err = snap.Get(context.TODO(), []byte{'a'})
	require.True(t, terror.ErrorEqual(ErrNotExist, err))
	require.Nil(t, b)

	bs, err = snap.BatchGet(context.Background(), []Key{[]byte("a")})
	require.Nil(t, err)
	require.Len(t, bs, 0)

	err = txn.Commit(context.Background())
	require.NotNil(t, err)
	require.True(t, terror.ErrorEqual(err, ErrTxnRetryable))
}
