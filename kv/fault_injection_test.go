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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestFaultInjectionBasic(t *testing.T) {
	t.Parallel()

	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)

	storage := NewInjectedStore(newMockStorage(), &cfg)
	txn, err := storage.Begin()
	require.Nil(t, err)

	_, err = storage.BeginWithOption(tikv.DefaultStartTSOption().SetTxnScope(GlobalTxnScope).SetStartTS(0))
	require.Nil(t, err)

	ver := Version{Ver: 1}
	snap := storage.GetSnapshot(ver)
	b, err := txn.Get(context.TODO(), []byte{'a'})
	assert.NotNil(t, err)
	assert.Equal(t, err1.Error(), err.Error())
	assert.Nil(t, b)

	b, err = snap.Get(context.TODO(), []byte{'a'})
	assert.NotNil(t, err)
	assert.Equal(t, err1.Error(), err.Error())
	assert.Nil(t, b)

	bs, err := snap.BatchGet(context.Background(), nil)
	assert.NotNil(t, err)
	assert.Equal(t, err1.Error(), err.Error())
	assert.Nil(t, bs)

	bs, err = txn.BatchGet(context.Background(), nil)
	assert.NotNil(t, err)
	assert.Equal(t, err1.Error(), err.Error())
	assert.Nil(t, bs)

	err = txn.Commit(context.Background())
	assert.NotNil(t, err)
	assert.Equal(t, err1.Error(), err.Error())

	cfg.SetGetError(nil)
	cfg.SetCommitError(nil)

	storage = NewInjectedStore(newMockStorage(), &cfg)
	txn, err = storage.Begin()
	assert.Nil(t, err)

	snap = storage.GetSnapshot(ver)
	b, err = txn.Get(context.TODO(), []byte{'a'})
	assert.Nil(t, err)
	assert.Nil(t, b)

	bs, err = txn.BatchGet(context.Background(), nil)
	assert.Nil(t, err)
	assert.Nil(t, bs)

	b, err = snap.Get(context.TODO(), []byte{'a'})
	assert.True(t, terror.ErrorEqual(ErrNotExist, err))
	assert.Nil(t, b)

	bs, err = snap.BatchGet(context.Background(), []Key{[]byte("a")})
	assert.Nil(t, err)
	assert.Len(t, bs, 0)

	err = txn.Commit(context.Background())
	assert.NotNil(t, err)
	assert.True(t, terror.ErrorEqual(err, ErrTxnRetryable))
}
