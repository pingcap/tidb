// Copyright 2015 PingCAP, Inc.
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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackOff(t *testing.T) {
	mustBackOff(t, 1, 2)
	mustBackOff(t, 2, 4)
	mustBackOff(t, 3, 8)
	mustBackOff(t, 100000, 100)
}

func mustBackOff(t *testing.T, cnt uint, sleep int) {
	assert.LessOrEqual(t, BackOff(cnt), sleep*int(time.Millisecond))
}

func TestRetryExceedCountError(t *testing.T) {
	t.Parallel()

	defer func(cnt uint) {
		maxRetryCnt = cnt
	}(maxRetryCnt)

	maxRetryCnt = 5
	err := RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return nil
	})
	assert.NotNil(t, err)

	err = RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return ErrTxnRetryable
	})
	assert.NotNil(t, err)

	err = RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return errors.New("do not retry")
	})
	assert.NotNil(t, err)

	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)
	storage := NewInjectedStore(newMockStorage(), &cfg)
	err = RunInNewTxn(context.Background(), storage, true, func(ctx context.Context, txn Transaction) error {
		return nil
	})
	assert.NotNil(t, err)
}
