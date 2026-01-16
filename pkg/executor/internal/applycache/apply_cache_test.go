// Copyright 2020 PingCAP, Inc.
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

package applycache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit/testflag"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestApplyCache(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuotaApplyCache = 100
	applyCache, err := NewApplyCache(ctx)
	require.NoError(t, err)

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	value := make([]*chunk.List, 3)
	key := make([][]byte, 3)
	for i := range 3 {
		value[i] = chunk.NewList(fields, 1, 1)
		srcChunk := chunk.NewChunkWithCapacity(fields, 1)
		srcChunk.AppendInt64(0, int64(i))
		srcRow := srcChunk.GetRow(0)
		value[i].AppendRow(srcRow)
		key[i] = []byte(strings.Repeat(strconv.Itoa(i), 100))

		// TODO: *chunk.List.GetMemTracker().BytesConsumed() is not accurate, fix it later.
		require.Equal(t, int64(100), applyCacheKVMem(key[i], value[i]))
	}

	ok, err := applyCache.Set(key[0], value[0])
	require.NoError(t, err)
	require.True(t, ok)
	result, err := applyCache.Get(key[0])
	require.NoError(t, err)
	require.NotNil(t, result)

	ok, err = applyCache.Set(key[1], value[1])
	require.NoError(t, err)
	require.True(t, ok)
	result, err = applyCache.Get(key[1])
	require.NoError(t, err)
	require.NotNil(t, result)

	ok, err = applyCache.Set(key[2], value[2])
	require.NoError(t, err)
	require.True(t, ok)
	result, err = applyCache.Get(key[2])
	require.NoError(t, err)
	require.NotNil(t, result)

	// Both key[0] and key[1] are not in the cache
	result, err = applyCache.Get(key[0])
	require.NoError(t, err)
	require.Nil(t, result)

	result, err = applyCache.Get(key[1])
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestApplyCacheConcurrent(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuotaApplyCache = 100
	applyCache, err := NewApplyCache(ctx)
	require.NoError(t, err)

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	value := make([]*chunk.List, 2)
	key := make([][]byte, 2)
	for i := range 2 {
		value[i] = chunk.NewList(fields, 1, 1)
		srcChunk := chunk.NewChunkWithCapacity(fields, 1)
		srcChunk.AppendInt64(0, int64(i))
		srcRow := srcChunk.GetRow(0)
		value[i].AppendRow(srcRow)
		key[i] = []byte(strings.Repeat(strconv.Itoa(i), 100))

		// TODO: *chunk.List.GetMemTracker().BytesConsumed() is not accurate, fix it later.
		require.Equal(t, int64(100), applyCacheKVMem(key[i], value[i]))
	}

	ok, err := applyCache.Set(key[0], value[0])
	require.NoError(t, err)
	require.True(t, ok)

	rounds := 20
	if testflag.Long() {
		rounds = 100
	}

	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 2)

	waitNonNil := func(key []byte, timeout time.Duration) error {
		deadline := time.Now().Add(timeout)
		for {
			if ctx2.Err() != nil {
				return nil
			}
			result, err := applyCache.Get(key)
			if err != nil {
				return err
			}
			if result != nil {
				return nil
			}
			if time.Now().After(deadline) {
				return errors.New("timeout waiting apply cache entry")
			}
			time.Sleep(time.Millisecond)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range rounds {
			if err := waitNonNil(key[0], time.Second); err != nil {
				cancel()
				errCh <- err
				return
			}
			ok, err := applyCache.Set(key[1], value[1])
			if err != nil {
				cancel()
				errCh <- err
				return
			}
			if !ok {
				cancel()
				errCh <- errors.New("applyCache.Set unexpectedly returned ok=false")
				return
			}
		}
		errCh <- nil
	}()

	go func() {
		defer wg.Done()
		for range rounds {
			if err := waitNonNil(key[1], time.Second); err != nil {
				cancel()
				errCh <- err
				return
			}
			ok, err := applyCache.Set(key[0], value[0])
			if err != nil {
				cancel()
				errCh <- err
				return
			}
			if !ok {
				cancel()
				errCh <- errors.New("applyCache.Set unexpectedly returned ok=false")
				return
			}
		}
		errCh <- nil
	}()

	wg.Wait()
	for range 2 {
		require.NoError(t, <-errCh)
	}
	result, err := applyCache.Get(key[0])
	require.NoError(t, err)
	require.NotNil(t, result)
}
