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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

// TestConcurrentAlloc is used for the test that
// multiple allocators allocate ID with the same table ID concurrently.
func TestConcurrentAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	autoid.SetStep(100)
	defer func() {
		autoid.SetStep(5000)
	}()

	dbID := int64(2)
	tblID := int64(100)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocIDs := func() {
		ctx := context.Background()
		alloc := autoid.NewAllocator(store, dbID, false, autoid.RowIDAllocType)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			_, id, err1 := alloc.Alloc(ctx, tblID, 1, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			mu.Lock()
			if _, ok := m[id]; ok {
				errCh <- fmt.Errorf("duplicate id:%v", id)
				mu.Unlock()
				break
			}
			m[id] = struct{}{}
			mu.Unlock()

			// test Alloc N
			N := rand.Uint64() % 100
			min, max, err1 := alloc.Alloc(ctx, tblID, N, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			for i := min + 1; i <= max; i++ {
				if _, ok := m[i]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", i)
					errFlag = true
					mu.Unlock()
					break
				}
				m[i] = struct{}{}
			}
			if errFlag {
				break
			}
			mu.Unlock()
		}
	}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocIDs()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	require.NoError(t, err)
}

func TestConcurrentAllocSequence(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	var seq *model.SequenceInfo
	var sequenceBase int64
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err1 := m.CreateDatabase(&model.DBInfo{ID: 2, Name: model.NewCIStr("a")})
		require.NoError(t, err1)
		seq = &model.SequenceInfo{
			Start:      100,
			Cycle:      false,
			Cache:      true,
			MinValue:   -100,
			MaxValue:   100,
			Increment:  -2,
			CacheValue: 3,
		}
		seqTable := &model.TableInfo{
			ID:       2,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		if seq.Increment >= 0 {
			sequenceBase = seq.Start - 1
		} else {
			sequenceBase = seq.Start + 1
		}
		err1 = m.CreateSequenceAndSetSeqValue(2, seqTable, sequenceBase)
		require.NoError(t, err1)
		return nil
	})
	require.NoError(t, err)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocSequence := func() {
		alloc := autoid.NewSequenceAllocator(store, 2, seq)
		for j := 0; j < 3; j++ {
			base, end, _, err1 := alloc.AllocSeqCache(2)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			// sequence is negative-growth here.
			for i := base - 1; i >= end; i-- {
				if _, ok := m[i]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", i)
					errFlag = true
					mu.Unlock()
					break
				}
				m[i] = struct{}{}
			}
			if errFlag {
				break
			}
			mu.Unlock()
		}
	}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(num int) {
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocSequence()
			wg.Done()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	require.NoError(t, err)
}
