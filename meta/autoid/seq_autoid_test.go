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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestSequenceAutoid(t *testing.T) {
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
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		seq = &model.SequenceInfo{
			Start:      1,
			Cycle:      true,
			Cache:      true,
			MinValue:   -10,
			MaxValue:   10,
			Increment:  2,
			CacheValue: 3,
		}
		seqTable := &model.TableInfo{
			ID:       1,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		sequenceBase = seq.Start - 1
		err = m.CreateSequenceAndSetSeqValue(1, seqTable, sequenceBase)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	alloc := autoid.NewSequenceAllocator(store, 1, 1, seq)
	require.NotNil(t, alloc)

	// allocate sequence cache.
	base, end, round, err := alloc.AllocSeqCache()
	require.NoError(t, err)
	require.Equal(t, int64(0), base)
	require.Equal(t, int64(5), end)
	require.Equal(t, int64(0), round)

	// test the sequence batch size.
	offset := seq.Start
	size, err := autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	require.NoError(t, err)
	require.Equal(t, end-base, size)

	// simulate the next value allocation.
	nextVal, ok := autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(1), nextVal)
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(3), nextVal)
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(5), nextVal)

	base, end, round, err = alloc.AllocSeqCache()
	require.NoError(t, err)
	require.Equal(t, int64(5), base)
	require.Equal(t, int64(10), end)
	require.Equal(t, int64(0), round)

	// test the sequence batch size.
	size, err = autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	require.NoError(t, err)
	require.Equal(t, end-base, size)

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(7), nextVal)
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(9), nextVal)
	base = nextVal

	_, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	// the rest in cache in not enough for next value.
	require.False(t, ok)

	base, end, round, err = alloc.AllocSeqCache()
	require.NoError(t, err)
	require.Equal(t, int64(-11), base)
	require.Equal(t, int64(-6), end)
	// the round is already in cycle.
	require.Equal(t, int64(1), round)

	// test the sequence batch size.
	size, err = autoid.CalcSequenceBatchSize(sequenceBase, seq.CacheValue, seq.Increment, offset, seq.MinValue, seq.MaxValue)
	require.NoError(t, err)
	require.Equal(t, end-base, size)

	offset = seq.MinValue
	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(-10), nextVal)
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(-8), nextVal)
	base = nextVal

	nextVal, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	require.True(t, ok)
	require.Equal(t, int64(-6), nextVal)
	base = nextVal

	_, ok = autoid.SeekToFirstSequenceValue(base, seq.Increment, offset, base, end)
	// the cache is already empty.
	require.False(t, ok)
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
	var wg util.WaitGroupWrapper
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocSequence := func() {
		alloc := autoid.NewSequenceAllocator(store, 2, 2, seq)
		for j := 0; j < 3; j++ {
			base, end, _, err1 := alloc.AllocSeqCache()
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
		num := i
		wg.Run(func() {
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocSequence()
		})
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	require.NoError(t, err)
}
