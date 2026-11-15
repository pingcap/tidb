// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkReadBudgetSharesCapacityAndCancelsWaiters(t *testing.T) {
	limiter := spfreshBulkReadLimiter{changed: make(chan struct{})}
	budget := func() uint64 { return 100 }
	first, err := limiter.acquire(context.Background(), 40, budget)
	require.NoError(t, err)
	second, err := limiter.acquire(context.Background(), 40, budget)
	require.NoError(t, err)
	require.Equal(t, uint64(80), limiter.used)
	ctx, cancel := context.WithCancel(context.Background())
	waiting, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := limiter.acquire(ctx, 30, func() uint64 { close(waiting); return 100 })
		done <- err
	}()
	<-waiting
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, uint64(80), limiter.used)
	first()
	second()
	require.Zero(t, limiter.used)
}

func TestSPFreshBulkReadBudgetRejectsImpossibleReservations(t *testing.T) {
	for _, limit := range []uint64{0, 100} {
		limiter := spfreshBulkReadLimiter{changed: make(chan struct{})}
		weight := spfreshBulkReadWeight(math.MaxInt64, math.MaxUint64)
		require.Equal(t, uint64(math.MaxUint64), weight)
		release, err := limiter.acquire(context.Background(), weight, func() uint64 { return limit })
		require.ErrorContains(t, err, "exceeds budget")
		require.Nil(t, release)
		require.Zero(t, limiter.used)
	}
}

func TestSPFreshBulkReadBudgetDrainsAfterQuotaReduction(t *testing.T) {
	limiter := spfreshBulkReadLimiter{changed: make(chan struct{})}
	first, err := limiter.acquire(context.Background(), 80, func() uint64 { return 100 })
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiting, done := make(chan struct{}, 1), make(chan func(), 1)
	go func() {
		release, err := limiter.acquire(ctx, 20, func() uint64 {
			select {
			case waiting <- struct{}{}:
			default:
			}
			return 40
		})
		if err != nil {
			done <- nil
			return
		}
		done <- release
	}()
	<-waiting
	select {
	case <-done:
		t.Fatal("admitted above the reduced quota")
	default:
	}
	first()
	release := <-done
	require.NotNil(t, release)
	release()
	require.Zero(t, limiter.used)
}

func TestSPFreshBulkImportLimitsReserveMemoryAndCPU(t *testing.T) {
	for _, tc := range []struct {
		memory uint64
		cpus   int
		batch  int64
		slots  int
	}{
		{0, 24, 0, 0},
		{1 << 30, 1, 64 << 20, 1},
		{1 << 30, 24, 64 << 20, 2},
		{16 << 30, 24, 256 << 20, 8},
		{16 << 30, 4, 256 << 20, 3},
		{16 << 30, 6, 256 << 20, 5},
		{16 << 30, 12, 256 << 20, 8},
		{256 << 30, 24, 256 << 20, 18},
	} {
		batch, slots := spfreshBulkImportLimits(tc.memory, tc.cpus)
		require.Equal(t, tc.batch, batch)
		require.Equal(t, tc.slots, slots)
		require.LessOrEqual(t, uint64(batch)*uint64(slots)*2, tc.memory/4)
	}
}

func TestSPFreshBulkReadBudgetRespectsCurrentMemoryLimit(t *testing.T) {
	oldTotal, oldLimit := memory.MemTotal, memory.ServerMemoryLimit.Load()
	t.Cleanup(func() { memory.MemTotal = oldTotal; memory.ServerMemoryLimit.Store(oldLimit) })
	memory.MemTotal = func() (uint64, error) { return 800, nil }
	memory.ServerMemoryLimit.Store(400)
	require.Equal(t, uint64(200), spfreshBulkReadMemoryBudget())
	memory.ServerMemoryLimit.Store(0)
	require.Equal(t, uint64(400), spfreshBulkReadMemoryBudget())
	memory.MemTotal = func() (uint64, error) { return 0, errors.New("unavailable") }
	require.Zero(t, spfreshBulkReadMemoryBudget())
}

func TestSPFreshBulkReadBudgetUsesRecordViewsInsteadOfPayloadMultiplier(t *testing.T) {
	const size = int64(1 << 30)
	const records = uint64(1 << 20)
	weight := spfreshBulkReadWeight(size, records)
	require.Equal(t, uint64(size)+uint64(size)/8+records*48+(2<<20), weight)
	require.Less(t, weight, uint64(size)*2)
	require.Greater(t, spfreshBulkReadWeight(size, uint64(size)/12), weight)
	require.Equal(t, uint64(math.MaxUint64), spfreshBulkReadWeight(size, math.MaxUint64))
	limiter := spfreshBulkReadLimiter{changed: make(chan struct{})}
	budget := func() uint64 { return 3 << 30 }
	first, err := limiter.acquire(context.Background(), weight, budget)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := limiter.acquire(ctx, weight, budget)
	require.NoError(t, err)
	first()
	second()
	require.Zero(t, limiter.used)
}
