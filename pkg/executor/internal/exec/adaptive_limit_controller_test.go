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

package exec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func reserveOuterForTest(t testing.TB, controller *AdaptiveLimitController, maxRows int) (int, bool) {
	t.Helper()
	reserved, ok, err := controller.ReserveOuter(context.Background(), maxRows)
	require.NoError(t, err)
	return reserved, ok
}

func reserveLookupForTest(t testing.TB, controller *AdaptiveLimitController, maxRows int) (int, bool) {
	t.Helper()
	reserved, ok, err := controller.ReserveLookup(context.Background(), maxRows)
	require.NoError(t, err)
	return reserved, ok
}

func TestAdaptiveLimitControllerUsesCurrentExecutionYield(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)

	reserved, ok := reserveOuterForTest(t, controller, 25000)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	controller.CommitOuter(reserved, reserved)
	controller.ObserveJoinProgress(32, 32)

	reserved, ok = reserveOuterForTest(t, controller, 25000)
	require.True(t, ok)
	require.Equal(t, 64, reserved)

	snapshot := controller.Snapshot()
	require.Equal(t, uint64(32), snapshot.OuterFetched)
	require.Equal(t, uint64(32), snapshot.OuterConsumed)
	require.Equal(t, uint64(32), snapshot.OutputRows)

	tailController := NewAdaptiveLimitController(1000, 1024, 100000, 1024, 100000)
	reserved, ok = reserveOuterForTest(t, tailController, 25000)
	require.True(t, ok)
	require.Equal(t, 1000, reserved)
	tailController.CommitOuter(reserved, reserved)
	tailController.ObserveJoinProgress(999, 999)
	require.Equal(t, uint64(1), tailController.Snapshot().OuterWindow)

	midController := NewAdaptiveLimitController(1000, 500, 100000, 500, 100000)
	reserved, ok = reserveOuterForTest(t, midController, 25000)
	require.True(t, ok)
	require.Equal(t, 500, reserved)
	midController.CommitOuter(reserved, reserved)
	midController.ObserveJoinProgress(500, 500)
	require.Equal(t, uint64(563), midController.Snapshot().OuterWindow)

	phaseController := NewAdaptiveLimitController(1000, 1000, 100000, 1000, 100000)
	reserved, ok = reserveOuterForTest(t, phaseController, 1000)
	require.True(t, ok)
	phaseController.CommitOuter(reserved, reserved)
	phaseController.ObserveJoinProgress(900, 900)
	phaseController.ObserveJoinProgress(100, 10)
	require.Equal(t, uint64(99), phaseController.Snapshot().OuterWindow)
}

func TestAdaptiveLimitControllerAllowsOneGrowthPerProgressEpoch(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)

	reserved, ok := reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)
	controller.ObserveJoinProgress(1, 1)
	require.Equal(t, uint64(64), controller.Snapshot().OuterWindow)

	// More callbacks from the same fetched batch must not compound the growth.
	controller.ObserveJoinProgress(1, 1)
	controller.ObserveJoinProgress(1, 1)
	require.Equal(t, uint64(64), controller.Snapshot().OuterWindow)

	reserved, ok = reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)
	controller.ObserveJoinProgress(29, 29)
	require.Equal(t, uint64(64), controller.Snapshot().OuterWindow)
	controller.ObserveJoinProgress(1, 1)
	require.Equal(t, uint64(128), controller.Snapshot().OuterWindow)

	lookupController := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)
	reserved, ok = reserveLookupForTest(t, lookupController, 32)
	require.True(t, ok)
	lookupController.CompleteLookup(reserved, reserved, 1)
	require.Equal(t, uint64(64), lookupController.Snapshot().LookupWindow)

	reserved, ok = reserveOuterForTest(t, lookupController, 32)
	require.True(t, ok)
	lookupController.CommitOuter(reserved, reserved)
	lookupController.ObserveJoinProgress(1, 1)
	require.Equal(t, uint64(64), lookupController.Snapshot().LookupWindow)

	reserved, ok = reserveLookupForTest(t, lookupController, 64)
	require.True(t, ok)
	lookupController.CompleteLookup(reserved, reserved, 1)
	require.Equal(t, uint64(128), lookupController.Snapshot().LookupWindow)
}

func TestAdaptiveLimitControllerPairsOutputWithCompletedOuterRows(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)
	reserved, ok := reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)

	// One outer row can fill several result chunks before it is fully consumed.
	for range 4 {
		controller.ObserveJoinProgress(0, 8)
	}
	snapshot := controller.Snapshot()
	require.Equal(t, uint64(32), snapshot.OutputRows)
	require.Equal(t, uint64(32), snapshot.OuterWindow)
	recentInput, recentOutput := controller.recentOuterYield.totals()
	require.Zero(t, recentInput)
	require.Zero(t, recentOutput)

	controller.ObserveJoinProgress(1, 0)
	snapshot = controller.Snapshot()
	require.Equal(t, uint64(1), snapshot.OuterConsumed)
	require.Equal(t, uint64(39), snapshot.OuterWindow)
	recentInput, recentOutput = controller.recentOuterYield.totals()
	require.Equal(t, uint64(1), recentInput)
	require.Equal(t, uint64(32), recentOutput)
}

func TestAdaptiveLimitControllerGrowsWhenConsumedInputHasNoOutput(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)

	for _, expected := range []int{32, 64, 128} {
		reserved, ok := reserveOuterForTest(t, controller, 25000)
		require.True(t, ok)
		require.Equal(t, expected, reserved)
		controller.CommitOuter(reserved, reserved)
		controller.ObserveJoinProgress(reserved, 0)
	}

	// A sparse phase must recover even after an earlier high-yield phase shrank
	// the window to one row.
	controller = NewAdaptiveLimitController(1000, 1000, 100000, 1000, 100000)
	reserved, ok := reserveOuterForTest(t, controller, 1000)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)
	controller.ObserveJoinProgress(999, 999)
	require.Equal(t, uint64(1), controller.Snapshot().OuterWindow)
	controller.ObserveJoinProgress(1, 0)
	require.Equal(t, uint64(2), controller.Snapshot().OuterWindow)
	recentInput, recentOutput := controller.recentOuterYield.totals()
	require.Zero(t, recentInput)
	require.Zero(t, recentOutput)
	for _, expected := range []uint64{4, 8} {
		reserved, ok = reserveOuterForTest(t, controller, 1000)
		require.True(t, ok)
		controller.CommitOuter(reserved, reserved)
		controller.ObserveJoinProgress(reserved, 0)
		require.Equal(t, expected, controller.Snapshot().OuterWindow)
	}
}

func TestAdaptiveLimitControllerStopInterruptsReservation(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)
	reserved, ok := reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)
	lookupReserved, ok := reserveLookupForTest(t, controller, 32)
	require.True(t, ok)

	type reservationResult struct {
		admitted bool
		err      error
	}
	result := make(chan reservationResult, 1)
	go func() {
		_, admitted, err := controller.ReserveOuter(context.Background(), 32)
		result <- reservationResult{admitted: admitted, err: err}
	}()

	select {
	case <-result:
		require.Fail(t, "reservation should wait for consumption or stop")
	case <-time.After(20 * time.Millisecond):
	}

	controller.Stop()
	snapshot := controller.Snapshot()
	require.True(t, snapshot.Stopped)
	require.Zero(t, snapshot.OuterWindow)
	require.Zero(t, snapshot.LookupWindow)
	require.Zero(t, snapshot.LookupReserved)
	require.Equal(t, uint64(reserved), snapshot.OuterOutstandingAtStop)
	require.Equal(t, uint64(lookupReserved), snapshot.LookupOutstandingAtStop)
	select {
	case result := <-result:
		require.NoError(t, result.err)
		require.False(t, result.admitted)
	case <-time.After(time.Second):
		require.Fail(t, "stop did not wake the blocked reservation")
	}

	controller.Reset()
	reserved, ok = reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	snapshot = controller.Snapshot()
	require.False(t, snapshot.Stopped)
	require.Zero(t, snapshot.OuterFetched)
	require.Zero(t, snapshot.OuterOutstandingAtStop)
	require.Zero(t, snapshot.LookupOutstandingAtStop)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok, err := controller.ReserveOuter(canceledCtx, 1)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	controller.Stop()
	controller.Stop()

	availableController := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)
	_, ok, err = availableController.ReserveOuter(canceledCtx, 1)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, availableController.Snapshot().OuterReserved)
	_, ok, err = availableController.ReserveLookup(canceledCtx, 1)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, availableController.Snapshot().LookupReserved)

	blockedOuterController := NewAdaptiveLimitController(1000, 1, 1, 1, 1)
	reserved, ok = reserveOuterForTest(t, blockedOuterController, 1)
	require.True(t, ok)
	blockedOuterController.CommitOuter(reserved, reserved)
	blockedOuterCtx, cancelBlockedOuter := context.WithCancel(context.Background())
	blockedOuterResult := make(chan reservationResult, 1)
	go func() {
		_, admitted, err := blockedOuterController.ReserveOuter(blockedOuterCtx, 1)
		blockedOuterResult <- reservationResult{admitted: admitted, err: err}
	}()
	select {
	case <-blockedOuterResult:
		require.Fail(t, "outer reservation did not block")
	case <-time.After(20 * time.Millisecond):
	}
	cancelBlockedOuter()
	select {
	case result := <-blockedOuterResult:
		require.False(t, result.admitted)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "cancel did not wake the blocked outer reservation")
	}
	require.Zero(t, blockedOuterController.Snapshot().OuterReserved)

	blockedLookupController := NewAdaptiveLimitController(1000, 1, 1, 1, 1)
	reserved, ok = reserveLookupForTest(t, blockedLookupController, 1)
	require.True(t, ok)
	blockedLookupCtx, cancelBlockedLookup := context.WithCancel(context.Background())
	blockedLookupResult := make(chan reservationResult, 1)
	go func() {
		_, admitted, err := blockedLookupController.ReserveLookup(blockedLookupCtx, 1)
		blockedLookupResult <- reservationResult{admitted: admitted, err: err}
	}()
	select {
	case <-blockedLookupResult:
		require.Fail(t, "lookup reservation did not block")
	case <-time.After(20 * time.Millisecond):
	}
	cancelBlockedLookup()
	select {
	case result := <-blockedLookupResult:
		require.False(t, result.admitted)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "cancel did not wake the blocked lookup reservation")
	}
	require.Equal(t, uint64(reserved), blockedLookupController.Snapshot().LookupReserved)

	pendingController := NewAdaptiveLimitController(1000, 64, 100000, 32, 100000)
	fetched, ok := reserveOuterForTest(t, pendingController, 32)
	require.True(t, ok)
	pendingController.CommitOuter(fetched, fetched)
	pending, ok := reserveOuterForTest(t, pendingController, 32)
	require.True(t, ok)
	pendingController.Stop()
	require.Equal(t, uint64(fetched+pending), pendingController.Snapshot().OuterOutstandingAtStop)
}

func TestAdaptiveLimitControllerBoundsLookupAndScanAdmission(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)

	reserved, ok := reserveLookupForTest(t, controller, 1000)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	require.Equal(t, 1, controller.SuggestedScanConcurrency(15))

	controller.CompleteLookup(reserved, reserved, 1)
	reserved, ok = reserveOuterForTest(t, controller, 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, 32)
	controller.ObserveJoinProgress(32, 32)
	snapshot := controller.Snapshot()
	require.Equal(t, uint64(64), snapshot.OuterWindow)
	require.Equal(t, uint64(64), snapshot.LookupWindow)
	require.Equal(t, uint64(32), snapshot.LookupHandles)
	require.Equal(t, uint64(1), snapshot.LookupRows)
	batchSize := controller.SuggestedBatchSize(1000)
	require.Equal(t, 64, batchSize)
	require.Equal(t, 2, controller.SuggestedScanConcurrency(15))

	reserved, ok = reserveLookupForTest(t, controller, 1000)
	require.True(t, ok)
	controller.AbortLookup(reserved)
	snapshot = controller.Snapshot()
	require.Zero(t, snapshot.LookupReserved)
	require.Equal(t, uint64(32), snapshot.LookupHandles)
	require.Equal(t, uint64(1), snapshot.LookupRows)

	phaseController := NewAdaptiveLimitController(1000, 64, 100000, 32, 100000)
	reserved, ok = reserveLookupForTest(t, phaseController, 1000)
	require.True(t, ok)
	phaseController.CompleteLookup(reserved, reserved, reserved)
	require.Equal(t, uint64(32), phaseController.Snapshot().LookupWindow)
	for _, expected := range []uint64{64, 128} {
		reserved, ok = reserveLookupForTest(t, phaseController, 1000)
		require.True(t, ok)
		phaseController.CompleteLookup(reserved, reserved, 0)
		require.Equal(t, expected, phaseController.Snapshot().LookupWindow)
		require.Equal(t, int(expected/32), phaseController.SuggestedScanConcurrency(15))
	}
	recentInput, recentOutput := phaseController.recentLookupYield.totals()
	require.Zero(t, recentInput)
	require.Zero(t, recentOutput)

	localYieldController := NewAdaptiveLimitController(1000, 64, 100000, 32, 100000)
	reserved, ok = reserveLookupForTest(t, localYieldController, 32)
	require.True(t, ok)
	localYieldController.CompleteLookup(reserved, reserved, reserved)
	reserved, ok = reserveLookupForTest(t, localYieldController, 32)
	require.True(t, ok)
	localYieldController.CompleteLookup(reserved, reserved, 4)
	require.Equal(t, uint64(50), localYieldController.Snapshot().LookupWindow)

	var recent adaptiveYieldWindow
	recent.add(900, 900)
	for range 3 {
		recent.add(100, 10)
	}
	inputs, outputs := recent.totals()
	require.Equal(t, uint64(1200), inputs)
	require.Equal(t, uint64(930), outputs)
	recent.add(100, 10)
	inputs, outputs = recent.totals()
	require.Equal(t, uint64(400), inputs)
	require.Equal(t, uint64(40), outputs)

	partialController := NewAdaptiveLimitController(1000, 32, 100000, 32, 100000)
	reserved, ok = reserveLookupForTest(t, partialController, 1000)
	require.True(t, ok)
	partialController.AbortLookup(reserved - 10)
	partialController.CompleteLookup(10, 10, 5)
	snapshot = partialController.Snapshot()
	require.Zero(t, snapshot.LookupReserved)
	require.Equal(t, uint64(10), snapshot.LookupHandles)
	require.Equal(t, uint64(5), snapshot.LookupRows)
}

func BenchmarkAdaptiveLimitControllerObserveJoinProgress(b *testing.B) {
	controller := NewAdaptiveLimitController(^uint64(0), 1024, 100000, 1024, 100000)
	// Keep fetched input available throughout the benchmark so every iteration
	// exercises yield sampling and window recomputation.
	controller.outerFetched = ^uint64(0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		controller.ObserveJoinProgress(1, 1)
	}
}

func BenchmarkAdaptiveLimitControllerReservationRoundTrip(b *testing.B) {
	controller := NewAdaptiveLimitController(^uint64(0), 1, 1, 1, 1)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		reserved, ok, err := controller.ReserveOuter(ctx, 1)
		if err != nil || !ok {
			b.Fatalf("failed to reserve outer rows: ok=%v err=%v", ok, err)
		}
		controller.CommitOuter(reserved, reserved)
		controller.ObserveJoinProgress(reserved, reserved)
	}
}
