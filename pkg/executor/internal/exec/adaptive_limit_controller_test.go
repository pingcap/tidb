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

func TestAdaptiveLimitControllerUsesCurrentExecutionYield(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000)

	reserved, ok := controller.ReserveOuter(context.Background(), 25000)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	controller.CommitOuter(reserved, reserved)
	controller.ObserveJoinProgress(32, 32)

	reserved, ok = controller.ReserveOuter(context.Background(), 25000)
	require.True(t, ok)
	require.Greater(t, reserved, 32)
	require.LessOrEqual(t, reserved, 1250)

	snapshot := controller.Snapshot()
	require.Equal(t, uint64(32), snapshot.OuterFetched)
	require.Equal(t, uint64(32), snapshot.OuterConsumed)
	require.Equal(t, uint64(32), snapshot.OutputRows)

	tailController := NewAdaptiveLimitController(1000, 1024, 100000)
	reserved, ok = tailController.ReserveOuter(context.Background(), 25000)
	require.True(t, ok)
	require.Equal(t, 1000, reserved)
	tailController.CommitOuter(reserved, reserved)
	tailController.ObserveJoinProgress(999, 999)
	require.Equal(t, uint64(1), tailController.Snapshot().DesiredWindow)

	midController := NewAdaptiveLimitController(1000, 500, 100000)
	reserved, ok = midController.ReserveOuter(context.Background(), 25000)
	require.True(t, ok)
	require.Equal(t, 500, reserved)
	midController.CommitOuter(reserved, reserved)
	midController.ObserveJoinProgress(500, 500)
	require.Equal(t, uint64(563), midController.Snapshot().DesiredWindow)
}

func TestAdaptiveLimitControllerGrowsWhenConsumedInputHasNoOutput(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000)

	for _, expected := range []int{32, 64, 128} {
		reserved, ok := controller.ReserveOuter(context.Background(), 25000)
		require.True(t, ok)
		require.Equal(t, expected, reserved)
		controller.CommitOuter(reserved, reserved)
		controller.ObserveJoinProgress(reserved, 0)
	}
}

func TestAdaptiveLimitControllerStopInterruptsReservation(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000)
	reserved, ok := controller.ReserveOuter(context.Background(), 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, reserved)
	lookupReserved, ok := controller.ReserveLookup(context.Background(), 32)
	require.True(t, ok)

	result := make(chan bool, 1)
	go func() {
		_, admitted := controller.ReserveOuter(context.Background(), 32)
		result <- admitted
	}()

	select {
	case <-result:
		require.Fail(t, "reservation should wait for consumption or stop")
	case <-time.After(20 * time.Millisecond):
	}

	controller.Stop()
	snapshot := controller.Snapshot()
	require.True(t, snapshot.Stopped)
	require.Zero(t, snapshot.DesiredWindow)
	require.Zero(t, snapshot.LookupReserved)
	require.Equal(t, uint64(lookupReserved), snapshot.LookupDiscarded)
	select {
	case admitted := <-result:
		require.False(t, admitted)
	case <-time.After(time.Second):
		require.Fail(t, "stop did not wake the blocked reservation")
	}

	controller.Reset()
	reserved, ok = controller.ReserveOuter(context.Background(), 32)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	snapshot = controller.Snapshot()
	require.False(t, snapshot.Stopped)
	require.Zero(t, snapshot.OuterFetched)
	require.Zero(t, snapshot.LookupDiscarded)
}

func TestAdaptiveLimitControllerBoundsLookupAndScanAdmission(t *testing.T) {
	controller := NewAdaptiveLimitController(1000, 32, 100000)

	reserved, ok := controller.ReserveLookup(context.Background(), 1000)
	require.True(t, ok)
	require.Equal(t, 32, reserved)
	require.Equal(t, 1, controller.ScanConcurrencyLimit(15, 32))

	controller.ReleaseLookup(reserved)
	reserved, ok = controller.ReserveOuter(context.Background(), 32)
	require.True(t, ok)
	controller.CommitOuter(reserved, 32)
	controller.ObserveJoinProgress(32, 32)
	require.Greater(t, controller.ScanConcurrencyLimit(15, 32), 1)
	require.LessOrEqual(t, controller.ScanConcurrencyLimit(15, 32), 15)
}

func BenchmarkAdaptiveLimitControllerObserveJoinProgress(b *testing.B) {
	controller := NewAdaptiveLimitController(^uint64(0), 1024, 100000)
	reserved, ok := controller.ReserveOuter(context.Background(), 1024)
	if !ok {
		b.Fatal("failed to reserve the initial outer window")
	}
	controller.CommitOuter(reserved, reserved)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		controller.ObserveJoinProgress(1, 1)
	}
}
