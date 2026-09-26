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

package sqlkiller

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSQLKillerConcurrentReset(t *testing.T) {
	assertStateLockHeld := func(t *testing.T, killer *SQLKiller) {
		t.Helper()
		if killer.killEvent.TryLock() {
			killer.killEvent.Unlock()
			require.FailNow(t, "the SQLKiller state mutex is not held")
		}
	}
	getKillEventState := func(killer *SQLKiller) (bool, string) {
		killer.killEvent.Lock()
		defer killer.killEvent.Unlock()
		return killer.killEvent.triggered, killer.killEvent.desc
	}
	assertChanOpen := func(t *testing.T, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
			require.Fail(t, "kill event channel is closed")
		default:
		}
	}
	assertChanClosed := func(t *testing.T, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		default:
			require.Fail(t, "kill event channel is open")
		}
	}

	t.Run("reset after successful kill signal CAS", func(t *testing.T) {
		killer := &SQLKiller{}
		core, logs := observer.New(zap.WarnLevel)
		restoreLogger := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.WarnLevel),
		})
		defer restoreLogger()

		const beforeLogFailpoint = "github.com/pingcap/tidb/pkg/util/sqlkiller/" +
			"beforeLogKillSignal"
		testfailpoint.EnableCall(t, beforeLogFailpoint, func() {
			killer.Reset()
		})

		require.NotPanics(t, func() {
			killer.SendKillSignal(QueryInterrupted)
		})
		initiatedLogs := logs.FilterMessage("kill initiated").All()
		require.Len(t, initiatedLogs, 1)
		require.Equal(t, killer.getKillError(QueryInterrupted, "").Error(),
			initiatedLogs[0].ContextMap()["reason"])

		require.Equal(t, UnspecifiedKillSignal, killer.GetKillSignal())
		require.NoError(t, killer.HandleSignal())
		triggered, desc := getKillEventState(killer)
		require.False(t, triggered)
		require.Empty(t, desc)
		assertChanOpen(t, killer.GetKillEventChan())
	})

	t.Run("kill signal after reset clear", func(t *testing.T) {
		killer := &SQLKiller{}
		killSent := make(chan struct{})
		const reason = "memory usage exceeds the instance limit"
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/util/sqlkiller/afterResetKillSignalSwap", func() {
			assertStateLockHeld(t, killer)
			go func() {
				defer close(killSent)
				killer.SendKillSignalWithKillEventReason(KilledByMemArbitrator, reason)
			}()
		})

		killer.Reset()
		<-killSent

		require.Equal(t, KilledByMemArbitrator, killer.GetKillSignal())
		require.ErrorContains(t, killer.HandleSignal(), reason)
		triggered, desc := getKillEventState(killer)
		require.True(t, triggered)
		require.Equal(t, reason, desc)
		assertChanClosed(t, killer.GetKillEventChan())
	})
}

// TestConnCancelInvokedOnKill verifies the ConnCancel hook is invoked exactly
// once when a kill signal wins the first-writer CAS, and is left untouched by
// subsequent (losing) signals.
func TestConnCancelInvokedOnKill(t *testing.T) {
	killer := &SQLKiller{}
	var calls atomic.Int32
	cancel := func() { calls.Add(1) }
	killer.ConnCancel.Store(&cancel)

	// First signal wins the CAS and must invoke the hook.
	killer.SendKillSignal(QueryInterrupted)
	require.Equal(t, int32(1), calls.Load())

	// A second signal loses the CAS and must not invoke the hook again.
	killer.SendKillSignal(MaxExecTimeExceeded)
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, QueryInterrupted, killer.GetKillSignal())
}

// TestConnCancelNilIsSafe verifies sending a kill signal does not panic when no
// ConnCancel hook has been installed (the common short-statement path).
func TestConnCancelNilIsSafe(t *testing.T) {
	killer := &SQLKiller{}
	require.NotPanics(t, func() {
		killer.SendKillSignal(QueryInterrupted)
	})
	require.Equal(t, QueryInterrupted, killer.GetKillSignal())
}

// TestResetClearsConnCancel verifies Reset drops the ConnCancel hook so a stale
// cancel from a previous statement cannot fire on a reused session.
func TestResetClearsConnCancel(t *testing.T) {
	killer := &SQLKiller{}
	var calls atomic.Int32
	cancel := func() { calls.Add(1) }
	killer.ConnCancel.Store(&cancel)

	killer.Reset()
	require.Nil(t, killer.ConnCancel.Load())

	// After Reset the hook is gone, so a fresh kill must not reach it.
	killer.SendKillSignal(QueryInterrupted)
	require.Equal(t, int32(0), calls.Load())
}

// TestConnCancelRaceSignalVsRegistration races kill-signal delivery against
// ConnCancel registration. Regardless of interleaving, once a kill signal has
// been observed the cancel hook must have run at least once, so an in-flight
// RPC is always aborted instead of blocking until CoprReqTimeout.
func TestConnCancelRaceSignalVsRegistration(t *testing.T) {
	for range 200 {
		killer := &SQLKiller{}
		var calls atomic.Int32
		cancel := func() { calls.Add(1) }

		var wg sync.WaitGroup
		wg.Add(2)

		// Registration goroutine: install the hook via the production path, which
		// reconciles with any kill signal that already landed.
		go func() {
			defer wg.Done()
			killer.InstallConnCancel(&cancel)
		}()

		// Kill goroutine: deliver the signal concurrently.
		go func() {
			defer wg.Done()
			killer.SendKillSignal(QueryInterrupted)
		}()

		wg.Wait()

		require.Equal(t, QueryInterrupted, killer.GetKillSignal())
		require.GreaterOrEqual(t, calls.Load(), int32(1),
			"cancel hook must fire at least once when a kill signal is delivered")
	}
}
