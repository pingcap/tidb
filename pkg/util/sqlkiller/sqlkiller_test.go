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

<<<<<<< HEAD
import "testing"

func TestConnectionAliveCallbackCleanupKeepsNewerCallback(t *testing.T) {
	var killer SQLKiller
	var oldCallbackCalls, newCallbackCalls int

	oldCallback := func() bool {
		oldCallbackCalls++
		return false
	}
	newCallback := func() bool {
		newCallbackCalls++
		return true
	}

	killer.IsConnectionAlive.Store(&oldCallback)
	killer.IsConnectionAlive.Store(&newCallback)

	killer.IsConnectionAlive.CompareAndSwap(&oldCallback, nil)
	killer.CheckConnectionAlive()

	if oldCallbackCalls != 0 {
		t.Fatalf("expected old callback not to be called, got %d calls", oldCallbackCalls)
	}
	if newCallbackCalls != 1 {
		t.Fatalf("expected newer callback to remain installed, got %d calls", newCallbackCalls)
	}
	if killer.GetKillSignal() != UnspecifiedKillSignal {
		t.Fatalf("expected no kill signal from the newer alive callback, got %d", killer.GetKillSignal())
	}

	killer.IsConnectionAlive.CompareAndSwap(&newCallback, nil)
	killer.CheckConnectionAlive()
	if newCallbackCalls != 1 {
		t.Fatalf("expected newer callback to be cleared, got %d calls", newCallbackCalls)
	}
=======
import (
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
>>>>>>> 5af03a2e108 (util: prevent SQLKiller state races during concurrent reset (#70557))
}
