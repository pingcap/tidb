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

//go:build intest

package mvservice

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitValueNoTime(t *testing.T, ch <-chan time.Time, spins int) time.Time {
	t.Helper()
	for range spins {
		select {
		case v := <-ch:
			return v
		default:
			runtime.Gosched()
		}
	}
	panic("unreachable")
}

func waitSignalNoTime(t *testing.T, ch <-chan struct{}, spins int) {
	t.Helper()
	for range spins {
		select {
		case <-ch:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("signal not received within spin budget")
}

func TestTimeProxyMockModuleClockOps(t *testing.T) {
	start := time.Date(2026, 2, 10, 9, 8, 7, 0, time.UTC)
	module := NewMockTimeModule(start)
	restore := InstallMockTimeModuleForTest(module)
	t.Cleanup(restore)

	require.Equal(t, start, mvsNow())
	require.Equal(t, 0*time.Second, mvsSince(start))
	require.Equal(t, 3*time.Second, mvsUntil(start.Add(3*time.Second)))

	module.Advance(5 * time.Second)
	require.Equal(t, start.Add(5*time.Second), mvsNow())
	require.Equal(t, 5*time.Second, mvsSince(start))
	require.Equal(t, 2*time.Second, mvsUntil(start.Add(7*time.Second)))

	require.Equal(t, time.Unix(123, 456), mvsUnix(123, 456))
	require.Equal(t, time.UnixMilli(789), mvsUnixMilli(789))
}

func TestTimeProxyMockModuleAfterAndSleep(t *testing.T) {
	start := time.Date(2026, 2, 10, 10, 0, 0, 0, time.UTC)
	module := NewMockTimeModule(start)
	restore := InstallMockTimeModuleForTest(module)
	t.Cleanup(restore)

	ch := mvsAfter(2 * time.Second)
	select {
	case <-ch:
		t.Fatal("after should not fire before due")
	default:
	}

	module.Advance(time.Second)
	select {
	case <-ch:
		t.Fatal("after should not fire before due")
	default:
	}

	module.Advance(time.Second)
	got := waitValueNoTime(t, ch, 10000)
	require.Equal(t, start.Add(2*time.Second), got)

	module.Set(start)
	mvsSleep(3 * time.Second)
	require.Equal(t, start.Add(3*time.Second), mvsNow())

	module.Set(start)
	module.SetAutoAdvanceOnSleep(false)
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(ready)
		mvsSleep(4 * time.Second)
		close(done)
	}()
	waitSignalNoTime(t, ready, 10000)

	module.Advance(3 * time.Second)
	select {
	case <-done:
		t.Fatal("sleep should still be blocked before due")
	default:
	}

	module.Advance(time.Second)
	waitSignalNoTime(t, done, 10000)
}

func TestTimeProxyMockModuleTimer(t *testing.T) {
	start := time.Date(2026, 2, 10, 11, 0, 0, 0, time.UTC)
	module := NewMockTimeModule(start)
	restore := InstallMockTimeModuleForTest(module)
	t.Cleanup(restore)

	timer := mvsNewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		t.Fatal("timer should not fire before due")
	default:
	}

	module.Advance(time.Second)
	select {
	case <-timer.C:
		t.Fatal("timer should not fire before due")
	default:
	}

	module.Advance(time.Second)
	_ = waitValueNoTime(t, timer.C, 10000)

	require.False(t, timer.Reset(3*time.Second))
	module.Advance(2 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("timer should not fire before reset due")
	default:
	}

	module.Advance(time.Second)
	_ = waitValueNoTime(t, timer.C, 10000)

	require.False(t, timer.Stop())
	require.False(t, timer.Reset(5*time.Second))
	require.True(t, timer.Reset(5*time.Second))
	require.True(t, timer.Stop())
	module.Advance(10 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("timer should not fire after stop")
	default:
	}
}

func TestTimeProxyFallbackWhenModuleNotInstalled(t *testing.T) {
	prev := activeMockTimeModule.Load()
	activeMockTimeModule.Store(nil)
	t.Cleanup(func() {
		activeMockTimeModule.Store(prev)
	})

	now := time.Now()
	got := mvsNow()
	require.WithinDuration(t, now, got, 100*time.Millisecond)

	ch := mvsAfter(10 * time.Millisecond)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected mvsAfter fallback to real timer")
	}

	timer := mvsNewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-time.After(time.Second):
		t.Fatal("expected mvsNewTimer fallback to real timer")
	}
}

func TestTimeProxyInstallPanicWhenModuleNil(t *testing.T) {
	require.PanicsWithValue(t, "mock time module is nil", func() {
		_ = InstallMockTimeModuleForTest(nil)
	})
}
