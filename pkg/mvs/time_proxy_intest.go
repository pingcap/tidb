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

package mvs

import (
	"sync"
	"sync/atomic"
	"time"
)

// MockTimeModule provides a controllable clock for tests.
// It supports virtual Now/After/Sleep/Since/Until and can be advanced manually.
type MockTimeModule struct {
	mu sync.Mutex

	now                time.Time
	autoAdvanceOnSleep bool
	afterWaiters       []*afterWaiter
}

type afterWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

type moduleTimerState struct {
	mu sync.Mutex

	ch      chan time.Time
	seq     uint64
	stopped bool
}

type mockTimerBackend struct {
	module *MockTimeModule
	state  *moduleTimerState
}

func newAfterWaiter(deadline time.Time) *afterWaiter {
	return &afterWaiter{
		deadline: deadline,
		ch:       make(chan time.Time, 1),
	}
}

// NewMockTimeModule creates a virtual clock module with initial time.
func NewMockTimeModule(initial time.Time) *MockTimeModule {
	return &MockTimeModule{
		now:                initial,
		autoAdvanceOnSleep: true,
	}
}

// SetAutoAdvanceOnSleep controls whether Sleep(d) auto-advances clock by d.
func (m *MockTimeModule) SetAutoAdvanceOnSleep(v bool) {
	m.mu.Lock()
	m.autoAdvanceOnSleep = v
	m.mu.Unlock()
}

// Now returns current virtual time.
func (m *MockTimeModule) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Since returns virtual elapsed duration from t to Now.
func (m *MockTimeModule) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// Until returns virtual duration from Now to t.
func (m *MockTimeModule) Until(t time.Time) time.Duration {
	return t.Sub(m.Now())
}

// Set sets virtual time and triggers due waiters.
func (m *MockTimeModule) Set(now time.Time) {
	m.setNow(now)
}

// Advance moves virtual time forward and triggers due waiters.
func (m *MockTimeModule) Advance(d time.Duration) {
	m.mu.Lock()
	next := m.now.Add(d)
	m.mu.Unlock()
	m.setNow(next)
}

func (m *MockTimeModule) setNow(now time.Time) {
	m.mu.Lock()
	m.now = now
	m.fireDueWaitersLocked()
	m.mu.Unlock()
}

// After returns a channel that receives once when virtual time reaches now+d.
func (m *MockTimeModule) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	if d <= 0 {
		ch := make(chan time.Time, 1)
		ch <- m.now
		return ch
	}

	w := newAfterWaiter(m.now.Add(d))
	m.afterWaiters = append(m.afterWaiters, w)
	return w.ch
}

// Sleep either auto-advances virtual clock or blocks on After(d), based on config.
func (m *MockTimeModule) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	m.mu.Lock()
	autoAdvance := m.autoAdvanceOnSleep
	m.mu.Unlock()
	if autoAdvance {
		m.Advance(d)
		return
	}
	<-m.After(d)
}

func (m *MockTimeModule) fireDueWaitersLocked() {
	now := m.now
	if len(m.afterWaiters) == 0 {
		return
	}
	dst := m.afterWaiters[:0]
	for _, w := range m.afterWaiters {
		if !w.deadline.After(now) {
			w.ch <- now
			continue
		}
		dst = append(dst, w)
	}
	m.afterWaiters = dst
}

func (m *MockTimeModule) newTimer(d time.Duration) *mvsTimer {
	state := &moduleTimerState{
		ch: make(chan time.Time, 1),
	}
	t := &mvsTimer{
		C: state.ch,
		mock: &mockTimerBackend{
			module: m,
			state:  state,
		},
	}
	t.Reset(d)
	return t
}

func (m *MockTimeModule) stopTimer(state *moduleTimerState) bool {
	state.mu.Lock()
	defer state.mu.Unlock()

	wasActive := !state.stopped
	state.seq++
	state.stopped = true
	return wasActive
}

func (m *MockTimeModule) resetTimer(state *moduleTimerState, d time.Duration) bool {
	state.mu.Lock()
	wasActive := !state.stopped
	state.seq++
	seq := state.seq
	state.stopped = false
	ch := state.ch
	state.mu.Unlock()

	afterCh := m.After(d)
	go func() {
		firedAt := <-afterCh

		state.mu.Lock()
		if state.stopped || state.seq != seq {
			state.mu.Unlock()
			return
		}
		state.mu.Unlock()

		select {
		case ch <- firedAt:
		default:
		}
	}()

	return wasActive
}

var activeMockTimeModule atomic.Pointer[MockTimeModule]

// InstallMockTimeModuleForTest installs module into time proxy and returns restore func.
func InstallMockTimeModuleForTest(module *MockTimeModule) (restore func()) {
	prev := activeMockTimeModule.Swap(module)
	return func() {
		activeMockTimeModule.Store(prev)
	}
}

func currentMockTimeModule() *MockTimeModule {
	return activeMockTimeModule.Load()
}

func mvsNow() time.Time {
	if module := currentMockTimeModule(); module != nil {
		return module.Now()
	}
	return time.Now()
}

func mvsAfter(d time.Duration) <-chan time.Time {
	if module := currentMockTimeModule(); module != nil {
		return module.After(d)
	}
	return time.After(d)
}

func mvsSince(t time.Time) time.Duration {
	if module := currentMockTimeModule(); module != nil {
		return module.Since(t)
	}
	return time.Since(t)
}

func mvsUntil(t time.Time) time.Duration {
	if module := currentMockTimeModule(); module != nil {
		return module.Until(t)
	}
	return time.Until(t)
}

type mvsTimer struct {
	C <-chan time.Time

	timer *time.Timer
	mock  *mockTimerBackend
}

func newRealMVSTimer(d time.Duration) *mvsTimer {
	rt := time.NewTimer(d)
	return &mvsTimer{
		C:     rt.C,
		timer: rt,
	}
}

func mvsNewTimer(d time.Duration) *mvsTimer {
	if module := currentMockTimeModule(); module != nil {
		return module.newTimer(d)
	}
	return newRealMVSTimer(d)
}

func (t *mvsTimer) Stop() bool {
	if t == nil {
		return false
	}
	if t.mock != nil {
		return t.mock.module.stopTimer(t.mock.state)
	}
	if t.timer == nil {
		return false
	}
	return t.timer.Stop()
}

func (t *mvsTimer) Reset(d time.Duration) bool {
	if t == nil {
		return false
	}
	if t.mock != nil {
		return t.mock.module.resetTimer(t.mock.state, d)
	}
	if t.timer == nil {
		return false
	}
	return t.timer.Reset(d)
}

func mvsSleep(d time.Duration) {
	if module := currentMockTimeModule(); module != nil {
		module.Sleep(d)
		return
	}
	time.Sleep(d)
}

func mvsUnix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

func mvsUnixMilli(ms int64) time.Time {
	return time.UnixMilli(ms)
}
