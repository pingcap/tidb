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

//go:build !intest

package mvs

import "time"

type MockTimeModule struct{}

func NewMockTimeModule(time.Time) *MockTimeModule {
	return &MockTimeModule{}
}

func InstallMockTimeModuleForTest(*MockTimeModule) (restore func()) {
	return func() {}
}

func mvsNow() time.Time {
	return time.Now()
}

func mvsAfter(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func mvsSince(t time.Time) time.Duration {
	return time.Since(t)
}

func mvsUntil(t time.Time) time.Duration {
	return time.Until(t)
}

type mvsTimer struct {
	time.Timer
}

func newRealMVSTimer(d time.Duration) *mvsTimer {
	rt := time.NewTimer(d)
	return &mvsTimer{
		Timer: *rt,
	}
}

func mvsNewTimer(d time.Duration) *mvsTimer {
	return newRealMVSTimer(d)
}

func (t *mvsTimer) Stop() bool {
	if t == nil {
		return false
	}
	return t.Timer.Stop()
}

func (t *mvsTimer) Reset(d time.Duration) bool {
	if t == nil {
		return false
	}
	return t.Timer.Reset(d)
}

func mvsSleep(d time.Duration) {
	time.Sleep(d)
}

func mvsUnix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

func mvsUnixMilli(ms int64) time.Time {
	return time.UnixMilli(ms)
}
