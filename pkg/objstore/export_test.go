// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"context"
	"time"
)

// Re-exports of internal sentinel errors so tests in objstore_test can match
// them with errors.Is.
var (
	TEST_ErrRenewTxnIDMismatch = errRenewTxnIDMismatch
	TEST_ErrRenewLeaseExpired  = errRenewLeaseExpired
)

// TEST_TryRenew exposes the unexported tryRenew primitive for direct testing.
func TEST_TryRenew(l *RemoteLock, ctx context.Context) error {
	return l.tryRenew(ctx)
}

// TEST_StopRenewal signals the renewal goroutine to stop and waits for it to
// exit. Used by tests that exercise StartRenewal without going through the
// full Unlock path. Returns immediately if no renewal was started.
func TEST_StopRenewal(l *RemoteLock) {
	l.stopRenewalIfStarted()
}

// TEST_SetLeaseConstants overrides the lease-related timing knobs for a test.
// The returned restore function must be called (typically via t.Cleanup) so
// later tests see production values again.
func TEST_SetLeaseConstants(ttl, interval time.Duration, maxRetries int, baseBackoff time.Duration) (restore func()) {
	oldTTL, oldInterval := LeaseTTL, renewInterval
	oldMax, oldBackoff := renewMaxRetries, renewBaseBackoff
	LeaseTTL = ttl
	renewInterval = interval
	renewMaxRetries = maxRetries
	renewBaseBackoff = baseBackoff
	return func() {
		LeaseTTL, renewInterval = oldTTL, oldInterval
		renewMaxRetries, renewBaseBackoff = oldMax, oldBackoff
	}
}

// TEST_SetNow overrides nowFunc for deterministic time-based tests.
// Callers should invoke the returned restore function in a defer or t.Cleanup.
func TEST_SetNow(fn func() time.Time) (restore func()) {
	old := nowFunc
	nowFunc = fn
	return func() { nowFunc = old }
}
