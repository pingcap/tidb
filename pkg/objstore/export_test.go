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

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// Re-exports of internal sentinel errors so tests in objstore_test can match
// them with errors.Is.
var (
	TESTRenewTxnIDMismatch = errRenewTxnIDMismatch
	TESTRenewLeaseExpired  = errRenewLeaseExpired
)

// TESTTryRenew exposes the unexported tryRenew primitive for direct testing.
func TESTTryRenew(ctx context.Context, l *RemoteLock) error {
	return l.tryRenew(ctx)
}

// TESTTryLockRemoteExact exposes exact-target conditionalPut for tests that
// need to exercise assertOnlyMyIntent without lock-family verification.
func TESTTryLockRemoteExact(ctx context.Context, storage storeapi.Storage, physicalPath, hint string) (*RemoteLock, error) {
	return tryLockRemoteExact(ctx, storage, physicalPath, hint, nil)
}

// TESTStopRenewal signals the renewal goroutine to stop and waits for it to
// exit. Used by tests that exercise StartRenewal without going through the
// full Unlock path. Returns immediately if no renewal was started.
func TESTStopRenewal(l *RemoteLock) {
	l.stopRenewalIfStarted()
}

// TESTSetLeaseConstants overrides the lease-related timing knobs for a test.
// The returned restore function must be called (typically via t.Cleanup) so
// later tests see production values again.
func TESTSetLeaseConstants(ttl, interval time.Duration, maxRetries int, baseBackoff time.Duration) (restore func()) {
	return SetLeaseConstantsForTest(ttl, interval, maxRetries, baseBackoff)
}

// TESTSetNow overrides nowFunc for deterministic time-based tests.
// Callers should invoke the returned restore function in a defer or t.Cleanup.
func TESTSetNow(fn func() time.Time) (restore func()) {
	old := nowFunc
	nowFunc = fn
	return func() { nowFunc = old }
}
