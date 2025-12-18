// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

package core

import (
	"sync/atomic"
	"time"
)

// RemotePlanFeedback tracks whether remote execution forwarding should be temporarily disabled
// for a prepared statement based on observed forwarded result sizes.
//
// It is intentionally lightweight: it only keeps a consecutive "large result" counter and a
// disable-until timestamp.
type RemotePlanFeedback struct {
	disabledUntilUnixNano atomic.Int64
	consecutiveBad        atomic.Int32
}

// ForwardingDisabled returns true if forwarding is currently disabled.
func (f *RemotePlanFeedback) ForwardingDisabled(now time.Time) bool {
	if f == nil {
		return false
	}
	until := f.disabledUntilUnixNano.Load()
	if until <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.UnixNano() < until
}

// DisabledUntilUnixNano returns the disable-until timestamp in UnixNano, or 0 if never disabled.
func (f *RemotePlanFeedback) DisabledUntilUnixNano() int64 {
	if f == nil {
		return 0
	}
	return f.disabledUntilUnixNano.Load()
}

// RecordResult records the result size of a forwarded execution.
//
// If resultBytes exceeds bytesThreshold for disableAfter consecutive times, forwarding is disabled
// for cooldown duration. It returns true if this call sets/extends the disable-until timestamp.
func (f *RemotePlanFeedback) RecordResult(now time.Time, resultBytes int64, bytesThreshold int64, disableAfter int32, cooldown time.Duration) bool {
	if f == nil || bytesThreshold <= 0 {
		return false
	}
	return f.RecordObservation(now, resultBytes >= bytesThreshold, disableAfter, cooldown)
}

// RecordObservation records whether a forwarded execution is considered "bad" by the caller.
//
// When bad is observed for disableAfter consecutive times, forwarding is disabled for cooldown duration.
// It returns true if this call sets/extends the disable-until timestamp.
func (f *RemotePlanFeedback) RecordObservation(now time.Time, bad bool, disableAfter int32, cooldown time.Duration) bool {
	if f == nil || disableAfter <= 0 || cooldown <= 0 {
		return false
	}
	if !bad {
		f.consecutiveBad.Store(0)
		return false
	}
	if f.consecutiveBad.Add(1) < disableAfter {
		return false
	}
	f.consecutiveBad.Store(0)

	// Avoid calling time.Now() on every observation: we only need the current time when we actually disable forwarding.
	if now.IsZero() {
		now = time.Now()
	}
	newUntil := now.Add(cooldown).UnixNano()
	for {
		oldUntil := f.disabledUntilUnixNano.Load()
		if oldUntil >= newUntil {
			return false
		}
		if f.disabledUntilUnixNano.CompareAndSwap(oldUntil, newUntil) {
			return true
		}
	}
}
