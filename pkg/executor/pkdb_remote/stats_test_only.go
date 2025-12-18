// Copyright 2024 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

//go:build intest

package pkdbremote

import "sync/atomic"

// ForwardingStats tracks statistics about remote plan forwarding for testing.
// This implementation is only used when building with the "intest" tag.
type ForwardingStats struct {
	// forwardAttempts is the number of times TryEarlyForwardExecute was called with forwarding enabled
	forwardAttempts atomic.Int64
	// forwardSuccesses is the number of times forwarding was actually performed
	forwardSuccesses atomic.Int64
	// forwardSkipped is the number of times forwarding was skipped (due to conditions not met)
	forwardSkipped atomic.Int64
	// forwardErrors is the number of times forwarding failed with an error
	forwardErrors atomic.Int64
}

// Reset resets all counters to zero
func (s *ForwardingStats) Reset() {
	s.forwardAttempts.Store(0)
	s.forwardSuccesses.Store(0)
	s.forwardSkipped.Store(0)
	s.forwardErrors.Store(0)
}

// RecordAttempt increments the attempt counter.
func (s *ForwardingStats) RecordAttempt() {
	s.forwardAttempts.Add(1)
}

// RecordSuccess increments the success counter.
func (s *ForwardingStats) RecordSuccess() {
	s.forwardSuccesses.Add(1)
}

// RecordSkipped increments the skipped counter.
func (s *ForwardingStats) RecordSkipped() {
	s.forwardSkipped.Add(1)
}

// RecordError increments the error counter.
func (s *ForwardingStats) RecordError() {
	s.forwardErrors.Add(1)
}

// GetAttempts returns the number of forwarding attempts.
func (s *ForwardingStats) GetAttempts() int64 {
	return s.forwardAttempts.Load()
}

// GetSuccesses returns the number of successful forwards.
func (s *ForwardingStats) GetSuccesses() int64 {
	return s.forwardSuccesses.Load()
}

// GetSkipped returns the number of skipped forwards.
func (s *ForwardingStats) GetSkipped() int64 {
	return s.forwardSkipped.Load()
}

// GetErrors returns the number of forwarding errors.
func (s *ForwardingStats) GetErrors() int64 {
	return s.forwardErrors.Load()
}

// GlobalForwardingStats is the global instance for tracking forwarding statistics.
// In test builds, this tracks actual statistics.
var GlobalForwardingStats = &ForwardingStats{}
