// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

//go:build !intest

package pkdbremote

// ForwardingStats is a no-op implementation for production builds.
// In production, we don't want to incur the overhead of atomic operations
// for tracking forwarding statistics.
type ForwardingStats struct{}

// Reset is a no-op in production.
func (s *ForwardingStats) Reset() {}

// RecordAttempt is a no-op in production.
func (s *ForwardingStats) RecordAttempt() {}

// RecordSuccess is a no-op in production.
func (s *ForwardingStats) RecordSuccess() {}

// RecordSkipped is a no-op in production.
func (s *ForwardingStats) RecordSkipped() {}

// RecordError is a no-op in production.
func (s *ForwardingStats) RecordError() {}

// GetAttempts returns 0 in production.
func (s *ForwardingStats) GetAttempts() int64 { return 0 }

// GetSuccesses returns 0 in production.
func (s *ForwardingStats) GetSuccesses() int64 { return 0 }

// GetSkipped returns 0 in production.
func (s *ForwardingStats) GetSkipped() int64 { return 0 }

// GetErrors returns 0 in production.
func (s *ForwardingStats) GetErrors() int64 { return 0 }

// GlobalForwardingStats is the global instance for tracking forwarding statistics.
// In production, this is a no-op implementation.
var GlobalForwardingStats = &ForwardingStats{}
