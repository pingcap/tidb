// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

//go:build intest

package pkdbremote

// SetDefaultClientForTest overrides the default remote execution client for tests.
// It returns a restore function to reset the previous value.
func SetDefaultClientForTest(client Client) func() {
	prev := defaultClientOverride
	defaultClientOverride = client
	return func() {
		defaultClientOverride = prev
	}
}
