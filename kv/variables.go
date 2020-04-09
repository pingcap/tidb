// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

// Variables defines the variables used by KV storage.
type Variables struct {
	// BackoffLockFast specifies the LockFast backoff base duration in milliseconds.
	BackoffLockFast int

	// BackOffWeight specifies the weight of the max back off time duration.
	BackOffWeight int

	// BackOffKVBusy specifies the weight of the max back off time duration for kv is busy.
	BackOffKVBusy int

	// Hook is used for test to verify the variable take effect.
	Hook func(name string, vars *Variables)

	// Pointer to SessionVars.Killed
	// Killed is a flag to indicate that this query is killed.
	Killed *uint32
}

// NewVariables create a new Variables instance with default values.
func NewVariables(killed *uint32) *Variables {
	return &Variables{
		BackoffLockFast: DefBackoffLockFast,
		BackOffWeight:   DefBackOffWeight,
		BackOffKVBusy:   DefBackOffKVBusy,
		Killed:          killed,
	}
}

var ignoreKill uint32

// DefaultVars is the default variables instance.
var DefaultVars = NewVariables(&ignoreKill)

// Default values
const (
	DefBackoffLockFast = 100
	DefBackOffWeight   = 2
	DefBackOffKVBusy   = 10
)
