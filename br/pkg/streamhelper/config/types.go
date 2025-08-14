// Copyright 2025 PingCAP, Inc.
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

package config

import "time"

var (
	DefaultMaxConcurrencyAdvance = 8
)

type Config interface {
	// GetBackoffTime returns the gap between two retries.
	GetBackoffTime() time.Duration
	// TickTimeout returns the max duration for each tick.
	TickTimeout() time.Duration
	// GetDefaultStartPollThreshold returns the threshold of begin polling the checkpoint
	// in the normal condition (the subscribe manager is available.)
	GetDefaultStartPollThreshold() time.Duration
	// GetSubscriberErrorStartPollThreshold returns the threshold of begin polling the checkpoint
	// when the subscriber meets error.
	GetSubscriberErrorStartPollThreshold() time.Duration
	// The maximum lag could be tolerated for the checkpoint lag.
	GetCheckPointLagLimit() time.Duration
}
