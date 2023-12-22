// Copyright 2023 PingCAP, Inc.
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

package backoff

import "time"

// Backoffer is the interface to get backoff.
type Backoffer interface {
	// Backoff returns the duration to wait for the retryCnt-th retry.
	// retryCnt starts from 0.
	Backoff(retryCnt int) time.Duration
}

// Exponential implements the exponential backoff algorithm without jitter.
// should create one instance for each operation that need retry.
type Exponential struct {
	baseBackoff time.Duration
	multiplier  float64
	maxBackoff  time.Duration

	nextBackoff time.Duration
}

var _ Backoffer = &Exponential{}

// NewExponential creates a new Exponential backoff.
func NewExponential(baseBackoff time.Duration, multiplier float64, maxBackoff time.Duration) *Exponential {
	return &Exponential{
		baseBackoff: baseBackoff,
		multiplier:  multiplier,
		maxBackoff:  maxBackoff,
		nextBackoff: baseBackoff,
	}
}

// Backoff returns the duration to wait for the retryCnt-th retry.
// retryCnt starts from 0.
func (b *Exponential) Backoff(retryCnt int) time.Duration {
	if retryCnt == 0 {
		b.nextBackoff = b.baseBackoff
		return b.nextBackoff
	}

	b.nextBackoff = time.Duration(float64(b.nextBackoff) * b.multiplier)
	if b.nextBackoff > b.maxBackoff {
		b.nextBackoff = b.maxBackoff
	}
	return b.nextBackoff
}
