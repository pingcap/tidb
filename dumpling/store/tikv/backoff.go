// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"math"
	"math/rand"
	"time"

	"github.com/juju/errors"
)

const (
	// NoJitter makes the backoff sequence strict exponential.
	NoJitter = 1 + iota
	// FullJitter applies random factors to strict exponential.
	FullJitter
	// EqualJitter is also randomized, but prevents very short sleeps.
	EqualJitter
	// DecorrJitter increases the maximum jitter based on the last random value.
	DecorrJitter
)

// NewBackoff creates a backoff func which implements exponential backoff with
// optional jitters.
// See: http://www.awsarchitectureblog.com/2015/03/backoff.html
func NewBackoff(retry, base, cap, jitter int) func() error {
	attempts := 0
	totalSleep := 0
	lastSleep := base
	return func() error {
		if attempts >= retry {
			return errors.Errorf("still fail after %d retries, total sleep %dms", attempts, totalSleep)
		}
		var sleep int
		switch jitter {
		case NoJitter:
			sleep = expo(base, cap, attempts)
		case FullJitter:
			v := expo(base, cap, attempts)
			sleep = rand.Intn(v)
		case EqualJitter:
			v := expo(base, cap, attempts)
			sleep = v/2 + rand.Intn(v/2)
		case DecorrJitter:
			sleep = int(math.Min(float64(cap), float64(base+rand.Intn(lastSleep*3-base))))
		}
		time.Sleep(time.Duration(sleep) * time.Millisecond)

		attempts++
		totalSleep += sleep
		lastSleep = sleep
		return nil
	}
}

func expo(base, cap, n int) int {
	return int(math.Min(float64(cap), float64(base)*math.Pow(2.0, float64(n))))
}
