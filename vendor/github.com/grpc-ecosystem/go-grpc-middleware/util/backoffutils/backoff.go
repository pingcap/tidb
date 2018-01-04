// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

/*
Backoff Helper Utilities

Implements common backoff features.
*/
package backoffutils

import (
	"math/rand"
	"time"
)

// JitterUp adds random jitter to the duration.
//
// This adds or substracts time from the duration within a given jitter fraction.
// For example for 10s and jitter 0.1, it will returna  time within [9s, 11s])
func JitterUp(duration time.Duration, jitter float64) time.Duration {
	multiplier := jitter * (rand.Float64()*2 - 1)
	return time.Duration(float64(duration) * (1 + multiplier))
}
