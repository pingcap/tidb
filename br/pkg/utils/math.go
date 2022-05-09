// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

// ClampInt restrict a value to a certain interval.
func ClampInt(n, min, max int) int {
	if min > max {
		log.Error("clamping integer with min > max", zap.Int("min", min), zap.Int("max", max))
	}

	return mathutil.Min(max, mathutil.Max(min, n))
}

// NextPowerOfTwo returns the smallest power of two greater than or equal to `i`
// Caller should guarantee that i > 0 and the return value is not overflow.
func NextPowerOfTwo(i int64) int64 {
	if i&(i-1) == 0 {
		return i
	}
	i *= 2
	for i&(i-1) != 0 {
		i &= i - 1
	}
	return i
}
