// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package utils

// GetOrZero returns the value pointed to by p, or a zero value of
// its type if p is nil.
func GetOrZero[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}
