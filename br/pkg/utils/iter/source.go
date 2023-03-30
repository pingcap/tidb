// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"golang.org/x/exp/constraints"
)

// FromSlice creates an iterator from a slice, the iterator would
func FromSlice[T any](s []T) TryNextor[T] {
	sa := fromSlice[T](s)
	return &sa
}

// FromArray creates an iterator that can return the next element
// and its index in the array.
func FromArray[T any](s []T) TryNextEnumor[T] {
	return &fromArray[T]{
		data:    s,
		current: 0,
	}
}

// OfRange creates an iterator that yields elements in the integer range.
func OfRange[T constraints.Integer](begin, end T) TryNextor[T] {
	return &ofRange[T]{
		end:          end,
		endExclusive: true,

		current: begin,
	}
}

// Fail creates an iterator always fail.
func Fail[T any](err error) TryNextor[T] {
	return failure[T]{error: err}
}
