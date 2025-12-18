// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bytes implements functions for the manipulation of byte slices.
// It is analogous to the facilities of the strings package.

// this is copy from `bytes/bytes.go`

package mydump

import "sync"

// byteSet represents the presence of any byte value in the set.
//
// It stores a compact bitset and lazily builds a 0/1 lookup table (length 256)
// for fast membership checks on hot paths.
type byteSet struct {
	bits [8]uint32

	once  sync.Once
	table [256]byte
}

// makeByteSet creates a set of byte value.
func makeByteSet(chars []byte) (as *byteSet) {
	as = &byteSet{}
	for i := range chars {
		c := chars[i]
		as.bits[c>>5] |= 1 << uint(c&31)
	}
	return as
}

func (as *byteSet) init01Table() {
	// Build a 256-entry lookup table where table[c] == 1 indicates c is in the set.
	for i := range 256 {
		c := byte(i)
		if (as.bits[c>>5] & (1 << uint(c&31))) != 0 {
			as.table[i] = 1
		}
	}
}

// IndexAnyByte returns the byte index of the first occurrence in s of any of the byte
// points in chars. It returns -1 if  there is no code point in common.
func IndexAnyByte(s []byte, as *byteSet) int {
	as.once.Do(as.init01Table)
	for i := range s {
		if as.table[s[i]] != 0 {
			return i
		}
	}
	return -1
}
