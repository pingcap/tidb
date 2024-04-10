// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bytes implements functions for the manipulation of byte slices.
// It is analogous to the facilities of the strings package.

// this is copy from `bytes/bytes.go`

package mydump

// byteSet is a 32-byte value, where each bit represents the presence of a
// given byte value in the set.
type byteSet [8]uint32

// makeByteSet creates a set of byte value.
func makeByteSet(chars []byte) (as byteSet) {
	for i := 0; i < len(chars); i++ {
		c := chars[i]
		as[c>>5] |= 1 << uint(c&31)
	}
	return as
}

// contains reports whether c is inside the set.
func (as *byteSet) contains(c byte) bool {
	return (as[c>>5] & (1 << uint(c&31))) != 0
}

// IndexAnyByte returns the byte index of the first occurrence in s of any of the byte
// points in chars. It returns -1 if  there is no code point in common.
func IndexAnyByte(s []byte, as *byteSet) int {
	for i, c := range s {
		if as.contains(c) {
			return i
		}
	}
	return -1
}
