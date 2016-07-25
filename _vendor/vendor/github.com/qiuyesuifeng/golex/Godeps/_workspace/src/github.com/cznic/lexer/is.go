// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is a modified version of the code originally found in $GOROOT/src/pkg/unicode/letter.go

package lexer

import (
	"unicode"
)

// is16 uses binary search to test whether arune is in the specified slice of 16-bit ranges.
func is16(ranges []unicode.Range16, arune uint16) bool {
	// binary search over ranges
	lo := 0
	hi := len(ranges)
	for lo < hi {
		m := lo + (hi-lo)/2
		r := ranges[m]
		if r.Lo <= arune && arune <= r.Hi {
			return (arune-r.Lo)%r.Stride == 0
		}

		if arune < r.Lo {
			hi = m
		} else {
			lo = m + 1
		}
	}
	return false
}

// is32 uses binary search to test whether arune is in the specified slice of 32-bit ranges.
func is32(ranges []unicode.Range32, arune uint32) bool {
	// binary search over ranges
	lo := 0
	hi := len(ranges)
	for lo < hi {
		m := lo + (hi-lo)/2
		r := ranges[m]
		if r.Lo <= arune && arune <= r.Hi {
			return (arune-r.Lo)%r.Stride == 0
		}

		if arune < r.Lo {
			hi = m
		} else {
			lo = m + 1
		}
	}
	return false
}

// unicodeIs tests whether arune is in the specified table of ranges.
func unicodeIs(rangeTab *unicode.RangeTable, arune rune) bool {
	r16 := rangeTab.R16
	if len(r16) > 0 && arune <= rune(r16[len(r16)-1].Hi) && is16(r16, uint16(arune)) {
		return true
	}

	r32 := rangeTab.R32
	if len(r32) > 0 && arune >= rune(r32[0].Lo) {
		return is32(r32, uint32(arune))
	}

	return false
}
