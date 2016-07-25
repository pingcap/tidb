// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"sort"
	"unicode"
)

type rangeSlice []unicode.Range32

// sort.Interface
func (r rangeSlice) Len() int {
	return len(r)
}

// sort.Interface
func (pr *rangeSlice) Less(i, j int) bool {
	r := *pr
	return r[i].Lo < r[j].Lo
}

// sort.Interface
func (pr *rangeSlice) Swap(i, j int) {
	r := *pr
	r[i], r[j] = r[j], r[i]
}

// Limited normalization of ranges produced by ParseRE. Doesn't handle stride != 1.
func (pr *rangeSlice) normalize() {
	sort.Sort(pr)
	r := *pr
	for ok := false; !ok; {
		ok = true
		for i := len(r) - 1; i > 0; i-- {
			y, z := r[i-1], r[i]
			if y.Hi >= z.Lo || y.Hi+1 == z.Lo { // overlap or join point found
				ok = false
				if z.Hi > y.Hi {
					y.Hi = z.Hi
				}
				r[i-1] = y
				r = append(r[:i], r[i+1:]...) // remove z
			}
		}
	}
	*pr = r
}
