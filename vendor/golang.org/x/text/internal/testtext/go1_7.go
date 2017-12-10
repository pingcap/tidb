// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.7

package testtext

import "testing"

func Run(t *testing.T, name string, fn func(t *testing.T)) bool {
	return t.Run(name, fn)
}

func Bench(b *testing.B, name string, fn func(b *testing.B)) bool {
	return b.Run(name, fn)
}
