//go:build go1.22

package fastrand

import (
	_ "unsafe" // required by go:linkname
)

// Uint32 returns a lock free uint32 value.
//
//go:linkname Uint32 runtime.cheaprand
func Uint32() uint32
