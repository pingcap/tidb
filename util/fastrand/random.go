// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package fastrand

import (
	_ "unsafe" // required by go:linkname
)

// Buf generates a random string using ASCII characters but avoid separator character.
// See https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435
func Buf(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte(Uint32N(127))
		if buf[i] == 0 || buf[i] == byte('$') {
			buf[i]++
		}
	}
	return buf
}

// Uint32 returns a lock free uint32 value.
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// Uint32N returns, as an uint32, a pseudo-random number in [0,n).
func Uint32N(n uint32) uint32 {
	if n&(n-1) == 0 { // n is power of two, can mask
		return Uint32() & (n - 1)
	}
	return Uint32() % n
}

// Uint64N returns, as an uint64, a pseudo-random number in [0,n).
func Uint64N(n uint64) uint64 {
	a := Uint32()
	b := Uint32()
	v := uint64(a)<<32 + uint64(b)
	if n&(n-1) == 0 { // n is power of two, can mask
		return v & (n - 1)
	}
	return v % n
}
