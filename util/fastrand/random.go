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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fastrand

import (
	"math/bits"
	_ "unsafe" // required by go:linkname
)

// wyrand is a fast PRNG. See https://github.com/wangyi-fudan/wyhash
type wyrand uint64

func _wymix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func (r *wyrand) Next() uint64 {
	*r += wyrand(0xa0761d6478bd642f)
	return _wymix(uint64(*r), uint64(*r^wyrand(0xe7037ed1a0b428db)))
}

// Buf generates a random string using ASCII characters but avoid separator character.
// See https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435
func Buf(size int) []byte {
	buf := make([]byte, size)
	r := wyrand(Uint32())
	for i := 0; i < size; i++ {
		// This is similar to Uint32() % n, but faster.
		// See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
		buf[i] = byte(uint32(uint64(uint32(r.Next())) * uint64(127) >> 32))
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
	// This is similar to Uint32() % n, but faster.
	// See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint32(uint64(Uint32()) * uint64(n) >> 32)
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
