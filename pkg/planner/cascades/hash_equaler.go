// Copyright 2024 PingCAP, Inc.
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

package cascades

import (
	"bytes"
	"math"
)

const (
	// both offset and prime are used to compute the fnv-1a's
	// hash value which is more unique efficient than fnv-1.
	//
	// offset64 is ported from fnv.go from go library.
	offset64 = 14695981039346656037

	// prime64 is ported from fnv.go from go library.
	prime64 = 1099511628211
)

type Hasher interface {
	HashBool(val bool)
	HashInt(val int)
	HashInt64(val int64)
	HashUint64(val uint64)
	HashFloat64(val float64)
	HashRune(val rune)
	HashString(val string)
	HashByte(val byte)
	HashBytes(val []byte)
	Reset()
	Sum64() uint64
}

type Hash64a uint64

// Hasher is a helper struct that's used for computing **fnv-1a** hash values and tell
// the equivalence on expression/operators. To use, first call the init method, then
// a series of hash methods. The final value is stored in the hash64a field.
type hasher struct {
	// hash stores the hash value as it is incrementally computed.
	hash64a Hash64a
}

// NewHashEqualer creates a new HashEqualer.
func NewHashEqualer() Hasher {
	return &hasher{
		hash64a: offset64,
	}
}

// Reset resets the Hasher to its initial state, reusing the internal bytes slice.
func (h *hasher) Reset() {
	h.hash64a = offset64
}

func (h *hasher) Sum64() uint64 {
	return uint64(h.hash64a)
}

// ------------------------------ Hash functions ----------------------------------------
// Previously, expressions' hashcode are computed by encoding meta layer by layer from the
// bottom up. This is not efficient and oom risky because each expression has cached numerous
// hash bytes on their own.
//
// The new hash function is based on the fnv-1a hash algorithm, outputting the uint64 only.
// To avoid the OOM during the hash computation, we use a shared bytes slice to take in primitive
// types from targeted expressions/operators. The bytes slice is reused and reset after each
// usage of them.
//
// The standardized fnv-1a lib only takes in bytes slice as input, so we need to convert every
// primitive type to bytes slice inside Hash function implementation of every expression/operators
// by allocating some temporary slice. This is undesirable, and we just made the Hasher to take in
// primitive type directly.
// ---------------------------------------------------------------------------------------

// HashBool hashes a Boolean value.
func (h *hasher) HashBool(val bool) {
	i := 0
	if val {
		i = 1
	}
	h.hash64a ^= Hash64a(i)
	h.hash64a *= prime64
}

// HashInt hashes an integer value.
func (h *hasher) HashInt(val int) {
	h.hash64a ^= Hash64a(val)
	h.hash64a *= prime64
}

// HashInt64 hashes an int64 value.
func (h *hasher) HashInt64(val int64) {
	h.hash64a ^= Hash64a(val)
	h.hash64a *= prime64
}

// HashUint64 hashes a uint64 value.
func (h *hasher) HashUint64(val uint64) {
	h.hash64a ^= Hash64a(val)
	h.hash64a *= prime64
}

// HashFloat64 hashes a float64 value.
func (h *hasher) HashFloat64(val float64) {
	h.hash64a ^= Hash64a(math.Float64bits(val))
	h.hash64a *= prime64
}

// HashRune hashes a rune value.
func (h *hasher) HashRune(val rune) {
	h.hash64a ^= Hash64a(val)
	h.hash64a *= prime64
}

// HashString hashes a string value.
// eg: "我是谁" is with 3 rune inside, each rune of them takes up 3-4 bytes.
func (h *hasher) HashString(val string) {
	h.HashInt(len(val))
	for _, c := range val {
		h.HashRune(c)
	}
}

// HashByte hashes a byte value.
// a byte can be treated as a simple rune as well.
func (h *hasher) HashByte(val byte) {
	h.HashRune(rune(val))
}

// HashBytes hashes a byte slice value.
func (h *hasher) HashBytes(val []byte) {
	h.HashInt(len(val))
	for _, c := range val {
		h.HashByte(c)
	}
}

// ------------------------------ Equal functions ----------------------------------------
// The equal functions are used to compare the equivalence of two primitive types. The
// implementation is straightforward and simple.
//
// For composed and complex user defined structure or types, the implementor should implement
// the equal functions for them. The equal functions should be implemented in a way that
// the hash functions are implemented.
//
// Equal functions is called from the hash collision detection algorithm to determine whether
// two expressions/operators are equivalent. For primitive types, we can directly compare them.
// while for composed and complex user defined structure or types, we should compare them field
// by field and recursively call down of them if it's embedded.
// ---------------------------------------------------------------------------------------

// EqualBool compares two Boolean values.
func (h *hasher) EqualBool(l, r bool) bool {
	return l == r
}

// EqualInt compares two integer values.
func (h *hasher) EqualInt(l, r int) bool {
	return l == r
}

// EqualInt64 compares two int64 values.
func (h *hasher) EqualInt64(l, r int64) bool {
	return l == r
}

// EqualUint64 compares two uint64 values.
func (h *hasher) EqualUint64(l, r uint64) bool {
	return l == r
}

// EqualFloat64 compares two uint64 values.
func (h *hasher) EqualFloat64(l, r float64) bool {
	// Compare bit representations so that NaN == NaN and 0 != -0.
	return math.Float64bits(l) == math.Float64bits(r)
}

// EqualRune compares two rune values.
func (h *hasher) EqualRune(l, r rune) bool {
	return l == r
}

// EqualString compares two string values.
func (h *hasher) EqualString(l, r string) bool {
	return l == r
}

// EqualByte compares two byte values.
func (h *hasher) EqualByte(l, r byte) bool {
	return l == r
}

// EqualBytes compares two byte slice values.
func (h *hasher) EqualBytes(l, r []byte) bool {
	return bytes.Equal(l, r)
}

// ------------------------------ Object Implementation -------------------------------------
// For primitive type, we can directly hash them and compare them. Based on the primitive
// interface call listed here, we can easily implement the hash and equal functions for other
// composed and complex user defined structure or types.
//
// Say we have a structure like this:
// type MyStruct struct {
//     a int
// 	   b string
//     c OtherStruct
//     d Pointer
// }
// so we can implement the hash and equal functions like this:
// func (val *MyStruct) Hash64(h Hasher) {
//     h.HashInt(val.a)
//     h.HashString(val.b)
// 	   // for c here, it calls for the hash function of OtherStruct implementor.
//     c.Hash64(h)
//	   // for pointer, how it could be hashed is up to the implementor.
//     h.HashUint64(uint64(val.d))
// }
//
// func (val1 *MyStruct) Equal(val1 *MyStruct) bool {
//     return val1.a == val2.a && val1.b == val2.b && val1.c.Equal(val2.c) && val1.d == val2.d
// }
// ------------------------------------------------------------------------------------------
