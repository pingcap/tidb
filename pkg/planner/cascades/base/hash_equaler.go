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

package base

import (
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

// Hasher is the interface for computing hash values of different types.
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
	SetCache([]byte)
	Cache() []byte
	Sum64() uint64
}

// NilFlag and NotNilFlag are used to indicate whether a pointer/interface type field inside struct is nil or not.
// like a structure:
//
//	type MyStruct struct {
//		 a *OtherStruct
//	}
//
// Once a is nil, we should hash the NilFlag, otherwise, we should hash the NotNilFlag
// nil     : [0]
// not nil : [1] [xxx]
// the NotNilFlag should not be missed, otherwise, once [xxx] is []byte{0}, it will be treated as nil.
const (
	NilFlag    byte = 0
	NotNilFlag byte = 1
)

// Hash64a is the type for the hash value.
type Hash64a uint64

// hasher is a helper struct that's used for computing **fnv-1a** hash values and tell
// the equivalence on expression/operators. To use, first call the init method, then
// a series of hash methods. The final value is stored in the hash64a field.
type hasher struct {
	// hash stores the hash value as it is incrementally computed.
	hash64a Hash64a

	// cache is the internal bytes slice that's will be reused for some special tmp encoding like datum.
	cache []byte
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
	h.cache = h.cache[:0]
}

// Cache returns the internal bytes slice for re-usage.
func (h *hasher) Cache() []byte {
	return h.cache
}

// SetCache sets the internal bytes slice for reu-sage.
func (h *hasher) SetCache(cache []byte) {
	h.cache = cache
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
