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
	"testing"

	"github.com/stretchr/testify/require"
)

type TmpStr struct {
	str1 string
	str2 string
}

func (ts *TmpStr) Hash64(h Hasher) {
	h.HashString(ts.str1)
	h.HashString(ts.str2)
}

func TestStringLen(t *testing.T) {
	hasher1 := NewHashEqualer()
	hasher2 := NewHashEqualer()
	a := TmpStr{
		str1: "abc",
		str2: "def",
	}
	b := TmpStr{
		str1: "abcdef",
		str2: "",
	}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
}

type SX interface {
	Hash64(h Hasher)
	Equal(SX) bool
}

type SA struct {
	a int
	b string
}

func (sa *SA) Hash64(h Hasher) {
	h.HashInt(sa.a)
	h.HashString(sa.b)
}

func (sa *SA) Equal(sx SX) bool {
	if sa2, ok := sx.(*SA); ok {
		return sa.a == sa2.a && sa.b == sa2.b
	}
	return false
}

type SB struct {
	a int
	b string
}

func (sb *SB) Hash64(h Hasher) {
	h.HashInt(sb.a)
	h.HashString(sb.b)
}

func (sb *SB) Equal(sx SX) bool {
	if sb2, ok := sx.(*SB); ok {
		return sb.a == sb2.a && sb.b == sb2.b
	}
	return false
}

func TestStructType(t *testing.T) {
	hasher1 := NewHashEqualer()
	hasher2 := NewHashEqualer()
	a := SA{
		a: 1,
		b: "abc",
	}
	b := SB{
		a: 1,
		b: "abc",
	}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	// As you see from the above, the two structs are different types, but they have the same fields.
	// For the Hash64 function, it will hash the fields of the struct, so the hash result should be the same.
	// From theoretical point of view, the hash result should NOT be the same because of different types.
	//
	// While the Equal function is used to compare the two structs, so the result should be false. We don't
	// have to hash the golang struct type, because the dynamic runtime type pointer from reflecting is not
	// that elegant, we resort to Equal function to compare the two structs completely once two obj has the
	// same hash.
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.Equal(t, a.Equal(&b), false)
}

func TestHash64a(t *testing.T) {
	hasher1 := NewHashEqualer()
	hasher2 := NewHashEqualer()
	hasher1.HashBool(true)
	hasher2.HashBool(true)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashBool(false)
	hasher2.HashBool(false)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashInt(199)
	hasher2.HashInt(199)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashInt64(13534523462346)
	hasher2.HashInt64(13534523462346)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashUint64(13534523462346)
	hasher2.HashUint64(13534523462346)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashString("hello")
	hasher2.HashString("hello")
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashBytes([]byte("world"))
	hasher2.HashBytes([]byte("world"))
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.HashRune('我')
	hasher1.HashRune('是')
	hasher1.HashRune('谁')
	hasher2.HashRune('我')
	hasher2.HashRune('是')
	hasher2.HashRune('谁')
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	hasher1.Reset()
	hasher2.Reset()
	hasher1.HashString("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	hasher2.HashString("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
}
