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
	"testing"

	"github.com/stretchr/testify/require"
)

type TmpStr struct {
	str1 string
	str2 string
}

func (ts *TmpStr) Hash64(h *Hasher) {
	h.HashString(ts.str1)
	h.HashString(ts.str2)
}

func TestStringLen(t *testing.T) {
	hasher1 := NewHasher()
	hasher2 := NewHasher()
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
	require.NotEqual(t, hasher1.hash64a, hasher2.hash64a)
}

func TestHash64a(t *testing.T) {
	hasher1 := NewHasher()
	hasher2 := NewHasher()
	hasher1.HashBool(true)
	hasher2.HashBool(true)
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashBool(false)
	hasher2.HashBool(false)
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashInt(199)
	hasher2.HashInt(199)
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashInt64(13534523462346)
	hasher2.HashInt64(13534523462346)
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashUint64(13534523462346)
	hasher2.HashUint64(13534523462346)
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashString("hello")
	hasher2.HashString("hello")
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashBytes([]byte("world"))
	hasher2.HashBytes([]byte("world"))
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.HashRune('我')
	hasher1.HashRune('是')
	hasher1.HashRune('谁')
	hasher2.HashRune('我')
	hasher2.HashRune('是')
	hasher2.HashRune('谁')
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
	hasher1.Reset()
	hasher2.Reset()
	hasher1.HashString("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	hasher2.HashString("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	require.Equal(t, hasher1.hash64a, hasher2.hash64a)
}
