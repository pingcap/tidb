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

package context

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionalPropKeySet(t *testing.T) {
	var keySet OptionalEvalPropKeySet
	require.True(t, keySet.IsEmpty())
	require.False(t, keySet.IsFull())
	require.False(t, keySet.Contains(OptPropCurrentUser))

	// Add one key
	keySet2 := keySet.Add(OptPropCurrentUser)
	require.True(t, keySet2.Contains(OptPropCurrentUser))
	require.False(t, keySet2.IsEmpty())
	require.True(t, keySet2.IsFull())

	// old key is not affected
	require.True(t, keySet.IsEmpty())

	// remove one key
	keySet3 := keySet2.Remove(OptPropCurrentUser)
	require.True(t, keySet3.IsEmpty())
	require.False(t, keySet2.IsEmpty())
}

func TestOptionalPropKeySetWithUnusedBits(t *testing.T) {
	require.Less(t, OptPropsCnt, 64)
	full := OptionalEvalPropKeySet(math.MaxUint64)

	bits := full << OptionalEvalPropKeySet(OptPropsCnt)
	require.True(t, bits.IsEmpty())
	require.False(t, bits.Contains(OptPropCurrentUser))
	bits = bits.Add(OptPropCurrentUser)
	require.True(t, bits.Contains(OptPropCurrentUser))

	bits = full >> (64 - OptPropsCnt)
	require.True(t, bits.IsFull())
	require.True(t, bits.Contains(OptPropCurrentUser))
	bits = bits.Remove(OptPropCurrentUser)
	require.False(t, bits.Contains(OptPropCurrentUser))
}

func TestOptionalPropKey(t *testing.T) {
	keySet := OptPropCurrentUser.AsPropKeySet()
	require.True(t, keySet.Contains(OptPropCurrentUser))
	keySet = keySet.Remove(OptPropCurrentUser)
	require.True(t, keySet.IsEmpty())
}

func TestOptionalPropDescList(t *testing.T) {
	require.Equal(t, OptPropsCnt, len(optionalPropertyDescList))
	for i := 0; i < OptPropsCnt; i++ {
		key := OptionalEvalPropKey(i)
		require.Equal(t, key, optionalPropertyDescList[i].Key())
		require.Same(t, &optionalPropertyDescList[i], key.Desc())
	}
}
