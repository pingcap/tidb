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
	require.False(t, keySet2.IsFull())

	// old key is not affected
	require.True(t, keySet.IsEmpty())

	// Add second key
	keySet3 := keySet2.Add(OptPropDDLOwnerInfo)
	require.True(t, keySet3.Contains(OptPropCurrentUser))
	require.True(t, keySet3.Contains(OptPropDDLOwnerInfo))
	require.False(t, keySet3.IsEmpty())
	require.False(t, keySet3.IsFull())
	require.False(t, keySet2.Contains(OptPropDDLOwnerInfo))

	// remove one key
	keySet4 := keySet3.Remove(OptPropCurrentUser)
	require.False(t, keySet4.Contains(OptPropCurrentUser))
	require.True(t, keySet4.Contains(OptPropDDLOwnerInfo))
	require.False(t, keySet4.IsFull())
	require.False(t, keySet4.IsEmpty())

	// add all other keys
	keySet4 = keySet3.
		Add(OptPropSessionVars).
		Add(OptPropInfoSchema).
		Add(OptPropKVStore).
		Add(OptPropSQLExecutor).
		Add(OptPropSequenceOperator).
		Add(OptPropAdvisoryLock)
	require.True(t, keySet4.IsFull())
	require.False(t, keySet4.IsEmpty())
}

func TestOptionalPropKeySetWithUnusedBits(t *testing.T) {
	require.Less(t, OptPropsCnt, 64)
	full := OptionalEvalPropKeySet(math.MaxUint64)

	bits := full << OptionalEvalPropKeySet(OptPropsCnt)
	require.True(t, bits.IsEmpty())
	require.False(t, bits.Contains(OptPropCurrentUser))
	require.False(t, bits.Contains(OptPropDDLOwnerInfo))
	bits = bits.Add(OptPropCurrentUser)
	require.True(t, bits.Contains(OptPropCurrentUser))

	bits = full >> (64 - OptPropsCnt)
	require.True(t, bits.IsFull())
	require.True(t, bits.Contains(OptPropCurrentUser))
	require.True(t, bits.Contains(OptPropDDLOwnerInfo))
	bits = bits.Remove(OptPropCurrentUser)
	require.False(t, bits.Contains(OptPropCurrentUser))
}

func TestOptionalPropKey(t *testing.T) {
	for i := 0; i < OptPropsCnt; i++ {
		key := OptionalEvalPropKey(i)
		keySet := key.AsPropKeySet()
		// keySet should contain the specified key
		require.True(t, keySet.Contains(key))
		// desc in optionalPropertyDescList should be the same with key.Desc()
		require.Equal(t, key, optionalPropertyDescList[i].Key())
		require.Same(t, &optionalPropertyDescList[i], key.Desc())
		// keySet should not contain other keys
		for j := 0; j < OptPropsCnt; j++ {
			if i != j {
				key2 := OptionalEvalPropKey(j)
				require.False(t, keySet.Contains(key2))
			}
		}
		// If key removed from the keySet, the keySet should be empty
		keySet = keySet.Remove(key)
		require.True(t, keySet.IsEmpty())
	}
}
