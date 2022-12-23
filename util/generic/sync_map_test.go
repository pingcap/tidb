// Copyright 2022 PingCAP, Inc.
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

package generic_test

import (
	"sort"
	"testing"

	"github.com/pingcap/tidb/util/generic"
	"github.com/stretchr/testify/require"
)

func TestSyncMap(t *testing.T) {
	sm := generic.NewSyncMap[int64, string](10)
	sm.Store(1, "a")
	sm.Store(2, "b")
	// Load an exist key.
	v, ok := sm.Load(1)
	require.True(t, ok)
	require.Equal(t, "a", v)
	// Load a non-exist key.
	v, ok = sm.Load(3)
	require.False(t, ok)
	require.Equal(t, "", v)
	// Overwrite the value.
	sm.Store(1, "c")
	v, ok = sm.Load(1)
	require.True(t, ok)
	require.Equal(t, "c", v)
	// Drop an exist key.
	sm.Delete(1)
	v, ok = sm.Load(1)
	require.False(t, ok)
	require.Equal(t, "", v)
	// Drop a non-exist key.
	sm.Delete(3)
	require.Equal(t, []int64{2}, sm.Keys())
	v, ok = sm.Load(3)
	require.False(t, ok)
	require.Equal(t, "", v)

	// Test the Keys() method.
	sm = generic.NewSyncMap[int64, string](10)
	sm.Store(2, "b")
	sm.Store(1, "a")
	sm.Store(3, "c")
	keys := sm.Keys()
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	require.Equal(t, []int64{1, 2, 3}, keys)
}
