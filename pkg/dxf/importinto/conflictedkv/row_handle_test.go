// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictedkv

import (
	"sync/atomic"
	"testing"

	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandleFilter(t *testing.T) {
	var hf *HandleFilter
	// we allow nil HandleFilter
	require.False(t, hf.needSkip(tidbkv.IntHandle(1)))

	sharedSize := atomic.Int64{}
	set := NewBoundedHandleSet(nil, &sharedSize, 1024)
	set.Add(tidbkv.IntHandle(1))
	hf = NewHandleFilter(set)
	require.True(t, hf.needSkip(tidbkv.IntHandle(1)))
	require.False(t, hf.needSkip(tidbkv.IntHandle(2)))
}

func TestBoundedHandleSet(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	sharedSize := atomic.Int64{}
	limit := 3 * (1 + handleMapEntryShallowSize)
	set := NewBoundedHandleSet(logger, &sharedSize, limit)
	require.False(t, set.Contains(tidbkv.IntHandle(1)))

	// add handles within limit
	for i := range 3 {
		set.Add(tidbkv.IntHandle(i + 1))
		require.True(t, set.Contains(tidbkv.IntHandle(i+1)))
	}
	require.EqualValues(t, limit, sharedSize.Load())
	require.True(t, set.BoundExceeded())

	// adding another handle exceeds limit
	set.Add(tidbkv.IntHandle(4))
	require.False(t, set.Contains(tidbkv.IntHandle(4)))

	// create another set with the shared current size, it should exceed limit directly
	set2 := NewBoundedHandleSet(logger, &sharedSize, limit)
	require.True(t, set2.BoundExceeded())
	set2.Add(tidbkv.IntHandle(5))
	require.False(t, set2.Contains(tidbkv.IntHandle(5)))

	// merge sets
	set2.Merge(nil)
	require.Empty(t, set2.handles)
	set2.Merge(set)
	require.EqualValues(t, set.handles, set2.handles)
}
