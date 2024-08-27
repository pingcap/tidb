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

package util

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestShardPoolMap(t *testing.T) {
	rc := 10
	pm := NewShardPoolMap()
	for i := 0; i < rc; i++ {
		id := strconv.FormatInt(int64(i), 10)
		require.NoError(t, pm.Add(id, &PoolContainer{Pool: NewMockGPool(id, 10), Component: DDL}))
	}
	if !intest.InTest {
		require.Error(t, pm.Add("1", &PoolContainer{Pool: NewMockGPool("1", 10), Component: DDL}))
	}
	var cnt atomic.Int32
	pm.Iter(func(pool *PoolContainer) {
		cnt.Add(1)
	})
	require.Equal(t, rc, int(cnt.Load()))

	for i := 0; i < rc; i++ {
		id := strconv.FormatInt(int64(i), 10)
		pm.Del(id)
	}
	cnt.Store(0)
	pm.Iter(func(pool *PoolContainer) {
		cnt.Add(1)
	})
	require.Equal(t, 0, int(cnt.Load()))
	id := strconv.FormatInt(int64(0), 10)
	pm.Del(id)
}
