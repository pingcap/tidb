// Copyright 2019 PingCAP, Inc.
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

package worker_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/stretchr/testify/require"
)

func TestApplyRecycle(t *testing.T) {
	pool := worker.NewPool(context.Background(), 3, "test")

	w1, w2, w3 := pool.Apply(), pool.Apply(), pool.Apply()
	require.Equal(t, int64(1), w1.ID)
	require.Equal(t, int64(2), w2.ID)
	require.Equal(t, int64(3), w3.ID)
	require.Equal(t, false, pool.HasWorker())

	pool.Recycle(w3)
	require.Equal(t, true, pool.HasWorker())
	require.Equal(t, w3, pool.Apply())
	pool.Recycle(w2)
	require.Equal(t, w2, pool.Apply())
	pool.Recycle(w1)
	require.Equal(t, w1, pool.Apply())

	require.Equal(t, false, pool.HasWorker())

	require.PanicsWithValue(t, "invalid restore worker", func() { pool.Recycle(nil) })
}
