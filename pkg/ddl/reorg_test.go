// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReorgCtxSetMaxProgress(t *testing.T) {
	rc := &reorgCtx{}

	require.Equal(t, float64(0), rc.maxProgress.Load())

	result := rc.setMaxProgress(0.5)
	require.Equal(t, 0.5, result)
	require.Equal(t, 0.5, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.3)
	require.Equal(t, 0.7, result)                // Returns old max
	require.Equal(t, 0.7, rc.maxProgress.Load()) // Value unchanged

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.9)
	require.Equal(t, 0.9, result)
	require.Equal(t, 0.9, rc.maxProgress.Load())
}

func TestReorgCtxSetMaxProgressConcurrent(t *testing.T) {
	rc := &reorgCtx{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(progress float64) {
			defer wg.Done()
			rc.setMaxProgress(progress)
		}(float64(i) / 100.0)
	}

	wg.Wait()

	require.Equal(t, 0.99, rc.maxProgress.Load())
}
