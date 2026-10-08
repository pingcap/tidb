// Copyright 2026 PingCAP, Inc.
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

package diagnosticmode

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestInitialize(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		resetModeForTest(t)

		require.False(t, Enabled())
		require.NoError(t, Initialize(false))
		require.False(t, Enabled())
		require.NoError(t, Initialize(false))
		require.ErrorContains(t, Initialize(true), "cannot be changed to enabled")
		require.False(t, Enabled())
	})

	t.Run("enabled", func(t *testing.T) {
		resetModeForTest(t)

		require.NoError(t, Initialize(true))
		require.True(t, Enabled())
		require.NoError(t, Initialize(true))
		require.ErrorContains(t, Initialize(false), "cannot be changed to disabled")
		require.True(t, Enabled())
	})
}

func TestSetForTest(t *testing.T) {
	if !intest.InTest {
		t.Skip("SetForTest requires the intest build tag")
	}
	resetModeForTest(t)

	restore := SetForTest(true)
	require.True(t, Enabled())
	restore()
	require.False(t, Enabled())
}

func resetModeForTest(t *testing.T) {
	t.Helper()
	original := currentMode.Load()
	currentMode.Store(uint32(modeUninitialized))
	t.Cleanup(func() {
		currentMode.Store(original)
	})
}
