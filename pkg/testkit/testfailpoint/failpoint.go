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

package testfailpoint

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

// Enable enables fail-point, and disable it when test finished.
func Enable(t testing.TB, name, expr string) {
	require.NoError(t, failpoint.Enable(name, expr))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(name))
	})
}

// EnableCall enables fail-point, and disable it when test finished.
func EnableCall(t testing.TB, name string, fn any) {
	require.NoError(t, failpoint.EnableCall(name, fn))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(name))
	})
}
