// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kerneltype_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func RunKernelType(t *testing.T) {
	require.Equal(t, !kerneltype.IsClassic(), kerneltype.IsNextGen())
	require.Equal(t, kerneltype.IsClassic(), !kerneltype.IsNextGen())
}

func RunIsMatch(t *testing.T) {
	if kerneltype.IsClassic() {
		require.True(t, kerneltype.IsMatch(""))
		require.True(t, kerneltype.IsMatch("Classic"))
	} else if kerneltype.IsNextGen() {
		require.True(t, kerneltype.IsMatch("Next Generation"))
	}
	require.False(t, kerneltype.IsMatch("Unknown"))
}
