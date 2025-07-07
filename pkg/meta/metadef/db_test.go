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

package metadef

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMemDB(t *testing.T) {
	require.True(t, IsMemDB("information_schema"))
	require.True(t, IsMemDB("performance_schema"))
	require.True(t, IsMemDB("metrics_schema"))
	require.False(t, IsMemDB("mysql"))
}

func TestIsSystemRelatedDB(t *testing.T) {
	require.True(t, IsSystemRelatedDB("mysql"))
	require.True(t, IsSystemRelatedDB("sys"))
	require.True(t, IsSystemRelatedDB("workload_schema"))
	require.False(t, IsSystemRelatedDB("INFORMATION_SCHEMA"))
}

func TestIsSystemDB(t *testing.T) {
	require.True(t, IsSystemDB("mysql"))
	require.False(t, IsSystemDB("sys"))
}
