// Copyright 2021 PingCAP, Inc.
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

package errno

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopySafety(t *testing.T) {
	IncrementError(123, "user", "host")
	IncrementError(321, "user2", "host2")
	IncrementWarning(123, "user", "host")
	IncrementWarning(999, "user", "host")
	IncrementWarning(222, "u", "h")

	globalCopy := GlobalStats()
	userCopy := UserStats()
	hostCopy := HostStats()

	IncrementError(123, "user", "host")
	IncrementError(999, "user2", "host2")
	IncrementError(123, "user3", "host")
	IncrementWarning(123, "user", "host")
	IncrementWarning(222, "u", "h")
	IncrementWarning(222, "a", "b")
	IncrementWarning(333, "c", "d")

	// global stats
	require.Equal(t, 3, stats.global[123].ErrorCount)
	require.Equal(t, 1, globalCopy[123].ErrorCount)

	// user stats
	require.Len(t, stats.users, 6)
	require.Len(t, userCopy, 3)
	require.Equal(t, 2, stats.users["user"][123].ErrorCount)
	require.Equal(t, 2, stats.users["user"][123].WarningCount)
	require.Equal(t, 1, userCopy["user"][123].ErrorCount)
	require.Equal(t, 1, userCopy["user"][123].WarningCount)

	// ensure there is no user3 in userCopy
	_, ok := userCopy["user3"]
	require.False(t, ok)
	_, ok = stats.users["user3"]
	require.True(t, ok)
	_, ok = userCopy["a"]
	require.False(t, ok)
	_, ok = stats.users["a"]
	require.True(t, ok)

	// host stats
	require.Len(t, stats.hosts, 5)
	require.Len(t, hostCopy, 3)

	IncrementError(123, "user3", "newhost")
	require.Len(t, stats.hosts, 6)
	require.Len(t, hostCopy, 3)

	// ensure there is no newhost in hostCopy
	_, ok = hostCopy["newhost"]
	require.False(t, ok)
	_, ok = stats.hosts["newhost"]
	require.True(t, ok)
	_, ok = hostCopy["b"]
	require.False(t, ok)
	_, ok = stats.hosts["b"]
	require.True(t, ok)
}
