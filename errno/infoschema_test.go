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
// See the License for the specific language governing permissions and
// limitations under the License.

package errno

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopySafety(t *testing.T) {
	t.Parallel()

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
	assert.Equal(t, 3, stats.global[123].ErrorCount)
	assert.Equal(t, 1, globalCopy[123].ErrorCount)

	// user stats
	assert.Len(t, stats.users, 6)
	assert.Len(t, userCopy, 3)
	assert.Equal(t, 2, stats.users["user"][123].ErrorCount)
	assert.Equal(t, 2, stats.users["user"][123].WarningCount)
	assert.Equal(t, 1, userCopy["user"][123].ErrorCount)
	assert.Equal(t, 1, userCopy["user"][123].WarningCount)

	// ensure there is no user3 in userCopy
	_, ok := userCopy["user3"]
	assert.False(t, ok)
	_, ok = stats.users["user3"]
	assert.True(t, ok)
	_, ok = userCopy["a"]
	assert.False(t, ok)
	_, ok = stats.users["a"]
	assert.True(t, ok)

	// host stats
	assert.Len(t, stats.hosts, 5)
	assert.Len(t, hostCopy, 3)

	IncrementError(123, "user3", "newhost")
	assert.Len(t, stats.hosts, 6)
	assert.Len(t, hostCopy, 3)

	// ensure there is no newhost in hostCopy
	_, ok = hostCopy["newhost"]
	assert.False(t, ok)
	_, ok = stats.hosts["newhost"]
	assert.True(t, ok)
	_, ok = hostCopy["b"]
	assert.False(t, ok)
	_, ok = stats.hosts["b"]
	assert.True(t, ok)
}
