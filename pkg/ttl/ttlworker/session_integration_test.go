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

package ttlworker_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/stretchr/testify/require"
)

func TestGetSessionAndTrack(t *testing.T) {
	_, do := testkit.CreateMockStoreAndDomain(t)
	pool := ttlworker.NewSessionPool(do.SysSessionPool(), do.SysProcTracker(), do.NextConnID)
	se, trackID, err := pool.GetSessionAndTrack()
	require.NoError(t, err)
	require.Greater(t, trackID, uint64(0))
	// GetSessionAndTrack should return a tracked session.
	procs := do.SysProcTracker().GetSysProcessList()
	require.Equal(t, 1, len(procs))
	_, ok := procs[trackID]
	require.True(t, ok)
	// Close should untracked the session.
	se.Close()
	require.Empty(t, do.SysProcTracker().GetSysProcessList())
	// GetSession should return a session without tracking.
	se, err = pool.GetSession()
	require.NoError(t, err)
	require.Empty(t, do.SysProcTracker().GetSysProcessList())
	se.Close()
}
