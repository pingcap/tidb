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

package sqlkiller

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandleSignalConnectionAlive(t *testing.T) {
	var killer SQLKiller
	alive := func() bool { return true }
	killer.IsConnectionAlive.Store(&alive)

	require.NoError(t, killer.HandleSignal())
	require.NotNil(t, killer.lastCheckTime.Load())

	expired := time.Now().Add(-2 * time.Second)
	killer.lastCheckTime.Store(&expired)
	require.NoError(t, killer.HandleSignal())

	dead := func() bool { return false }
	killer.IsConnectionAlive.Store(&dead)
	killer.lastCheckTime.Store(&expired)
	require.Error(t, killer.HandleSignal())
	require.Equal(t, QueryInterrupted, killer.GetKillSignal())

	killer.Reset()
	require.Equal(t, UnspecifiedKillSignal, killer.GetKillSignal())
	require.Nil(t, killer.lastCheckTime.Load())
}

func BenchmarkSQLKillerHandleSignal(b *testing.B) {
	b.Run("live-connection/recent-check", func(b *testing.B) {
		var killer SQLKiller
		alive := func() bool { return true }
		killer.IsConnectionAlive.Store(&alive)
		checkedAt := time.Now()
		killer.lastCheckTime.Store(&checkedAt)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := killer.HandleSignal(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("no-connection-checker", func(b *testing.B) {
		var killer SQLKiller

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := killer.HandleSignal(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("live-connection/elapsed-check", func(b *testing.B) {
		var checks atomic.Int64
		var killer SQLKiller
		alive := func() bool {
			checks.Add(1)
			return true
		}
		killer.IsConnectionAlive.Store(&alive)
		expired := time.Now().Add(-2 * time.Second)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			killer.lastCheckTime.Store(&expired)
			if err := killer.HandleSignal(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.Equal(b, int64(b.N), checks.Load())
	})
}
