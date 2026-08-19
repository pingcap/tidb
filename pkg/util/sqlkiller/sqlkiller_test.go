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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestSQLKillerConcurrentReset(t *testing.T) {
	t.Run("reset after successful kill signal CAS", func(t *testing.T) {
		killer := &SQLKiller{}
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/util/sqlkiller/afterSendKillSignalCAS", func() {
			resetDone := make(chan struct{})
			go func() {
				killer.Reset()
				close(resetDone)
			}()
			<-resetDone
		})

		require.NotPanics(t, func() {
			killer.SendKillSignal(QueryInterrupted)
		})
	})

	t.Run("kill signal after reset clear", func(t *testing.T) {
		killer := &SQLKiller{}
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/util/sqlkiller/afterResetKillSignalSwap", func() {
			killSent := make(chan struct{})
			go func() {
				killer.sendKillSignal(QueryInterrupted)
				close(killSent)
			}()
			<-killSent
		})

		killer.Reset()
		require.Equal(t, QueryInterrupted, killer.GetKillSignal())
	})
}
