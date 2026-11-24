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

package executor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/diagnosticlog"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestDiagnosticDeadlockHistoryIndependence(t *testing.T) {
	previous := deadlockhistory.GlobalDeadlockHistory
	deadlockhistory.GlobalDeadlockHistory = deadlockhistory.NewDeadlockHistory(10)
	defer func() { deadlockhistory.GlobalDeadlockHistory = previous; require.NoError(t, diagnosticlog.Close()) }()
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(c *config.Config) { c.PessimisticTxn.DeadlockHistoryCollectRetryable = false })
	directory := t.TempDir()
	require.NoError(t, diagnosticlog.Initialize([]string{"deadlock"}, directory))
	event := &tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: true}
	recordDeadlock(9007199254740993, event)
	require.Empty(t, deadlockhistory.GlobalDeadlockHistory.GetAll())
	var content []byte
	require.Eventually(t, func() bool {
		var err error
		content, err = os.ReadFile(filepath.Join(directory, "tidb-deadlock.log"))
		return err == nil && len(content) > 0
	}, 3*time.Second, time.Millisecond)
	require.Contains(t, string(content), `"txn_id":"9007199254740993"`)
	require.Contains(t, string(content), `"retryable":true`)
	event.IsRetryable = false
	recordDeadlock(9007199254740993, event)
	require.Len(t, deadlockhistory.GlobalDeadlockHistory.GetAll(), 1)
	require.Eventually(t, func() bool {
		var err error
		content, err = os.ReadFile(filepath.Join(directory, "tidb-deadlock.log"))
		return err == nil && len(strings.Split(strings.TrimSpace(string(content)), "\n")) == 2
	}, 3*time.Second, time.Millisecond)
	// Disabled file selection does not disable the original memory collection.
	require.NoError(t, diagnosticlog.Initialize(nil, directory))
	recordDeadlock(1, event)
	require.Len(t, deadlockhistory.GlobalDeadlockHistory.GetAll(), 2)
}
