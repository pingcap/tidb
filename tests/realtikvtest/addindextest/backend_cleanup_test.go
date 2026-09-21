// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package addindextest

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestCleanupSortPathDuringBackendRegistration(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires real TiKV")
	}
	ingest.InitGlobalLightningEnv()
	mgr := ingest.LitBackCtxMgr
	const jobID int64 = 100000001
	jobPath := filepath.Join(ingest.ConfigSortPath(), ingest.EncodeBackendTag(jobID))
	fp := "github.com/pingcap/tidb/ddl/ingest/afterCreateLocalBackend"
	require.NoError(t, failpoint.Enable(fp, "pause"))
	defer func() { _ = failpoint.Disable(fp) }()
	registered := make(chan error, 1)
	go func() {
		_, err := mgr.Register(context.Background(), true, jobID, nil)
		registered <- err
	}()
	require.Eventually(t, func() bool {
		matches, _ := filepath.Glob(filepath.Join(jobPath, "duplicates", "OPTIONS-*"))
		return len(matches) > 0
	}, 20*time.Second, 10*time.Millisecond)
	cleanupStarted := make(chan struct{})
	cleaned := make(chan error, 1)
	go func() {
		close(cleanupStarted)
		cleaned <- mgr.CleanupSortPath(context.Background(), jobID+1)
	}()
	<-cleanupStarted
	earlyCleanup := false
	var cleanupErr error
	select {
	case cleanupErr = <-cleaned:
		earlyCleanup = true
	case <-time.After(200 * time.Millisecond):
	}
	require.NoError(t, failpoint.Disable(fp))
	select {
	case err := <-registered:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("backend registration did not finish")
	}
	defer mgr.Unregister(jobID)
	if earlyCleanup {
		t.Fatal("cleanup completed before the new backend was published")
	}
	select {
	case cleanupErr = <-cleaned:
		require.NoError(t, cleanupErr)
	case <-time.After(20 * time.Second):
		t.Fatal("directory cleanup did not finish")
	}
	require.DirExists(t, jobPath)
	mgr.Unregister(jobID)
	require.NoError(t, mgr.CleanupSortPath(context.Background(), jobID+1))
	require.NoDirExists(t, jobPath)
}
