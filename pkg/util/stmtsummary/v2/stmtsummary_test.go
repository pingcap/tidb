// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"testing"

<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
>>>>>>> 0a42bea5f50 (stmtsummary: surface logger init failures, drop leaked time-excluded FDs, fix absolute-path file pruning (#70175))
	"github.com/stretchr/testify/require"
)

func TestStmtWindow(t *testing.T) {
	ss := NewStmtSummary4Test(5)
	defer ss.Close()
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	require.Equal(t, 5, ss.window.lru.Size())
	require.Equal(t, 2, ss.window.evicted.count())
	require.Equal(t, int64(4), ss.window.evicted.other.ExecCount) // digest1 digest1 digest2 digest2
	ss.Clear()
	require.Equal(t, 0, ss.window.lru.Size())
	require.Equal(t, 0, ss.window.evicted.count())
	require.Equal(t, int64(0), ss.window.evicted.other.ExecCount)
}

func TestStmtSummary(t *testing.T) {
	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	w := ss.window
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	w = ss.window
	require.Equal(t, 2, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())

	ss.Clear()
	require.Equal(t, 0, w.lru.Size())
}

func TestStmtSummaryFlush(t *testing.T) {
	storage := &mockStmtStorage{}
	ss := NewStmtSummary4Test(1000)
	ss.storage = storage

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.Close()

	storage.Lock()
	require.Equal(t, 3, len(storage.windows))
	storage.Unlock()
}
<<<<<<< HEAD
=======

func TestDefaultConfig(t *testing.T) {
	cfg := &Config{
		Filename: filepath.Join(t.TempDir(), "test.log"),
	}
	ss, err := NewStmtSummary(cfg)
	require.NoError(t, err)
	defer ss.Close()

	// Verify RefreshInterval (should be 1800 = 30 min)
	require.Equal(t, uint32(1800), ss.RefreshInterval())
}

// TestNewStmtSummaryLoggerInitError closes V2-11 in the statement-summary
// audit: when the configured stmt log file cannot be opened,
// log.InitLogger returns an error and NewStmtSummary must surface that error
// instead of silently degrading to a no-op logger. A no-op fallback would make
// persistent mode look enabled while silently dropping every rotated window.
//
// We trigger the error by pointing Filename at an existing directory, which
// `log.InitLogger` rejects with "can't use directory as log file name" without
// relying on filesystem permission differences between platforms.
func TestNewStmtSummaryLoggerInitError(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewStmtSummary(&Config{Filename: dir})
	require.Error(t, err)
	require.Nil(t, ss)
}

// TestSetupDisablesPersistentOnLoggerInitError exercises the *startup call
// chain* that produces the V2-11 nil-panic regression pointed out in review.
//
// When NewStmtSummary fails, publishing its nil result while the cluster
// config still has `tidb_stmt_summary_enable_persistent = true` lets every
// public proxy in this package (Add, Enabled, ...) dereference nil.
// On the buggy code the very first SQL call would dereference a nil
// pointer and the server would crash again; the logger init error had traded
// silent data loss for a hard boot-loop. Now Setup must remedy the half-
// constructed state by explicitly disabling persistent mode so the wrappers
// fall back to the in-memory v1 aggregation (stmtsummary.StmtSummaryByDigestMap).
//
// The test goes end-to-end through:
//   - the public Setup entrypoint (the same one cmd/tidb-server calls);
//   - the post-Setup invariant that StmtSummaryEnablePersistent flipped off;
//   - an actual Add() probe, which used to be the line that panicked.
func TestSetupDisablesPersistentOnLoggerInitError(t *testing.T) {
	// Preserve the global v2 instance and install a sentinel to verify that a
	// failed setup does not publish a nil result over an existing instance.
	prev := GlobalStmtSummary
	t.Cleanup(func() { GlobalStmtSummary = prev })
	existing := &StmtSummary{}
	GlobalStmtSummary = existing

	// Preserve the cluster config too; Setup mutates it on the fix branch.
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	// Mirror the operator's intent: allocate persistent mode but point the log
	// file at an *existing directory*. log.InitLogger refuses this with
	// "can't use directory as log file name" deterministically across
	// darwin/linux, so the chosen failure trigger does not depend on permission
	// quirks of the CI container.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = true
		conf.Instance.StmtSummaryFilename = t.TempDir()
	})

	// Simulate the startup call: cmd/tidb-server/main.go#setupStmtSummary.
	err := Setup(&Config{
		Filename: config.GetGlobalConfig().Instance.StmtSummaryFilename,
	})
	require.Error(t, err, "Setup must surface the logger init error")
	require.ErrorContains(t, err, "stmtsummary v2 persistent mode disabled; falling back to v1 in-memory aggregation")

	// NewStmtSummary returned (nil, error), so Setup must not publish that nil
	// result over the previously installed instance.
	require.Same(t, existing, GlobalStmtSummary)

	// This is the invariant the reviewer asked for: persistent mode MUST be
	// flipped off so the v2 proxy functions become no-ops and the v1 path
	// (StmtSummaryByDigestMap, which is always available) absorbs traffic
	// instead of dereferencing a nil GlobalStmtSummary.
	require.False(t,
		config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent,
		"V2-11 follow-up: Setup must disable persistent mode on init failure to avoid nil deref in Add/Enabled wrappers")

	// Direct evidence that the runtime no longer crashes: the Add() proxy is
	// the line that panicked under the original (return-nil) fix. With the
	// persistent flag flipped off it must route to v1 without panicking.
	require.NotPanics(t, func() {
		Add(GenerateStmtExecInfo4Test("digest_setup_fallback_does_not_panic"))
	})
}

// TestEvictedConcurrentWithRotate verifies that Evicted() is safe to call
// concurrently with rotate (V2-25 data race fix).
func TestEvictedConcurrentWithRotate(t *testing.T) {
	ss := NewStmtSummary4Test(2)
	defer ss.Close()

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = ss.Evicted()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ss.windowLock.Lock()
			ss.rotate(timeNow())
			ss.windowLock.Unlock()
			ss.Add(GenerateStmtExecInfo4Test("digest_new"))
			ss.Add(GenerateStmtExecInfo4Test("digest_new2"))
			ss.Add(GenerateStmtExecInfo4Test("digest_new3"))
		}
	}()

	wg.Wait()
}
>>>>>>> 0a42bea5f50 (stmtsummary: surface logger init failures, drop leaked time-excluded FDs, fix absolute-path file pruning (#70175))
