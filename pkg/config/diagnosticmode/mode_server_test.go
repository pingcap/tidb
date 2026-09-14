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

package diagnosticmode_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestTiDBServerGoroutinesInDiagnosticMode(t *testing.T) {
	if !intest.InTest {
		t.Skip("diagnosticmode.SetForTest requires the intest build tag")
	}
	testsetup.SetupForCommonTest()
	restoreMode := diagnosticmode.SetForTest(true)
	t.Cleanup(restoreMode)
	enableServerRunInGoTest(t)

	require.True(t, diagnosticmode.Enabled())

	server, cfg := startTiDBServer(t)
	require.True(t, cfg.Status.ReportStatus)
	statusOn, statusAddr := server.GetStatusServerAddr()
	require.True(t, statusOn)
	require.NotEmpty(t, statusAddr)

	var buf bytes.Buffer
	goroutineProfile := pprof.Lookup("goroutine")
	require.NotNil(t, goroutineProfile)
	// RunInGoTestChan closes after launching the listener, but the goroutine
	// may not have entered startNetworkListener yet. Wait for that frame before
	// using the snapshot to check the diagnostic startup behavior.
	require.Eventually(t, func() bool {
		buf.Reset()
		if err := goroutineProfile.WriteTo(&buf, 1); err != nil {
			return false
		}
		return bytes.Contains(buf.Bytes(), []byte("github.com/pingcap/tidb/pkg/server.(*Server).startNetworkListener"))
	}, 10*time.Second, 10*time.Millisecond, "network listener did not appear in the goroutine profile")

	dump := buf.String()
	require.Contains(t, dump, "goroutine ")
	require.Contains(t, dump, "github.com/pingcap/tidb/pkg/server.(*Server).startNetworkListener")

	assertDiagnosticGoroutineAllowlist(t, buf.Bytes())
	t.Logf("TiDB goroutine dump in diagnostic mode:\n%s", dump)
}

func assertDiagnosticGoroutineAllowlist(t *testing.T, dump []byte) {
	t.Helper()
	// Outside the embedded storage fixture, each group must contain an explicitly
	// allowed function. Match the worker rather than its changing leaf frame.
	// Do not allow generic wrappers such as WaitGroup.Run or testing.tRunner.
	// This allowlist covers the mockstore fixture, not a real PD/TiKV deployment.
	allowedFunctions := []string{
		// Test harness, profile collection, and process-wide observability.
		"testing.(*M).Run",
		"TestTiDBServerGoroutinesInDiagnosticMode",
		"go.opencensus.io/stats/view.(*worker).start",
		"github.com/golang/glog.(*fileSink).flushDaemon",

		// unistore and cache
		"badger", "unistore", "ristretto", "gp.worker",

		// Storage client metadata, timestamps, and resource control.
		"github.com/tikv/client-go/v2/internal/locate.(*bgRunner).schedule.func1",
		"github.com/tikv/client-go/v2/internal/locate.(*bgRunner).scheduleWithTrigger.func1",
		"github.com/tikv/client-go/v2/oracle/oracles.(*pdOracle).updateTS",
		"github.com/tikv/client-go/v2/tikv.(*KVStore).runTxnSafePointUpdater",
		"github.com/tikv/client-go/v2/tikv.(*KVStore).safeTSUpdater",
		"github.com/tikv/pd/client/resource_group/controller.(*ResourceGroupsController).Start.func1",

		// Diagnostic Domain coordination, cache refresh, and statistics readers.
		"ServerInfoSyncLoop",
		"TopologySyncLoop",
		"MDLCheckLoop",
		"(*Syncer).SyncLoop",
		"topNSlowQueryLoop",
		"LoadPrivilegeLoop",
		"LoadSysVarCacheLoop",
		"LoadBindingLoop",
		"LoadStatsDiagnostic",
		"(*statsSyncLoad).SubLoadWorker",

		// SQL listener and the diagnostic HTTP allowlist.
		"server.(*Server).Run",
		"server.(*Server).startNetworkListener",
		"server.(*Server).startDiagnosticHTTP.func1",
	}

	header, stacks, ok := bytes.Cut(bytes.TrimSpace(dump), []byte("\n"))
	require.True(t, ok, "missing goroutine profile header or stacks")
	var total int
	_, err := fmt.Sscanf(string(header), "goroutine profile: total %d", &total)
	require.NoError(t, err, "invalid goroutine profile header: %s", header)
	require.Positive(t, total)

	groups := bytes.Split(bytes.TrimSpace(stacks), []byte("\n\n"))
	var checked int
	for _, group := range groups {
		groupHeader, _, ok := bytes.Cut(group, []byte("\n"))
		require.True(t, ok, "goroutine group has no frames: %s", group)
		var count int
		var marker string
		_, err := fmt.Sscanf(string(groupHeader), "%d %s", &count, &marker)
		require.NoError(t, err, "invalid goroutine group header: %s", groupHeader)
		require.Equal(t, "@", marker)
		require.Positive(t, count)
		checked += count

		allowed := false
		for _, function := range allowedFunctions {
			// The tab and '+' delimit the full function name in debug=1 output,
			// preventing a match on an unlisted closure or a similar name.
			if bytes.Contains(group, []byte(function)) {
				allowed = true
				break
			}
		}
		if !allowed {
			t.Errorf("goroutine group is not in the diagnostic allowlist (%d goroutines):\n%s", count, group)
		}
	}
	require.Equal(t, total, checked, "not all goroutines were checked")
	t.Logf("Checked %d goroutines in %d aggregated groups against the diagnostic allowlist", checked, len(groups))
}

func startTiDBServer(t *testing.T) (*tidbserver.Server, *config.Config) {
	t.Helper()
	if kerneltype.IsNextGen() {
		testenv.UpdateConfigForNextgen(t)
	}

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	t.Cleanup(view.Stop)

	session.DisableStats4Test()
	// Diagnostic startup requires an existing bootstrap version and system tables.
	func() {
		restoreMode := diagnosticmode.SetForTest(false)
		defer restoreMode()

		bootstrapDom, err := session.BootstrapSession(store)
		require.NoError(t, err)
		// Stop normal-mode workers before collecting diagnostic goroutines,
		// while retaining the bootstrapped data in the same store.
		bootstrapDom.Close()
	}()

	require.True(t, diagnosticmode.Enabled())
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	t.Cleanup(dom.Close)

	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.Socket = ""
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 0

	server, err := tidbserver.NewServer(cfg, tidbserver.NewTiDBDriver(store))
	require.NoError(t, err)
	server.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(server)

	runErr := make(chan error, 1)
	go func() {
		runErr <- server.Run(nil)
	}()
	select {
	case <-tidbserver.RunInGoTestChan:
	case err := <-runErr:
		server.Close()
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		server.Close()
		require.FailNow(t, "timed out waiting for TiDB server to start")
	}

	t.Cleanup(func() {
		server.Close()
		select {
		case err := <-runErr:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			require.Fail(t, "timed out waiting for TiDB server to stop")
		}
	})
	return server, cfg
}

func enableServerRunInGoTest(t *testing.T) {
	t.Helper()
	originalRunInGoTest := tidbserver.RunInGoTest
	originalRunInGoTestChan := tidbserver.RunInGoTestChan
	tidbserver.RunInGoTest = true
	tidbserver.RunInGoTestChan = make(chan struct{})
	t.Cleanup(func() {
		tidbserver.RunInGoTest = originalRunInGoTest
		tidbserver.RunInGoTestChan = originalRunInGoTestChan
	})
}

func TestDiagnosticModeDoesNotUpgradeBootstrapVersion(t *testing.T) {
	if !intest.InTest {
		t.Skip("diagnosticmode.SetForTest requires the intest build tag")
	}
	testsetup.SetupForCommonTest()
	t.Cleanup(config.RestoreFunc())
	if kerneltype.IsNextGen() {
		testenv.UpdateConfigForNextgen(t)
	}
	// This test exercises bootstrap without starting a Server/SessionManager.
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.Performance.SkipInitStats = true
	})
	statsLease := vardef.GetStatsLease()
	t.Cleanup(func() { vardef.SetStatsLease(statsLease) })
	session.DisableStats4Test()
	t.Cleanup(diagnosticmode.SetForTest(false))

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	t.Cleanup(view.Stop)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)

	readMetaVersion := func(t *testing.T) int64 {
		t.Helper()
		var version int64
		err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
			var err error
			version, err = meta.NewReader(txn).GetBootstrapVersion()
			return err
		})
		require.NoError(t, err)
		return version
	}
	bootstrapSession := func(t *testing.T, diagnostic bool) sessionapi.Session {
		t.Helper()
		t.Cleanup(diagnosticmode.SetForTest(diagnostic))
		// A cache hit returns the binary's current version and could mask an
		// unintended normal bootstrap. Each phase must read persisted metadata.
		session.ResetStoreForWithTiKVTest(store)
		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)
		t.Cleanup(dom.Close)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		t.Cleanup(se.Close)
		return se
	}
	assertVersions := func(t *testing.T, se sessionapi.Session, expected int64) {
		t.Helper()
		version, err := session.GetBootstrapVersion(se)
		require.NoError(t, err)
		require.Equal(t, expected, version, "mysql.tidb bootstrap version")
		require.Equal(t, expected, readMetaVersion(t), "KV metadata bootstrap version")
	}

	var currentVersion, oldVersion int64
	if !t.Run("prepare_old_version", func(t *testing.T) {
		se := bootstrapSession(t, false)
		currentVersion = readMetaVersion(t)
		require.Greater(t, currentVersion, int64(1))
		assertVersions(t, se, currentVersion)
		oldVersion = currentVersion - 1

		// Keep current system-table schemas and lower both version records to
		// test upgrade suppression independently of schema-read compatibility.
		session.MustExec(t, se, fmt.Sprintf("UPDATE mysql.tidb SET variable_value='%d' WHERE variable_name='tidb_server_version'", oldVersion))
		require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
			return meta.NewMutator(txn).FinishBootstrap(oldVersion)
		}))
		assertVersions(t, se, oldVersion)
	}) {
		return
	}
	// Subtest cleanup closes the session and Domain before switching modes,
	// while the parent test keeps the same store alive for all three phases.
	if !t.Run("diagnostic_preserves_version", func(t *testing.T) {
		se := bootstrapSession(t, true)
		assertVersions(t, se, oldVersion)
	}) {
		return
	}
	t.Run("normal_upgrades_version", func(t *testing.T) {
		se := bootstrapSession(t, false)
		assertVersions(t, se, currentVersion)
	})
}
