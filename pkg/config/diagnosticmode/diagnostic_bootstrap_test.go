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
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
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
