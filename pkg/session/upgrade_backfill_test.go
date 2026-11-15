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

package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveLimitScanClusterDefaults(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)
	assertValue := func(expected string) {
		res := MustExecToRecodeSet(t, se, fmt.Sprintf(
			"select variable_value from mysql.GLOBAL_VARIABLES where variable_name='%s'",
			variable.TiDBEnableAdaptiveLimitScan,
		))
		chk := res.NewChunk(nil)
		require.NoError(t, res.Next(ctx, chk))
		require.Equal(t, 1, chk.NumRows())
		require.Equal(t, expected, chk.GetRow(0).GetString(0))
		require.NoError(t, res.Close())

		res = MustExecToRecodeSet(t, se, "select @@global.tidb_enable_adaptive_limit_scan, @@session.tidb_enable_adaptive_limit_scan")
		chk = res.NewChunk(nil)
		require.NoError(t, res.Next(ctx, chk))
		require.Equal(t, 1, chk.NumRows())
		require.Equal(t, expected == variable.On, chk.GetRow(0).GetInt64(0) == 1)
		require.Equal(t, expected == variable.On, chk.GetRow(0).GetInt64(1) == 1)
		require.NoError(t, res.Close())
	}

	// Initial bootstrap uses the new-cluster policy.
	assertValue(variable.On)

	// The upgrade is idempotent and must not overwrite an existing setting.
	MustExec(t, se, "set global tidb_enable_adaptive_limit_scan=ON")
	upgradeToVer234(se, version233)
	assertValue(variable.On)

	// Simulate a v233 cluster where this unreleased variable does not exist.
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishBootstrap(int64(version233)))
	revertVersionAndVariables(t, se, version233)
	MustExec(t, se, fmt.Sprintf(
		"delete from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		variable.TiDBEnableAdaptiveLimitScan,
	))
	require.NoError(t, txn.Commit(ctx))
	unsetStoreBootstrapped(store.UUID())

	dom.Close()
	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	se = CreateSessionAndSetID(t, store)

	ver, err := GetBootstrapVersion(se)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)
	assertValue(variable.Off)
}
