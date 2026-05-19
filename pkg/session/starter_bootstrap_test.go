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
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

// withStarterMode flips deploy mode to starter and registers cleanup. The
// caller must skip on classic kernels since starter requires nextgen.
func withStarterMode(t *testing.T) {
	t.Helper()
	original := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(original))
	})
}

func TestPrefixedStarterUser(t *testing.T) {
	// Default (non-starter) mode preserves the bare name.
	require.Equal(t, "root", prefixedStarterUser("root"))
	require.Equal(t, "cloud_admin", prefixedStarterUser("cloud_admin"))

	if !kerneltype.IsNextGen() {
		return
	}
	withStarterMode(t)
	originalKS := config.GetGlobalKeyspaceName()
	config.UpdateGlobal(func(c *config.Config) { c.KeyspaceName = "tenant1" })
	t.Cleanup(func() {
		config.UpdateGlobal(func(c *config.Config) { c.KeyspaceName = originalKS })
	})
	require.Equal(t, "tenant1.root", prefixedStarterUser("root"))
	require.Equal(t, "tenant1.cloud_admin", prefixedStarterUser("cloud_admin"))

	// Empty keyspace falls back to bare name (e.g., during early startup).
	config.UpdateGlobal(func(c *config.Config) { c.KeyspaceName = "" })
	require.Equal(t, "root", prefixedStarterUser("root"))
}

func TestStarterBootstrapWritesStateInStarterMode(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter mode requires nextgen kernel")
	}
	withStarterMode(t)

	store, dom := CreateStoreAndBootstrap(t)
	defer dom.Close()
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()

	// starter_version is recorded.
	r := MustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'starter_version'`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, "1", req.GetRow(0).GetString(0))
	require.NoError(t, r.Close())

	// cloud_admin and role_admin exist.
	r = MustExecToRecodeSet(t, se, `SELECT User FROM mysql.user WHERE User IN ('cloud_admin','role_admin') OR User LIKE '%.cloud_admin' ORDER BY User`)
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 2, req.NumRows())
	require.NoError(t, r.Close())

	// Starter variables are applied.
	r = MustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE FROM mysql.global_variables WHERE VARIABLE_NAME = 'tidb_redact_log'`)
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, "ON", req.GetRow(0).GetString(0))
	require.NoError(t, r.Close())
}

func TestStarterBootstrapNoopInClassicMode(t *testing.T) {
	// In non-starter mode (the default), no starter state is written.
	require.False(t, deploymode.IsStarter())

	store, dom := CreateStoreAndBootstrap(t)
	defer dom.Close()
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()

	r := MustExecToRecodeSet(t, se, `SELECT COUNT(*) FROM mysql.tidb WHERE VARIABLE_NAME = 'starter_version'`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, int64(0), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())

	r = MustExecToRecodeSet(t, se, `SELECT COUNT(*) FROM mysql.user WHERE User IN ('cloud_admin','role_admin')`)
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, int64(0), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())
}

func TestStarterAmendIsIdempotent(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter mode requires nextgen kernel")
	}
	withStarterMode(t)

	store, dom := CreateStoreAndBootstrap(t)
	defer dom.Close()
	defer func() { require.NoError(t, store.Close()) }()
	se := CreateSessionAndSetID(t, store)
	ctx := context.Background()

	// Drop the admin set and re-amend; users come back, and starter_amended
	// is recorded so a second amend short-circuits.
	MustExec(t, se, `DELETE FROM mysql.user`)
	originalIsBranch := config.GetGlobalConfig().Starter.IsBranch
	config.UpdateGlobal(func(c *config.Config) { c.Starter.IsBranch = true })
	t.Cleanup(func() {
		config.UpdateGlobal(func(c *config.Config) { c.Starter.IsBranch = originalIsBranch })
	})

	runStarterBranchAmend(store)

	r := MustExecToRecodeSet(t, se, `SELECT COUNT(*) FROM mysql.user WHERE User = 'role_admin'`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, int64(1), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())

	r = MustExecToRecodeSet(t, se, `SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'starter_amended'`)
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, "True", req.GetRow(0).GetString(0))
	require.NoError(t, r.Close())

	// Second amend must be a no-op: delete users again, then amend; the
	// short-circuit should leave the table empty.
	MustExec(t, se, `DELETE FROM mysql.user`)
	runStarterBranchAmend(store)
	r = MustExecToRecodeSet(t, se, `SELECT COUNT(*) FROM mysql.user WHERE User = 'role_admin'`)
	req = r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, int64(0), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())
}
