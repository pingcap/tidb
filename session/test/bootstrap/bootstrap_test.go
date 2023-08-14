// Copyright 2023 PingCAP, Inc.
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

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/stretchr/testify/require"
)

func globalVarsCount() int64 {
	var count int64
	for _, v := range variable.GetSysVars() {
		if v.HasGlobalScope() {
			count++
		}
	}
	return count
}

// This test file have many problem.
// 1. Please use testkit to create dom, session and store.
// 2. Don't use CreateStoreAndBootstrap and BootstrapSession together. It will cause data race.
// Please do not add any test here. You can add test case at the bootstrap_update_test.go. After All problem fixed,
// We will overwrite this file by update_test.go.
func TestBootstrap(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se := session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "set global tidb_txn_mode=''")
	session.MustExec(t, se, "use mysql")
	r := session.MustExecToRecodeSet(t, se, "select * from user")
	require.NotNil(t, r)

	ctx := context.Background()
	req := r.NewChunk(nil)
	err := r.Next(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, 0, req.NumRows())

	rows := statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", nil, nil, nil, "", "N", time.Now(), nil)
	r.Close()

	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte(""), nil))

	session.MustExec(t, se, "use test")

	// Check privilege tables.
	session.MustExec(t, se, "SELECT * from mysql.global_priv")
	session.MustExec(t, se, "SELECT * from mysql.db")
	session.MustExec(t, se, "SELECT * from mysql.tables_priv")
	session.MustExec(t, se, "SELECT * from mysql.columns_priv")
	session.MustExec(t, se, "SELECT * from mysql.global_grants")

	// Check privilege tables.
	r = session.MustExecToRecodeSet(t, se, "SELECT COUNT(*) from mysql.global_variables")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, globalVarsCount(), req.GetRow(0).GetInt64(0))
	require.NoError(t, r.Close())

	// Check a storage operations are default autocommit after the second start.
	session.MustExec(t, se, "USE test")
	session.MustExec(t, se, "drop table if exists t")
	session.MustExec(t, se, "create table t (id int)")
	session.UnsetStoreBootstrapped(store.UUID())
	se.Close()

	se, err = session.CreateSession4Test(store)
	require.NoError(t, err)
	session.MustExec(t, se, "USE test")
	session.MustExec(t, se, "insert t values (?)", 3)

	se, err = session.CreateSession4Test(store)
	require.NoError(t, err)
	session.MustExec(t, se, "USE test")
	r = session.MustExecToRecodeSet(t, se, "select * from t")
	require.NotNil(t, r)

	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	rows = statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(t, rows, 3)
	session.MustExec(t, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstrapped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	se, err = session.CreateSession4Test(store)
	require.NoError(t, err)
	session.DoDMLWorks(se)
	r = session.MustExecToRecodeSet(t, se, "select * from mysql.expr_pushdown_blacklist where name = 'date_add'")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
	se.Close()
}

func TestIssue17979_1(t *testing.T) {
	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	// test issue 20900, upgrade from v3.0 to v4.0.11+
	seV3 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(58))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.MustExec(t, seV3, "update mysql.tidb set variable_value='58' where variable_name='tidb_server_version'")
	session.MustExec(t, seV3, "delete from mysql.tidb where variable_name='default_oom_action'")
	session.MustExec(t, seV3, "commit")
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV3)
	require.NoError(t, err)
	require.Equal(t, int64(58), ver)
	dom.Close()
	domV4, err := session.BootstrapSession(store)
	require.NoError(t, err)
	seV4 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV4)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, variable.OOMActionLog, req.GetRow(0).GetString(0))
	domV4.Close()
}
