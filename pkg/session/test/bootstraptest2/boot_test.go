// Copyright 2026 PingCAP, Inc.
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

package bootstraptest2

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestWriteDDLTableVersionToMySQLTiDBWhenUpgradingTo178(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	ddlTableVer, err := m.GetDDLTableVersion()
	require.NoError(t, err)

	// bootstrap as version177
	ver177 := 177
	seV177 := session.CreateSessionAndSetID(t, store)
	err = m.FinishBootstrap(int64(ver177))
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV177, ver177)
	// remove the ddl_table_version entry from mysql.tidb table
	session.MustExec(t, seV177, fmt.Sprintf("delete from mysql.tidb where VARIABLE_NAME='%s'", session.TiDBDDLTableVersionForTest))
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV177)
	require.NoError(t, err)
	require.Equal(t, int64(ver177), ver)

	// upgrade to current version
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	// check if the DDLTableVersion has been set in the `mysql.tidb` table during upgrade
	r := session.MustExecToRecodeSet(t, seCurVer, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s'`, session.TiDBDDLTableVersionForTest))
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, fmt.Appendf(nil, "%d", ddlTableVer), req.GetRow(0).GetBytes(0))
	require.NoError(t, r.Close())
}

func TestTiDBUpgradeToVer179(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, do := session.CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver178 := 178
	seV178 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver178))
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV178, ver178)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV178)
	require.NoError(t, err)
	require.Equal(t, int64(ver178), ver)

	do.Close()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	ver, err = session.GetBootstrapVersion(seV178)
	require.NoError(t, err)
	require.Less(t, int64(ver178), ver)

	r := session.MustExecToRecodeSet(t, seV178, "desc mysql.global_variables")
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, req.NumRows())
	require.Equal(t, []byte("varchar(16383)"), req.GetRow(1).GetBytes(1))
	require.NoError(t, r.Close())

	dom.Close()
}

func testTiDBUpgradeWithDistTask(t *testing.T, injectQuery string, fatal bool) {
	store, do := session.CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver249 := 249
	seV249 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver249))
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV249, ver249)
	session.MustExec(t, seV249, injectQuery)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV249)
	require.NoError(t, err)
	require.Equal(t, int64(ver249), ver)

	conf := new(log.Config)
	lg, p, e := log.InitLogger(conf, zap.WithFatalHook(zapcore.WriteThenPanic))
	require.NoError(t, e)
	rs := log.ReplaceGlobals(lg, p)
	defer func() {
		rs()
	}()

	do.Close()
	fatal2panic := false
	fc := func() {
		defer func() {
			if err := recover(); err != nil {
				fatal2panic = true
			}
		}()
		_, _ = session.BootstrapSession(store)
	}
	fc()
	var dom *domain.Domain
	dom, err = session.GetDomain(store)
	require.NoError(t, err)
	dom.Close()
	require.Equal(t, fatal, fatal2panic)
}

func TestTiDBUpgradeWithDistTaskEnable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("the schema version of the first next-gen kernel release is 250, no need to go through upgrade operations below it, skip it")
	}
	t.Run("test enable dist task", func(t *testing.T) { testTiDBUpgradeWithDistTask(t, "set global tidb_enable_dist_task = 1", false) })
}

func TestTiDBUpgradeWithDistTaskRunning(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("the schema version of the first next-gen kernel release is 250, no need to go through upgrade operations below it, skip it")
	}
	testTiDBUpgradeWithDistTask(t, "insert into mysql.tidb_global_task (id, task_key, `type`, state) values "+
		"(1, 'aaa1', 'aaa', 'running'),"+
		"(2, 'aaa2', 'aaa', 'succeed'),"+
		"(3, 'aaa3', 'aaa', 'failed'),"+
		"(4, 'aaa4', 'aaa', 'reverted'),"+
		"(5, 'aaa5', 'aaa', 'paused'),"+
		"(6, 'aaa6', 'aaa', 'other')", false)
}

func TestTiDBUpgradeToVer211(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, do := session.CreateStoreAndBootstrap(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ver210 := 210
	seV210 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver210))
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV210, ver210)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	store.SetOption(session.StoreBootstrappedKey, nil)
	ver, err := session.GetBootstrapVersion(seV210)
	require.NoError(t, err)
	require.Equal(t, int64(ver210), ver)
	session.MustExec(t, seV210, "alter table mysql.tidb_background_subtask_history drop column summary;")

	do.Close()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	newSe := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(newSe)
	require.NoError(t, err)
	require.Less(t, int64(ver210), ver)

	r := session.MustExecToRecodeSet(t, newSe, "select count(summary) from mysql.tidb_background_subtask_history;")
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.NoError(t, r.Close())

	dom.Close()
}

func TestTiDBUpgradeToVer212(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// bootstrap as version198, version 199~208 is reserved for v8.1.x bugfix patch.
	ver198 := 198
	seV198 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver198))
	require.NoError(t, err)
	session.RevertVersionAndVariables(t, seV198, ver198)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	store.SetOption(session.StoreBootstrappedKey, nil)

	// upgrade to ver212
	dom.Close()
	domCurVer, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := session.CreateSessionAndSetID(t, store)
	ver, err := session.GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	// the columns are changed automatically
	session.MustExec(t, seCurVer, "select sample_sql, start_time, plan_digest from mysql.tidb_runaway_queries")
}
