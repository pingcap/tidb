// Copyright 2022 PingCAP, Inc.
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

package bootstraptest_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestUpgradeVersion83(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	ver, err := session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	statsHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"stats_data", "longblob"},
		{"seq_no", "bigint(64)"},
		{"version", "bigint(64)"},
		{"create_time", "datetime(6)"},
	}
	rStatsHistoryTbl, err := tk.Exec(`desc mysql.stats_history`)
	require.NoError(t, err)
	req := rStatsHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsHistoryTbl.Next(ctx, req))
	require.Equal(t, 5, req.NumRows())
	for i := 0; i < 5; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}
}

func TestUpgradeVersion84(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	ver, err := session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	statsHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"modify_count", "bigint(64)"},
		{"count", "bigint(64)"},
		{"version", "bigint(64)"},
		{"source", "varchar(40)"},
		{"create_time", "datetime(6)"},
	}
	rStatsHistoryTbl, err := tk.Exec(`desc mysql.stats_meta_history`)
	require.NoError(t, err)
	req := rStatsHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsHistoryTbl.Next(ctx, req))
	require.Equal(t, 6, req.NumRows())
	for i := 0; i < 6; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}
}

func TestUpgradeVersion66(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	seV65 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(65))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.MustExec(t, seV65, "update mysql.tidb set variable_value='65' where variable_name='tidb_server_version'")
	session.MustExec(t, seV65, "set @@global.tidb_track_aggregate_memory_usage = 0")
	session.MustExec(t, seV65, "commit")
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV65)
	require.NoError(t, err)
	require.Equal(t, int64(65), ver)
	dom.Close()
	domV66, err := session.BootstrapSession(store)
	require.NoError(t, err)

	seV66 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV66)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV66, `select @@global.tidb_track_aggregate_memory_usage, @@session.tidb_track_aggregate_memory_usage`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(1), row.GetInt64(0))
	require.Equal(t, int64(1), row.GetInt64(1))
	domV66.Close()
}

func TestUpgradeVersion74(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		oldValue int
		newValue int
	}{
		{200, 3000},
		{3000, 3000},
		{3001, 3001},
	}

	for _, ca := range cases {
		func() {
			store, dom := session.CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			seV73 := session.CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(73))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			session.MustExec(t, seV73, "update mysql.tidb set variable_value='72' where variable_name='tidb_server_version'")
			session.MustExec(t, seV73, "set @@global.tidb_stmt_summary_max_stmt_count = "+strconv.Itoa(ca.oldValue))
			session.MustExec(t, seV73, "commit")
			session.UnsetStoreBootstrapped(store.UUID())
			ver, err := session.GetBootstrapVersion(seV73)
			require.NoError(t, err)
			require.Equal(t, int64(72), ver)
			dom.Close()
			domV74, err := session.BootstrapSession(store)
			require.NoError(t, err)
			defer domV74.Close()
			seV74 := session.CreateSessionAndSetID(t, store)
			ver, err = session.GetBootstrapVersion(seV74)
			require.NoError(t, err)
			require.Equal(t, session.CurrentBootstrapVersion, ver)
			r := session.MustExecToRecodeSet(t, seV74, `SELECT @@global.tidb_stmt_summary_max_stmt_count`)
			req := r.NewChunk(nil)
			require.NoError(t, r.Next(ctx, req))
			require.Equal(t, 1, req.NumRows())
			row := req.GetRow(0)
			require.Equal(t, strconv.Itoa(ca.newValue), row.GetString(0))
		}()
	}
}

func TestUpgradeVersion75(t *testing.T) {
	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV74 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(74))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	session.MustExec(t, seV74, "update mysql.tidb set variable_value='74' where variable_name='tidb_server_version'")
	session.MustExec(t, seV74, "commit")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user DROP PRIMARY KEY")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(64)")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user ADD PRIMARY KEY(Host, User)")
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV74)
	require.NoError(t, err)
	require.Equal(t, int64(74), ver)
	r := session.MustExecToRecodeSet(t, seV74, `desc mysql.user`)
	req := r.NewChunk(nil)
	row := req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(64)", strings.ToLower(row.GetString(1)))
	dom.Close()
	domV75, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domV75.Close()
	seV75 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV75)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r = session.MustExecToRecodeSet(t, seV75, `desc mysql.user`)
	req = r.NewChunk(nil)
	row = req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(255)", strings.ToLower(row.GetString(1)))
}
