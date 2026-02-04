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

package bootstraptest_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestBootstrapMaterializedViewSystemTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	for _, tbl := range []string{
		"tidb_mview_refresh",
		"tidb_mlog_purge",
		"tidb_mview_refresh_hist",
		"tidb_mlog_purge_hist",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='" + tbl + "'").Check(testkit.Rows("1"))
	}

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mlog_id"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_newest' order by seq_in_index").
		Check(testkit.Rows("mview_id", "is_newest_refresh"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_newest' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "is_newest_purge"))
}

func TestUpgradeToVer221MaterializedViewSystemTables(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV220 := session.CreateSessionAndSetID(t, store)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.FinishBootstrap(int64(220)))
	require.NoError(t, txn.Commit(ctx))

	revertVersionAndVariables(t, seV220, 220)
	session.MustExec(t, seV220, "drop table if exists mysql.tidb_mview_refresh, mysql.tidb_mlog_purge, mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist")
	session.MustExec(t, seV220, "commit")

	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV220)
	require.NoError(t, err)
	require.Equal(t, int64(220), ver)

	dom.Close()
	domUpgraded, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domUpgraded.Close()

	tk := testkit.NewTestKit(t, store)
	for _, tbl := range []string{
		"tidb_mview_refresh",
		"tidb_mlog_purge",
		"tidb_mview_refresh_hist",
		"tidb_mlog_purge_hist",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='" + tbl + "'").Check(testkit.Rows("1"))
	}
}
