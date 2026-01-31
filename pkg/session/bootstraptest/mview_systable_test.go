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
		"tidb_mviews",
		"tidb_mlogs",
		"tidb_mview_refresh_hist",
		"tidb_mlog_purge_hist",
		"tidb_mlog_job",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='"+tbl+"'").Check(testkit.Rows("1"))
	}

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mviews' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mview_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mviews' and index_name='uniq_mview_name' order by seq_in_index").
		Check(testkit.Rows("table_schema", "mview_name"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlogs' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mlog_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlogs' and index_name='uniq_base_table' order by seq_in_index").
		Check(testkit.Rows("base_table_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlogs' and index_name='uniq_mlog_name' order by seq_in_index").
		Check(testkit.Rows("table_schema", "mlog_name"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mview_refresh_hist' and index_name='idx_mview_newest' order by seq_in_index").
		Check(testkit.Rows("mview_id", "is_newest_refresh"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_purge_hist' and index_name='idx_mlog_newest' order by seq_in_index").
		Check(testkit.Rows("mlog_id", "is_newest_purge"))

	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_job' and index_name='PRIMARY' order by seq_in_index").
		Check(testkit.Rows("mlog_id"))
	tk.MustQuery("select lower(column_name) from information_schema.statistics where table_schema='mysql' and table_name='tidb_mlog_job' and index_name='uniq_job_base_table' order by seq_in_index").
		Check(testkit.Rows("base_table_id"))
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
	session.MustExec(t, seV220, "drop table if exists mysql.tidb_mviews, mysql.tidb_mlogs, mysql.tidb_mview_refresh_hist, mysql.tidb_mlog_purge_hist, mysql.tidb_mlog_job")
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
		"tidb_mviews",
		"tidb_mlogs",
		"tidb_mview_refresh_hist",
		"tidb_mlog_purge_hist",
		"tidb_mlog_job",
	} {
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='mysql' and table_name='"+tbl+"'").Check(testkit.Rows("1"))
	}
}

