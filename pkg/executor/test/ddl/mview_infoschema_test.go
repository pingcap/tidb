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

package ddl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestInfoSchemaTiDBMViewsAndMLogsBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone='+00:00'")

	tk.MustExec("create database is_mview")
	tk.MustExec("use is_mview")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table `$mlog$t` (a int)")
	tk.MustExec("create table mv1 (a int)")

	is := dom.InfoSchema()
	db, ok := is.SchemaByName(pmodel.NewCIStr("is_mview"))
	require.True(t, ok)

	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mviewTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview"), pmodel.NewCIStr("mv1"))
	require.NoError(t, err)

	baseID := baseTable.Meta().ID
	mlogID := mlogTable.Meta().ID
	mviewID := mviewTable.Meta().ID

	baseTblInfo := baseTable.Meta().Clone()
	baseTblInfo.MaterializedViewBase = &model.MaterializedViewBaseInfo{
		MLogID:   mlogID,
		MViewIDs: []int64{mviewID},
	}
	mlogTblInfo := mlogTable.Meta().Clone()
	mlogTblInfo.MaterializedViewLog = &model.MaterializedViewLogInfo{
		BaseTableID:    baseID,
		Columns:        []pmodel.CIStr{pmodel.NewCIStr("a")},
		PurgeMethod:    "IMMEDIATE",
		PurgeStartWith: "'2026-01-02 03:04:05'",
		PurgeNext:      "600",
	}
	mviewTblInfo := mviewTable.Meta().Clone()
	mviewTblInfo.Comment = "mv comment"
	mviewTblInfo.MaterializedView = &model.MaterializedViewInfo{
		BaseTableIDs:     []int64{baseID},
		SQLContent:       "select a, count(*) from t group by a",
		RefreshMethod:    "FAST",
		RefreshStartWith: "'2026-01-02 03:04:05'",
		RefreshNext:      "300",
	}

	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		_, err := m.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, m.UpdateTable(db.ID, baseTblInfo))
		require.NoError(t, m.UpdateTable(db.ID, mlogTblInfo))
		require.NoError(t, m.UpdateTable(db.ID, mviewTblInfo))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, dom.Reload())

	tk.MustQuery(`
		select table_schema, mview_name, refresh_method, refresh_start, refresh_interval
		from information_schema.tidb_mviews
		where table_schema='is_mview' and mview_name='mv1'`,
	).Check(testkit.Rows("is_mview mv1 FAST 2026-01-02 03:04:05 300"))

	tk.MustQuery(`
		select base_table_schema, base_table_name, purge_method, purge_start, purge_interval
		from information_schema.tidb_mlogs
		where base_table_schema='is_mview' and base_table_name='t'`,
	).Check(testkit.Rows("is_mview t IMMEDIATE 2026-01-02 03:04:05 600"))

	// Ensure mlog columns list is exposed.
	tk.MustQuery(`
		select mlog_columns
		from information_schema.tidb_mlogs
		where base_table_schema='is_mview' and base_table_name='t'`,
	).Check(testkit.Rows("a"))

	// The MVIEW_ID/MLOG_ID should match the underlying physical table IDs.
	rows := tk.MustQuery("select mview_id from information_schema.tidb_mviews where table_schema='is_mview' and mview_name='mv1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, fmt.Sprint(mviewID), fmt.Sprint(rows[0][0]))
	rows = tk.MustQuery("select mlog_id from information_schema.tidb_mlogs where table_schema='is_mview' and mlog_name='$mlog$t'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, fmt.Sprint(mlogID), fmt.Sprint(rows[0][0]))
}

func TestInfoSchemaTiDBMViewsAndMLogsPrivilegeFilteringBySchema(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone='+00:00'")

	// db1 objects
	tk.MustExec("create database is_mview_priv1")
	tk.MustExec("use is_mview_priv1")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table `$mlog$t` (a int)")
	tk.MustExec("create table mv1 (a int)")

	// db2 objects
	tk.MustExec("create database is_mview_priv2")
	tk.MustExec("use is_mview_priv2")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table `$mlog$t` (a int)")
	tk.MustExec("create table mv2 (a int)")

	is := dom.InfoSchema()
	db1, ok := is.SchemaByName(pmodel.NewCIStr("is_mview_priv1"))
	require.True(t, ok)
	db2, ok := is.SchemaByName(pmodel.NewCIStr("is_mview_priv2"))
	require.True(t, ok)

	base1, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv1"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlog1, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv1"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mv1, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv1"), pmodel.NewCIStr("mv1"))
	require.NoError(t, err)

	base2, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv2"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlog2, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv2"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mv2, err := is.TableByName(context.Background(), pmodel.NewCIStr("is_mview_priv2"), pmodel.NewCIStr("mv2"))
	require.NoError(t, err)

	base1Info := base1.Meta().Clone()
	base1Info.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: mlog1.Meta().ID, MViewIDs: []int64{mv1.Meta().ID}}
	mlog1Info := mlog1.Meta().Clone()
	mlog1Info.MaterializedViewLog = &model.MaterializedViewLogInfo{BaseTableID: base1.Meta().ID, Columns: []pmodel.CIStr{pmodel.NewCIStr("a")}, PurgeMethod: "IMMEDIATE"}
	mv1Info := mv1.Meta().Clone()
	mv1Info.MaterializedView = &model.MaterializedViewInfo{BaseTableIDs: []int64{base1.Meta().ID}, SQLContent: "select 1", RefreshMethod: "FAST"}

	base2Info := base2.Meta().Clone()
	base2Info.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: mlog2.Meta().ID, MViewIDs: []int64{mv2.Meta().ID}}
	mlog2Info := mlog2.Meta().Clone()
	mlog2Info.MaterializedViewLog = &model.MaterializedViewLogInfo{BaseTableID: base2.Meta().ID, Columns: []pmodel.CIStr{pmodel.NewCIStr("a")}, PurgeMethod: "IMMEDIATE"}
	mv2Info := mv2.Meta().Clone()
	mv2Info.MaterializedView = &model.MaterializedViewInfo{BaseTableIDs: []int64{base2.Meta().ID}, SQLContent: "select 1", RefreshMethod: "FAST"}

	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		_, err := m.GenSchemaVersion()
		require.NoError(t, err)

		require.NoError(t, m.UpdateTable(db1.ID, base1Info))
		require.NoError(t, m.UpdateTable(db1.ID, mlog1Info))
		require.NoError(t, m.UpdateTable(db1.ID, mv1Info))

		require.NoError(t, m.UpdateTable(db2.ID, base2Info))
		require.NoError(t, m.UpdateTable(db2.ID, mlog2Info))
		require.NoError(t, m.UpdateTable(db2.ID, mv2Info))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, dom.Reload())

	tk.MustExec("create user 'mv_priv_u1'@'%'")
	tk.MustExec("grant select on is_mview_priv1.* to 'mv_priv_u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_priv_u1", Hostname: "%"}, nil, nil, nil))

	tkUser.MustQuery("select table_schema, mview_name from information_schema.tidb_mviews order by table_schema, mview_name").
		Check(testkit.Rows("is_mview_priv1 mv1"))
	tkUser.MustQuery("select base_table_schema, base_table_name from information_schema.tidb_mlogs order by base_table_schema, base_table_name").
		Check(testkit.Rows("is_mview_priv1 t"))
}
