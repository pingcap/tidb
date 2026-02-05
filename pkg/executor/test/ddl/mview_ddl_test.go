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
	"testing"

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDDLBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")

	// Base table must have MV LOG first.
	err := tk.ExecToErr("create materialized view mv_no_log (a, cnt) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "materialized view log does not exist")

	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, cnt) refresh fast next 300 as select a, count(1) from t group by a")

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='mv'").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)

	require.NotNil(t, mvTable.Meta().MaterializedView)
	require.Equal(t, []int64{baseTable.Meta().ID}, mvTable.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, "FAST", mvTable.Meta().MaterializedView.RefreshMethod)
	require.Equal(t, "NOW()", mvTable.Meta().MaterializedView.RefreshStartWith)
	require.Equal(t, "300", mvTable.Meta().MaterializedView.RefreshNext)

	// Base table reverse mapping maintained by DDL.
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, mvTable.Meta().ID)

	// MV must contain count(*|1).
	err = tk.ExecToErr("create materialized view mv_bad (a, s) as select a, sum(b) from t group by a")
	require.ErrorContains(t, err, "must contain count(*)/count(1)")

	// MV LOG cannot be dropped while dependent MVs exist.
	err = tk.ExecToErr("drop materialized view log on t")
	require.ErrorContains(t, err, "dependent materialized views exist")

	// Drop MV and then drop MV LOG.
	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view log on t")

	// Reverse mapping cleared.
	is = dom.InfoSchema()
	baseTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}
