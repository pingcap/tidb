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

package materializedview

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestInformationSchemaMaterializedView(t *testing.T) {
	t.Run("TiDBMViews", testInformationSchemaTiDBMViews)
	t.Run("TiDBMLogs", testInformationSchemaTiDBMLogs)
}

func testInformationSchemaTiDBMViews(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_meta (a, s, cnt) comment='mv-meta' refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_meta"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID

	tk.MustQuery("select table_catalog, table_schema, mview_id, mview_name, mview_comment, refresh_method, refresh_start, refresh_next from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'").
		Check(testkit.Rows(fmt.Sprintf("def test %d mv_meta mv-meta FAST  DATE_ADD(NOW(), INTERVAL 1 HOUR)", mvID)))
	tk.MustQuery("select mview_modify_time is not null from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'").
		Check(testkit.Rows("1"))
	tk.MustQuery("select mview_sql_content like 'SELECT%' from information_schema.tidb_mviews where table_schema = 'test' and mview_name = 'mv_meta'").
		Check(testkit.Rows("1"))
}

func testInformationSchemaTiDBMLogs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b) purge start with now() next date_add(now(), interval 1 hour)")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mlogID := baseTable.Meta().MaterializedViewBase.MLogID
	baseID := baseTable.Meta().ID

	tk.MustQuery("select table_catalog, table_schema, mlog_id, mlog_name, mlog_columns, base_table_catalog, base_table_schema, base_table_id, base_table_name, purge_method, purge_start, purge_next from information_schema.tidb_mlogs where table_schema = 'test' and mlog_name = '$mlog$t'").
		Check(testkit.Rows(fmt.Sprintf("def test %d $mlog$t a,b def test %d t DEFERRED NOW() DATE_ADD(NOW(), INTERVAL 1 HOUR)", mlogID, baseID)))
}
