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
	"time"

	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewLogBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")

	tk.MustExec("create materialized view log on t (a) purge start with '2026-01-02 03:04:05' next 600")

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='$mlog$t'").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)

	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, baseTable.Meta().ID, mlogInfo.BaseTableID)
	require.Equal(t, []pmodel.CIStr{pmodel.NewCIStr("a")}, mlogInfo.Columns)
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "'2026-01-02 03:04:05'", mlogInfo.PurgeStartWith)
	require.Equal(t, "600", mlogInfo.PurgeNext)

	// Meta columns should exist on the log table.
	var hasDMLType, hasOldNew bool
	for _, c := range mlogTable.Meta().Columns {
		if c.Name.L == "dml_type" {
			hasDMLType = true
		}
		if c.Name.L == "old_new" {
			hasOldNew = true
			require.Equal(t, mysql.TypeTiny, c.FieldType.GetType())
		}
	}
	require.True(t, hasDMLType)
	require.True(t, hasOldNew)

	// Duplicated MV LOG should fail (same derived table name).
	tk.MustGetErrMsg("create materialized view log on t (a)", "[schema:1050]Table 'test.$mlog$t' already exists")
}

func TestCreateMaterializedViewLogTiFlashReplica(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 50*time.Millisecond, mockstore.WithMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tiflash (a int)")

	tk.MustExec("create materialized view log on t_tiflash (a) tiflash replica 1 purge immediate")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t_tiflash"))
	require.NoError(t, err)

	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	require.NotNil(t, mlogTable.Meta().TiFlashReplica)
	require.Equal(t, uint64(1), mlogTable.Meta().TiFlashReplica.Count)

	// Purge immediate only records method, without start/next.
	require.Equal(t, "IMMEDIATE", mlogTable.Meta().MaterializedViewLog.PurgeMethod)
	require.Empty(t, mlogTable.Meta().MaterializedViewLog.PurgeStartWith)
	require.Empty(t, mlogTable.Meta().MaterializedViewLog.PurgeNext)
}
